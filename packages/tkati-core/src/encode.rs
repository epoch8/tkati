//! Arrow record batches -> one JSON payload (and optional key) per row,
//! parallelised with rayon.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::timezone::Tz;
use arrow_array::types::{Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type};
use arrow_array::{Array, RecordBatch};
use arrow_buffer::ScalarBuffer;
use arrow_json::writer::{Encoder, EncoderFactory, EncoderOptions, NullableEncoder, make_encoder};
use arrow_schema::{ArrowError, DataType, FieldRef, TimeUnit};
use chrono::{DateTime, Utc};
use rayon::prelude::*;

/// Encoded messages stored back to back: payload `i` is
/// `data[offsets[i]..offsets[i + 1]]`.
#[derive(Default)]
pub struct Messages {
    pub data: Vec<u8>,
    pub offsets: Vec<usize>,
    pub keys: Vec<Option<String>>,
}

impl Messages {
    pub fn new() -> Self {
        Self {
            offsets: vec![0],
            ..Default::default()
        }
    }

    pub fn len(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn payload(&self, idx: usize) -> &[u8] {
        &self.data[self.offsets[idx]..self.offsets[idx + 1]]
    }

    pub fn push(&mut self, payload: &[u8], key: Option<String>) {
        self.data.extend_from_slice(payload);
        self.offsets.push(self.data.len());
        self.keys.push(key);
    }

    fn append(&mut self, other: Messages) {
        let base = self.data.len();
        self.data.extend_from_slice(&other.data);
        self.offsets.extend(other.offsets[1..].iter().map(|o| o + base));
        self.keys.extend(other.keys);
    }
}

/// Whether `key_for_row` can reproduce Python's `str(value)` for this type.
/// Callers fall back to computing keys in Python for anything else.
pub fn is_native_key_type(dt: &DataType) -> bool {
    dt.is_integer()
        || matches!(
            dt,
            DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View | DataType::Boolean | DataType::Null
        )
}

/// Python's `str()` of the row's value, which is what the key has always
/// been — including `"None"` for a null.
fn key_for_row(array: &dyn Array, row: usize) -> String {
    // A NullArray has no validity buffer, so `is_null` is false on it.
    if array.is_null(row) || array.data_type() == &DataType::Null {
        return "None".to_owned();
    }
    macro_rules! int {
        ($t:ty) => {
            array.as_primitive::<$t>().value(row).to_string()
        };
    }
    match array.data_type() {
        DataType::Utf8 => array.as_string::<i32>().value(row).to_owned(),
        DataType::LargeUtf8 => array.as_string::<i64>().value(row).to_owned(),
        DataType::Utf8View => array.as_string_view().value(row).to_owned(),
        DataType::Boolean => (if array.as_boolean().value(row) { "True" } else { "False" }).to_owned(),
        DataType::Int8 => int!(Int8Type),
        DataType::Int16 => int!(Int16Type),
        DataType::Int32 => int!(Int32Type),
        DataType::Int64 => int!(Int64Type),
        DataType::UInt8 => int!(UInt8Type),
        DataType::UInt16 => int!(UInt16Type),
        DataType::UInt32 => int!(UInt32Type),
        DataType::UInt64 => int!(UInt64Type),
        other => unreachable!("key type {other} rejected by is_native_key_type"),
    }
}

/// Renders timestamps the way `to_pylist()` + orjson did, i.e. as Python's
/// `datetime.isoformat()`: microseconds only when non-zero and then always
/// six digits, and a `+HH:MM` offset (never `Z`) when the column has a time
/// zone. arrow-json's own format differs on both counts, and a downstream
/// consumer parsing these strings shouldn't notice the switch.
#[derive(Debug)]
struct PythonTimestamps;

impl EncoderFactory for PythonTimestamps {
    fn make_default_encoder<'a>(
        &self,
        _field: &'a FieldRef,
        array: &'a dyn Array,
        _options: &'a EncoderOptions,
    ) -> Result<Option<NullableEncoder<'a>>, ArrowError> {
        use arrow_array::types::{
            TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType,
        };
        let DataType::Timestamp(unit, tz) = array.data_type() else {
            return Ok(None);
        };
        let values = match unit {
            TimeUnit::Second => array.as_primitive::<TimestampSecondType>().values().clone(),
            TimeUnit::Millisecond => array.as_primitive::<TimestampMillisecondType>().values().clone(),
            TimeUnit::Microsecond => array.as_primitive::<TimestampMicrosecondType>().values().clone(),
            TimeUnit::Nanosecond => array.as_primitive::<TimestampNanosecondType>().values().clone(),
        };
        let encoder = TimestampEncoder {
            values,
            unit: *unit,
            tz: tz.as_deref().map(str::parse).transpose()?,
        };
        Ok(Some(NullableEncoder::new(Box::new(encoder), array.nulls().cloned())))
    }
}

struct TimestampEncoder {
    values: ScalarBuffer<i64>,
    unit: TimeUnit,
    tz: Option<Tz>,
}

impl Encoder for TimestampEncoder {
    fn encode(&mut self, idx: usize, out: &mut Vec<u8>) {
        use std::io::Write;
        let v = self.values[idx];
        let per_sec: i64 = match self.unit {
            TimeUnit::Second => 1,
            TimeUnit::Millisecond => 1_000,
            TimeUnit::Microsecond => 1_000_000,
            TimeUnit::Nanosecond => 1_000_000_000,
        };
        let nanos = (v.rem_euclid(per_sec) * (1_000_000_000 / per_sec)) as u32;
        let Some(utc) = DateTime::<Utc>::from_timestamp(v.div_euclid(per_sec), nanos) else {
            // Out of chrono's range, which is wider than Python's (years
            // 1..=9999) — to_pylist() would have raised on it.
            out.extend_from_slice(b"null");
            return;
        };
        // Python datetimes stop at microseconds; truncate, as to_pylist() did.
        let micros = nanos / 1_000;
        out.push(b'"');
        let _ = match &self.tz {
            None => write!(out, "{}", utc.naive_utc().format("%Y-%m-%dT%H:%M:%S")),
            Some(tz) => write!(out, "{}", utc.with_timezone(tz).format("%Y-%m-%dT%H:%M:%S")),
        };
        if micros != 0 {
            let _ = write!(out, ".{micros:06}");
        }
        if let Some(tz) = &self.tz {
            let _ = write!(out, "{}", utc.with_timezone(tz).format("%:z"));
        }
        out.push(b'"');
    }
}

/// Rows per rayon task. Each task builds its own column encoders, so tiny
/// tasks would spend more on setup than on encoding.
const MIN_ROWS_PER_TASK: usize = 512;

/// Encode every row of `batches` as a JSON object, with nulls written out
/// explicitly (as `to_pylist()` + orjson did) and fields in schema order.
/// When `key_column` names a column present in the schema, each row's key is
/// that column's value as Python's `str()` would render it.
pub fn encode_rows(batches: &[RecordBatch], key_column: Option<&str>) -> Result<Messages, ArrowError> {
    let mut out = Messages::new();
    for batch in batches {
        let n = batch.num_rows();
        let key_idx = key_column.and_then(|k| batch.schema().index_of(k).ok());
        if let Some(i) = key_idx {
            let dt = batch.column(i).data_type();
            if !is_native_key_type(dt) {
                return Err(ArrowError::InvalidArgumentError(format!(
                    "key column type {dt} is not supported natively"
                )));
            }
        }
        let size = n.div_ceil(rayon::current_num_threads()).max(MIN_ROWS_PER_TASK);
        let parts = (0..n)
            .step_by(size)
            .collect::<Vec<_>>()
            .into_par_iter()
            .map(|start| encode_range(batch, start, (start + size).min(n), key_idx))
            .collect::<Result<Vec<_>, _>>()?;
        for part in parts {
            out.append(part);
        }
    }
    Ok(out)
}

fn encode_range(batch: &RecordBatch, start: usize, end: usize, key_idx: Option<usize>) -> Result<Messages, ArrowError> {
    let options = EncoderOptions::default()
        .with_explicit_nulls(true)
        .with_encoder_factory(Arc::new(PythonTimestamps));
    let schema = batch.schema();
    let names: Vec<Vec<u8>> = schema
        .fields()
        .iter()
        .map(|f| {
            let mut name = serde_json::to_vec(f.name()).expect("a string always serializes");
            name.push(b':');
            name
        })
        .collect();
    let mut encoders = schema
        .fields()
        .iter()
        .zip(batch.columns())
        .map(|(field, column)| make_encoder(field, column.as_ref(), &options))
        .collect::<Result<Vec<_>, _>>()?;

    let mut out = Messages::new();
    out.data.reserve((end - start) * 64 * names.len().max(1));
    for row in start..end {
        out.data.push(b'{');
        for (i, (name, encoder)) in names.iter().zip(encoders.iter_mut()).enumerate() {
            if i > 0 {
                out.data.push(b',');
            }
            out.data.extend_from_slice(name);
            if encoder.is_null(row) {
                out.data.extend_from_slice(b"null");
            } else {
                encoder.encode(row, &mut out.data);
            }
        }
        out.data.push(b'}');
        out.offsets.push(out.data.len());
        out.keys
            .push(key_idx.map(|k| key_for_row(batch.column(k).as_ref(), row)));
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, BooleanArray, Int64Array, StringArray, TimestampMillisecondArray};

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter([
            ("id", Arc::new(StringArray::from(vec![Some("a\"b"), None])) as ArrayRef),
            ("n", Arc::new(Int64Array::from(vec![Some(-1), Some(2)])) as ArrayRef),
            ("b", Arc::new(BooleanArray::from(vec![Some(true), None])) as ArrayRef),
        ])
        .unwrap()
    }

    #[test]
    fn encodes_rows_with_explicit_nulls() {
        let m = encode_rows(&[batch()], None).unwrap();
        assert_eq!(m.len(), 2);
        assert_eq!(m.payload(0), br#"{"id":"a\"b","n":-1,"b":true}"#);
        assert_eq!(m.payload(1), br#"{"id":null,"n":2,"b":null}"#);
        assert_eq!(m.keys, vec![None, None]);
    }

    #[test]
    fn keys_match_python_str() {
        let keys = |col| encode_rows(&[batch()], Some(col)).unwrap().keys;
        assert_eq!(keys("id"), vec![Some("a\"b".into()), Some("None".into())]);
        assert_eq!(keys("n"), vec![Some("-1".into()), Some("2".into())]);
        assert_eq!(keys("b"), vec![Some("True".into()), Some("None".into())]);
        assert_eq!(keys("absent"), vec![None, None]);
    }

    #[test]
    fn timestamps_match_python_isoformat() {
        let ts = |tz: Option<&str>| {
            let arr = TimestampMillisecondArray::from(vec![Some(1_700_000_000_123), Some(1_700_000_000_000), None])
                .with_timezone_opt(tz);
            let b = RecordBatch::try_from_iter([("t", Arc::new(arr) as ArrayRef)]).unwrap();
            let m = encode_rows(&[b], None).unwrap();
            (0..m.len())
                .map(|i| String::from_utf8(m.payload(i).to_vec()).unwrap())
                .collect::<Vec<_>>()
        };
        assert_eq!(
            ts(None),
            [
                r#"{"t":"2023-11-14T22:13:20.123000"}"#,
                r#"{"t":"2023-11-14T22:13:20"}"#,
                r#"{"t":null}"#
            ]
        );
        assert_eq!(ts(Some("UTC"))[0], r#"{"t":"2023-11-14T22:13:20.123000+00:00"}"#);
        assert_eq!(ts(Some("+05:30"))[1], r#"{"t":"2023-11-15T03:43:20+05:30"}"#);
    }

    #[test]
    fn parallel_split_preserves_order() {
        let n = 10_000;
        let b = RecordBatch::try_from_iter([("n", Arc::new(Int64Array::from_iter_values(0..n)) as ArrayRef)]).unwrap();
        let m = encode_rows(&[b.clone(), b], None).unwrap();
        assert_eq!(m.len(), 2 * n as usize);
        assert_eq!(m.payload(n as usize + 7), br#"{"n":7}"#);
        assert_eq!(m.payload(n as usize - 1), format!(r#"{{"n":{}}}"#, n - 1).as_bytes());
    }
}
