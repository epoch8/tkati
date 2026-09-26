//! `tkati_core._native`: the Kafka client and JSON codec behind
//! `tkati_core.kafka.consumer.KafkaConsumer` / `producer.KafkaProducer`.
//!
//! Everything heavy runs with the GIL released. Encoding Arrow rows to JSON is
//! spread over rayon's pool (sized by `RAYON_NUM_THREADS`, default: available
//! cores). Decoding is left to pyarrow's multi-threaded C++ JSON reader, which
//! is faster per core than arrow-json: what it needed from Rust is to be handed
//! one contiguous NDJSON buffer without the payloads ever becoming Python
//! objects. The Python wrappers own logging and `LoopStats` phase timing.

mod encode;
mod kafka;
mod payloads;

use std::ffi::c_int;

use std::time::{Duration, Instant};

use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyRuntimeError, PyValueError};
use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict};
use pyo3_arrow::input::AnyRecordBatch;

use crate::encode::Messages;
use crate::payloads::Payloads;

create_exception!(_native, KafkaError, PyException, "An error reported by librdkafka.");

fn kafka_err(err: rdkafka::error::KafkaError) -> PyErr {
    KafkaError::new_err(err.to_string())
}

fn closed_err(msg: &'static str) -> PyErr {
    PyRuntimeError::new_err(msg)
}

/// librdkafka takes every property as a string. Python callers pass bools
/// (`"enable.auto.commit": False`), which confluent-kafka accepted, so they are
/// lowered the same way: `false`, not Python's `False`.
fn config_entries(config: &Bound<'_, PyDict>) -> PyResult<Vec<(String, String)>> {
    config
        .iter()
        .map(|(k, v)| {
            let value = if v.is_instance_of::<PyBool>() {
                if v.extract::<bool>()? { "true" } else { "false" }.to_owned()
            } else {
                v.str()?.to_string()
            };
            Ok((k.extract()?, value))
        })
        .collect()
}

/// Where a consumed batch starts and ends in each partition. Opaque to
/// Python: it is only handed back to `NativeConsumer.commit` / `rewind`.
#[pyclass(frozen, skip_from_py_object, module = "tkati_core._native")]
#[derive(Clone, Default)]
struct BatchOffsets {
    ranges: kafka::Offsets,
}

#[pymethods]
impl BatchOffsets {
    /// An empty set, for tests that mock a consumer.
    #[new]
    fn new() -> Self {
        Self::default()
    }

    fn __repr__(&self) -> String {
        let ranges: Vec<String> = self
            .ranges
            .iter()
            .map(|(partition, (first, next))| format!("{partition}: {first}..{next}"))
            .collect();
        format!("BatchOffsets({{{}}})", ranges.join(", "))
    }
}

/// One consumed batch: raw payloads, where they came from, and the consumer
/// errors met while polling for it.
#[pyclass(frozen, module = "tkati_core._native")]
struct RawBatch {
    payloads: Payloads,
    offsets: kafka::Offsets,
    #[pyo3(get)]
    errors: Vec<String>,
}

#[pymethods]
impl RawBatch {
    /// Build a batch from in-memory payloads (`None` for a tombstone), for
    /// tests and benchmarks that don't want a broker.
    #[staticmethod]
    fn from_payloads(payloads: Vec<Option<Vec<u8>>>) -> Self {
        let mut p = Payloads::default();
        for payload in &payloads {
            p.push(payload.as_deref());
        }
        Self {
            payloads: p,
            offsets: kafka::Offsets::new(),
            errors: Vec::new(),
        }
    }

    fn __len__(&self) -> usize {
        self.payloads.len()
    }

    /// Where the batch lies in each partition, to commit or rewind it by.
    /// Empty for a batch built with `from_payloads`.
    #[getter]
    fn offsets(&self) -> BatchOffsets {
        BatchOffsets {
            ranges: self.offsets.clone(),
        }
    }

    /// Messages without a value. They have no line in the buffer.
    #[getter]
    fn tombstones(&self) -> usize {
        self.payloads.tombstones()
    }

    /// Each payload as `bytes`, or `None` for a tombstone.
    fn payloads<'py>(&self, py: Python<'py>) -> Vec<Option<Bound<'py, PyBytes>>> {
        (0..self.payloads.len())
            .map(|i| self.payloads.get(i).map(|p| PyBytes::new(py, p)))
            .collect()
    }

    /// Read-only buffer protocol over the newline-delimited payloads, so
    /// `pyarrow.BufferReader(batch)` reads them without a copy. The class is
    /// frozen, so the buffer can't change while a view is held.
    unsafe fn __getbuffer__(slf: PyRef<'_, Self>, view: *mut ffi::Py_buffer, flags: c_int) -> PyResult<()> {
        let bytes = slf.payloads.joined();
        let len = bytes
            .len()
            .try_into()
            .map_err(|_| PyValueError::new_err("batch too large"))?;
        // SAFETY: `view` is supplied by the interpreter; the exporter object
        // (slf) is kept alive by the view's reference and owns `bytes`.
        let ret = unsafe { ffi::PyBuffer_FillInfo(view, slf.as_ptr(), bytes.as_ptr() as *mut _, len, 1, flags) };
        if ret == -1 {
            return Err(PyErr::fetch(slf.py()));
        }
        Ok(())
    }

    unsafe fn __releasebuffer__(&self, _view: *mut ffi::Py_buffer) {}
}

/// Messages ready to enqueue: one payload and optional key each.
#[pyclass(frozen, module = "tkati_core._native")]
struct EncodedBatch {
    messages: Messages,
}

#[pymethods]
impl EncodedBatch {
    /// Wrap payloads that were already encoded in Python.
    #[staticmethod]
    fn from_payloads(messages: Vec<(Vec<u8>, Option<String>)>) -> Self {
        let mut m = Messages::new();
        for (payload, key) in messages {
            m.push(&payload, key);
        }
        Self { messages: m }
    }

    fn __len__(&self) -> usize {
        self.messages.len()
    }

    fn payloads<'py>(&self, py: Python<'py>) -> Vec<Bound<'py, PyBytes>> {
        (0..self.messages.len())
            .map(|i| PyBytes::new(py, self.messages.payload(i)))
            .collect()
    }

    fn keys(&self) -> Vec<Option<String>> {
        self.messages.keys.clone()
    }
}

/// Encode each row of a pyarrow Table / RecordBatch (anything exporting the
/// Arrow C stream or array interface) as one JSON object. With `keys`, those
/// are used as the message keys; otherwise, when `key_column` is in the
/// schema, each row's key is `str()` of that column's value — which only
/// string, integer, boolean and null columns support; key anything else by
/// passing `keys`.
#[pyfunction]
#[pyo3(signature = (data, key_column=None, keys=None))]
fn encode_arrow(
    py: Python<'_>,
    data: AnyRecordBatch,
    key_column: Option<String>,
    keys: Option<Vec<Option<String>>>,
) -> PyResult<EncodedBatch> {
    // Collected with the GIL held: a stream's producer may be Python code.
    let batches = data
        .into_reader()?
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    let key_column = if keys.is_some() { None } else { key_column };
    let mut messages = py
        .detach(|| encode::encode_rows(&batches, key_column.as_deref()))
        .map_err(|e| PyValueError::new_err(e.to_string()))?;
    if let Some(keys) = keys {
        if keys.len() != messages.len() {
            return Err(PyValueError::new_err(format!(
                "{} keys for {} rows",
                keys.len(),
                messages.len()
            )));
        }
        messages.keys = keys;
    }
    Ok(EncodedBatch { messages })
}

#[pyclass(frozen, module = "tkati_core._native")]
struct NativeConsumer {
    inner: kafka::KafkaConsumer,
}

#[pymethods]
impl NativeConsumer {
    #[new]
    fn new(config: &Bound<'_, PyDict>, topic: &str) -> PyResult<Self> {
        let entries = config_entries(config)?;
        Ok(Self {
            inner: kafka::KafkaConsumer::new(&entries, topic).map_err(kafka_err)?,
        })
    }

    /// Poll until `max_messages` have arrived or `timeout` seconds have
    /// passed. Checks for signals every `POLL_STEP`, so KeyboardInterrupt
    /// isn't delayed by the batch timeout.
    fn poll_batch(&self, py: Python<'_>, timeout: f64, max_messages: usize) -> PyResult<RawBatch> {
        let deadline = Instant::now() + Duration::from_secs_f64(timeout.max(0.0));
        let mut payloads = Payloads::default();
        let mut offsets = kafka::Offsets::new();
        let mut errors = Vec::new();
        while payloads.len() < max_messages && Instant::now() < deadline {
            py.detach(|| {
                self.inner
                    .poll_step(deadline, max_messages, &mut payloads, &mut offsets, &mut errors)
            })
            .map_err(closed_err)?;
            py.check_signals()?;
        }
        Ok(RawBatch {
            payloads,
            offsets,
            errors,
        })
    }

    /// Commit the end of each partition the batch read from.
    fn commit(&self, py: Python<'_>, offsets: Py<BatchOffsets>) -> PyResult<()> {
        let offsets = offsets.get();
        py.detach(|| self.inner.commit(&offsets.ranges))
            .map_err(closed_err)?
            .map_err(kafka_err)
    }

    /// Seek back to the start of each partition the batch read from, so it is
    /// delivered again.
    fn rewind(&self, py: Python<'_>, offsets: Py<BatchOffsets>) -> PyResult<()> {
        let offsets = offsets.get();
        py.detach(|| self.inner.rewind(&offsets.ranges))
            .map_err(closed_err)?
            .map_err(kafka_err)
    }

    fn close(&self, py: Python<'_>) {
        py.detach(|| self.inner.close());
    }
}

#[pyclass(frozen, module = "tkati_core._native")]
struct NativeProducer {
    inner: kafka::KafkaProducer,
}

#[pymethods]
impl NativeProducer {
    #[new]
    fn new(config: &Bound<'_, PyDict>, topic: &str) -> PyResult<Self> {
        let entries = config_entries(config)?;
        Ok(Self {
            inner: kafka::KafkaProducer::new(&entries, topic).map_err(kafka_err)?,
        })
    }

    fn enqueue(&self, py: Python<'_>, batch: Py<EncodedBatch>) -> PyResult<()> {
        let batch = batch.get();
        py.detach(|| self.inner.enqueue(&batch.messages)).map_err(kafka_err)
    }

    /// Block until every enqueued message is delivered (or has failed),
    /// checking for signals between waits.
    fn flush(&self, py: Python<'_>) -> PyResult<()> {
        while !py
            .detach(|| self.inner.flush_step(kafka::POLL_STEP))
            .map_err(kafka_err)?
        {
            py.check_signals()?;
        }
        Ok(())
    }
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<BatchOffsets>()?;
    m.add_class::<RawBatch>()?;
    m.add_class::<EncodedBatch>()?;
    m.add_class::<NativeConsumer>()?;
    m.add_class::<NativeProducer>()?;
    m.add_function(wrap_pyfunction!(encode_arrow, m)?)?;
    m.add("KafkaError", m.py().get_type::<KafkaError>())?;
    Ok(())
}
