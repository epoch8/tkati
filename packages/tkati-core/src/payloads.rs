//! A consumed batch's payloads, kept in the newline-delimited layout that
//! pyarrow's JSON reader parses directly.

/// Payloads stored back to back, each followed by `\n`, so that `joined()` is
/// NDJSON that `pyarrow.json.read_json` can parse zero-copy and
/// multi-threaded. Tombstones (messages without a value) take up no space in
/// the buffer; they are only counted.
#[derive(Default)]
pub struct Payloads {
    joined: Vec<u8>,
    spans: Vec<Option<(usize, usize)>>,
    tombstones: usize,
}

impl Payloads {
    pub fn push(&mut self, payload: Option<&[u8]>) {
        self.spans.push(payload.map(|p| {
            let start = self.joined.len();
            self.joined.extend_from_slice(p);
            self.joined.push(b'\n');
            (start, start + p.len())
        }));
        self.tombstones += usize::from(payload.is_none());
    }

    pub fn len(&self) -> usize {
        self.spans.len()
    }

    pub fn tombstones(&self) -> usize {
        self.tombstones
    }

    pub fn joined(&self) -> &[u8] {
        &self.joined
    }

    pub fn get(&self, idx: usize) -> Option<&[u8]> {
        self.spans[idx].map(|(start, end)| &self.joined[start..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn joins_with_newlines_and_skips_tombstones() {
        let mut p = Payloads::default();
        p.push(Some(b"{\"a\":1}"));
        p.push(None);
        p.push(Some(b"{}"));
        assert_eq!(p.joined(), b"{\"a\":1}\n{}\n");
        assert_eq!((p.len(), p.tombstones()), (3, 1));
        assert_eq!(p.get(0), Some(&b"{\"a\":1}"[..]));
        assert_eq!(p.get(1), None);
        assert_eq!(p.get(2), Some(&b"{}"[..]));
    }
}
