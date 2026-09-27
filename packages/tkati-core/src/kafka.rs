//! librdkafka consumer and producer, driven without holding the GIL.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Condvar, Mutex, RwLock};
use std::thread;
use std::time::{Duration, Instant};

use rdkafka::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::producer::{BaseRecord, NoCustomPartitioner, Producer, ProducerContext, ThreadedProducer};
use rdkafka::{Offset, TopicPartitionList};

use crate::encode::Messages;
use crate::payloads::Payloads;

/// Forwards librdkafka's logs and client errors to stderr, as confluent-kafka
/// does by default. rdkafka's default context routes them to the `log` crate,
/// where with no logger installed broker connection problems would vanish.
pub struct StderrContext;

impl ClientContext for StderrContext {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        eprintln!("%{}|{fac}| {log_message}", level as i32);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        eprintln!("librdkafka error: {error}: {reason}");
    }
}

impl ConsumerContext for StderrContext {}

/// Messages in flight per tag, and the first failure among them. A tag names
/// the messages one caller wants to wait for as a group (the harness uses one
/// per input batch); tag 0 is never tracked.
#[derive(Default)]
struct TagState {
    pending: usize,
    failed: Option<String>,
}

/// Delivery bookkeeping, fed by the producer's delivery callback on
/// librdkafka's background thread: every message in flight, for `flush`, and
/// per tag, for `wait`.
#[derive(Default)]
pub struct Deliveries {
    state: Mutex<DeliveryState>,
    settled: Condvar,
}

#[derive(Default)]
struct DeliveryState {
    tags: HashMap<usize, TagState>,
    // Every message handed to librdkafka whose report hasn't come back yet,
    // tagged or not.
    in_flight: usize,
}

impl Deliveries {
    fn lock(&self) -> std::sync::MutexGuard<'_, DeliveryState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Expect `count` more deliveries for `tag`. Called before the messages
    /// are sent, so a report can never arrive for a message not yet counted.
    fn add(&self, tag: usize, count: usize) {
        if count == 0 {
            return;
        }
        let mut state = self.lock();
        state.in_flight += count;
        if tag != 0 {
            state.tags.entry(tag).or_default().pending += count;
        }
    }

    /// Take back `count` expected deliveries that will never come, for
    /// messages `add` counted but that were never handed to librdkafka.
    fn cancel(&self, tag: usize, count: usize) {
        self.settle(tag, count, None);
    }

    fn settle(&self, tag: usize, count: usize, error: Option<String>) {
        let mut state = self.lock();
        state.in_flight = state.in_flight.saturating_sub(count);
        // Absent once `wait` has returned a failure for it: nobody is
        // waiting any more.
        if let Some(tag_state) = state.tags.get_mut(&tag) {
            tag_state.pending = tag_state.pending.saturating_sub(count);
            if tag_state.failed.is_none() {
                tag_state.failed = error;
            }
        }
        drop(state);
        self.settled.notify_all();
    }

    /// Wait up to `step` for `tag`'s messages. `Some(Ok(()))` once all of
    /// them are delivered, `Some(Err(_))` as soon as one has failed (the
    /// first error), `None` if still pending when `step` runs out. A settled
    /// tag is forgotten; a tag never added counts as delivered.
    pub fn wait(&self, tag: usize, step: Duration) -> Option<Result<(), String>> {
        let deadline = Instant::now() + step;
        let mut state = self.lock();
        loop {
            match state.tags.get(&tag) {
                None => return Some(Ok(())),
                Some(t) if t.failed.is_some() || t.pending == 0 => {
                    let t = state.tags.remove(&tag).unwrap_or_default();
                    return Some(t.failed.map_or(Ok(()), Err));
                }
                Some(_) => {}
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return None;
            }
            state = self
                .settled
                .wait_timeout(state, remaining)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
    }

    /// Wait up to `step` until no message is in flight at all. True once
    /// none is.
    pub fn wait_all(&self, step: Duration) -> bool {
        let deadline = Instant::now() + step;
        let mut state = self.lock();
        while state.in_flight > 0 {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return false;
            }
            state = self
                .settled
                .wait_timeout(state, remaining)
                .unwrap_or_else(|e| e.into_inner())
                .0;
        }
        true
    }
}

/// The producer's context: logs like `StderrContext`, and records every
/// delivery report against the tag its message was sent with.
#[derive(Default)]
pub struct ProducerDeliveryContext {
    deliveries: Deliveries,
}

impl ClientContext for ProducerDeliveryContext {
    fn log(&self, level: RDKafkaLogLevel, fac: &str, log_message: &str) {
        StderrContext.log(level, fac, log_message);
    }

    fn error(&self, error: KafkaError, reason: &str) {
        StderrContext.error(error, reason);
    }
}

impl ProducerContext<NoCustomPartitioner> for ProducerDeliveryContext {
    type DeliveryOpaque = usize;

    fn delivery(&self, result: &rdkafka::message::DeliveryResult<'_>, tag: usize) {
        let error = result.as_ref().err().map(|(err, _)| err.to_string());
        self.deliveries.settle(tag, 1, error);
    }
}

pub fn client_config(entries: &[(String, String)]) -> ClientConfig {
    let mut config = ClientConfig::new();
    for (k, v) in entries {
        config.set(k, v);
    }
    config
}

/// How long one `poll_batch` step may block. Bounded so the caller can check
/// for signals between steps and Ctrl-C isn't held up for a whole batch
/// timeout.
pub const POLL_STEP: Duration = Duration::from_millis(100);

/// How long `rewind` waits for the fetcher to move to the new position. Once
/// it has, librdkafka guarantees nothing fetched from the old position is
/// handed out, so the next poll starts from the rewound offset.
const SEEK_TIMEOUT: Duration = Duration::from_secs(10);

/// Where a batch starts and ends in each partition it read from: partition →
/// (first offset, last offset + 1). Keyed by partition alone, as a consumer
/// subscribes to exactly one topic.
pub type Offsets = BTreeMap<i32, (i64, i64)>;

/// Extend `offsets` with a message at `offset`. Offsets only grow within a
/// partition, so the first one recorded is the batch's start.
fn record_offset(offsets: &mut Offsets, partition: i32, offset: i64) {
    offsets.entry(partition).or_insert((offset, offset + 1)).1 = offset + 1;
}

pub struct KafkaConsumer {
    // A read lock for polling, committing and seeking, which librdkafka
    // allows from several threads at once (a harness reader thread polls
    // while the loop commits); the write lock only for `close`, which takes
    // the client out.
    inner: RwLock<Option<BaseConsumer<StderrContext>>>,
    // Set by `close` before it waits for the write lock. Checked before every
    // read lock, so a thread polling in steps stops re-taking the lock at its
    // next step instead of keeping `close` waiting for its whole batch
    // timeout.
    closing: AtomicBool,
    topic: String,
}

impl KafkaConsumer {
    pub fn new(entries: &[(String, String)], topic: &str) -> Result<Self, KafkaError> {
        let consumer: BaseConsumer<StderrContext> = client_config(entries).create_with_context(StderrContext)?;
        consumer.subscribe(&[topic])?;
        Ok(Self {
            inner: RwLock::new(Some(consumer)),
            closing: AtomicBool::new(false),
            topic: topic.to_owned(),
        })
    }

    fn with<T>(&self, f: impl FnOnce(&BaseConsumer<StderrContext>) -> T) -> Result<T, &'static str> {
        if self.closing.load(Ordering::Acquire) {
            return Err("consumer is closed");
        }
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard.as_ref().map(f).ok_or("consumer is closed")
    }

    /// Poll until `deadline`, `max_messages` payloads have been collected, or
    /// `POLL_STEP` has elapsed, whichever comes first. Errors are collected
    /// rather than returned, matching how the Python loop logged and skipped
    /// them.
    pub fn poll_step(
        &self,
        deadline: Instant,
        max_messages: usize,
        payloads: &mut Payloads,
        offsets: &mut Offsets,
        errors: &mut Vec<String>,
    ) -> Result<(), &'static str> {
        self.with(|consumer| {
            let step_end = (Instant::now() + POLL_STEP).min(deadline);
            while payloads.len() < max_messages {
                let remaining = step_end.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    break;
                }
                match consumer.poll(remaining) {
                    None => break,
                    Some(Ok(msg)) => {
                        record_offset(offsets, msg.partition(), msg.offset());
                        payloads.push(msg.payload());
                    }
                    Some(Err(err)) => errors.push(err.to_string()),
                }
            }
        })
    }

    /// The partitions of `offsets` still assigned to this consumer, each at
    /// the offset `pick` chooses from its range. After a rebalance the others
    /// belong to another group member, and are not ours to commit or seek.
    fn assigned(
        &self,
        consumer: &BaseConsumer<StderrContext>,
        offsets: &Offsets,
        pick: impl Fn((i64, i64)) -> i64,
    ) -> Result<TopicPartitionList, KafkaError> {
        let assignment = consumer.assignment()?;
        let mut tpl = TopicPartitionList::new();
        for (&partition, &range) in offsets {
            if assignment.find_partition(&self.topic, partition).is_some() {
                tpl.add_partition_offset(&self.topic, partition, Offset::Offset(pick(range)))?;
            }
        }
        Ok(tpl)
    }

    /// Commit the end of each partition's range: the batch is done.
    pub fn commit(&self, offsets: &Offsets) -> Result<Result<(), KafkaError>, &'static str> {
        self.with(|c| -> Result<(), KafkaError> {
            let tpl = self.assigned(c, offsets, |(_, next)| next)?;
            // librdkafka rejects an empty list rather than doing nothing.
            if tpl.count() == 0 {
                return Ok(());
            }
            // Async, as confluent-kafka's `commit()` defaults to.
            c.commit(&tpl, CommitMode::Async)
        })
    }

    /// Seek each partition back to the start of its range, so the batch (and
    /// anything polled after it) is delivered again.
    pub fn rewind(&self, offsets: &Offsets) -> Result<Result<(), KafkaError>, &'static str> {
        self.with(|c| -> Result<(), KafkaError> {
            let tpl = self.assigned(c, offsets, |(first, _)| first)?;
            if tpl.count() == 0 {
                return Ok(());
            }
            for elem in c.seek_partitions(tpl, SEEK_TIMEOUT)?.elements() {
                elem.error()?;
            }
            Ok(())
        })
    }

    /// Leave the group and release the client. Idempotent.
    pub fn close(&self) {
        self.closing.store(true, Ordering::Release);
        let consumer = self.inner.write().unwrap_or_else(|e| e.into_inner()).take();
        drop(consumer);
    }
}

pub struct KafkaProducer {
    inner: ThreadedProducer<ProducerDeliveryContext>,
    topic: String,
}

impl KafkaProducer {
    pub fn new(entries: &[(String, String)], topic: &str) -> Result<Self, KafkaError> {
        Ok(Self {
            inner: client_config(entries).create_with_context(ProducerDeliveryContext::default())?,
            topic: topic.to_owned(),
        })
    }

    /// Hand every message to librdkafka, tagged with `tag` (0: untracked).
    /// When its local queue is full, wait for the background thread to drain
    /// some of it and retry, instead of failing the batch halfway through.
    pub fn enqueue(&self, messages: &Messages, tag: usize) -> Result<(), KafkaError> {
        let deliveries = &self.inner.context().deliveries;
        deliveries.add(tag, messages.len());
        for idx in 0..messages.len() {
            let key = messages.keys[idx].as_deref();
            let mut record =
                BaseRecord::<str, [u8], usize>::with_opaque_to(&self.topic, tag).payload(messages.payload(idx));
            if let Some(k) = key {
                record = record.key(k);
            }
            loop {
                match self.inner.send(record) {
                    Ok(()) => break,
                    Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), r)) => {
                        record = r;
                        thread::sleep(Duration::from_millis(10));
                    }
                    Err((err, _)) => {
                        // This message and the rest were never sent, so no
                        // report will come for them.
                        deliveries.cancel(tag, messages.len() - idx);
                        return Err(err);
                    }
                }
            }
        }
        Ok(())
    }

    /// See `Deliveries::wait`.
    pub fn wait_delivered(&self, tag: usize, step: Duration) -> Option<Result<(), String>> {
        self.inner.context().deliveries.wait(tag, step)
    }

    /// Wait up to `step` for outstanding deliveries. Returns true once
    /// nothing is left in flight.
    ///
    /// Waits on the delivery reports rather than librdkafka's own flush:
    /// with a `ThreadedProducer` the reports are served by its background
    /// thread, so `rd_kafka_flush` found nothing to serve and returned only
    /// once its whole timeout had passed, adding one `POLL_STEP` (100 ms) to
    /// every flush.
    pub fn flush_step(&self, step: Duration) -> Result<bool, KafkaError> {
        Ok(self.inner.context().deliveries.wait_all(step))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_tag_is_delivered_once_every_message_is() {
        let d = Deliveries::default();
        d.add(1, 2);
        d.settle(1, 1, None);
        assert_eq!(d.wait(1, Duration::ZERO), None);
        d.settle(1, 1, None);
        assert_eq!(d.wait(1, Duration::ZERO), Some(Ok(())));
        // Forgotten once settled; unknown tags count as delivered.
        assert_eq!(d.wait(1, Duration::ZERO), Some(Ok(())));
    }

    #[test]
    fn the_first_failure_is_reported_without_waiting_for_the_rest() {
        let d = Deliveries::default();
        d.add(3, 3);
        d.settle(3, 1, Some("first".into()));
        d.settle(3, 1, Some("second".into()));
        assert_eq!(d.wait(3, Duration::ZERO), Some(Err("first".into())));
        // A late report for a tag already reported as failed is ignored.
        d.settle(3, 1, None);
        assert_eq!(d.wait(3, Duration::ZERO), Some(Ok(())));
    }

    #[test]
    fn tag_zero_and_cancelled_messages_are_not_waited_for() {
        let d = Deliveries::default();
        d.add(0, 5);
        assert_eq!(d.wait(0, Duration::ZERO), Some(Ok(())));
        d.add(2, 4);
        d.settle(2, 1, None);
        d.cancel(2, 3);
        assert_eq!(d.wait(2, Duration::ZERO), Some(Ok(())));
    }

    #[test]
    fn wait_wakes_up_when_a_report_arrives() {
        let d = std::sync::Arc::new(Deliveries::default());
        d.add(7, 1);
        let reporter = {
            let d = d.clone();
            thread::spawn(move || {
                thread::sleep(Duration::from_millis(20));
                d.settle(7, 1, None);
            })
        };
        assert_eq!(d.wait(7, Duration::from_secs(5)), Some(Ok(())));
        reporter.join().unwrap();
    }

    #[test]
    fn wait_all_counts_tagged_and_untagged_messages() {
        let d = Deliveries::default();
        d.add(0, 2);
        d.add(5, 1);
        assert!(!d.wait_all(Duration::ZERO));
        d.settle(0, 2, None);
        assert!(!d.wait_all(Duration::ZERO));
        d.settle(5, 1, Some("failed".into()));
        // Settled either way: nothing is in flight any more.
        assert!(d.wait_all(Duration::ZERO));
        d.add(0, 3);
        d.cancel(0, 3);
        assert!(d.wait_all(Duration::ZERO));
    }

    #[test]
    fn records_first_and_next_offset_per_partition() {
        let mut offsets = Offsets::new();
        record_offset(&mut offsets, 0, 10);
        record_offset(&mut offsets, 1, 3);
        record_offset(&mut offsets, 0, 11);
        record_offset(&mut offsets, 0, 14);
        assert_eq!(offsets, Offsets::from([(0, (10, 15)), (1, (3, 4))]));
    }
}
