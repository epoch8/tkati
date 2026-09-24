//! librdkafka consumer and producer, driven without holding the GIL.

use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant};

use rdkafka::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, CommitMode, Consumer, ConsumerContext};
use rdkafka::error::{KafkaError, RDKafkaErrorCode};
use rdkafka::message::Message;
use rdkafka::producer::{BaseRecord, NoCustomPartitioner, Producer, ProducerContext, ThreadedProducer};
use rdkafka::util::Timeout;

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

impl ProducerContext<NoCustomPartitioner> for StderrContext {
    type DeliveryOpaque = ();
    // Delivery failures are not reported per message, as with confluent-kafka
    // and no `on_delivery` callback.
    fn delivery(&self, _: &rdkafka::message::DeliveryResult<'_>, _: ()) {}
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

pub struct KafkaConsumer {
    inner: Mutex<Option<BaseConsumer<StderrContext>>>,
}

impl KafkaConsumer {
    pub fn new(entries: &[(String, String)], topic: &str) -> Result<Self, KafkaError> {
        let consumer: BaseConsumer<StderrContext> = client_config(entries).create_with_context(StderrContext)?;
        consumer.subscribe(&[topic])?;
        Ok(Self {
            inner: Mutex::new(Some(consumer)),
        })
    }

    fn with<T>(&self, f: impl FnOnce(&BaseConsumer<StderrContext>) -> T) -> Result<T, &'static str> {
        let guard = self.inner.lock().unwrap_or_else(|e| e.into_inner());
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
                    Some(Ok(msg)) => payloads.push(msg.payload()),
                    Some(Err(err)) => errors.push(err.to_string()),
                }
            }
        })
    }

    pub fn commit(&self) -> Result<Result<(), KafkaError>, &'static str> {
        // Async, as confluent-kafka's `commit()` defaults to.
        self.with(|c| c.commit_consumer_state(CommitMode::Async))
    }

    /// Leave the group and release the client. Idempotent.
    pub fn close(&self) {
        let consumer = self.inner.lock().unwrap_or_else(|e| e.into_inner()).take();
        drop(consumer);
    }
}

pub struct KafkaProducer {
    inner: ThreadedProducer<StderrContext>,
    topic: String,
}

impl KafkaProducer {
    pub fn new(entries: &[(String, String)], topic: &str) -> Result<Self, KafkaError> {
        Ok(Self {
            inner: client_config(entries).create_with_context(StderrContext)?,
            topic: topic.to_owned(),
        })
    }

    /// Hand every message to librdkafka. When its local queue is full, wait
    /// for the background thread to drain some of it and retry, instead of
    /// failing the batch halfway through.
    pub fn enqueue(&self, messages: &Messages) -> Result<(), KafkaError> {
        for idx in 0..messages.len() {
            let key = messages.keys[idx].as_deref();
            let mut record = BaseRecord::<str, [u8]>::to(&self.topic).payload(messages.payload(idx));
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
                    Err((err, _)) => return Err(err),
                }
            }
        }
        Ok(())
    }

    /// Wait up to `step` for outstanding deliveries. Returns true once
    /// nothing is left in flight.
    pub fn flush_step(&self, step: Duration) -> Result<bool, KafkaError> {
        match self.inner.flush(Timeout::After(step)) {
            Ok(()) => Ok(true),
            Err(KafkaError::Flush(RDKafkaErrorCode::OperationTimedOut)) => Ok(false),
            Err(err) => Err(err),
        }
    }
}
