//! Redpanda producer wrapper (feature `redpanda`).
//!
//! Publishes `CdcEvent`s as JSON with the item pk as message key,
//! `acks=all` + idempotence, awaiting delivery before the caller checkpoints.
//! Also exposes [`CdcProducer::send_raw`] so the worker can reuse the same
//! producer for DLQ publishes.

use std::time::Duration;

use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord};

use super::cdc_event::CdcEvent;

/// How long to wait for a delivery acknowledgement before giving up.
const MESSAGE_TIMEOUT_MS: &str = "30000";

#[derive(Clone)]
pub struct CdcProducer {
    inner: FutureProducer,
}

impl CdcProducer {
    /// Build a producer against the given bootstrap servers
    /// (`EventsConfig::brokers`).
    pub fn new(brokers: &str) -> Result<Self, String> {
        let inner = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("acks", "all")
            .set("enable.idempotence", "true")
            .set("message.timeout.ms", MESSAGE_TIMEOUT_MS)
            .create()
            .map_err(|e| format!("failed to create kafka producer ({brokers}): {e}"))?;
        Ok(Self { inner })
    }

    /// JSON-serialize a `CdcEvent` and produce it, keyed by the item pk so
    /// per-entity ordering is preserved. Resolves only after the broker
    /// acknowledges delivery.
    pub async fn send(&self, topic: &str, event: &CdcEvent) -> Result<(), String> {
        let payload = serde_json::to_string(event)
            .map_err(|e| format!("failed to serialize CdcEvent: {e}"))?;
        let key = event.pk().unwrap_or("").to_string();
        self.send_raw(topic, &key, &payload).await
    }

    /// Produce a pre-serialized JSON payload. Used by `send` and by the
    /// worker's DLQ path.
    pub async fn send_raw(&self, topic: &str, key: &str, payload_json: &str) -> Result<(), String> {
        let record = FutureRecord::to(topic).key(key).payload(payload_json);
        self.inner
            .send(record, Duration::from_secs(30))
            .await
            .map(|_| ())
            .map_err(|(e, _msg)| format!("kafka delivery to {topic} failed: {e}"))
    }
}
