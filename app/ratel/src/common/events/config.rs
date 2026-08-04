//! Runtime configuration for the Redpanda event pipeline.
//!
//! Unlike the rest of the app (compile-time `option_env!`), everything here is
//! read at **runtime** via `std::env::var` so a single binary/image can serve
//! dev and prod on k3s.

/// Where the CDC producer starts when a shard has no checkpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CdcStart {
    Latest,
    TrimHorizon,
}

#[derive(Debug, Clone)]
pub struct EventsConfig {
    /// Kafka bootstrap servers, e.g.
    /// `redpanda-0.redpanda.infra.svc.cluster.local:9093`.
    pub brokers: String,
    /// CDC topic, default `dev.ratel.cdc.v1`.
    pub topic: String,
    /// DLQ topic, default `{topic}.dlq`.
    pub dlq_topic: String,
    /// Start position when no checkpoint exists.
    pub start: CdcStart,
    /// Worker role: `default` | `analyze` | `egress` | `all`.
    pub worker_role: String,
    /// Consumer group id, default `ratel-worker-{role}`.
    pub worker_group: String,
}

impl EventsConfig {
    /// Read configuration from the environment. Fails only when
    /// `RATEL_KAFKA_BROKERS` is missing.
    pub fn from_env() -> Result<Self, String> {
        let brokers = std::env::var("RATEL_KAFKA_BROKERS")
            .map_err(|_| "RATEL_KAFKA_BROKERS is required (e.g. redpanda-0.redpanda.infra.svc.cluster.local:9093)".to_string())?;
        let topic = std::env::var("RATEL_CDC_TOPIC").unwrap_or_else(|_| "dev.ratel.cdc.v1".into());
        let dlq_topic =
            std::env::var("RATEL_CDC_DLQ_TOPIC").unwrap_or_else(|_| format!("{topic}.dlq"));
        let start = match std::env::var("RATEL_CDC_START").as_deref() {
            Ok("trim_horizon") => CdcStart::TrimHorizon,
            _ => CdcStart::Latest,
        };
        let worker_role =
            std::env::var("RATEL_WORKER_ROLE").unwrap_or_else(|_| "all".to_string());
        let worker_group = std::env::var("RATEL_WORKER_GROUP")
            .unwrap_or_else(|_| format!("ratel-worker-{worker_role}"));

        Ok(Self {
            brokers,
            topic,
            dlq_topic,
            start,
            worker_role,
            worker_group,
        })
    }
}

/// `RATEL_STREAM_POLLER=off` disables the in-process local-dev stream poller
/// so the CDC + worker pipeline can be tested without double-processing.
pub fn stream_poller_enabled() -> bool {
    !matches!(
        std::env::var("RATEL_STREAM_POLLER").as_deref(),
        Ok("off") | Ok("0") | Ok("false")
    )
}
