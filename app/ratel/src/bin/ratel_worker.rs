//! Redpanda worker: consumes CDC events from the Redpanda topic and runs the
//! unified dispatcher (`common::events::dispatcher`) for the configured role.
//!
//! Replaces the EventBridge Rule → Lambda targets of the AWS architecture.
//! Handlers reached through `dispatch()` use the app's lazily-initialized
//! server config singletons (DynamoDB etc.), same as the other server bins —
//! no extra setup is needed here.
//!
//! Usage:
//!   RATEL_KAFKA_BROKERS=<host:port> [RATEL_WORKER_ROLE=default|analyze|egress|all] \
//!     cargo run --bin ratel_worker --features redpanda
//!
//! See docs/k3s-migration/02-redpanda-implementation.md §1-4 for all env vars.

#[cfg(not(feature = "redpanda"))]
fn main() {
    eprintln!("ratel_worker requires --features redpanda");
    std::process::exit(1);
}

#[cfg(feature = "redpanda")]
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    use app_shell::common::events::config::EventsConfig;
    use app_shell::common::events::consumer::run_worker;

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,app_shell=debug")),
        )
        .init();

    let config = EventsConfig::from_env()?;
    tracing::info!(
        brokers = %config.brokers,
        topic = %config.topic,
        dlq_topic = %config.dlq_topic,
        group = %config.worker_group,
        role = %config.worker_role,
        "starting ratel_worker"
    );

    run_worker(&config).await?;

    tracing::info!("ratel_worker stopped");
    Ok(())
}
