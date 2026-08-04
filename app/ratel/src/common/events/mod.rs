//! Redpanda-based event pipeline (k3s migration).
//!
//! Replaces the DynamoDB Streams → EventBridge Pipes → Lambda architecture:
//!
//! ```text
//! DynamoDB Streams → ratel_cdc (bin, checkpointed) → Redpanda topic (key=pk)
//!                                                        → ratel_worker (bin) → dispatcher
//! ```
//!
//! - `cdc_event`   — wire format (`CdcEvent`) shared by producer and consumers
//! - `config`      — runtime env configuration (`EventsConfig`)
//! - `dispatcher`  — unified filter + dispatch logic, ported 1:1 from
//!   `cdk/lib/dynamo-stream-event.ts` pipe filters (spec:
//!   `docs/k3s-migration/03-filter-matrix.md`)
//! - `checkpoint`  — CDC shard checkpoints stored in the main DynamoDB table
//! - `producer`    — Redpanda producer wrapper (feature `redpanda`)
//! - `consumer`    — Redpanda consumer loop for workers (feature `redpanda`)

pub mod cdc_event;
pub mod config;
pub mod dispatcher;

#[cfg(feature = "redpanda")]
pub mod checkpoint;
#[cfg(feature = "redpanda")]
pub mod consumer;
#[cfg(feature = "redpanda")]
pub mod producer;

pub use cdc_event::*;
pub use config::*;
pub use dispatcher::{DispatchSummary, EventClass, RoleSet, dispatch};
