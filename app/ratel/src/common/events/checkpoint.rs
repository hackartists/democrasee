//! CDC shard checkpoints, stored in the main DynamoDB table.
//!
//! Row shape: `pk = "CDC_CHECKPOINT#{table_name}"`, `sk = "SHARD#{shard_id}"`,
//! attributes `seq` (last produced sequence number, S) and `updated_at`
//! (epoch ms, N).
//!
//! The store uses raw `aws_sdk_dynamodb` calls on purpose — this is infra
//! plumbing for the CDC producer and must stay dependency-light (no
//! `DynamoEntity` derive). The producer skips stream records whose pk starts
//! with [`CDC_CHECKPOINT_PK_PREFIX`] so these writes never feed back into the
//! CDC topic.

use std::collections::HashMap;

use aws_sdk_dynamodb::types::AttributeValue;

use super::cdc_event::CDC_CHECKPOINT_PK_PREFIX;

/// The sk prefix for per-shard checkpoint rows.
pub const CDC_CHECKPOINT_SK_PREFIX: &str = "SHARD#";

/// The main table name, resolved at **runtime** so a single binary/image can
/// serve dev and prod on k3s: `DYNAMO_TABLE_PREFIX` env var first, then
/// `RATEL_DYNAMO_TABLE_PREFIX`, then the compile-time `DYNAMO_TABLE_PREFIX`
/// (same default the local-dev stream poller uses).
pub fn main_table_name() -> String {
    let prefix = std::env::var("DYNAMO_TABLE_PREFIX")
        .ok()
        .or_else(|| std::env::var("RATEL_DYNAMO_TABLE_PREFIX").ok())
        .or_else(|| option_env!("DYNAMO_TABLE_PREFIX").map(str::to_string))
        .unwrap_or_else(|| "ratel-local".to_string());
    format!("{prefix}-main")
}

/// Shard checkpoint store on the main DynamoDB table.
#[derive(Debug, Clone)]
pub struct CheckpointStore {
    client: aws_sdk_dynamodb::Client,
    table_name: String,
    /// `CDC_CHECKPOINT#{table_name}` — one partition holds every shard row.
    pk: String,
}

impl CheckpointStore {
    pub fn new(client: aws_sdk_dynamodb::Client, table_name: String) -> Self {
        let pk = format!("{CDC_CHECKPOINT_PK_PREFIX}{table_name}");
        Self {
            client,
            table_name,
            pk,
        }
    }

    /// Load every shard checkpoint: `shard_id → last produced sequence number`.
    pub async fn load_all(&self) -> Result<HashMap<String, String>, String> {
        let mut checkpoints = HashMap::new();
        let mut start_key: Option<HashMap<String, AttributeValue>> = None;

        loop {
            let mut req = self
                .client
                .query()
                .table_name(&self.table_name)
                .key_condition_expression("pk = :pk")
                .expression_attribute_values(":pk", AttributeValue::S(self.pk.clone()));
            if let Some(key) = start_key.take() {
                req = req.set_exclusive_start_key(Some(key));
            }

            let output = req
                .send()
                .await
                .map_err(|e| format!("checkpoint query failed: {e}"))?;

            for item in output.items() {
                let sk = match item.get("sk") {
                    Some(AttributeValue::S(s)) => s.as_str(),
                    _ => continue,
                };
                let seq = match item.get("seq") {
                    Some(AttributeValue::S(s)) => s.clone(),
                    _ => continue,
                };
                let shard_id = sk.strip_prefix(CDC_CHECKPOINT_SK_PREFIX).unwrap_or(sk);
                checkpoints.insert(shard_id.to_string(), seq);
            }

            start_key = output.last_evaluated_key().cloned();
            if start_key.is_none() {
                break;
            }
        }

        Ok(checkpoints)
    }

    /// Persist the last produced sequence number for a shard. Call only after
    /// every produce in the batch has been delivery-acknowledged.
    pub async fn save(&self, shard_id: &str, seq: &str) -> Result<(), String> {
        self.client
            .put_item()
            .table_name(&self.table_name)
            .item("pk", AttributeValue::S(self.pk.clone()))
            .item(
                "sk",
                AttributeValue::S(format!("{CDC_CHECKPOINT_SK_PREFIX}{shard_id}")),
            )
            .item("seq", AttributeValue::S(seq.to_string()))
            .item("updated_at", AttributeValue::N(epoch_ms().to_string()))
            .send()
            .await
            .map(|_| ())
            .map_err(|e| format!("checkpoint save failed (shard {shard_id}): {e}"))
    }
}

/// Current wall-clock time in epoch milliseconds.
pub fn epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or_default()
}
