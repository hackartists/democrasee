//! Wire format for CDC events on the Redpanda topic.
//!
//! One `CdcEvent` per DynamoDB Streams record, serialized as JSON. The Kafka
//! message key is the item's `pk` string so per-entity ordering is preserved
//! (same guarantee DynamoDB Streams gives within a shard).
//!
//! Unlike the EventBridge path, REMOVE events carry the old item in
//! `old_image` honestly — there is no `inputTemplate` trick that smuggles
//! `OldImage` into a `newImage` field.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

pub type Image = HashMap<String, serde_dynamo::AttributeValue>;

pub const CDC_SCHEMA_VERSION: u32 = 1;

/// The pk prefix used for CDC checkpoint rows. The CDC producer must skip
/// stream records whose pk starts with this to avoid a self-feeding loop.
pub const CDC_CHECKPOINT_PK_PREFIX: &str = "CDC_CHECKPOINT#";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CdcEvent {
    pub schema_version: u32,
    /// "INSERT" | "MODIFY" | "REMOVE"
    pub event_name: String,
    pub keys: Image,
    pub new_image: Option<Image>,
    pub old_image: Option<Image>,
    pub approximate_creation_ms: Option<i64>,
    pub sequence_number: Option<String>,
    pub source_table: String,
}

impl CdcEvent {
    /// The item's partition key as a plain string — used as the Kafka message
    /// key. Falls back to reading pk from new/old image when `keys` is absent.
    pub fn pk(&self) -> Option<&str> {
        image_str(&self.keys, "pk")
            .or_else(|| self.new_image.as_ref().and_then(|i| image_str(i, "pk")))
            .or_else(|| self.old_image.as_ref().and_then(|i| image_str(i, "pk")))
    }

    /// The item's sort key as a plain string.
    pub fn sk(&self) -> Option<&str> {
        image_str(&self.keys, "sk")
            .or_else(|| self.new_image.as_ref().and_then(|i| image_str(i, "sk")))
            .or_else(|| self.old_image.as_ref().and_then(|i| image_str(i, "sk")))
    }
}

/// Read a string attribute from a DynamoDB image.
pub fn image_str<'a>(image: &'a Image, field: &str) -> Option<&'a str> {
    match image.get(field) {
        Some(serde_dynamo::AttributeValue::S(s)) => Some(s.as_str()),
        _ => None,
    }
}

/// Convert a DynamoDB Streams SDK `AttributeValue` into a
/// `serde_dynamo::AttributeValue`.
///
/// Mirrors `stream_poller::convert_av` (local-dev gated there); this copy is
/// available to every server build so the CDC producer can use it.
pub fn convert_streams_av(
    av: &aws_sdk_dynamodbstreams::types::AttributeValue,
) -> serde_dynamo::AttributeValue {
    use aws_sdk_dynamodbstreams::types::AttributeValue as Sav;
    match av {
        Sav::S(s) => serde_dynamo::AttributeValue::S(s.clone()),
        Sav::N(n) => serde_dynamo::AttributeValue::N(n.clone()),
        Sav::Bool(b) => serde_dynamo::AttributeValue::Bool(*b),
        Sav::Null(b) => serde_dynamo::AttributeValue::Null(*b),
        Sav::M(m) => serde_dynamo::AttributeValue::M(
            m.iter()
                .map(|(k, v)| (k.clone(), convert_streams_av(v)))
                .collect(),
        ),
        Sav::L(l) => {
            serde_dynamo::AttributeValue::L(l.iter().map(convert_streams_av).collect())
        }
        Sav::Ss(ss) => serde_dynamo::AttributeValue::Ss(ss.clone()),
        Sav::Ns(ns) => serde_dynamo::AttributeValue::Ns(ns.clone()),
        _ => serde_dynamo::AttributeValue::Null(true),
    }
}

/// Convert an optional Streams SDK image map into our `Image`.
pub fn convert_streams_image(
    image: Option<&HashMap<String, aws_sdk_dynamodbstreams::types::AttributeValue>>,
) -> Option<Image> {
    image.map(|img| {
        img.iter()
            .map(|(k, v)| (k.clone(), convert_streams_av(v)))
            .collect()
    })
}

/// Build a `CdcEvent` from a DynamoDB Streams record.
pub fn cdc_event_from_stream_record(
    record: &aws_sdk_dynamodbstreams::types::Record,
    source_table: &str,
) -> CdcEvent {
    let dynamodb = record.dynamodb();
    CdcEvent {
        schema_version: CDC_SCHEMA_VERSION,
        event_name: record
            .event_name()
            .map(|e| e.as_str().to_string())
            .unwrap_or_else(|| "UNKNOWN".to_string()),
        keys: dynamodb
            .and_then(|d| convert_streams_image(d.keys()))
            .unwrap_or_default(),
        new_image: dynamodb.and_then(|d| convert_streams_image(d.new_image())),
        old_image: dynamodb.and_then(|d| convert_streams_image(d.old_image())),
        approximate_creation_ms: dynamodb
            .and_then(|d| d.approximate_creation_date_time())
            .map(|t| t.to_millis().unwrap_or_default()),
        sequence_number: dynamodb.and_then(|d| d.sequence_number().map(str::to_string)),
        source_table: source_table.to_string(),
    }
}
