/// Local-dev DynamoDB Stream record handler.
///
/// Thin compatibility wrapper over the unified event dispatcher
/// (`common::events::dispatcher`): builds a `CdcEvent` from the raw stream
/// images and dispatches it with the full `RoleSet` (including `Sse`, since
/// the local poller runs inside the API process that owns the SSE hub).
///
/// All routing/filter logic lives in `dispatcher.rs` — the single source of
/// truth ported 1:1 from the EventBridge Pipe filters
/// (`docs/k3s-migration/03-filter-matrix.md`).
#[cfg(feature = "server")]
pub async fn handle_stream_record(
    event_name: &str,
    new_image: Option<&std::collections::HashMap<String, serde_dynamo::AttributeValue>>,
    old_image: Option<&std::collections::HashMap<String, serde_dynamo::AttributeValue>>,
) -> crate::common::Result<()> {
    use crate::common::events::{dispatch, CdcEvent, Image, RoleSet, CDC_SCHEMA_VERSION};
    use crate::common::utils::InfraError;

    let mut keys = Image::new();
    if let Some(image) = new_image.or(old_image) {
        for key in ["pk", "sk"] {
            if let Some(v) = image.get(key) {
                keys.insert(key.to_string(), v.clone());
            }
        }
    }

    let event = CdcEvent {
        schema_version: CDC_SCHEMA_VERSION,
        event_name: event_name.to_string(),
        keys,
        new_image: new_image.cloned(),
        old_image: old_image.cloned(),
        approximate_creation_ms: None,
        sequence_number: None,
        source_table: "local".to_string(),
    };

    let summary = dispatch(&event, RoleSet::all()).await;
    if summary.is_ok() {
        Ok(())
    } else {
        // Per-rule errors were already logged inside `dispatch`.
        tracing::error!(
            event_name = %event_name,
            failed = ?summary.failed,
            "stream: dispatch had failing rules"
        );
        Err(InfraError::StreamDispatchFailed.into())
    }
}
