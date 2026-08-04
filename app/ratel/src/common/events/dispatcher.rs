//! Unified event dispatcher.
//!
//! Single source of truth for "which DynamoDB change triggers which handler",
//! replacing three divergent copies:
//! 1. EventBridge Pipe filters (`cdk/lib/dynamo-stream-event.ts`, 39 pipes)
//! 2. `EventBridgeEnvelope::proc()` match arms (lambda path)
//! 3. `stream_handler.rs` sk-prefix dispatch (local-dev poller path)
//!
//! Filter spec: `docs/k3s-migration/03-filter-matrix.md` — every pipe's
//! sk-prefix + eventName + field conditions (status/visibility/state/
//! publish_state, OldImage comparisons) must be represented here 1:1.
//!
//! Consumed by:
//! - `ratel_worker` (Redpanda consumer) with the role's `RoleSet`
//! - `stream_handler::handle_stream_record` (local-dev poller) with
//!   `RoleSet::all()` (including `Sse`)

use super::cdc_event::{image_str, CdcEvent, Image};

/// Coarse routing class for worker deployments, mirroring the three Lambda
/// targets of the EventBridge architecture plus the SSE-only local class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EventClass {
    /// Timeline, popular, notifications, XP, essence, vector indexing,
    /// space lifecycle, character XP, activity score. (default Lambda)
    Default,
    /// AnalyzeReportInProgress / AnalyzeDiscussionInProgress. (analyze Lambda)
    Analyze,
    /// Cross-posting syndication + FCM push fanout. (non-VPC egress Lambda)
    Egress,
    /// FactFoldChat SSE hub fan-out — must run inside the API process that
    /// holds the SSE connections; excluded from worker roles.
    Sse,
}

/// Which event classes this process handles.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RoleSet {
    pub default_: bool,
    pub analyze: bool,
    pub egress: bool,
    pub sse: bool,
}

impl RoleSet {
    /// Everything, including SSE — used by the in-process local-dev poller.
    pub fn all() -> Self {
        Self { default_: true, analyze: true, egress: true, sse: true }
    }

    /// Parse a worker role string (`default` | `analyze` | `egress` | `all`).
    /// Worker `all` deliberately excludes `Sse` (no SSE hub in a worker).
    pub fn from_worker_role(role: &str) -> Self {
        match role {
            "default" => Self { default_: true, analyze: false, egress: false, sse: false },
            "analyze" => Self { default_: false, analyze: true, egress: false, sse: false },
            "egress" => Self { default_: false, analyze: false, egress: true, sse: false },
            _ => Self { default_: true, analyze: true, egress: true, sse: false },
        }
    }

    pub fn contains(&self, class: EventClass) -> bool {
        match class {
            EventClass::Default => self.default_,
            EventClass::Analyze => self.analyze,
            EventClass::Egress => self.egress,
            EventClass::Sse => self.sse,
        }
    }
}

/// Outcome of dispatching one `CdcEvent`.
#[derive(Debug, Default)]
pub struct DispatchSummary {
    /// Rule names that matched the event's filters (within the RoleSet).
    pub matched: Vec<&'static str>,
    /// Rule names whose handler returned an error. Empty ⇒ success.
    pub failed: Vec<&'static str>,
}

impl DispatchSummary {
    pub fn is_ok(&self) -> bool {
        self.failed.is_empty()
    }
}

// ─── Rule table ──────────────────────────────────────────────────────────
//
// One rule per detailType (pipes feeding the same detailType are merged with
// OR'd sk conditions — see the filter matrix doc for the pipe↔rule mapping).
// Filters are pure functions over the CdcEvent so they can be evaluated
// without running handlers (`matched_rules`).

struct Rule {
    /// The EventBridge detailType this rule replaces (or the local-only name
    /// for rules without a prod pipe).
    name: &'static str,
    class: EventClass,
    filter: fn(&CdcEvent) -> bool,
}

fn new_str<'a>(e: &'a CdcEvent, field: &str) -> Option<&'a str> {
    e.new_image.as_ref().and_then(|i| image_str(i, field))
}

fn old_str<'a>(e: &'a CdcEvent, field: &str) -> Option<&'a str> {
    e.old_image.as_ref().and_then(|i| image_str(i, field))
}

fn new_sk_is(e: &CdcEvent, sk: &str) -> bool {
    new_str(e, "sk") == Some(sk)
}

fn new_sk_starts(e: &CdcEvent, prefix: &str) -> bool {
    new_str(e, "sk").is_some_and(|s| s.starts_with(prefix))
}

fn old_sk_is(e: &CdcEvent, sk: &str) -> bool {
    old_str(e, "sk") == Some(sk)
}

fn old_sk_starts(e: &CdcEvent, prefix: &str) -> bool {
    old_str(e, "sk").is_some_and(|s| s.starts_with(prefix))
}

/// EventBridge `{ N: [{ exists: true }] }` — the field exists as a Number.
fn new_num_exists(e: &CdcEvent, field: &str) -> bool {
    matches!(
        e.new_image.as_ref().and_then(|i| i.get(field)),
        Some(serde_dynamo::AttributeValue::N(_))
    )
}

/// EventBridge `{ S: [{ exists: true }] }` — the field exists as a String.
fn new_s_exists(e: &CdcEvent, field: &str) -> bool {
    matches!(
        e.new_image.as_ref().and_then(|i| i.get(field)),
        Some(serde_dynamo::AttributeValue::S(_))
    )
}

fn is_insert(e: &CdcEvent) -> bool {
    e.event_name == "INSERT"
}

fn is_modify(e: &CdcEvent) -> bool {
    e.event_name == "MODIFY"
}

fn is_upsert(e: &CdcEvent) -> bool {
    is_insert(e) || is_modify(e)
}

fn is_remove(e: &CdcEvent) -> bool {
    e.event_name == "REMOVE"
}

static RULES: &[Rule] = &[
    Rule {
        name: "TimelineUpdate",
        class: EventClass::Default,
        filter: |e| {
            is_modify(e) && new_sk_starts(e, "POST") && new_str(e, "status") == Some("PUBLISHED")
        },
    },
    Rule {
        name: "PostVectorIndex",
        class: EventClass::Default,
        filter: |e| {
            is_upsert(e) && new_sk_starts(e, "POST") && new_str(e, "status") == Some("PUBLISHED")
        },
    },
    Rule {
        name: "PostVectorDelete",
        class: EventClass::Default,
        filter: |e| {
            is_remove(e) && old_sk_starts(e, "POST") && old_str(e, "status") == Some("PUBLISHED")
        },
    },
    Rule {
        name: "PopularPostUpdate",
        class: EventClass::Default,
        filter: |e| {
            is_modify(e)
                && new_sk_starts(e, "POST")
                && new_str(e, "status") == Some("PUBLISHED")
                && new_num_exists(e, "likes")
        },
    },
    Rule {
        name: "PopularSpaceUpdate",
        class: EventClass::Default,
        filter: |e| {
            is_modify(e) && new_sk_is(e, "SPACE_COMMON") && new_num_exists(e, "participants")
        },
    },
    Rule {
        name: "AiModeratorReplyCheck",
        class: EventClass::Default,
        filter: |e| {
            is_modify(e) && new_sk_starts(e, "SPACE_POST#") && new_num_exists(e, "comments")
        },
    },
    Rule {
        name: "AiModeratorReplyIndex",
        class: EventClass::Default,
        filter: |e| {
            is_insert(e) && new_sk_starts(e, "SPACE_POST_COMMENT#") && new_s_exists(e, "content")
        },
    },
    Rule {
        name: "NotificationSend",
        class: EventClass::Default,
        filter: |e| {
            is_insert(e)
                && new_sk_starts(e, "NOTIFICATION#")
                && new_str(e, "pk").is_some_and(|s| s.starts_with("NOTIFICATION#"))
        },
    },
    Rule {
        name: "InboxPushFanout",
        class: EventClass::Egress,
        filter: |e| is_insert(e) && new_sk_starts(e, "USER_INBOX_NOTIFICATION#"),
    },
    Rule {
        name: "ActivityScoreAggregate",
        class: EventClass::Default,
        filter: |e| is_insert(e) && new_sk_starts(e, "SPACE_ACTIVITY#"),
    },
    Rule {
        name: "SpaceStatusChangeEvent",
        class: EventClass::Default,
        filter: |e| is_insert(e) && new_sk_starts(e, "SPACE_STATUS_CHANGE_EVENT#"),
    },
    Rule {
        name: "SpaceActionStatusChange",
        class: EventClass::Default,
        filter: |e| {
            is_modify(e)
                && new_sk_is(e, "SPACE_ACTION")
                && new_str(e, "status") == Some("Ongoing")
                && old_str(e, "status") == Some("Designing")
        },
    },
    Rule {
        name: "PollXpRecord",
        class: EventClass::Default,
        filter: |e| is_insert(e) && new_sk_starts(e, "SPACE_POLL_USER_ANSWER#"),
    },
    Rule {
        name: "QuizXpRecord",
        class: EventClass::Default,
        filter: |e| is_insert(e) && new_sk_starts(e, "SPACE_QUIZ_ATTEMPT#"),
    },
    // Merges DiscussionCommentXpPipe + DiscussionReplyXpPipe.
    Rule {
        name: "DiscussionXpRecord",
        class: EventClass::Default,
        filter: |e| {
            is_insert(e)
                && (new_sk_starts(e, "SPACE_POST_COMMENT#")
                    || new_sk_starts(e, "SPACE_POST_COMMENT_REPLY#"))
        },
    },
    Rule {
        name: "FollowXpRecord",
        class: EventClass::Default,
        filter: |e| is_insert(e) && new_sk_starts(e, "FOLLOWER#"),
    },
    Rule {
        name: "EssenceIndexPost",
        class: EventClass::Default,
        filter: |e| is_upsert(e) && new_sk_is(e, "POST"),
    },
    Rule {
        name: "EssenceDeletePost",
        class: EventClass::Default,
        filter: |e| is_remove(e) && old_sk_is(e, "POST"),
    },
    // Merges EssenceIndexPostCommentPipe + EssenceIndexPostCommentReplyPipe.
    Rule {
        name: "EssenceIndexPostComment",
        class: EventClass::Default,
        filter: |e| {
            is_upsert(e)
                && (new_sk_starts(e, "POST_COMMENT#") || new_sk_starts(e, "POST_COMMENT_REPLY#"))
        },
    },
    // Merges EssenceDeletePostCommentPipe + EssenceDeletePostCommentReplyPipe.
    Rule {
        name: "EssenceDeletePostComment",
        class: EventClass::Default,
        filter: |e| {
            is_remove(e)
                && (old_sk_starts(e, "POST_COMMENT#") || old_sk_starts(e, "POST_COMMENT_REPLY#"))
        },
    },
    // Merges EssenceIndexDiscussionCommentPipe + ...ReplyPipe.
    Rule {
        name: "EssenceIndexDiscussionComment",
        class: EventClass::Default,
        filter: |e| {
            is_upsert(e)
                && (new_sk_starts(e, "SPACE_POST_COMMENT#")
                    || new_sk_starts(e, "SPACE_POST_COMMENT_REPLY#"))
        },
    },
    // Merges EssenceDeleteDiscussionCommentPipe + ...ReplyPipe.
    Rule {
        name: "EssenceDeleteDiscussionComment",
        class: EventClass::Default,
        filter: |e| {
            is_remove(e)
                && (old_sk_starts(e, "SPACE_POST_COMMENT#")
                    || old_sk_starts(e, "SPACE_POST_COMMENT_REPLY#"))
        },
    },
    Rule {
        name: "EssenceIndexPoll",
        class: EventClass::Default,
        filter: |e| is_upsert(e) && new_sk_starts(e, "SPACE_POLL#"),
    },
    Rule {
        name: "EssenceDeletePoll",
        class: EventClass::Default,
        filter: |e| is_remove(e) && old_sk_starts(e, "SPACE_POLL#"),
    },
    Rule {
        name: "EssenceIndexQuiz",
        class: EventClass::Default,
        filter: |e| is_upsert(e) && new_sk_starts(e, "SPACE_QUIZ#"),
    },
    Rule {
        name: "EssenceDeleteQuiz",
        class: EventClass::Default,
        filter: |e| is_remove(e) && old_sk_starts(e, "SPACE_QUIZ#"),
    },
    Rule {
        name: "EssenceActionMetadataUpdate",
        class: EventClass::Default,
        filter: |e| is_upsert(e) && new_sk_is(e, "SPACE_ACTION"),
    },
    // Prod pipe is MODIFY-only; the INSERT case is an intentional addition
    // for direct announcements that write `status: Published` in a single
    // Put (filter matrix row 41, stream_handler.rs:213 parity).
    Rule {
        name: "SubTeamAnnouncementPublished",
        class: EventClass::Default,
        filter: |e| {
            is_upsert(e)
                && new_sk_starts(e, "SUB_TEAM_ANNOUNCEMENT#")
                && new_str(e, "status") == Some("Published")
        },
    },
    Rule {
        name: "SpacePublished",
        class: EventClass::Default,
        // `SpacePublishState` derives DynamoEnum → UpperSnake "PUBLISHED",
        // not the Rust variant name "Published".
        filter: |e| {
            is_modify(e)
                && new_sk_is(e, "SPACE_COMMON")
                && new_str(e, "publish_state") == Some("PUBLISHED")
        },
    },
    Rule {
        name: "AnalyzeReportInProgress",
        class: EventClass::Analyze,
        filter: |e| {
            is_insert(e)
                && new_sk_starts(e, "SPACE_ANALYZE_REPORT#")
                && new_str(e, "status") == Some("in_progress")
        },
    },
    Rule {
        name: "AnalyzeDiscussionInProgress",
        class: EventClass::Analyze,
        filter: |e| is_insert(e) && new_sk_starts(e, "SPACE_ANALYZE_DISCUSSION_RESULT#"),
    },
    Rule {
        name: "CharacterXpDelta",
        class: EventClass::Default,
        // SpaceScore is a unit EntityType variant — bare "SPACE_SCORE", no '#'.
        filter: |e| is_upsert(e) && new_sk_is(e, "SPACE_SCORE"),
    },
    Rule {
        name: "PostPublishedForSyndication",
        class: EventClass::Egress,
        // EventBridge `anything-but` requires the field to exist with a
        // non-matching value, so a missing OldImage.status does NOT match.
        filter: |e| {
            is_modify(e)
                && new_sk_starts(e, "POST")
                && new_str(e, "status") == Some("PUBLISHED")
                && new_str(e, "visibility") == Some("PUBLIC")
                && old_str(e, "status").is_some_and(|s| s != "PUBLISHED")
        },
    },
    Rule {
        name: "SyndicationJobReady",
        class: EventClass::Egress,
        filter: |e| {
            is_upsert(e)
                && new_sk_starts(e, "SYNDICATION_JOB#")
                && new_str(e, "state") == Some("pending")
        },
    },
    // Local/API-process only — no prod pipe (filter matrix row 40).
    Rule {
        name: "FactFoldChat",
        class: EventClass::Sse,
        filter: |e| is_insert(e) && new_sk_starts(e, "FACT_FOLD_CHAT#"),
    },
];

/// Dry-run filter evaluation: which rule names would fire for `event` under
/// `roles`. `dispatch` uses this same path before running handlers.
pub fn matched_rules(event: &CdcEvent, roles: RoleSet) -> Vec<&'static str> {
    RULES
        .iter()
        .filter(|r| roles.contains(r.class) && (r.filter)(event))
        .map(|r| r.name)
        .collect()
}

/// Evaluate every rule's filter against `event` and run the matching handlers
/// whose class is contained in `roles`.
///
/// Handler errors are logged and recorded in `DispatchSummary::failed`; one
/// failing rule does not prevent the others from running (same isolation as
/// independent EventBridge rules).
pub async fn dispatch(event: &CdcEvent, roles: RoleSet) -> DispatchSummary {
    let matched = matched_rules(event, roles);
    let mut summary = DispatchSummary { matched: matched.clone(), failed: Vec::new() };

    for name in matched {
        if let Err(e) = run_rule(name, event).await {
            tracing::error!(rule = name, error = %e, "dispatch: rule handler failed");
            summary.failed.push(name);
        }
    }

    summary
}

// ─── Handler wiring ──────────────────────────────────────────────────────
//
// Mirrors `EventBridgeEnvelope::proc()` arm by arm, including the
// `fanout_hot_space` / `fanout_if_some` post-processing. REMOVE-based rules
// deserialize `old_image`; everything else `new_image`.

fn deserialize<T: serde::de::DeserializeOwned>(image: Option<&Image>) -> crate::common::Result<T> {
    use crate::common::utils::InfraError;
    use crate::common::Error;

    let image = image.ok_or(Error::from(InfraError::StreamMissingImage))?;
    serde_dynamo::from_item(image.clone()).map_err(|e| {
        tracing::error!("dispatch deserialize: {e}");
        Error::from(InfraError::StreamDeserializeFailed)
    })
}

async fn run_rule(name: &'static str, event: &CdcEvent) -> crate::common::Result<()> {
    let new = event.new_image.as_ref();
    let old = event.old_image.as_ref();

    match name {
        "TimelineUpdate" => {
            crate::features::timeline::services::fan_out_timeline_entries(deserialize(new)?).await
        }
        "PopularPostUpdate" => {
            crate::features::timeline::services::fan_out_popular_post(deserialize(new)?).await
        }
        "PopularSpaceUpdate" => {
            let space: crate::common::models::space::SpaceCommon = deserialize(new)?;
            let space_pk = space.pk.clone();
            let r = crate::features::timeline::services::fan_out_popular_space(space).await;
            fanout_hot_space(&space_pk).await;
            r
        }
        "NotificationSend" => {
            let notification: crate::common::models::notification::Notification =
                deserialize(new)?;
            notification.process().await
        }
        "PostVectorIndex" => {
            let post: crate::features::posts::models::Post = deserialize(new)?;
            crate::features::rag::qdrant::indexers::post_indexer::index_post(post).await
        }
        "PostVectorDelete" => {
            let post: crate::features::posts::models::Post = deserialize(old)?;
            crate::features::rag::qdrant::indexers::post_indexer::delete_post_index(post).await
        }
        "AiModeratorReplyCheck" => {
            let post: crate::features::spaces::pages::actions::actions::discussion::SpacePost =
                deserialize(new)?;
            crate::features::ai_moderator::services::event_handler::handle_ai_moderator_event(post)
                .await
        }
        "AiModeratorReplyIndex" => {
            let comment: crate::features::spaces::pages::actions::actions::discussion::SpacePostComment =
                deserialize(new)?;
            crate::features::rag::qdrant::indexers::reply_indexer::index_reply(comment).await
        }
        "ActivityScoreAggregate" => {
            let activity: crate::features::activity::models::SpaceActivity = deserialize(new)?;
            let space_pk = activity.space_pk.clone();
            let r = crate::features::activity::services::aggregate_score(activity).await;
            fanout_hot_space(&space_pk).await;
            r
        }
        "SpaceStatusChangeEvent" => {
            let event: crate::common::models::space::SpaceStatusChangeEvent = deserialize(new)?;
            let space_pk = event.space_pk.clone();
            let r = crate::features::spaces::space_common::services::handle_space_status_change(
                event,
            )
            .await;
            fanout_hot_space(&space_pk).await;
            r
        }
        "SpaceActionStatusChange" => {
            let action: crate::features::spaces::pages::actions::models::SpaceAction =
                deserialize(new)?;
            let space_pk = action.space_pk.clone();
            let r =
                crate::features::spaces::pages::actions::services::notify_action_ongoing(action)
                    .await;
            fanout_hot_space(&space_pk).await;
            r
        }
        "PollXpRecord" => {
            let answer: crate::features::spaces::pages::actions::actions::poll::SpacePollUserAnswer =
                deserialize(new)?;
            let space_pk = space_pk_from_id_str(answer.space_id.as_deref());
            let r = crate::features::activity::services::handle_poll_xp(answer).await;
            fanout_if_some(space_pk.as_ref()).await;
            r
        }
        "QuizXpRecord" => {
            let attempt: crate::features::spaces::pages::actions::actions::quiz::SpaceQuizAttempt =
                deserialize(new)?;
            let space_pk = space_pk_from_id_str(attempt.space_id.as_deref());
            let r = crate::features::activity::services::handle_quiz_xp(attempt).await;
            fanout_if_some(space_pk.as_ref()).await;
            r
        }
        "DiscussionXpRecord" => {
            let comment: crate::features::spaces::pages::actions::actions::discussion::SpacePostComment =
                deserialize(new)?;
            let space_pk = comment.space_pk.clone();
            let r = crate::features::activity::services::handle_discussion_xp(comment).await;
            fanout_if_some(space_pk.as_ref()).await;
            r
        }
        "FollowXpRecord" => {
            let follow: crate::common::models::auth::UserFollow = deserialize(new)?;
            let space_pk = space_pk_from_id_str(follow.space_id.as_deref());
            let r = crate::features::activity::services::handle_follow_xp(follow).await;
            fanout_if_some(space_pk.as_ref()).await;
            r
        }
        "EssenceIndexPost" => {
            let post: crate::features::posts::models::Post = deserialize(new)?;
            let cfg = crate::common::CommonConfig::default();
            let cli = cfg.dynamodb();
            crate::features::essence::services::index_post(cli, &post).await
        }
        "EssenceIndexPostComment" => {
            let comment: crate::features::posts::models::PostComment = deserialize(new)?;
            let cfg = crate::common::CommonConfig::default();
            let cli = cfg.dynamodb();
            crate::features::essence::services::index_post_comment(cli, &comment).await
        }
        "EssenceIndexDiscussionComment" => {
            let comment: crate::features::spaces::pages::actions::actions::discussion::SpacePostComment =
                deserialize(new)?;
            let space_pk = comment.space_pk.clone();
            let cfg = crate::common::CommonConfig::default();
            let cli = cfg.dynamodb();
            let r =
                crate::features::essence::services::index_discussion_comment(cli, &comment).await;
            fanout_if_some(space_pk.as_ref()).await;
            r
        }
        "EssenceIndexPoll" => {
            let poll: crate::features::spaces::pages::actions::actions::poll::SpacePoll =
                deserialize(new)?;
            let space_pk = poll.pk.clone();
            let cfg = crate::common::CommonConfig::default();
            let cli = cfg.dynamodb();
            let r = crate::features::essence::services::index_poll(cli, &poll).await;
            fanout_hot_space(&space_pk).await;
            r
        }
        "EssenceIndexQuiz" => {
            let quiz: crate::features::spaces::pages::actions::actions::quiz::SpaceQuiz =
                deserialize(new)?;
            let space_pk = quiz.pk.clone();
            let cfg = crate::common::CommonConfig::default();
            let cli = cfg.dynamodb();
            let r = crate::features::essence::services::index_quiz(cli, &quiz).await;
            fanout_hot_space(&space_pk).await;
            r
        }
        "EssenceActionMetadataUpdate" => {
            use crate::features::spaces::pages::actions::actions::quiz::SpaceQuiz;
            use crate::features::spaces::pages::actions::models::SpaceAction;
            use crate::features::spaces::pages::actions::types::SpaceActionType;
            let action: SpaceAction = deserialize(new)?;
            let space_pk: crate::common::types::Partition = action.pk.0.clone().into();
            let r = if matches!(action.space_action_type, SpaceActionType::Quiz) {
                let cfg = crate::common::CommonConfig::default();
                let cli = cfg.dynamodb();
                let quiz_sk = crate::common::types::EntityType::SpaceQuiz(action.pk.1.clone());
                match SpaceQuiz::get(cli, &space_pk, Some(quiz_sk)).await {
                    Ok(Some(quiz)) => {
                        crate::features::essence::services::index_quiz(cli, &quiz).await
                    }
                    Ok(None) => Ok(()),
                    Err(e) => Err(e),
                }
            } else {
                Ok(())
            };
            fanout_hot_space(&space_pk).await;
            r
        }
        "EssenceDeletePost" => {
            let post: crate::features::posts::models::Post = deserialize(old)?;
            let cfg = crate::common::CommonConfig::default();
            let cli = cfg.dynamodb();
            crate::features::essence::services::detach_post(cli, &post).await
        }
        "EssenceDeletePostComment" => {
            let comment: crate::features::posts::models::PostComment = deserialize(old)?;
            let cfg = crate::common::CommonConfig::default();
            let cli = cfg.dynamodb();
            crate::features::essence::services::detach_post_comment(cli, &comment).await
        }
        "EssenceDeleteDiscussionComment" => {
            let comment: crate::features::spaces::pages::actions::actions::discussion::SpacePostComment =
                deserialize(old)?;
            let space_pk = comment.space_pk.clone();
            let cfg = crate::common::CommonConfig::default();
            let cli = cfg.dynamodb();
            let r =
                crate::features::essence::services::detach_discussion_comment(cli, &comment).await;
            fanout_if_some(space_pk.as_ref()).await;
            r
        }
        "EssenceDeletePoll" => {
            let poll: crate::features::spaces::pages::actions::actions::poll::SpacePoll =
                deserialize(old)?;
            let space_pk = poll.pk.clone();
            let cfg = crate::common::CommonConfig::default();
            let cli = cfg.dynamodb();
            let r = crate::features::essence::services::detach_poll(cli, &poll).await;
            fanout_hot_space(&space_pk).await;
            r
        }
        "EssenceDeleteQuiz" => {
            let quiz: crate::features::spaces::pages::actions::actions::quiz::SpaceQuiz =
                deserialize(old)?;
            let space_pk = quiz.pk.clone();
            let cfg = crate::common::CommonConfig::default();
            let cli = cfg.dynamodb();
            let r = crate::features::essence::services::detach_quiz(cli, &quiz).await;
            fanout_hot_space(&space_pk).await;
            r
        }
        "SubTeamAnnouncementPublished" => {
            let announcement: crate::features::sub_team::models::SubTeamAnnouncement =
                deserialize(new)?;
            let cfg = crate::common::CommonConfig::default();
            let cli = cfg.dynamodb();
            crate::features::sub_team::services::announcement_fanout::handle_announcement_published(
                cli,
                announcement,
            )
            .await
        }
        "SpacePublished" => {
            let space: crate::common::models::space::SpaceCommon = deserialize(new)?;
            let cfg = crate::common::CommonConfig::default();
            let cli = cfg.dynamodb();
            crate::features::sub_team::services::announcement_fanout::handle_space_published(
                cli, space,
            )
            .await
        }
        "AnalyzeReportInProgress" => {
            let report: crate::features::spaces::pages::apps::apps::analyzes::SpaceAnalyzeReport =
                deserialize(new)?;
            let cfg = crate::common::CommonConfig::default();
            let cli = cfg.dynamodb();
            crate::features::spaces::pages::apps::apps::analyzes::services::auto_analysis::process_analyze_report(cli, &report).await
        }
        "AnalyzeDiscussionInProgress" => {
            let row: crate::features::spaces::pages::apps::apps::analyzes::SpaceAnalyzeDiscussionResult =
                deserialize(new)?;
            let cfg = crate::common::CommonConfig::default();
            let cli = cfg.dynamodb();
            crate::features::spaces::pages::apps::apps::analyzes::services::discussion_analysis::process_discussion_analysis(cli, &row).await
        }
        "CharacterXpDelta" => {
            let score: crate::features::activity::models::SpaceScore = deserialize(new)?;
            let cfg = crate::common::CommonConfig::default();
            let cli = cfg.dynamodb();
            crate::features::character::services::apply_character_xp_delta(cli, score).await
        }
        "PostPublishedForSyndication" => {
            let post: crate::features::posts::models::Post = deserialize(new)?;
            crate::features::cross_posting::services::factory::handle_post_published_for_syndication(post).await
        }
        "SyndicationJobReady" => {
            let job: crate::features::cross_posting::models::SyndicationJob = deserialize(new)?;
            crate::features::cross_posting::services::dispatcher::handle_syndication_job_ready(job)
                .await
        }
        "InboxPushFanout" => {
            let notification: crate::common::models::notification::UserInboxNotification =
                deserialize(new)?;
            // Best-effort: `fan_out_push` logs and swallows its own errors
            // (the inbox row is already written), so it returns `()`.
            crate::features::notifications::services::fan_out_push(notification).await;
            Ok(())
        }
        "FactFoldChat" => handle_fact_fold_chat_inserted(deserialize(new)?).await,
        _ => Ok(()),
    }
}

/// Re-snapshot HotSpace (and per-viewer rows) for `space_pk`. Best-effort —
/// `services::space_fanout::upsert_hot_space` swallows its own errors so a
/// fanout miss never blocks the rule handler that triggered us. Same
/// post-processing `EventBridgeEnvelope::proc()` runs on the lambda path.
async fn fanout_hot_space(space_pk: &crate::common::types::Partition) {
    let cfg = crate::common::CommonConfig::default();
    let cli = cfg.dynamodb();
    crate::features::spaces::space_common::services::upsert_hot_space(cli, space_pk).await;
}

async fn fanout_if_some(space_pk: Option<&crate::common::types::Partition>) {
    if let Some(pk) = space_pk {
        fanout_hot_space(pk).await;
    }
}

/// XP records carry the space id as a bare string (not a `Partition`); rebuild
/// the `Partition::Space` form for fanout. Empty/None inputs return None so
/// callers can skip cleanly.
fn space_pk_from_id_str(space_id: Option<&str>) -> Option<crate::common::types::Partition> {
    space_id
        .filter(|s| !s.is_empty())
        .map(|s| crate::common::types::Partition::Space(s.to_string()))
}

/// FactFoldChat INSERT fan-out helper. Pulls the round id out of the row's pk
/// (`FACT_FOLD#{round_id}`) and publishes a `chat_message` event to the
/// matching `fof.chat:{round_id}` channel on the per-process hub. Every API
/// process consumes this same event and publishes to its own hub, so all SSE
/// subscribers get the message regardless of which process holds their
/// connection.
async fn handle_fact_fold_chat_inserted(
    row: crate::features::arcade::games::fact_or_fold::models::FactFoldChatMessage,
) -> crate::common::Result<()> {
    use crate::features::arcade::games::fact_or_fold::realtime::chat_payload_from;
    use crate::features::arcade::games::fact_or_fold::types::ChatMessagePayload;
    use crate::features::arcade::realtime::channel::ChannelId;
    use crate::features::arcade::realtime::hub::global_hub;

    let round_id = row
        .pk
        .to_string()
        .strip_prefix("FACT_FOLD#")
        .unwrap_or(&row.pk.to_string())
        .to_string();
    let channel = ChannelId(format!("fof.chat:{round_id}"));

    let payload: ChatMessagePayload = chat_payload_from(row);
    let payload = serde_json::to_value(payload).map_err(|e| {
        crate::error!("FactFoldChat fan-out serialize failed: {e}");
        crate::features::arcade::ArcadeError::ChannelPayloadInvalid
    })?;

    let _id = global_hub()
        .publish(&channel, "chat_message", payload)
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_dynamo::AttributeValue as AV;

    fn s(v: &str) -> AV {
        AV::S(v.to_string())
    }

    fn n(v: &str) -> AV {
        AV::N(v.to_string())
    }

    fn image(fields: &[(&str, AV)]) -> Image {
        fields
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect()
    }

    fn event(name: &str, new: Option<Image>, old: Option<Image>) -> CdcEvent {
        CdcEvent {
            schema_version: super::super::cdc_event::CDC_SCHEMA_VERSION,
            event_name: name.to_string(),
            keys: Image::new(),
            new_image: new,
            old_image: old,
            approximate_creation_ms: None,
            sequence_number: None,
            source_table: "test".to_string(),
        }
    }

    fn names(e: &CdcEvent) -> Vec<&'static str> {
        matched_rules(e, RoleSet::all())
    }

    #[test]
    fn timeline_update_matches_published_modify_only() {
        let published = event(
            "MODIFY",
            Some(image(&[("sk", s("POST")), ("status", s("PUBLISHED"))])),
            None,
        );
        assert!(names(&published).contains(&"TimelineUpdate"));
        assert!(names(&published).contains(&"PostVectorIndex"));
        assert!(names(&published).contains(&"EssenceIndexPost"));

        let draft = event(
            "MODIFY",
            Some(image(&[("sk", s("POST")), ("status", s("DRAFT"))])),
            None,
        );
        assert!(!names(&draft).contains(&"TimelineUpdate"));
        assert!(!names(&draft).contains(&"PostVectorIndex"));

        let insert = event(
            "INSERT",
            Some(image(&[("sk", s("POST")), ("status", s("PUBLISHED"))])),
            None,
        );
        assert!(!names(&insert).contains(&"TimelineUpdate"));
        assert!(names(&insert).contains(&"PostVectorIndex"));
    }

    #[test]
    fn popular_post_requires_likes_number() {
        let with_likes = event(
            "MODIFY",
            Some(image(&[
                ("sk", s("POST")),
                ("status", s("PUBLISHED")),
                ("likes", n("3")),
            ])),
            None,
        );
        assert!(names(&with_likes).contains(&"PopularPostUpdate"));

        let without_likes = event(
            "MODIFY",
            Some(image(&[("sk", s("POST")), ("status", s("PUBLISHED"))])),
            None,
        );
        assert!(!names(&without_likes).contains(&"PopularPostUpdate"));
    }

    #[test]
    fn post_vector_delete_reads_old_image_status() {
        let published = event(
            "REMOVE",
            None,
            Some(image(&[("sk", s("POST")), ("status", s("PUBLISHED"))])),
        );
        assert!(names(&published).contains(&"PostVectorDelete"));
        assert!(names(&published).contains(&"EssenceDeletePost"));

        let draft = event(
            "REMOVE",
            None,
            Some(image(&[("sk", s("POST")), ("status", s("DRAFT"))])),
        );
        assert!(!names(&draft).contains(&"PostVectorDelete"));
        // Essence delete has no status condition — still fires for drafts.
        assert!(names(&draft).contains(&"EssenceDeletePost"));
    }

    #[test]
    fn space_action_status_change_requires_designing_to_ongoing() {
        let transition = event(
            "MODIFY",
            Some(image(&[("sk", s("SPACE_ACTION")), ("status", s("Ongoing"))])),
            Some(image(&[("status", s("Designing"))])),
        );
        assert!(names(&transition).contains(&"SpaceActionStatusChange"));

        let no_transition = event(
            "MODIFY",
            Some(image(&[("sk", s("SPACE_ACTION")), ("status", s("Ongoing"))])),
            Some(image(&[("status", s("Ongoing"))])),
        );
        assert!(!names(&no_transition).contains(&"SpaceActionStatusChange"));
        // EssenceActionMetadataUpdate has no status condition — still fires.
        assert!(names(&no_transition).contains(&"EssenceActionMetadataUpdate"));
    }

    #[test]
    fn syndication_stage1_requires_draft_to_published_public() {
        let matching = event(
            "MODIFY",
            Some(image(&[
                ("sk", s("POST")),
                ("status", s("PUBLISHED")),
                ("visibility", s("PUBLIC")),
            ])),
            Some(image(&[("status", s("DRAFT"))])),
        );
        assert!(names(&matching).contains(&"PostPublishedForSyndication"));

        let already_published = event(
            "MODIFY",
            Some(image(&[
                ("sk", s("POST")),
                ("status", s("PUBLISHED")),
                ("visibility", s("PUBLIC")),
            ])),
            Some(image(&[("status", s("PUBLISHED"))])),
        );
        assert!(!names(&already_published).contains(&"PostPublishedForSyndication"));

        let private = event(
            "MODIFY",
            Some(image(&[
                ("sk", s("POST")),
                ("status", s("PUBLISHED")),
                ("visibility", s("PRIVATE")),
            ])),
            Some(image(&[("status", s("DRAFT"))])),
        );
        assert!(!names(&private).contains(&"PostPublishedForSyndication"));

        // `anything-but` requires OldImage.status to exist.
        let missing_old_status = event(
            "MODIFY",
            Some(image(&[
                ("sk", s("POST")),
                ("status", s("PUBLISHED")),
                ("visibility", s("PUBLIC")),
            ])),
            Some(image(&[])),
        );
        assert!(!names(&missing_old_status).contains(&"PostPublishedForSyndication"));
    }

    #[test]
    fn syndication_stage2_requires_pending_state() {
        let pending = event(
            "INSERT",
            Some(image(&[
                ("sk", s("SYNDICATION_JOB#abc")),
                ("state", s("pending")),
            ])),
            None,
        );
        assert!(names(&pending).contains(&"SyndicationJobReady"));

        let done = event(
            "MODIFY",
            Some(image(&[
                ("sk", s("SYNDICATION_JOB#abc")),
                ("state", s("done")),
            ])),
            None,
        );
        assert!(!names(&done).contains(&"SyndicationJobReady"));
    }

    #[test]
    fn space_published_requires_upper_snake_publish_state() {
        let published = event(
            "MODIFY",
            Some(image(&[
                ("sk", s("SPACE_COMMON")),
                ("publish_state", s("PUBLISHED")),
            ])),
            None,
        );
        assert!(names(&published).contains(&"SpacePublished"));

        let draft = event(
            "MODIFY",
            Some(image(&[
                ("sk", s("SPACE_COMMON")),
                ("publish_state", s("DRAFT")),
            ])),
            None,
        );
        assert!(!names(&draft).contains(&"SpacePublished"));
    }

    #[test]
    fn analyze_report_requires_in_progress_status() {
        let in_progress = event(
            "INSERT",
            Some(image(&[
                ("sk", s("SPACE_ANALYZE_REPORT#r1")),
                ("status", s("in_progress")),
            ])),
            None,
        );
        assert!(names(&in_progress).contains(&"AnalyzeReportInProgress"));

        let finished = event(
            "INSERT",
            Some(image(&[
                ("sk", s("SPACE_ANALYZE_REPORT#r1")),
                ("status", s("finish")),
            ])),
            None,
        );
        assert!(!names(&finished).contains(&"AnalyzeReportInProgress"));

        // Analyze rules are excluded from the default worker role.
        let default_role = RoleSet::from_worker_role("default");
        assert!(!matched_rules(&in_progress, default_role).contains(&"AnalyzeReportInProgress"));
    }

    #[test]
    fn inbox_push_fanout_matches_insert() {
        let e = event(
            "INSERT",
            Some(image(&[("sk", s("USER_INBOX_NOTIFICATION#n1"))])),
            None,
        );
        assert!(names(&e).contains(&"InboxPushFanout"));
        // Egress class — excluded from the default worker role.
        assert!(
            !matched_rules(&e, RoleSet::from_worker_role("default"))
                .contains(&"InboxPushFanout")
        );
    }

    #[test]
    fn discussion_xp_matches_both_comment_and_reply_prefixes() {
        let comment = event(
            "INSERT",
            Some(image(&[("sk", s("SPACE_POST_COMMENT#c1"))])),
            None,
        );
        assert!(names(&comment).contains(&"DiscussionXpRecord"));
        assert!(names(&comment).contains(&"EssenceIndexDiscussionComment"));

        let reply = event(
            "INSERT",
            Some(image(&[("sk", s("SPACE_POST_COMMENT_REPLY#r1"))])),
            None,
        );
        assert!(names(&reply).contains(&"DiscussionXpRecord"));
        assert!(names(&reply).contains(&"EssenceIndexDiscussionComment"));

        // Like rows match neither prefix (the '#' boundary excludes them).
        let like = event(
            "INSERT",
            Some(image(&[("sk", s("SPACE_POST_COMMENT_LIKE#l1"))])),
            None,
        );
        assert!(!names(&like).contains(&"DiscussionXpRecord"));
        assert!(!names(&like).contains(&"EssenceIndexDiscussionComment"));
    }

    #[test]
    fn essence_poll_index_and_delete() {
        let index = event("MODIFY", Some(image(&[("sk", s("SPACE_POLL#p1"))])), None);
        assert!(names(&index).contains(&"EssenceIndexPoll"));

        let delete = event("REMOVE", None, Some(image(&[("sk", s("SPACE_POLL#p1"))])));
        assert!(names(&delete).contains(&"EssenceDeletePoll"));
        assert!(!names(&delete).contains(&"EssenceIndexPoll"));

        // SPACE_POLL_USER_ANSWER# must not match the SPACE_POLL# prefix.
        let answer = event(
            "INSERT",
            Some(image(&[("sk", s("SPACE_POLL_USER_ANSWER#a1"))])),
            None,
        );
        assert!(!names(&answer).contains(&"EssenceIndexPoll"));
        assert!(names(&answer).contains(&"PollXpRecord"));
    }

    #[test]
    fn character_xp_delta_requires_exact_space_score_sk() {
        let exact = event("INSERT", Some(image(&[("sk", s("SPACE_SCORE"))])), None);
        assert!(names(&exact).contains(&"CharacterXpDelta"));

        let modify = event("MODIFY", Some(image(&[("sk", s("SPACE_SCORE"))])), None);
        assert!(names(&modify).contains(&"CharacterXpDelta"));

        let suffixed = event("INSERT", Some(image(&[("sk", s("SPACE_SCORE#x"))])), None);
        assert!(!names(&suffixed).contains(&"CharacterXpDelta"));
    }

    #[test]
    fn sub_team_announcement_matches_insert_and_modify_published() {
        let modify = event(
            "MODIFY",
            Some(image(&[
                ("sk", s("SUB_TEAM_ANNOUNCEMENT#a1")),
                ("status", s("Published")),
            ])),
            None,
        );
        assert!(names(&modify).contains(&"SubTeamAnnouncementPublished"));

        // Intentional addition beyond prod pipes (filter matrix row 41).
        let insert = event(
            "INSERT",
            Some(image(&[
                ("sk", s("SUB_TEAM_ANNOUNCEMENT#a1")),
                ("status", s("Published")),
            ])),
            None,
        );
        assert!(names(&insert).contains(&"SubTeamAnnouncementPublished"));

        let draft = event(
            "MODIFY",
            Some(image(&[
                ("sk", s("SUB_TEAM_ANNOUNCEMENT#a1")),
                ("status", s("Draft")),
            ])),
            None,
        );
        assert!(!names(&draft).contains(&"SubTeamAnnouncementPublished"));
    }

    #[test]
    fn fact_fold_chat_only_matches_with_sse_role() {
        let e = event(
            "INSERT",
            Some(image(&[("sk", s("FACT_FOLD_CHAT#m1"))])),
            None,
        );
        assert!(matched_rules(&e, RoleSet::all()).contains(&"FactFoldChat"));
        // Worker `all` excludes Sse.
        assert!(!matched_rules(&e, RoleSet::from_worker_role("all")).contains(&"FactFoldChat"));
        assert!(!matched_rules(&e, RoleSet::from_worker_role("default")).contains(&"FactFoldChat"));
    }
}
