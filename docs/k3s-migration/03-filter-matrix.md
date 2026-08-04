# Filter Matrix: CDK Pipes → dispatcher.rs

**작성일**: 2026-08-04 · **브랜치**: `deploy/migrate-into-k3s`
**원본 스펙**: `cdk/lib/dynamo-stream-event.ts` (39 CfnPipes)
**이관 대상**: `app/ratel/src/common/events/dispatcher.rs`

One row per CfnPipe, plus 2 intentional local-parity additions (rows 40–41).
Multiple pipes feeding one detailType collapse into ONE dispatcher rule with an
OR'd sk condition — the "Rule" column shows the dispatcher rule name (always the
detailType). Payload column shows which stream image the handler deserializes
(REMOVE pipes smuggled `OldImage` into `newImage` via `inputTemplate`; the
dispatcher reads `old_image` honestly instead).

`sk=`: exact match · `sk^=`: prefix match · handler paths are relative to
`app/ratel/src/`.

| # | CfnPipe (CDK id) | eventName | sk condition | Extra field conditions | Payload | detailType / Rule | Handler | Class |
|---|---|---|---|---|---|---|---|---|
| 1 | TimelinePipe | MODIFY | New sk^=`POST` | New status=`PUBLISHED` | NewImage | TimelineUpdate | `features/timeline/services::fan_out_timeline_entries` | Default |
| 2 | PostVectorIndexPipe | INSERT, MODIFY | New sk^=`POST` | New status=`PUBLISHED` | NewImage | PostVectorIndex | `features/rag/qdrant/indexers/post_indexer::index_post` | Default |
| 3 | PostVectorDeletePipe | REMOVE | Old sk^=`POST` | Old status=`PUBLISHED` | OldImage | PostVectorDelete | `features/rag/qdrant/indexers/post_indexer::delete_post_index` | Default |
| 4 | PopularPostPipe | MODIFY | New sk^=`POST` | New status=`PUBLISHED`, New likes exists (N) | NewImage | PopularPostUpdate | `features/timeline/services::fan_out_popular_post` | Default |
| 5 | PopularSpacePipe | MODIFY | New sk=`SPACE_COMMON` | New participants exists (N) | NewImage | PopularSpaceUpdate | `features/timeline/services::fan_out_popular_space` + `fanout_hot_space` | Default |
| 6 | AiModeratorPipe | MODIFY | New sk^=`SPACE_POST#` | New comments exists (N) | NewImage | AiModeratorReplyCheck | `features/ai_moderator/services/event_handler::handle_ai_moderator_event` | Default |
| 7 | AiModeratorReplyIndexPipe | INSERT | New sk^=`SPACE_POST_COMMENT#` | New content exists (S) | NewImage | AiModeratorReplyIndex | `features/rag/qdrant/indexers/reply_indexer::index_reply` | Default |
| 8 | NotificationPipe | INSERT | New sk^=`NOTIFICATION#` | New pk^=`NOTIFICATION#` | NewImage | NotificationSend | `common/models/notification::Notification::process` | Default |
| 9 | InboxPushPipe | INSERT | New sk^=`USER_INBOX_NOTIFICATION#` | — | NewImage | InboxPushFanout | `features/notifications/services::fan_out_push` | Egress |
| 10 | ActivityScorePipe | INSERT | New sk^=`SPACE_ACTIVITY#` | — | NewImage | ActivityScoreAggregate | `features/activity/services::aggregate_score` + `fanout_hot_space` | Default |
| 11 | SpaceStatusChangeEventPipe | INSERT | New sk^=`SPACE_STATUS_CHANGE_EVENT#` | — | NewImage | SpaceStatusChangeEvent | `features/spaces/space_common/services::handle_space_status_change` + `fanout_hot_space` | Default |
| 12 | SpaceActionStatusChangePipe | MODIFY | New sk=`SPACE_ACTION` | New status=`Ongoing` AND Old status=`Designing` | NewImage | SpaceActionStatusChange | `features/spaces/pages/actions/services::notify_action_ongoing` + `fanout_hot_space` | Default |
| 13 | PollXpPipe | INSERT | New sk^=`SPACE_POLL_USER_ANSWER#` | — | NewImage | PollXpRecord | `features/activity/services::handle_poll_xp` + `fanout_if_some` | Default |
| 14 | QuizXpPipe | INSERT | New sk^=`SPACE_QUIZ_ATTEMPT#` | — | NewImage | QuizXpRecord | `features/activity/services::handle_quiz_xp` + `fanout_if_some` | Default |
| 15 | DiscussionCommentXpPipe | INSERT | New sk^=`SPACE_POST_COMMENT#` | — | NewImage | DiscussionXpRecord (OR'd with #16) | `features/activity/services::handle_discussion_xp` + `fanout_if_some` | Default |
| 16 | DiscussionReplyXpPipe | INSERT | New sk^=`SPACE_POST_COMMENT_REPLY#` | — | NewImage | DiscussionXpRecord (OR'd with #15) | same as #15 | Default |
| 17 | FollowXpPipe | INSERT | New sk^=`FOLLOWER#` | — | NewImage | FollowXpRecord | `features/activity/services::handle_follow_xp` + `fanout_if_some` | Default |
| 18 | EssenceIndexPostPipe | INSERT, MODIFY | New sk=`POST` | — | NewImage | EssenceIndexPost | `features/essence/services::index_post` | Default |
| 19 | EssenceDeletePostPipe | REMOVE | Old sk=`POST` | — | OldImage | EssenceDeletePost | `features/essence/services::detach_post` | Default |
| 20 | EssenceIndexPostCommentPipe | INSERT, MODIFY | New sk^=`POST_COMMENT#` | — | NewImage | EssenceIndexPostComment (OR'd with #21) | `features/essence/services::index_post_comment` | Default |
| 21 | EssenceIndexPostCommentReplyPipe | INSERT, MODIFY | New sk^=`POST_COMMENT_REPLY#` | — | NewImage | EssenceIndexPostComment (OR'd with #20) | same as #20 | Default |
| 22 | EssenceDeletePostCommentPipe | REMOVE | Old sk^=`POST_COMMENT#` | — | OldImage | EssenceDeletePostComment (OR'd with #23) | `features/essence/services::detach_post_comment` | Default |
| 23 | EssenceDeletePostCommentReplyPipe | REMOVE | Old sk^=`POST_COMMENT_REPLY#` | — | OldImage | EssenceDeletePostComment (OR'd with #22) | same as #22 | Default |
| 24 | EssenceIndexDiscussionCommentPipe | INSERT, MODIFY | New sk^=`SPACE_POST_COMMENT#` | — | NewImage | EssenceIndexDiscussionComment (OR'd with #25) | `features/essence/services::index_discussion_comment` + `fanout_if_some` | Default |
| 25 | EssenceIndexDiscussionCommentReplyPipe | INSERT, MODIFY | New sk^=`SPACE_POST_COMMENT_REPLY#` | — | NewImage | EssenceIndexDiscussionComment (OR'd with #24) | same as #24 | Default |
| 26 | EssenceDeleteDiscussionCommentPipe | REMOVE | Old sk^=`SPACE_POST_COMMENT#` | — | OldImage | EssenceDeleteDiscussionComment (OR'd with #27) | `features/essence/services::detach_discussion_comment` + `fanout_if_some` | Default |
| 27 | EssenceDeleteDiscussionCommentReplyPipe | REMOVE | Old sk^=`SPACE_POST_COMMENT_REPLY#` | — | OldImage | EssenceDeleteDiscussionComment (OR'd with #26) | same as #26 | Default |
| 28 | EssenceIndexPollPipe | INSERT, MODIFY | New sk^=`SPACE_POLL#` | — | NewImage | EssenceIndexPoll | `features/essence/services::index_poll` + `fanout_hot_space` | Default |
| 29 | EssenceDeletePollPipe | REMOVE | Old sk^=`SPACE_POLL#` | — | OldImage | EssenceDeletePoll | `features/essence/services::detach_poll` + `fanout_hot_space` | Default |
| 30 | EssenceIndexQuizPipe | INSERT, MODIFY | New sk^=`SPACE_QUIZ#` | — | NewImage | EssenceIndexQuiz | `features/essence/services::index_quiz` + `fanout_hot_space` | Default |
| 31 | EssenceDeleteQuizPipe | REMOVE | Old sk^=`SPACE_QUIZ#` | — | OldImage | EssenceDeleteQuiz | `features/essence/services::detach_quiz` + `fanout_hot_space` | Default |
| 32 | EssenceActionMetadataPipe | INSERT, MODIFY | New sk=`SPACE_ACTION` | — | NewImage | EssenceActionMetadataUpdate | inline (quiz lookup → `features/essence/services::index_quiz`) + `fanout_hot_space` | Default |
| 33 | SubTeamAnnouncementPublishedPipe | MODIFY | New sk^=`SUB_TEAM_ANNOUNCEMENT#` | New status=`Published` | NewImage | SubTeamAnnouncementPublished (merged with #41) | `features/sub_team/services/announcement_fanout::handle_announcement_published` | Default |
| 34 | SpacePublishedPipe | MODIFY | New sk=`SPACE_COMMON` | New publish_state=`PUBLISHED` (DynamoEnum UpperSnake) | NewImage | SpacePublished | `features/sub_team/services/announcement_fanout::handle_space_published` | Default |
| 35 | AnalyzeReportInProgressPipe | INSERT | New sk^=`SPACE_ANALYZE_REPORT#` | New status=`in_progress` | NewImage | AnalyzeReportInProgress | `features/spaces/pages/apps/apps/analyzes/services/auto_analysis::process_analyze_report` | Analyze |
| 36 | AnalyzeDiscussionInProgressPipe | INSERT | New sk^=`SPACE_ANALYZE_DISCUSSION_RESULT#` | — | NewImage | AnalyzeDiscussionInProgress | `features/spaces/pages/apps/apps/analyzes/services/discussion_analysis::process_discussion_analysis` | Analyze |
| 37 | SpaceScorePipe | INSERT, MODIFY | New sk=`SPACE_SCORE` (bare, no `#`) | — | NewImage | CharacterXpDelta | `features/character/services::apply_character_xp_delta` | Default |
| 38 | CrossPostingStage1Pipe | MODIFY | New sk^=`POST` | New status=`PUBLISHED`, New visibility=`PUBLIC`, Old status anything-but `PUBLISHED` (field must exist) | NewImage | PostPublishedForSyndication | `features/cross_posting/services/factory::handle_post_published_for_syndication` | Egress |
| 39 | CrossPostingStage2Pipe | INSERT, MODIFY | New sk^=`SYNDICATION_JOB#` | New state=`pending` | NewImage | SyndicationJobReady | `features/cross_posting/services/dispatcher::handle_syndication_job_ready` | Egress |
| 40 | *(local-only — no prod pipe)* | INSERT | New sk^=`FACT_FOLD_CHAT#` | — | NewImage | FactFoldChat | in-process SSE hub fan-out (`dispatcher::handle_fact_fold_chat_inserted`) | Sse |
| 41 | *(intentional addition beyond prod pipes)* | INSERT | New sk^=`SUB_TEAM_ANNOUNCEMENT#` | New status=`Published` | NewImage | SubTeamAnnouncementPublished (merged with #33) | same as #33 | Default |

## Notes / decisions

- **39 pipes → 35 dispatcher rules.** Rows 15+16, 20+21, 22+23, 24+25, 26+27
  each collapse into one rule with an OR'd sk-prefix condition; rows 33+41
  collapse into one rule accepting INSERT|MODIFY.
- **Prod/local divergences resolved in favor of PROD filters** (01 doc §1):
  1. `PopularPostUpdate` — now runs locally too (row 4, was missing in
     `stream_handler.rs`).
  2. `AnalyzeReportInProgress` — `status=in_progress` guard now enforced
     locally (row 35, local previously fired on every INSERT).
  3. `fanout_hot_space` post-processing — moved from `proc()` into the
     dispatcher, so local runs it too (rows 5, 10–17, 24–32 per Handler column).
  4. `FACT_FOLD_CHAT#` SSE fan-out — kept as local/API-process-only rule with
     `EventClass::Sse` (row 40); workers never include the Sse role.
  5. `SUB_TEAM_ANNOUNCEMENT#` INSERT-with-status-Published — kept as an
     intentional addition (row 41, matching `stream_handler.rs:213` behavior:
     direct announcements write `status: Published` on INSERT and would never
     hit the MODIFY-only prod pipe).
  6. REMOVE payload smuggling — REMOVE rules (rows 3, 19, 22–23, 26–27, 29,
     31) deserialize `old_image` directly; no `newImage` substitution.
- **`anything-but` semantics** (row 38): EventBridge `anything-but` requires
  the field to *exist* with a non-matching value. Ported as
  `old_image.status exists AND != "PUBLISHED"` — a MODIFY whose OldImage lacks
  `status` does NOT match (stricter than the old local check, faithful to prod).
- **`exists: true` semantics** (rows 4–7): the field must exist *as the
  declared DynamoDB type* (`N` for likes/participants/comments, `S` for
  content).
- **sk prefix `POST`** (rows 1–4, 38) also matches `POST_COMMENT#…` /
  `POST_COMMENT_REPLY#…` sks, exactly as the prod pipe pattern does; the
  status/visibility field conditions are what keep comment rows out in
  practice. Ported 1:1, not "fixed".
- **EventClass assignment** follows 02 doc §1-6: Analyze = rows 35–36,
  Egress = rows 9, 38, 39, Sse = row 40, Default = everything else.
