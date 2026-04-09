# DynamoDB Modeling

## Key Encoding Convention

### Partition (pk)

`UPPER_SNAKE_CASE#<id>`

```text
Partition::Space("abc123")       → "SPACE#abc123"
Partition::User("uuid-1")       → "USER#uuid-1"
Partition::Feed("uuid-1")       → "FEED#uuid-1"
Partition::Team("uuid-1")       → "TEAM#uuid-1"
```

### EntityType (sk)

`UPPER_SNAKE_CASE` for singletons, `UPPER_SNAKE_CASE#<id>` for parameterized variants.

```text
EntityType::SpaceCommon          → "SPACE_COMMON"
EntityType::User                 → "USER"
EntityType::SpaceChapter("ch-1") → "SPACE_CHAPTER#ch-1"
EntityType::SpacePoll("uuid")    → "SPACE_POLL#uuid"
```

### SubPartition (no prefix)

Strips the `UPPER_SNAKE#` prefix — serializes as just the ID.

```text
SpacePartition("abc123")             → "abc123"
FeedPartition("uuid-1")             → "uuid-1"
SpaceChapterEntityType("ch-1")      → "ch-1"
```

Conversion:

```rust
let space_pk: Partition = space_partition.into();       // "abc123" → "SPACE#abc123"
let space_partition: SpacePartition = partition.into();  // "SPACE#abc123" → "abc123"
```

### CompositePartition (`##` join)

```text
CompositePartition(Partition::Space("sid"), EntityType::SpaceDiscussion("did"))
  → "SPACE#sid##SPACE_DISCUSSION#did"

CompositePartition(SpacePartition("sid"), SpaceDiscussionEntityType("did"))
  → "sid##did"

CompositePartition(SpacePartition("sid"), String::from("action-id"))
  → "sid##action-id"
```

## Enum Serialization

| Derive | Format | Example |
|---|---|---|
| `DynamoEnum` | `UPPER_SNAKE_CASE` | `PostStatus::Published` → `"PUBLISHED"` |
| `serde_repr` (`#[repr(u8)]`) | Number (N type) | `PostType::Post` → `1` |
| `Serialize` (serde default) | PascalCase | `SpaceUserRole::Participant` → `"Participant"` |

### DynamoEnum enums (UPPER_SNAKE_CASE)

`PostStatus`, `Visibility`, `SpaceStatus`, `SpaceVisibility`, `SpacePublishState`

### serde_repr enums (numbers)

`PostType` (Post=1), `SpaceType` (Poll=2), `UserType` (Individual=1)

### Standard serde enums (PascalCase)

`SpaceUserRole`, `SpaceActionType`, `ChapterBenefit`, `CreatorRecipient`
