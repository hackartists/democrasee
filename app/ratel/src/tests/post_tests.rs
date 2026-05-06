use super::*;

// #[tokio::test]
// async fn test_create_post_by_user() {
//     let TestContext { app, test_user, .. } = TestContext::setup().await;

//     let (status, _headers, body) = crate::test_post! {
//         app: app,
//         path: "/api/posts",
//         headers: test_user.1.clone(),
//     };

//     assert_eq!(status, 200, "create post response: {:?}", body);
// }

#[tokio::test]
async fn test_create_post_without_auth() {
    let TestContext { app, .. } = TestContext::setup().await;

    let (status, _headers, _body) = crate::test_post! {
        app: app,
        path: "/api/posts",
    };

    assert_ne!(status, 200, "unauthenticated request should fail");
}

/// Legacy rows stored `html_contents` (a plain string) instead of the new
/// `body` (a tagged map).  The custom `ContentBody` deserializer must
/// accept both shapes; this unit test verifies that without a DynamoDB
/// round-trip.
#[test]
fn legacy_html_contents_loads_as_html_content_body_unit() {
    use crate::common::ContentBody;
    use aws_sdk_dynamodb::types::AttributeValue;

    let uuid = uuid::Uuid::new_v4().to_string();
    let pk = format!("FEED#{}", uuid);

    let item: std::collections::HashMap<String, AttributeValue> = [
        ("pk".to_string(), AttributeValue::S(pk)),
        ("sk".to_string(), AttributeValue::S("POST".to_string())),
        ("title".to_string(), AttributeValue::S("Legacy".to_string())),
        ("html_contents".to_string(), AttributeValue::S("<p>legacy body</p>".to_string())),
        ("post_type".to_string(), AttributeValue::N("1".to_string())),
        ("status".to_string(), AttributeValue::S("Published".to_string())),
        ("user_pk".to_string(), AttributeValue::S("USER#legacy".to_string())),
        ("shares".to_string(), AttributeValue::N("0".to_string())),
        ("likes".to_string(), AttributeValue::N("0".to_string())),
        ("comments".to_string(), AttributeValue::N("0".to_string())),
        ("reports".to_string(), AttributeValue::N("0".to_string())),
        ("created_at".to_string(), AttributeValue::N("0".to_string())),
        ("updated_at".to_string(), AttributeValue::N("0".to_string())),
        ("author_display_name".to_string(), AttributeValue::S("x".to_string())),
        ("author_profile_url".to_string(), AttributeValue::S("x".to_string())),
        ("author_username".to_string(), AttributeValue::S("x".to_string())),
        ("author_type".to_string(), AttributeValue::N("1".to_string())),
        ("urls".to_string(), AttributeValue::L(vec![])),
        ("categories".to_string(), AttributeValue::L(vec![])),
    ].into_iter().collect();

    let post: crate::features::posts::models::Post =
        serde_dynamo::from_item(item).expect("failed to deserialize legacy Post");
    assert_eq!(post.body, ContentBody::HtmlContent("<p>legacy body</p>".into()));
}

/// Integration test: write a legacy `html_contents` row directly to DynamoDB
/// and verify the new model reads it back as `ContentBody::HtmlContent`.
#[tokio::test]
async fn legacy_html_contents_string_loads_as_html_content_body() {
    use crate::common::ContentBody;
    use aws_sdk_dynamodb::types::AttributeValue;

    let ctx = TestContext::setup().await;
    let cli = &ctx.ddb;
    let table = std::env::var("DYNAMO_TABLE_PREFIX").unwrap() + "-main";
    let uuid = uuid::Uuid::new_v4().to_string();
    let pk = format!("FEED#{}", uuid);
    let sk = "POST".to_string();

    // Build a legacy item — `html_contents` (string) instead of `body` (map).
    let item: std::collections::HashMap<String, AttributeValue> = [
        ("pk".to_string(), AttributeValue::S(pk.clone())),
        ("sk".to_string(), AttributeValue::S(sk.clone())),
        ("title".to_string(), AttributeValue::S("Legacy".to_string())),
        ("html_contents".to_string(), AttributeValue::S("<p>legacy body</p>".to_string())),
        ("post_type".to_string(), AttributeValue::N("1".to_string())),
        ("status".to_string(), AttributeValue::S("Published".to_string())),
        ("user_pk".to_string(), AttributeValue::S("USER#legacy".to_string())),
        ("shares".to_string(), AttributeValue::N("0".to_string())),
        ("likes".to_string(), AttributeValue::N("0".to_string())),
        ("comments".to_string(), AttributeValue::N("0".to_string())),
        ("reports".to_string(), AttributeValue::N("0".to_string())),
        ("created_at".to_string(), AttributeValue::N("0".to_string())),
        ("updated_at".to_string(), AttributeValue::N("0".to_string())),
        ("author_display_name".to_string(), AttributeValue::S("x".to_string())),
        ("author_profile_url".to_string(), AttributeValue::S("x".to_string())),
        ("author_username".to_string(), AttributeValue::S("x".to_string())),
        ("author_type".to_string(), AttributeValue::N("1".to_string())),
        ("urls".to_string(), AttributeValue::L(vec![])),
        ("categories".to_string(), AttributeValue::L(vec![])),
    ].into_iter().collect();

    cli.put_item()
        .table_name(&table)
        .set_item(Some(item))
        .send()
        .await
        .unwrap();

    // Read via the new model.
    let res = cli.get_item()
        .table_name(&table)
        .key("pk", AttributeValue::S(pk.clone()))
        .key("sk", AttributeValue::S(sk.clone()))
        .send()
        .await
        .unwrap();

    let retrieved_item = res.item.unwrap();
    let post: crate::features::posts::models::Post =
        serde_dynamo::from_item(retrieved_item).expect("failed to deserialize Post from DynamoDB item");
    assert_eq!(post.body, ContentBody::HtmlContent("<p>legacy body</p>".into()));
}
