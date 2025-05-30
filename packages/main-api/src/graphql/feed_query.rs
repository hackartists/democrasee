use async_graphql::{Context, Object};
use dto::*;

pub struct FeedQuery;

#[Object]
impl FeedQuery {
    async fn feed<'a>(&self, ctx: &Context<'a>, id: i64) -> Feed {
        let pool = ctx
            .data::<sqlx::Pool<sqlx::Postgres>>()
            .expect("Database pool not found");

        Feed::query_builder()
            .id_equals(id)
            .query()
            .map(Feed::from)
            .fetch_one(pool)
            .await
            .unwrap_or_default()
    }

    async fn feeds<'a>(&self, ctx: &Context<'a>, params: FeedQuery) -> Vec<Feed> {
        let pool = ctx
            .data::<sqlx::Pool<sqlx::Postgres>>()
            .expect("Database pool not found");

        Feed::query_builder()
            .query()
            .map(Feed::from)
            .fetch_all(pool)
            .await
            .unwrap_or_default()
    }
}
