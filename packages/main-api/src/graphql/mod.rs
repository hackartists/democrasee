mod feed_query;

use async_graphql_axum::GraphQL;
use bdk::prelude::*;
use dto::sqlx::Postgres;

use async_graphql::{EmptyMutation, EmptySubscription, Object, Schema, http::GraphiQLSource};

pub struct QueryRoot;

pub fn get_graphql_service(
    pool: sqlx::Pool<Postgres>,
) -> GraphQL<Schema<QueryRoot, EmptyMutation, EmptySubscription>> {
    let schema = Schema::build(QueryRoot, EmptyMutation, EmptySubscription)
        .data(pool)
        .finish();

    let ret = GraphQL::new(schema);

    ret
}

pub async fn graphiql() -> impl by_axum::axum::response::IntoResponse {
    by_axum::axum::response::Html(GraphiQLSource::build().endpoint("/graphql").finish())
}

#[Object]
impl QueryRoot {
    async fn feeds(&self) -> &feed_query::FeedQuery {
        &feed_query::FeedQuery
    }
}
