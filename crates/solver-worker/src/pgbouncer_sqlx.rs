//! `SQLx` helpers for `PostgreSQL` transaction poolers.
//!
//! Supabase's 6543 pooler uses transaction pooling. Persistent `SQLx` prepared
//! statements are named per client connection, but transaction poolers reuse
//! backend sessions across clients, which can make names such as `sqlx_s_1`
//! collide or point to the wrong statement. Keep runtime queries non-persistent
//! so `SQLx` does not reuse named prepared statements across pooler sessions.
//!
//! `SQLx` still uses the prepared-statement protocol for bound `PostgreSQL`
//! queries. Hot queue operations can use a queue-only pool pointed at the
//! transaction pooler, then execute through `raw_sql` and validated queue-name
//! literals so they run through the simple query protocol instead.

pub use ::sqlx::*;

/// Executes through `PostgreSQL`'s simple-query protocol. This is required for
/// hot queue RPCs on Supabase's transaction pooler: even a non-persistent
/// bound query still uses the extended prepared-statement protocol.
#[must_use]
pub fn raw_query(sql: &str) -> ::sqlx::RawSql<'_> {
    ::sqlx::raw_sql(sql)
}

pub fn query(
    sql: &str,
) -> ::sqlx::query::Query<'_, ::sqlx::Postgres, ::sqlx::postgres::PgArguments> {
    ::sqlx::query::<::sqlx::Postgres>(sql).persistent(false)
}

pub fn query_scalar<'q, O>(
    sql: &'q str,
) -> ::sqlx::query::QueryScalar<'q, ::sqlx::Postgres, O, ::sqlx::postgres::PgArguments>
where
    O: Send,
    (O,): for<'r> ::sqlx::FromRow<'r, ::sqlx::postgres::PgRow>,
{
    ::sqlx::query_scalar::<::sqlx::Postgres, O>(sql).persistent(false)
}

#[cfg(test)]
mod tests {
    use super::{Execute, Postgres, RawSql, query, query_scalar, raw_query};

    #[test]
    fn query_helpers_disable_persistent_prepared_statements() {
        let query = query("SELECT 1");
        assert!(!::sqlx::Execute::persistent(&query));

        let scalar = query_scalar::<i64>("SELECT 1");
        assert!(!::sqlx::Execute::persistent(&scalar));
    }

    #[test]
    fn raw_queue_queries_use_simple_protocol_without_statement_or_arguments() {
        let mut query = raw_query("SELECT private.worker_claim_jobs('solver', 'test', 1, 1)");
        assert!(!<RawSql<'_> as Execute<'_, Postgres>>::persistent(&query));
        assert!(<RawSql<'_> as Execute<'_, Postgres>>::statement(&query).is_none());
        assert!(
            <RawSql<'_> as Execute<'_, Postgres>>::take_arguments(&mut query)
                .unwrap()
                .is_none()
        );
    }
}
