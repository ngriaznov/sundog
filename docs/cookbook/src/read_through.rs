//! Read-through caching over sqlx: reads fill the cache from the database on
//! a miss, and every write to the database drops the cached copy on every
//! node.

use std::collections::HashMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use sqlx::SqlitePool;
use sundog::{Cache, CacheError, Cluster, Mode};

// ANCHOR: types
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, sqlx::FromRow)]
pub struct Product {
    pub id: i64,
    pub name: String,
    pub price_cents: i64,
}
// ANCHOR_END: types

// ANCHOR: repo
/// Products, read through a cache of up to 100,000 entries per node.
#[derive(Clone)]
pub struct Products {
    db: SqlitePool,
    cache: Cache<i64, Option<Product>>,
}

impl Products {
    /// Opens the cache over `db`.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache cannot open.
    pub async fn open(cluster: &Cluster, db: SqlitePool) -> Result<Self, CacheError> {
        let cache = cluster
            .cache::<i64, Option<Product>>("products")
            .mode(Mode::Invalidation)
            .max_capacity(100_000)
            .ttl(Duration::from_secs(300))
            .open()
            .await?;
        Ok(Self { db, cache })
    }

    /// Reads one product. Concurrent misses on the same id run one query.
    ///
    /// # Errors
    ///
    /// Returns an error if the query fails.
    pub async fn get(&self, id: i64) -> Result<Option<Product>, CacheError> {
        let db = self.db.clone();
        self.cache
            .get_or_load(&id, async move |&id| {
                sqlx::query_as::<_, Product>(
                    "SELECT id, name, price_cents FROM products WHERE id = ?",
                )
                .bind(id)
                .fetch_optional(&db)
                .await
            })
            .await
    }

    /// Changes a price in the database, then drops the cached copy on every
    /// node so the next read loads the new row.
    ///
    /// # Errors
    ///
    /// Returns an error if the update or the cache removal fails.
    pub async fn set_price(&self, id: i64, price_cents: i64) -> anyhow::Result<()> {
        sqlx::query("UPDATE products SET price_cents = ? WHERE id = ?")
            .bind(price_cents)
            .bind(id)
            .execute(&self.db)
            .await?;
        self.cache.remove(&id).await?;
        Ok(())
    }
}
// ANCHOR_END: repo

// ANCHOR: loader
/// Products read through a loader the cache itself holds, so every node
/// loads a missed id the same way. The misses of every call within two
/// milliseconds reach the database as one query, and a hot id reloads in
/// the background when it enters the last fifth of its minute.
///
/// # Errors
///
/// Returns an error if the cache cannot open.
pub async fn open_loaded(
    cluster: &Cluster,
    db: SqlitePool,
) -> Result<Cache<i64, Product>, CacheError> {
    cluster
        .cache::<i64, Product>("loaded-products")
        .mode(Mode::distributed())
        .ttl(Duration::from_secs(60))
        .batch_loader(move |ids: Vec<i64>| {
            let db = db.clone();
            async move { products_by_id(&db, &ids).await }
        })
        .batch_window(Duration::from_millis(2), 500)
        .refresh_ahead(0.8)
        .open()
        .await
}

/// One `IN` query for every id; an id with no row is absent from the map.
async fn products_by_id(
    db: &SqlitePool,
    ids: &[i64],
) -> Result<HashMap<i64, Product>, sqlx::Error> {
    let mut query =
        sqlx::QueryBuilder::new("SELECT id, name, price_cents FROM products WHERE id IN (");
    let mut list = query.separated(", ");
    for id in ids {
        list.push_bind(*id);
    }
    list.push_unseparated(")");
    let rows = query.build_query_as::<Product>().fetch_all(db).await?;
    Ok(rows.into_iter().map(|row| (row.id, row)).collect())
}
// ANCHOR_END: loader

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::solo_cluster;

    async fn seeded_db() -> SqlitePool {
        let db = SqlitePool::connect("sqlite::memory:")
            .await
            .expect("an in-memory database opens");
        sqlx::query("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT NOT NULL, price_cents INTEGER NOT NULL)")
            .execute(&db)
            .await
            .expect("create table");
        sqlx::query("INSERT INTO products (id, name, price_cents) VALUES (1, 'kettle', 2500)")
            .execute(&db)
            .await
            .expect("seed row");
        db
    }

    #[tokio::test]
    async fn a_read_fills_from_the_database_and_a_write_drops_the_cached_copy() {
        let cluster = solo_cluster("cookbook-read-through").await;
        let db = seeded_db().await;
        let products = Products::open(&cluster, db.clone())
            .await
            .expect("cache opens");

        let kettle = products.get(1).await.expect("read").expect("row exists");
        assert_eq!(kettle.price_cents, 2500);

        // A change behind the cache's back is invisible until the entry goes.
        sqlx::query("UPDATE products SET price_cents = 9999 WHERE id = 1")
            .execute(&db)
            .await
            .expect("direct update");
        let cached = products.get(1).await.expect("read").expect("row exists");
        assert_eq!(cached.price_cents, 2500, "served from the cache");

        products
            .set_price(1, 2750)
            .await
            .expect("update through the repo");
        let fresh = products.get(1).await.expect("read").expect("row exists");
        assert_eq!(fresh.price_cents, 2750, "the removal forced a reload");
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn a_missing_row_is_cached_as_absent() {
        let cluster = solo_cluster("cookbook-read-through-absent").await;
        let db = seeded_db().await;
        let products = Products::open(&cluster, db.clone())
            .await
            .expect("cache opens");

        assert_eq!(products.get(42).await.expect("read"), None);
        sqlx::query("INSERT INTO products (id, name, price_cents) VALUES (42, 'mug', 900)")
            .execute(&db)
            .await
            .expect("insert behind the cache");
        assert_eq!(
            products.get(42).await.expect("read"),
            None,
            "the cached absence holds until its TTL or a removal"
        );
        products
            .set_price(42, 950)
            .await
            .expect("update through the repo");
        assert_eq!(
            products.get(42).await.expect("read").map(|p| p.price_cents),
            Some(950)
        );
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn the_registered_loader_fills_misses_in_one_call_and_skips_absent_rows() {
        let cluster = solo_cluster("cookbook-read-through-loader").await;
        let db = seeded_db().await;
        sqlx::query("INSERT INTO products (id, name, price_cents) VALUES (2, 'mug', 900)")
            .execute(&db)
            .await
            .expect("seed row");
        let products = open_loaded(&cluster, db.clone())
            .await
            .expect("cache opens");

        let found = products.load_many([1, 2, 3]).await.expect("batch load");
        assert_eq!(found.len(), 2, "the id with no row is absent from the map");
        assert_eq!(found[&1].price_cents, 2500);
        assert_eq!(found[&2].name, "mug");
        assert_eq!(products.load(&3).await.expect("load"), None);

        // A loaded row is cached, a missing one is not.
        sqlx::query("UPDATE products SET price_cents = 9999 WHERE id = 1")
            .execute(&db)
            .await
            .expect("direct update");
        sqlx::query("INSERT INTO products (id, name, price_cents) VALUES (3, 'jug', 1200)")
            .execute(&db)
            .await
            .expect("insert behind the cache");
        let kettle = products.load(&1).await.expect("load").expect("cached row");
        assert_eq!(kettle.price_cents, 2500, "served from the cache");
        let jug = products
            .load(&3)
            .await
            .expect("load")
            .expect("row now exists");
        assert_eq!(jug.name, "jug", "an absent id is asked for again");
        cluster.shutdown().await;
    }
}
