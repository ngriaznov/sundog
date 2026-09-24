//! Read-through caching over sqlx: reads fill the cache from the database on
//! a miss, and every write to the database drops the cached copy on every
//! node.

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
}
