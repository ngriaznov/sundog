//! A session store: sessions replicated to every node with a lifetime a
//! refresh restarts, so any node answers any request and a logout on one
//! node ends the session everywhere.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use sundog::{Cache, CacheError, Cluster, Mode};

// ANCHOR: types
/// An opaque session token. Generate it from a cryptographically secure
/// random source; the store only keys on it.
#[derive(Clone, Debug, Serialize, Deserialize, Hash, PartialEq, Eq)]
pub struct SessionId(pub String);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Session {
    pub user_id: u64,
    pub roles: Vec<String>,
}
// ANCHOR_END: types

// ANCHOR: store
#[derive(Clone)]
pub struct SessionStore {
    cache: Cache<SessionId, Session>,
    lifetime: Duration,
}

impl SessionStore {
    /// Opens the store. Every session lives `lifetime` from its login or its
    /// latest refresh.
    ///
    /// # Errors
    ///
    /// Returns an error if the cache cannot open.
    pub async fn open(cluster: &Cluster, lifetime: Duration) -> Result<Self, CacheError> {
        let cache = cluster
            .cache::<SessionId, Session>("sessions")
            .mode(Mode::Replicated)
            .open()
            .await?;
        Ok(Self { cache, lifetime })
    }

    /// Stores a new session on every node.
    ///
    /// # Errors
    ///
    /// Returns an error if the session fails to encode.
    pub async fn login(&self, id: SessionId, session: Session) -> Result<(), CacheError> {
        self.cache.insert_with_ttl(id, session, self.lifetime).await
    }

    /// Restarts the session's lifetime from now and keeps the session.
    /// Returns `false` for a session already ended or expired. A logout
    /// stamped after the login still wins on every node, even one this
    /// refresh reaches first.
    ///
    /// # Errors
    ///
    /// Returns an error if the id fails to encode.
    pub async fn refresh(&self, id: &SessionId) -> Result<bool, CacheError> {
        self.cache.expire(id, self.lifetime).await
    }

    /// The session behind `id`, from this node's own copy.
    pub async fn session(&self, id: &SessionId) -> Option<Session> {
        self.cache.get(id).await
    }

    /// Ends the session on every node. It never comes back, even from a
    /// node that was partitioned away when the logout ran.
    ///
    /// # Errors
    ///
    /// Returns an error if the id fails to encode.
    pub async fn logout(&self, id: &SessionId) -> Result<(), CacheError> {
        self.cache.remove(id).await
    }
}
// ANCHOR_END: store

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::solo_cluster;

    fn alice() -> Session {
        Session {
            user_id: 1,
            roles: vec!["admin".to_string()],
        }
    }

    #[tokio::test]
    async fn a_session_is_readable_after_login_and_gone_after_logout() {
        let cluster = solo_cluster("cookbook-sessions").await;
        let store = SessionStore::open(&cluster, Duration::from_secs(3600))
            .await
            .expect("store opens");
        let id = SessionId("token-1".to_string());

        store.login(id.clone(), alice()).await.expect("login");
        assert_eq!(store.session(&id).await, Some(alice()));

        store.logout(&id).await.expect("logout");
        assert_eq!(store.session(&id).await, None);
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn a_refresh_outlasts_the_login_lifetime_and_never_revives_a_logout() {
        let cluster = solo_cluster("cookbook-sessions-refresh").await;
        let store = SessionStore::open(&cluster, Duration::from_secs(1))
            .await
            .expect("store opens");
        let id = SessionId("token-3".to_string());

        store.login(id.clone(), alice()).await.expect("login");
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert!(store.refresh(&id).await.expect("refresh"));
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(
            store.session(&id).await,
            Some(alice()),
            "past the login's lifetime, inside the refresh's"
        );

        store.logout(&id).await.expect("logout");
        assert!(!store.refresh(&id).await.expect("refresh"));
        assert_eq!(store.session(&id).await, None);
        cluster.shutdown().await;
    }

    #[tokio::test]
    async fn a_session_expires_at_the_end_of_its_lifetime() {
        let cluster = solo_cluster("cookbook-sessions-expiry").await;
        let store = SessionStore::open(&cluster, Duration::from_millis(200))
            .await
            .expect("store opens");
        let id = SessionId("token-2".to_string());

        store.login(id.clone(), alice()).await.expect("login");
        assert_eq!(store.session(&id).await, Some(alice()));
        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(store.session(&id).await, None);
        cluster.shutdown().await;
    }
}
