//! Refresh-ahead: a read that finds an entry past its refresh point, the
//! last stretch of the default TTL before its expiry, asks for the key to
//! be reloaded before it expires. The cache layer's refresh task decides which node reloads a
//! key; the reload itself runs here, in [`Shard::refresh`], and stores its
//! value like any other load, which replicates it per the cache's mode.
//!
//! A read asks at most once per version of an entry: [`Refresh::seen`]
//! remembers the version asked about, so a hot key in its refresh window
//! costs one request, not one per read. A reload that fails is asked
//! about again after [`REFRESH_RETRY_MS`].

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Mutex as StdMutex, OnceLock, PoisonError};

use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;

use super::load::Fill;
use super::{Shard, encode_key, engine};
use crate::hlc::Hlc;

/// How long after a failed reload a read asks for the key again.
pub(crate) const REFRESH_RETRY_MS: u64 = 1_000;

/// How long after a refresh hint is sent the hinter's read may ask about the
/// same version again, covering a hint lost in flight or skipped by its
/// target. The reload of a delivered hint changes the version, so the retry
/// fires only when nothing did.
pub(crate) const REFRESH_HINT_RETRY_MS: u64 = 1_000;

/// How many refresh requests wait for the refresh task before a read stops
/// asking; a read that finds the queue full asks again on its next read.
const REFRESH_QUEUE: usize = 4_096;

/// Stripes in [`Refresh::seen`], spreading the reads of many keys at once.
const SEEN_STRIPES: usize = 64;

/// The most versions one [`Refresh::seen`] stripe remembers before it
/// forgets them all and starts over.
const SEEN_STRIPE_CAP: usize = 4_096;

/// One key to reload: from a read here, or `hinted` by a peer that read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RefreshRequest<K> {
    pub(crate) key: K,
    pub(crate) key_bytes: Bytes,
    /// The version of the entry the read found.
    pub(crate) ver: Hlc,
    /// Sent by a peer that picked this node to reload the key, so no
    /// further routing.
    pub(crate) hinted: bool,
}

/// What one [`Shard::refresh`] pass did, for the refresh task's metrics.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RefreshTally {
    /// Reloaded and stored.
    pub(crate) loaded: u64,
    /// Reloaded, and the value not stored: a write, removal, re-arm or
    /// invalidation overtook the reload, the memory ceiling refused it, or
    /// this node no longer owns the key's part.
    pub(crate) discarded: u64,
    /// The source no longer holds the key; the entry is left to expire.
    pub(crate) absent: u64,
    /// The loader failed; the entry keeps its value until it expires.
    pub(crate) failed: u64,
}

/// One [`Refresh::seen`] stripe: per key, the version last asked about and
/// when a read may ask about it again.
type SeenStripe = StdMutex<HashMap<Bytes, (Hlc, u64)>>;

/// A shard's refresh-ahead configuration and state.
pub(crate) struct Refresh<K> {
    /// The refresh point, in thousandths of the default TTL.
    permille: u64,
    requests: OnceLock<mpsc::Sender<RefreshRequest<K>>>,
    /// Per key, the version last asked about and when a read may ask
    /// about it again; `u64::MAX` until its reload fails.
    seen: Box<[SeenStripe]>,
}

impl<K> Refresh<K> {
    pub(crate) fn new(permille: u64) -> Self {
        Self {
            permille,
            requests: OnceLock::new(),
            seen: (0..SEEN_STRIPES)
                .map(|_| StdMutex::new(HashMap::new()))
                .collect::<Vec<_>>()
                .into_boxed_slice(),
        }
    }

    fn seen_stripe(&self, hash: u64) -> std::sync::MutexGuard<'_, HashMap<Bytes, (Hlc, u64)>> {
        let index = usize::try_from(hash % SEEN_STRIPES as u64)
            .expect("invariant: a value under SEEN_STRIPES fits in usize");
        self.seen[index]
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    /// Records that a reload of `key_bytes` at `ver` is asked for, unless it
    /// already is and may not be asked again before `now_ms`. Returns
    /// whether to ask.
    fn claim(&self, key_bytes: &Bytes, hash: u64, ver: Hlc, now_ms: u64) -> bool {
        let mut seen = self.seen_stripe(hash);
        if let Some(&(asked, retry_at)) = seen.get(key_bytes)
            && asked == ver
            && now_ms < retry_at
        {
            return false;
        }
        if seen.len() >= SEEN_STRIPE_CAP {
            seen.clear();
        }
        seen.insert(key_bytes.clone(), (ver, u64::MAX));
        true
    }

    /// Lets a read ask about `key_bytes` again from `retry_at`, or at once
    /// with `0`.
    fn release(&self, key_bytes: &Bytes, hash: u64, retry_at: u64) {
        if let Some(entry) = self.seen_stripe(hash).get_mut(key_bytes) {
            entry.1 = retry_at;
        }
    }
}

/// The refresh point, in thousandths of a lifetime, for a refresh-ahead
/// `fraction`: `None` unless it lies strictly between 0 and 1 after
/// rounding to a thousandth. Pure; unit tested directly.
#[must_use]
pub(crate) fn refresh_permille(fraction: f64) -> Option<u64> {
    let permille = (fraction * 1000.0).round();
    if !(1.0..=999.0).contains(&permille) {
        return None;
    }
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "bounded to 1..=999 just above"
    )]
    Some(permille as u64)
}

/// Whether an entry expiring at `expires_at_ms` is due for a reload at
/// `now_ms`: not yet expired, and with at most `1000 - permille`
/// thousandths of the default TTL, `ttl_ms`, left before it expires. The
/// window is measured back from the expiry alone, so an entry whose
/// lifetime was restarted by a touch or re-arm, or stamped by a leading
/// clock, is due at the same distance from its expiry as any other; an entry
/// of a lifetime no longer than the window is due from the start. An entry
/// that never expires never is. Pure; unit tested directly.
#[must_use]
pub(crate) fn refresh_due(
    now_ms: u64,
    expires_at_ms: Option<u64>,
    ttl_ms: u64,
    permille: u64,
) -> bool {
    let Some(expires_at_ms) = expires_at_ms else {
        return false;
    };
    let left = u128::from(expires_at_ms.saturating_sub(now_ms));
    now_ms < expires_at_ms
        && left * 1000 <= u128::from(ttl_ms) * u128::from(1000u64.saturating_sub(permille))
}

/// When a read may ask about a version again after its hint was sent at
/// `now_ms`. Pure; unit tested directly.
#[must_use]
pub(crate) fn hint_retry_at(now_ms: u64) -> u64 {
    now_ms.saturating_add(REFRESH_HINT_RETRY_MS)
}

impl<K, V> Shard<K, V>
where
    K: Hash + Eq + Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    V: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    /// Turns on refresh-ahead at `permille` thousandths of an entry's
    /// lifetime; see [`refresh_permille`].
    #[must_use]
    pub(crate) fn with_refresh_ahead(mut self, permille: u64) -> Self {
        self.refresh = Some(Refresh::new(permille));
        self
    }

    /// The queue of reloads reads ask for, for the refresh task to drain.
    /// `None` without refresh-ahead, or once taken.
    pub(crate) fn take_refresh_requests(&self) -> Option<mpsc::Receiver<RefreshRequest<K>>> {
        let refresh = self.refresh.as_ref()?;
        let (tx, rx) = mpsc::channel(REFRESH_QUEUE);
        refresh.requests.set(tx).ok()?;
        Some(rx)
    }

    /// A resident read of `key`, already encoded and hashed, that asks for a
    /// reload when refresh-ahead is on and the entry is due.
    pub(super) fn read_resident(
        &self,
        key: &K,
        key_bytes: &[u8],
        hash: u64,
        now_ms: u64,
    ) -> Option<V> {
        if self.refresh.is_none() {
            return self.engine.get_by_bytes(key_bytes, hash, now_ms);
        }
        let (value, ver, expires_at_ms) = self.engine.get_with_lifetime(key_bytes, hash, now_ms)?;
        self.note_read(key, key_bytes, hash, ver, expires_at_ms, now_ms);
        Some(value)
    }

    /// [`refresh_due`] under this shard's default TTL and refresh point:
    /// never without both.
    fn refresh_is_due(
        &self,
        refresh: &Refresh<K>,
        expires_at_ms: Option<u64>,
        now_ms: u64,
    ) -> bool {
        let Some(ttl) = self.ttl else {
            return false;
        };
        let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
        refresh_due(now_ms, expires_at_ms, ttl_ms, refresh.permille)
    }

    /// Records that a hint for `key_bytes` was sent to the peer chosen to
    /// reload it: the claim the read set stops holding the version for good
    /// and lets a read ask again after [`hint_retry_at`], so a hint lost in
    /// flight or skipped by its target does not leave the key unrefreshed.
    pub(crate) fn note_hint_sent(&self, key_bytes: &Bytes) {
        let Some(refresh) = &self.refresh else {
            return;
        };
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        refresh.release(key_bytes, hash, hint_retry_at(self.now_ms()));
    }

    /// [`Shard::read_resident`] for a caller holding only the key.
    pub(super) fn read_resident_key(&self, key: &K, now_ms: u64) -> Option<V> {
        if self.refresh.is_none() {
            return self.engine.get(key, now_ms);
        }
        let key_bytes = encode_key(key).ok()?;
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        self.read_resident(key, key_bytes.as_ref(), hash, now_ms)
    }

    /// Asks for a reload of `key` if the entry a read found, at `ver` and
    /// expiring at `expires_at_ms`, is past its refresh point and not
    /// already asked about. A read of another node's copy, such as a
    /// `Mode::Distributed` fetch, asks too.
    pub(crate) fn note_read(
        &self,
        key: &K,
        key_bytes: &[u8],
        hash: u64,
        ver: Hlc,
        expires_at_ms: Option<u64>,
        now_ms: u64,
    ) {
        let Some(refresh) = &self.refresh else {
            return;
        };
        if !self.refresh_is_due(refresh, expires_at_ms, now_ms) {
            return;
        }
        let Some(requests) = refresh.requests.get() else {
            return;
        };
        let key_bytes = Bytes::copy_from_slice(key_bytes);
        if !refresh.claim(&key_bytes, hash, ver, now_ms) {
            return;
        }
        let request = RefreshRequest {
            key: key.clone(),
            key_bytes: key_bytes.clone(),
            ver,
            hinted: false,
        };
        if requests.try_send(request).is_err() {
            refresh.release(&key_bytes, hash, 0);
        }
    }

    /// Takes a peer's hint that it read `key_bytes` at `ver` past its
    /// refresh point and picked this node to reload it: queues a reload
    /// unless this node's copy is newer, missing, or already asked about.
    /// The hinter judged the entry due, so the reload is not judged again on
    /// this node's clock.
    pub(crate) fn take_refresh_hint(&self, key_bytes: &Bytes, ver: Hlc) {
        let Some(refresh) = &self.refresh else {
            return;
        };
        let Some(requests) = refresh.requests.get() else {
            return;
        };
        let Ok(key) = postcard::from_bytes::<K>(key_bytes) else {
            return;
        };
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        let now_ms = self.now_ms();
        let Some((held, _)) = self.engine.lifetime_of(key_bytes, hash, now_ms) else {
            return;
        };
        if held > ver || !refresh.claim(key_bytes, hash, held, now_ms) {
            return;
        }
        let request = RefreshRequest {
            key,
            key_bytes: key_bytes.clone(),
            ver: held,
            hinted: true,
        };
        if requests.try_send(request).is_err() {
            refresh.release(key_bytes, hash, 0);
        }
    }

    /// Reloads the keys of `requests` whose entry here is still the version
    /// asked about, or older, still due unless a peer `hinted` the request,
    /// and in a part this node owns, in loader calls of at most
    /// [`super::Loader::max_keys`] keys. A value the loader returns replaces
    /// the entry only while it is still the version the refresh read: a
    /// write, removal, touch, expire or persist that lands during the call
    /// wins, and the value is discarded. A key the source no longer holds
    /// keeps its entry until it expires; a loader failure, a discarded
    /// reload and a request skipped as not due or already loading keep
    /// every entry and let reads ask again after [`REFRESH_RETRY_MS`].
    /// Reads keep answering from the old entries throughout.
    pub(crate) async fn refresh(&self, requests: Vec<RefreshRequest<K>>) -> RefreshTally {
        let max_keys = self
            .loader
            .as_ref()
            .map_or(usize::MAX, super::Loader::max_keys);
        let mut tally = RefreshTally::default();
        let mut requests = requests.into_iter().peekable();
        while requests.peek().is_some() {
            let chunk = requests.by_ref().take(max_keys).collect();
            let done = self.refresh_chunk(chunk).await;
            tally.loaded += done.loaded;
            tally.discarded += done.discarded;
            tally.absent += done.absent;
            tally.failed += done.failed;
        }
        tally
    }

    /// One loader call of [`Shard::refresh`].
    async fn refresh_chunk(&self, requests: Vec<RefreshRequest<K>>) -> RefreshTally {
        let mut tally = RefreshTally::default();
        let Some(refresh) = &self.refresh else {
            return tally;
        };
        let now_ms = self.now_ms();
        let retry_at = now_ms.saturating_add(REFRESH_RETRY_MS);
        let mut items = Vec::with_capacity(requests.len());
        for request in requests {
            let hash = engine::hash_key_bytes(request.key_bytes.as_ref());
            let Some((held, expires_at_ms)) =
                self.engine.lifetime_of(&request.key_bytes, hash, now_ms)
            else {
                continue;
            };
            if held > request.ver {
                continue;
            }
            // A hinted request was judged due by the node that read the
            // entry; this node's clock may run behind.
            if !request.hinted && !self.refresh_is_due(refresh, expires_at_ms, now_ms) {
                refresh.release(&request.key_bytes, hash, retry_at);
                continue;
            }
            // An entry held only through disown grace belongs to its new
            // owners: the reload would be stored nowhere (see `fill_route`),
            // so the loader does not run for it.
            // The read that asked claimed the version, so the claim is
            // released: a read after the node regains the part asks again.
            if !self.owns_key(request.key_bytes.as_ref()) {
                refresh.release(&request.key_bytes, hash, 0);
                continue;
            }
            let Some(inflight) = self.engine.begin_refresh(&request.key_bytes, hash) else {
                refresh.release(&request.key_bytes, hash, retry_at);
                continue;
            };
            let guard = self.engine.guard_inflight(
                request.key_bytes.clone(),
                hash,
                std::sync::Arc::clone(&inflight),
            );
            items.push((request, hash, (self.stamp_local(), held), inflight, guard));
        }
        if items.is_empty() {
            return tally;
        }
        let keys = items
            .iter()
            .map(|(request, ..)| request.key.clone())
            .collect();
        match self.call_loader(keys).await {
            Ok(mut values) => {
                for (request, hash, versions, inflight, guard) in items {
                    if let Some(value) = values.remove(&request.key) {
                        if self.store_reloaded(
                            &request.key,
                            &request.key_bytes,
                            hash,
                            versions,
                            &inflight,
                            Fill::loaded(value),
                        ) {
                            tally.loaded += 1;
                        } else {
                            // The version is not asked about again unless a
                            // read comes after the retry delay: a reload
                            // the ceiling refused retries then.
                            refresh.release(&request.key_bytes, hash, retry_at);
                            tally.discarded += 1;
                        }
                    } else {
                        self.engine
                            .abandon_inflight(&request.key_bytes, hash, &inflight);
                        tally.absent += 1;
                    }
                    guard.complete();
                }
            }
            Err(err) => {
                tracing::debug!(cache = %self.name, %err, "refresh-ahead reload failed; entries keep their values");
                for (request, hash, _, inflight, guard) in items {
                    self.engine
                        .abandon_inflight(&request.key_bytes, hash, &inflight);
                    refresh.release(&request.key_bytes, hash, retry_at);
                    tally.failed += 1;
                    guard.complete();
                }
            }
        }
        tally
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::Duration;

    use smol_str::SmolStr;

    use super::*;
    use crate::explain::LocalRecord;
    use crate::node::NodeId;
    use crate::store::{Event, Loader, Mode};

    #[derive(Debug, thiserror::Error)]
    #[error("source unavailable")]
    struct Down;

    const T0: u64 = 1_000_000;

    /// What the test loader does with each call.
    #[derive(Clone, Copy)]
    enum Source {
        /// Holds `v{key}.{call}` for every key.
        Holds,
        /// Holds nothing.
        Lost,
        /// Fails.
        Down,
    }

    /// A `Mode::Local` shard on `clock` with a 10s TTL, refresh-ahead at
    /// 0.8, and a loader that counts its calls in `calls`.
    fn refreshing_shard(
        clock: &Arc<AtomicU64>,
        calls: &Arc<AtomicU64>,
        source: Source,
    ) -> Shard<u32, String> {
        let now = Arc::clone(clock);
        let calls = Arc::clone(calls);
        Shard::new(
            SmolStr::new("test"),
            Mode::Local,
            NodeId::from(1),
            10_000,
            Some(Duration::from_secs(10)),
            None,
        )
        .with_clock(Arc::new(move || now.load(Ordering::SeqCst)))
        .with_loader(Loader::batch(move |keys: Vec<u32>| {
            let call = calls.fetch_add(1, Ordering::SeqCst) + 1;
            async move {
                match source {
                    Source::Holds => Ok(keys
                        .into_iter()
                        .map(|key| (key, format!("v{key}.{call}")))
                        .collect::<HashMap<_, _>>()),
                    Source::Lost => Ok(HashMap::new()),
                    Source::Down => Err(Down),
                }
            }
        }))
        .with_refresh_ahead(800)
    }

    fn ttl_left(s: &Shard<u32, String>, key: u32) -> Duration {
        match s.ttl_of(&key) {
            Some(crate::store::Ttl::Remaining(left)) => left,
            other => panic!("a remaining lifetime, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_read_past_the_refresh_point_asks_once_per_version() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Holds);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        assert!(
            s.take_refresh_requests().is_none(),
            "the queue is taken once"
        );
        s.insert(1, "a".to_string()).await.expect("insert");

        clock.store(T0 + 7_999, Ordering::SeqCst);
        assert_eq!(s.get(&1).await, Some("a".to_string()));
        assert!(requests.try_recv().is_err(), "not due before 80% of 10s");

        clock.store(T0 + 8_000, Ordering::SeqCst);
        assert_eq!(s.get(&1).await, Some("a".to_string()));
        let request = requests.try_recv().expect("a due read asks");
        assert_eq!(request.key, 1);
        assert!(!request.hinted);
        assert_eq!(s.get_sync(&1), Some("a".to_string()));
        assert_eq!(s.load(&1).await.expect("load"), Some("a".to_string()));
        assert!(
            requests.try_recv().is_err(),
            "every later read of that version, by any path, asks nothing"
        );
        assert_eq!(
            calls.load(Ordering::SeqCst),
            0,
            "reads never wait on a reload"
        );
    }

    /// `local_record` is not a read: inside an entry's refresh window it
    /// queues no reload and calls no loader, where a `get` of the same entry
    /// asks for one, and a key the shard lacks is neither loaded nor stored,
    /// where a `load` of it calls the loader.
    #[tokio::test]
    async fn local_record_in_the_refresh_window_asks_for_no_reload() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Holds);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        let held = encode_key(&1u32).expect("encodes");
        let lacking = encode_key(&2u32).expect("encodes");

        clock.store(T0 + 9_000, Ordering::SeqCst);
        let (at_ms, record) = s.local_record(&held);
        assert_eq!(at_ms, T0 + 9_000);
        assert!(
            matches!(
                record,
                LocalRecord::Live {
                    expires_at_ms: Some(expires),
                    ..
                } if expires == T0 + 10_000
            ),
            "{record:?}"
        );
        assert!(
            requests.try_recv().is_err(),
            "inspecting an entry past its refresh point asks for no reload"
        );
        assert_eq!(s.local_record(&lacking).1, LocalRecord::Absent);
        assert!(requests.try_recv().is_err());
        assert_eq!(calls.load(Ordering::SeqCst), 0, "no loader call");
        assert_eq!(s.keys(), vec![1], "the missing key is not stored");

        assert_eq!(s.get(&1).await, Some("a".to_string()));
        let request = requests
            .try_recv()
            .expect("a due read asks, so the inspections did not claim the version");
        assert_eq!(request.key, 1);
        assert!(!request.hinted);
        assert_eq!(
            s.load(&2).await.expect("load"),
            Some("v2.1".to_string()),
            "a load of the lacking key calls the loader"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_refresh_reloads_a_due_entry_and_restarts_its_lifetime() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Holds);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        clock.store(T0 + 9_000, Ordering::SeqCst);
        let _ = s.get(&1).await;
        let request = requests.try_recv().expect("a due read asks");
        let mut events = s.events();

        let tally = s.refresh(vec![request]).await;
        assert_eq!(
            tally,
            RefreshTally {
                loaded: 1,
                discarded: 0,
                absent: 0,
                failed: 0
            }
        );
        assert_eq!(s.get(&1).await, Some("v1.1".to_string()));
        assert!(
            ttl_left(&s, 1) > Duration::from_millis(9_900),
            "the reloaded entry lives a full TTL from the reload"
        );
        assert!(matches!(
            events.try_recv(),
            Ok(Event::Updated { key: 1, .. })
        ));
        clock.store(T0 + 12_000, Ordering::SeqCst);
        assert_eq!(
            s.get(&1).await,
            Some("v1.1".to_string()),
            "past the old expiry, the key still reads"
        );
    }

    #[tokio::test]
    async fn a_refresh_leaves_an_entry_a_write_replaced() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Holds);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        clock.store(T0 + 9_000, Ordering::SeqCst);
        let _ = s.get(&1).await;
        let request = requests.try_recv().expect("a due read asks");
        s.insert(1, "b".to_string()).await.expect("insert");

        assert_eq!(s.refresh(vec![request]).await, RefreshTally::default());
        assert_eq!(s.get(&1).await, Some("b".to_string()));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn a_failed_refresh_keeps_the_entry_and_asks_again_after_the_retry_delay() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Down);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        clock.store(T0 + 8_000, Ordering::SeqCst);
        let _ = s.get(&1).await;
        let request = requests.try_recv().expect("a due read asks");

        let tally = s.refresh(vec![request]).await;
        assert_eq!(tally.failed, 1);
        assert_eq!(
            s.get(&1).await,
            Some("a".to_string()),
            "the entry keeps its value"
        );
        assert!(requests.try_recv().is_err(), "no retry before the delay");
        clock.store(T0 + 8_000 + REFRESH_RETRY_MS, Ordering::SeqCst);
        let _ = s.get(&1).await;
        assert!(
            requests.try_recv().is_ok(),
            "a read after the delay asks again"
        );
        clock.store(T0 + 10_000, Ordering::SeqCst);
        assert_eq!(s.get(&1).await, None, "the entry expires on time");
    }

    #[tokio::test]
    async fn a_key_the_source_lost_keeps_its_entry_until_it_expires() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Lost);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        clock.store(T0 + 8_000, Ordering::SeqCst);
        let _ = s.get(&1).await;
        let request = requests.try_recv().expect("a due read asks");

        assert_eq!(s.refresh(vec![request]).await.absent, 1);
        assert_eq!(s.get(&1).await, Some("a".to_string()));
        assert!(
            requests.try_recv().is_err(),
            "that version is not asked about again"
        );
        clock.store(T0 + 10_000, Ordering::SeqCst);
        assert_eq!(s.get(&1).await, None);
    }

    #[tokio::test]
    async fn a_peer_hint_queues_a_reload_of_the_copy_here_unless_it_is_newer() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Holds);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        let key_bytes = encode_key(&1u32).expect("encodes");
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        let (held, _) = s
            .engine
            .lifetime_of(&key_bytes, hash, T0)
            .expect("live entry");

        let older = Hlc {
            wall_ms: held.wall_ms - 1,
            ..held
        };
        s.take_refresh_hint(&key_bytes, older);
        assert!(
            requests.try_recv().is_err(),
            "a hint about an older version than the copy here is stale"
        );
        s.take_refresh_hint(&key_bytes, held);
        let request = requests.try_recv().expect("the hint queues a reload");
        assert!(request.hinted);
        assert_eq!(request.ver, held);
        s.take_refresh_hint(&key_bytes, held);
        assert!(requests.try_recv().is_err(), "one request per version");
        s.take_refresh_hint(&encode_key(&2u32).expect("encodes"), held);
        assert!(
            requests.try_recv().is_err(),
            "a key not held here is not reloaded"
        );
    }

    #[test]
    fn refresh_permille_takes_a_fraction_strictly_inside_zero_and_one() {
        assert_eq!(refresh_permille(0.8), Some(800));
        assert_eq!(refresh_permille(0.001), Some(1));
        assert_eq!(refresh_permille(0.999), Some(999));
        for outside in [0.0, 0.0004, 0.9996, 1.0, 1.5, -0.2, f64::NAN, f64::INFINITY] {
            assert_eq!(refresh_permille(outside), None, "{outside}");
        }
    }

    #[test]
    fn refresh_due_opens_at_the_refresh_point_and_closes_at_expiry() {
        // A 10s default TTL refreshed at 0.8: due in the last 2s before
        // the expiry at 11_000, so from 9_000.
        let due = |now| refresh_due(now, Some(11_000), 10_000, 800);
        assert!(!due(1_000));
        assert!(!due(8_999));
        assert!(due(9_000));
        assert!(due(10_999));
        assert!(!due(11_000), "an expired entry is not refreshed");
        assert!(
            !refresh_due(50_000, None, 10_000, 800),
            "no expiry, no refresh"
        );
    }

    /// The window is judged from the expiry alone: where an entry was
    /// stamped, written or re-armed does not enter, and an entry whose
    /// lifetime differs from the default TTL is due in the same last stretch
    /// of the default.
    #[test]
    fn refresh_due_depends_on_the_expiry_and_the_default_ttl_only() {
        let ttl = 10_000;
        // A touch at 9_000 restarts the lifetime: the expiry is 19_000,
        // so 16_000 is 3s from it, outside the 2s window, and 17_000 is in.
        assert!(!refresh_due(16_000, Some(19_000), ttl, 800));
        assert!(refresh_due(17_000, Some(19_000), ttl, 800));
        // An expiry an hour out is not due until its last 2s.
        assert!(!refresh_due(1_000, Some(3_601_000), ttl, 800));
        assert!(refresh_due(3_599_000, Some(3_601_000), ttl, 800));
        // An entry whose whole lifetime fits the window is due at once.
        assert!(refresh_due(1_000, Some(2_000), ttl, 800));
        // A zero default TTL leaves no window beyond the expiry itself.
        assert!(!refresh_due(1_000, Some(2_000), 0, 800));
    }

    #[test]
    fn hint_retry_at_waits_the_hint_retry_delay() {
        assert_eq!(hint_retry_at(100), 100 + REFRESH_HINT_RETRY_MS);
        assert_eq!(hint_retry_at(u64::MAX), u64::MAX);
    }

    #[test]
    fn claim_asks_once_per_version_until_released() {
        let refresh = Refresh::<u32>::new(800);
        let key = Bytes::from_static(b"k");
        let ver = |wall_ms| Hlc {
            wall_ms,
            logical: 0,
            node: crate::node::NodeId::from(1),
        };
        assert!(refresh.claim(&key, 7, ver(1), 100));
        assert!(!refresh.claim(&key, 7, ver(1), 200), "asked already");
        assert!(
            refresh.claim(&key, 7, ver(2), 200),
            "a new version asks again"
        );
        refresh.release(&key, 7, 500);
        assert!(
            !refresh.claim(&key, 7, ver(2), 499),
            "not before the retry time"
        );
        assert!(refresh.claim(&key, 7, ver(2), 500));
    }

    /// A refreshing shard on `clock` whose loader parks until `release`
    /// fires, after telling `entered` it runs, and then holds `v{key}`.
    fn gated_shard(
        clock: &Arc<AtomicU64>,
        entered: &Arc<tokio::sync::Notify>,
        release: &Arc<tokio::sync::Notify>,
    ) -> Shard<u32, String> {
        let now = Arc::clone(clock);
        let (entered, release) = (Arc::clone(entered), Arc::clone(release));
        Shard::new(
            SmolStr::new("test"),
            Mode::Local,
            NodeId::from(1),
            10_000,
            Some(Duration::from_secs(10)),
            None,
        )
        .with_clock(Arc::new(move || now.load(Ordering::SeqCst)))
        .with_loader(Loader::batch(move |keys: Vec<u32>| {
            let (entered, release) = (Arc::clone(&entered), Arc::clone(&release));
            async move {
                entered.notify_one();
                release.notified().await;
                Ok::<_, Down>(
                    keys.into_iter()
                        .map(|key| (key, format!("v{key}")))
                        .collect::<HashMap<_, _>>(),
                )
            }
        }))
        .with_refresh_ahead(800)
    }

    /// Runs one reload of key 1, inserted at `T0` and read due at
    /// `T0 + 9_000`, with `act` run on the shard while its loader call is
    /// parked. Returns the shard, the tally, and the request queue.
    async fn reload_during(
        shard: impl FnOnce(Shard<u32, String>) -> Shard<u32, String>,
        act: impl AsyncFnOnce(&Shard<u32, String>),
    ) -> (
        Arc<Shard<u32, String>>,
        RefreshTally,
        mpsc::Receiver<RefreshRequest<u32>>,
    ) {
        let clock = Arc::new(AtomicU64::new(T0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let s = Arc::new(shard(gated_shard(&clock, &entered, &release)));
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        clock.store(T0 + 9_000, Ordering::SeqCst);
        let _ = s.get(&1).await;
        let request = requests.try_recv().expect("a due read asks");
        let task = {
            let s = Arc::clone(&s);
            tokio::spawn(async move { s.refresh(vec![request]).await })
        };
        entered.notified().await;
        act(&s).await;
        release.notify_one();
        let tally = task.await.expect("refresh task");
        (s, tally, requests)
    }

    fn assert_remaining_over(s: &Shard<u32, String>, floor: Duration) {
        match s.ttl_of(&1) {
            Some(crate::store::Ttl::Remaining(left)) => {
                assert!(left > floor, "kept the lifetime, got {left:?}");
            }
            other => panic!("a remaining lifetime, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn a_touch_does_not_move_the_refresh_point_inside_the_lifetime() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Holds);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        clock.store(T0 + 9_000, Ordering::SeqCst);
        assert!(s.touch(&1).await.expect("touch"));
        // The touch restarts the 10s lifetime: the entry expires at
        // T0 + 19_000 and is due from T0 + 17_000.
        clock.store(T0 + 16_000, Ordering::SeqCst);
        assert_eq!(s.get(&1).await, Some("a".to_string()));
        assert!(
            requests.try_recv().is_err(),
            "a read 7s into a lifetime a touch restarted is not due"
        );
        clock.store(T0 + 17_000, Ordering::SeqCst);
        assert_eq!(s.get(&1).await, Some("a".to_string()));
        assert!(requests.try_recv().is_ok(), "due in the last 2s");
    }

    #[tokio::test]
    async fn an_expire_judges_the_window_by_the_new_expiry() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Holds);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        s.insert(2, "b".to_string()).await.expect("insert");
        clock.store(T0 + 5_000, Ordering::SeqCst);
        assert!(
            s.expire(&1, Duration::from_secs(3_600))
                .await
                .expect("expire")
        );
        assert!(s.expire(&2, Duration::from_secs(1)).await.expect("expire"));
        let _ = s.get(&1).await;
        assert!(
            requests.try_recv().is_err(),
            "an hour left is outside the last 2s of the default TTL"
        );
        let _ = s.get(&2).await;
        assert_eq!(
            requests.try_recv().expect("a second left is inside it").key,
            2,
            "an entry whose lifetime is shorter than the window is due at once"
        );
    }

    #[tokio::test]
    async fn a_leading_clock_does_not_make_every_version_due() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Holds);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.observe_remote(crate::hlc::Hlc {
            wall_ms: T0 + 30_000,
            logical: 0,
            node: NodeId::from(2),
        });
        s.insert(1, "a".to_string()).await.expect("insert");
        assert_eq!(s.get(&1).await, Some("a".to_string()));
        assert!(
            requests.try_recv().is_err(),
            "a read at the moment of the write is not due"
        );
        clock.store(T0 + 8_000, Ordering::SeqCst);
        assert_eq!(s.get(&1).await, Some("a".to_string()));
        assert!(requests.try_recv().is_ok(), "due 2s before the expiry");
    }

    #[tokio::test]
    async fn a_get_or_load_hit_past_the_refresh_point_asks_once_per_version() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Holds);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        let own = async |_: &u32| Ok::<_, Down>("own".to_string());

        clock.store(T0 + 7_999, Ordering::SeqCst);
        assert_eq!(s.get_or_load(&1, own).await.expect("hit"), "a");
        assert!(requests.try_recv().is_err(), "not due before 80% of 10s");

        clock.store(T0 + 8_000, Ordering::SeqCst);
        assert_eq!(s.get_or_load(&1, own).await.expect("hit"), "a");
        let request = requests.try_recv().expect("a due hit asks");
        assert_eq!(request.key, 1);
        assert_eq!(
            s.get_or_insert_with(&1, async |_| "own".to_string())
                .await
                .expect("hit"),
            "a"
        );
        assert!(
            requests.try_recv().is_err(),
            "every later read of that version asks nothing"
        );
    }

    #[tokio::test]
    async fn a_hinted_request_is_not_judged_again_on_the_targets_clock() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Holds);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        let key_bytes = encode_key(&1u32).expect("encodes");
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        let (held, _) = s
            .engine
            .lifetime_of(&key_bytes, hash, T0)
            .expect("live entry");
        // The hinter read the entry past its refresh point; this node's
        // clock is still seconds short of it.
        clock.store(T0 + 1_000, Ordering::SeqCst);
        s.take_refresh_hint(&key_bytes, held);
        let request = requests.try_recv().expect("the hint queues a reload");
        assert!(request.hinted);

        let tally = s.refresh(vec![request]).await;
        assert_eq!(tally.loaded, 1, "{tally:?}");
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert_eq!(s.get(&1).await, Some("v1.1".to_string()));
    }

    #[tokio::test]
    async fn a_request_skipped_as_not_due_lets_a_read_ask_again_after_the_retry_delay() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Holds);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        clock.store(T0 + 8_000, Ordering::SeqCst);
        let _ = s.get(&1).await;
        let request = requests.try_recv().expect("a due read asks");

        // The pass runs on a clock behind the read's: not due there.
        clock.store(T0 + 1_000, Ordering::SeqCst);
        assert_eq!(s.refresh(vec![request]).await, RefreshTally::default());
        assert_eq!(calls.load(Ordering::SeqCst), 0, "no loader call");
        clock.store(T0 + 1_000 + REFRESH_RETRY_MS, Ordering::SeqCst);
        // Not due yet at this clock, so a read asks nothing; once the
        // clock reaches the window the released claim lets it ask.
        let _ = s.get(&1).await;
        assert!(requests.try_recv().is_err());
        clock.store(T0 + 8_000, Ordering::SeqCst);
        let _ = s.get(&1).await;
        assert!(
            requests.try_recv().is_ok(),
            "the skip released the claim, so the next due read asks"
        );
    }

    #[tokio::test]
    async fn a_request_skipped_for_a_load_in_flight_lets_a_read_ask_again_after_the_retry_delay() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Holds);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        clock.store(T0 + 8_000, Ordering::SeqCst);
        let _ = s.get(&1).await;
        let request = requests.try_recv().expect("a due read asks");
        let key_bytes = encode_key(&1u32).expect("encodes");
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        let busy = s
            .engine
            .begin_refresh(&key_bytes, hash)
            .expect("no load in flight yet");

        assert_eq!(s.refresh(vec![request]).await, RefreshTally::default());
        assert_eq!(calls.load(Ordering::SeqCst), 0, "no loader call");
        let _ = s.get(&1).await;
        assert!(requests.try_recv().is_err(), "no retry before the delay");
        clock.store(T0 + 8_000 + REFRESH_RETRY_MS, Ordering::SeqCst);
        let _ = s.get(&1).await;
        assert!(requests.try_recv().is_ok(), "a read after the delay asks");
        s.engine.abandon_inflight(&key_bytes, hash, &busy);
    }

    #[tokio::test]
    async fn a_read_asks_again_after_a_hint_sent_for_its_version_goes_unanswered() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Holds);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        let key_bytes = encode_key(&1u32).expect("encodes");
        clock.store(T0 + 8_000, Ordering::SeqCst);
        let _ = s.get(&1).await;
        let request = requests.try_recv().expect("a due read asks");
        s.note_hint_sent(&request.key_bytes);

        clock.store(T0 + 8_000 + REFRESH_HINT_RETRY_MS - 1, Ordering::SeqCst);
        let _ = s.get(&1).await;
        assert!(requests.try_recv().is_err(), "no retry before the deadline");
        clock.store(T0 + 8_000 + REFRESH_HINT_RETRY_MS, Ordering::SeqCst);
        let _ = s.get(&1).await;
        let again = requests.try_recv().expect("a read at the deadline asks");
        assert_eq!(again.key_bytes, key_bytes);
    }

    #[tokio::test]
    async fn a_persist_during_a_reload_survives_it() {
        let (s, tally, _) = reload_during(
            |s| s,
            async |s| {
                assert!(s.persist(&1).await.expect("persist"));
            },
        )
        .await;
        assert_eq!(s.ttl_of(&1), Some(crate::store::Ttl::Never));
        assert_eq!(s.get(&1).await, Some("a".to_string()));
        assert_eq!((tally.loaded, tally.discarded), (0, 1), "{tally:?}");
    }

    #[tokio::test]
    async fn an_expire_during_a_reload_survives_it() {
        let (s, tally, _) = reload_during(
            |s| s,
            async |s| {
                assert!(
                    s.expire(&1, Duration::from_secs(3_600))
                        .await
                        .expect("expire")
                );
            },
        )
        .await;
        assert_remaining_over(&s, Duration::from_secs(1_000));
        assert_eq!((tally.loaded, tally.discarded), (0, 1), "{tally:?}");
    }

    #[tokio::test]
    async fn a_touch_during_a_reload_survives_it() {
        let (s, tally, _) = reload_during(
            |s| s,
            async |s| {
                assert!(s.touch(&1).await.expect("touch"));
            },
        )
        .await;
        assert_eq!(s.get(&1).await, Some("a".to_string()));
        assert_eq!((tally.loaded, tally.discarded), (0, 1), "{tally:?}");
    }

    #[tokio::test]
    async fn a_removal_during_a_reload_is_not_resurrected_by_it() {
        let (s, tally, _) = reload_during(
            |s| s,
            async |s| {
                s.remove(&1).await.expect("remove");
            },
        )
        .await;
        assert_eq!(s.get(&1).await, None, "the removed key stays removed");
        assert_eq!((tally.loaded, tally.discarded), (0, 1), "{tally:?}");
    }

    #[tokio::test]
    async fn a_write_during_a_reload_wins_and_the_reload_counts_as_discarded() {
        let (s, tally, _) = reload_during(
            |s| s,
            async |s| {
                s.insert(1, "b".to_string()).await.expect("insert");
            },
        )
        .await;
        assert_eq!(s.get(&1).await, Some("b".to_string()), "the write wins");
        assert_eq!((tally.loaded, tally.discarded), (0, 1), "{tally:?}");
    }

    #[tokio::test]
    async fn a_reload_nothing_overtook_is_stored_and_counted_loaded() {
        let (s, tally, _) = reload_during(|s| s, async |_| {}).await;
        assert_eq!(s.get(&1).await, Some("v1".to_string()));
        assert_eq!((tally.loaded, tally.discarded), (1, 0), "{tally:?}");
    }

    #[tokio::test]
    async fn a_reload_the_ceiling_refused_is_discarded_and_asked_for_again_after_the_retry_delay() {
        let clock = Arc::new(AtomicU64::new(T0));
        let calls = Arc::new(AtomicU64::new(0));
        let s = refreshing_shard(&clock, &calls, Source::Holds).with_max_resident_bytes(1);
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        clock.store(T0 + 8_000, Ordering::SeqCst);
        let _ = s.get(&1).await;
        let request = requests.try_recv().expect("a due read asks");

        let tally = s.refresh(vec![request]).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1, "the loader ran");
        assert_eq!(s.get(&1).await, Some("a".to_string()), "nothing was stored");
        assert_eq!((tally.loaded, tally.discarded), (0, 1), "{tally:?}");
        assert!(requests.try_recv().is_err(), "no retry before the delay");
        clock.store(T0 + 8_000 + REFRESH_RETRY_MS, Ordering::SeqCst);
        let _ = s.get(&1).await;
        assert!(
            requests.try_recv().is_ok(),
            "the refused reload released its claim for a retry"
        );
    }

    /// Dropping a pass that waits on a hung loader frees its in-flight
    /// entries, so a later reload of the key is not turned away as already
    /// loading.
    #[tokio::test]
    async fn dropping_a_refresh_pass_mid_loader_frees_its_in_flight_entries() {
        let clock = Arc::new(AtomicU64::new(T0));
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let s = Arc::new(gated_shard(&clock, &entered, &release));
        let mut requests = s.take_refresh_requests().expect("refresh-ahead is on");
        s.insert(1, "a".to_string()).await.expect("insert");
        clock.store(T0 + 9_000, Ordering::SeqCst);
        let _ = s.get(&1).await;
        let request = requests.try_recv().expect("a due read asks");
        let key_bytes = request.key_bytes.clone();
        let hash = engine::hash_key_bytes(key_bytes.as_ref());
        let task = {
            let s = Arc::clone(&s);
            tokio::spawn(async move { s.refresh(vec![request]).await })
        };
        entered.notified().await;
        assert!(
            s.engine.begin_refresh(&key_bytes, hash).is_none(),
            "the parked reload holds the key"
        );

        task.abort();
        assert!(task.await.expect_err("aborted").is_cancelled());
        let freed = s
            .engine
            .begin_refresh(&key_bytes, hash)
            .expect("the dropped pass released the key");
        s.engine.abandon_inflight(&key_bytes, hash, &freed);
        assert_eq!(s.get(&1).await, Some("a".to_string()));
    }
}
