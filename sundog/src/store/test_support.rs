//! Constructors shared by the store's test modules: `engine`, `crdt::pn_counter`,
//! and `crdt::or_set` each built their own `hlc`/`wid` before this module
//! existed; `pn_counter` and `or_set` now share the ones here.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::hlc::Hlc;
use crate::node::NodeId;
use crate::store::crdt::WriterId;

/// An [`Hlc`] at `wall_ms`/`logical`, node fixed to `1`.
pub(crate) fn hlc(wall_ms: u64, logical: u32) -> Hlc {
    Hlc {
        wall_ms,
        logical,
        node: NodeId::from(1),
    }
}

/// A [`WriterId`] for `node` at `incarnation`.
pub(crate) fn wid(node: u64, incarnation: u64) -> WriterId {
    WriterId::new(NodeId::from(node), incarnation)
}

/// A gauge's name and `cache` label.
type GaugeKey = (String, Option<String>);

/// A recorder that keeps the last value each gauge was set to, read
/// without clearing it, which `DebuggingRecorder`'s snapshot does.
#[derive(Clone, Default)]
pub(crate) struct Gauges(Arc<Mutex<HashMap<GaugeKey, f64>>>);

struct StoredGauge {
    key: GaugeKey,
    values: Gauges,
}

impl metrics::GaugeFn for StoredGauge {
    fn increment(&self, value: f64) {
        *self
            .values
            .0
            .lock()
            .expect("lock")
            .entry(self.key.clone())
            .or_default() += value;
    }

    fn decrement(&self, value: f64) {
        self.increment(-value);
    }

    fn set(&self, value: f64) {
        self.values
            .0
            .lock()
            .expect("lock")
            .insert(self.key.clone(), value);
    }
}

impl metrics::Recorder for Gauges {
    fn describe_counter(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_gauge(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }
    fn describe_histogram(
        &self,
        _: metrics::KeyName,
        _: Option<metrics::Unit>,
        _: metrics::SharedString,
    ) {
    }

    fn register_counter(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::noop()
    }

    fn register_gauge(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        let cache = key
            .labels()
            .find(|label| label.key() == "cache")
            .map(|label| label.value().to_owned());
        metrics::Gauge::from_arc(Arc::new(StoredGauge {
            key: (key.name().to_owned(), cache),
            values: self.clone(),
        }))
    }

    fn register_histogram(
        &self,
        _: &metrics::Key,
        _: &metrics::Metadata<'_>,
    ) -> metrics::Histogram {
        metrics::Histogram::noop()
    }
}

impl Gauges {
    /// The last value of the gauge `name`, `cache`-labelled when `cache` is set.
    pub(crate) fn get(&self, name: &str, cache: Option<&str>) -> Option<f64> {
        self.0
            .lock()
            .expect("lock")
            .get(&(name.to_owned(), cache.map(str::to_owned)))
            .copied()
    }
}
