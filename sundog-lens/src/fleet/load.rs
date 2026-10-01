//! The load driver: operations against the fleet over control connections.
//!
//! One worker per node holds a pipelined connection to the node's control
//! port and sends a mix of reads, remote fetches, writes, deletes and churn
//! at a rate that swells and ebbs per node. A worker whose connection fails
//! parks and reconnects when the node is back; the driver never panics.
//! The rate, the operation mix and the key distribution are pure and tested.

use std::collections::HashMap;
use std::f64::consts::TAU;
use std::net::SocketAddr;
use std::time::Duration;

use rand::RngExt as _;
use rand::rngs::StdRng;
use smol_str::SmolStr;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Instant, MissedTickBehavior};

use super::control::Pipeline;

/// The period of the rate's swell.
pub const SWELL_PERIOD: Duration = Duration::from_secs(20);

/// How far the rate swells above and ebbs below its base, as a fraction.
pub const SWELL_DEPTH: f64 = 0.4;

/// The time between sends of one worker.
pub const SEND_TICK: Duration = Duration::from_millis(20);

/// How long a worker waits before it tries a failed connection again.
pub const RECONNECT_AFTER: Duration = Duration::from_millis(500);

/// The exponent of the key popularity.
pub const ZIPF_EXPONENT: f64 = 1.1;

/// Writes churn this many operations per `churn` command.
pub const CHURN_OPS: u32 = 64;

/// The operations per second at `elapsed` seconds into a run: `base` swelling
/// by [`SWELL_DEPTH`] over [`SWELL_PERIOD`] around `phase` radians, times
/// `burst`. Never negative.
#[must_use]
pub fn rate_at(elapsed: f64, base: f64, phase: f64, burst: f64) -> f64 {
    let swell = 1.0 + SWELL_DEPTH * (TAU * elapsed / SWELL_PERIOD.as_secs_f64() + phase).sin();
    (base * swell * burst).max(0.0)
}

/// A key popularity table: key `i` (0-based) is drawn with weight
/// `1 / (i + 1)^s`. A precomputed cumulative distribution and a binary
/// search draw a key without a new dependency.
#[derive(Debug, Clone, PartialEq)]
pub struct ZipfTable {
    cdf: Vec<f64>,
}

impl ZipfTable {
    /// The table for `keys` keys with exponent `s`. At least one key.
    #[must_use]
    pub fn new(keys: u64, s: f64) -> Self {
        let keys = keys.max(1);
        let mut total = 0.0;
        let mut cdf = Vec::new();
        for rank in 1..=keys {
            #[expect(clippy::cast_precision_loss, reason = "ranks stay far below 2^52")]
            let weight = (rank as f64).powf(-s);
            total += weight;
            cdf.push(total);
        }
        for entry in &mut cdf {
            *entry /= total;
        }
        Self { cdf }
    }

    /// How many keys the table covers.
    #[must_use]
    pub fn keys(&self) -> u64 {
        self.cdf.len() as u64
    }

    /// The key at cumulative probability `u`, from 0 up to but not including
    /// 1.
    #[must_use]
    pub fn sample(&self, u: f64) -> u64 {
        let index = self.cdf.partition_point(|&p| p <= u);
        index.min(self.cdf.len() - 1) as u64
    }
}

/// One control command of the load.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Op {
    /// `get k{key}`: a local read.
    Get(u64),
    /// `fetch k{key}`: a read that goes to the owner.
    Fetch(u64),
    /// `put k{key} v{n}`.
    Put(u64, u64),
    /// `del k{key}`.
    Del(u64),
    /// `churn 64`: writes on the replicated side cache.
    Churn,
}

impl Op {
    /// The control line of the operation.
    #[must_use]
    pub fn line(&self) -> String {
        match self {
            Self::Get(key) => format!("get k{key}"),
            Self::Fetch(key) => format!("fetch k{key}"),
            Self::Put(key, n) => format!("put k{key} v{n}"),
            Self::Del(key) => format!("del k{key}"),
            Self::Churn => format!("churn {CHURN_OPS}"),
        }
    }
}

/// Picks the next operation: 55% `get` and 20% `fetch` of a popular key, 15%
/// `put` and 5% `del` of a uniform key, 5% `churn`. `n` numbers the value of
/// a `put`.
pub fn pick_op<R: rand::Rng>(rng: &mut R, zipf: &ZipfTable, n: u64) -> Op {
    let roll: f64 = rng.random();
    if roll < 0.55 {
        Op::Get(zipf.sample(rng.random()))
    } else if roll < 0.75 {
        Op::Fetch(zipf.sample(rng.random()))
    } else if roll < 0.90 {
        Op::Put(rng.random_range(0..zipf.keys()), n)
    } else if roll < 0.95 {
        Op::Del(rng.random_range(0..zipf.keys()))
    } else {
        Op::Churn
    }
}

/// How many whole operations are due after `carry` fractional ones plus
/// `rate` operations per second over `dt`, and the fraction that remains.
#[must_use]
pub fn due(carry: f64, rate: f64, dt: Duration) -> (u64, f64) {
    let total = carry + rate * dt.as_secs_f64();
    let whole = total.floor().max(0.0);
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "floored and clamped at zero"
    )]
    let count = whole as u64;
    (count, total - whole)
}

/// The phase of the node in slot `slot` (1-based): nodes swell out of step.
#[must_use]
pub fn phase_of(slot: usize) -> f64 {
    #[expect(clippy::cast_precision_loss, reason = "slots are single digits")]
    let slot = slot as f64;
    slot * 1.3
}

enum Command {
    Add(SmolStr, SocketAddr, f64),
    Remove(SmolStr),
}

/// A handle on the running load driver. Cheap to clone.
#[derive(Debug, Clone)]
pub struct LoadHandle {
    commands: mpsc::UnboundedSender<Command>,
    running: watch::Sender<bool>,
}

impl LoadHandle {
    /// Starts the load at every node it knows.
    pub fn start(&self) {
        self.running.send_replace(true);
    }

    /// Stops the load; the nodes stay known.
    pub fn stop(&self) {
        self.running.send_replace(false);
    }

    /// Whether the load runs.
    #[must_use]
    pub fn is_running(&self) -> bool {
        *self.running.borrow()
    }

    /// Adds the node `label` at control address `control`, whose swell is out
    /// of step by `phase` radians. A node already known under the label is
    /// replaced: this is how a restarted node gets a fresh connection.
    pub fn add(&self, label: impl Into<SmolStr>, control: SocketAddr, phase: f64) {
        let _ = self
            .commands
            .send(Command::Add(label.into(), control, phase));
    }

    /// Forgets the node `label`.
    pub fn remove(&self, label: impl Into<SmolStr>) {
        let _ = self.commands.send(Command::Remove(label.into()));
    }
}

/// The load driver: owns the workers.
#[derive(Debug)]
pub struct Load {
    driver: JoinHandle<()>,
}

impl Load {
    /// Starts a driver that, once started through the handle, sends `rate`
    /// operations per second to each node over `keys` keys.
    #[must_use]
    pub fn spawn(rate: u64, keys: u64) -> (Self, LoadHandle) {
        let (commands, receiver) = mpsc::unbounded_channel();
        let (running, _) = watch::channel(false);
        let handle = LoadHandle {
            commands,
            running: running.clone(),
        };
        let driver = tokio::spawn(drive(rate, keys, running, receiver));
        (Self { driver }, handle)
    }

    /// Stops the driver and every worker.
    pub fn shutdown(self) {
        self.driver.abort();
    }
}

impl Drop for Load {
    fn drop(&mut self) {
        self.driver.abort();
    }
}

/// Holds the workers; aborts them all when the driver stops.
struct Workers(HashMap<SmolStr, JoinHandle<()>>);

impl Drop for Workers {
    fn drop(&mut self) {
        for worker in self.0.values() {
            worker.abort();
        }
    }
}

async fn drive(
    rate: u64,
    keys: u64,
    running: watch::Sender<bool>,
    mut commands: mpsc::UnboundedReceiver<Command>,
) {
    #[expect(clippy::cast_precision_loss, reason = "a rate is far below 2^52")]
    let base = rate as f64;
    let zipf = std::sync::Arc::new(ZipfTable::new(keys, ZIPF_EXPONENT));
    let mut workers = Workers(HashMap::new());
    while let Some(command) = commands.recv().await {
        match command {
            Command::Add(label, control, phase) => {
                if let Some(old) = workers.0.remove(&label) {
                    old.abort();
                }
                let worker = tokio::spawn(work(
                    control,
                    base,
                    phase,
                    std::sync::Arc::clone(&zipf),
                    running.subscribe(),
                ));
                workers.0.insert(label, worker);
            }
            Command::Remove(label) => {
                if let Some(old) = workers.0.remove(&label) {
                    old.abort();
                }
            }
        }
    }
}

/// One node's worker: waits for the load to run, connects, and sends until
/// the load stops or the connection fails; then parks and starts over.
async fn work(
    control: SocketAddr,
    base: f64,
    phase: f64,
    zipf: std::sync::Arc<ZipfTable>,
    mut running: watch::Receiver<bool>,
) {
    let mut rng: StdRng = rand::make_rng();
    let mut counter = 0u64;
    loop {
        while !*running.borrow_and_update() {
            if running.changed().await.is_err() {
                return;
            }
        }
        let Ok(mut pipe) = Pipeline::connect(control).await else {
            tokio::time::sleep(RECONNECT_AFTER).await;
            continue;
        };
        let started = Instant::now();
        let mut last = started;
        let mut carry = 0.0;
        let mut tick = tokio::time::interval(SEND_TICK);
        tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                changed = running.changed() => {
                    if changed.is_err() {
                        return;
                    }
                    if !*running.borrow() {
                        break;
                    }
                    continue;
                }
            }
            let now = Instant::now();
            let rate = rate_at(now.duration_since(started).as_secs_f64(), base, phase, 1.0);
            let (count, rest) = due(carry, rate, now.duration_since(last));
            carry = rest;
            last = now;
            let ops: Vec<String> = (0..count)
                .map(|_| {
                    counter += 1;
                    pick_op(&mut rng, &zipf, counter).line()
                })
                .collect();
            if pipe.send_all(ops).await.is_err() || pipe.is_closed() {
                tokio::time::sleep(RECONNECT_AFTER).await;
                break;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use rand::SeedableRng as _;
    use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};
    use tokio::net::TcpListener;

    use super::*;

    #[test]
    fn the_rate_swells_around_its_base_within_the_depth() {
        let base = 1500.0;
        for step in 0..400 {
            let t = f64::from(step) * 0.1;
            for phase in [0.0, 1.3, 2.6, 5.2] {
                let rate = rate_at(t, base, phase, 1.0);
                assert!(
                    rate >= base * (1.0 - SWELL_DEPTH) - 1e-9
                        && rate <= base * (1.0 + SWELL_DEPTH) + 1e-9,
                    "{rate} at {t} s phase {phase}"
                );
            }
        }
    }

    #[test]
    fn the_rate_starts_at_the_base_and_repeats_each_period() {
        assert!((rate_at(0.0, 1000.0, 0.0, 1.0) - 1000.0).abs() < 1e-9);
        let period = SWELL_PERIOD.as_secs_f64();
        assert!(
            (rate_at(3.7, 1000.0, 0.8, 1.0) - rate_at(3.7 + period, 1000.0, 0.8, 1.0)).abs() < 1e-6
        );
        // A quarter period in, the swell peaks.
        assert!((rate_at(period / 4.0, 1000.0, 0.0, 1.0) - 1400.0).abs() < 1e-6);
    }

    #[test]
    fn a_burst_scales_the_rate_and_it_never_goes_negative() {
        assert!((rate_at(0.0, 1000.0, 0.0, 2.0) - 2000.0).abs() < 1e-9);
        assert!(rate_at(0.0, 1000.0, 0.0, 0.0).abs() < f64::EPSILON);
        assert!(rate_at(0.0, 1000.0, 0.0, -3.0).abs() < f64::EPSILON);
        assert!(rate_at(5.0, 0.0, 0.0, 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn nodes_swell_out_of_step() {
        let phases: Vec<f64> = (1..=6).map(phase_of).collect();
        for pair in phases.windows(2) {
            assert!(pair[1] > pair[0]);
        }
        assert!((phase_of(1) - 1.3).abs() < 1e-12);
    }

    #[test]
    fn the_zipf_cdf_is_monotone_and_ends_at_one() {
        let table = ZipfTable::new(1000, ZIPF_EXPONENT);
        assert_eq!(table.keys(), 1000);
        assert!(table.cdf.windows(2).all(|pair| pair[1] > pair[0]));
        assert!((table.cdf[999] - 1.0).abs() < 1e-12);
        assert!(table.cdf[0] > 0.0);
    }

    #[test]
    fn a_zipf_draw_stays_in_range_and_favors_the_first_keys() {
        let table = ZipfTable::new(1000, ZIPF_EXPONENT);
        assert_eq!(table.sample(0.0), 0);
        assert!(table.sample(0.999_999_999) < 1000);
        assert!(table.sample(1.0) < 1000, "even u = 1 stays in range");
        let mut rng = StdRng::seed_from_u64(7);
        let mut hits = vec![0u32; 1000];
        for _ in 0..100_000 {
            hits[usize::try_from(table.sample(rng.random())).unwrap()] += 1;
        }
        assert!(hits[0] > hits[1] && hits[1] > hits[9]);
        let head: u32 = hits[..10].iter().sum();
        let tail: u32 = hits[500..].iter().sum();
        assert!(head > tail, "{head} vs {tail}");
        // Key 0's share is 1 / H(1000, 1.1), about 0.19.
        assert!(f64::from(hits[0]) / 100_000.0 > 0.15);
    }

    #[test]
    fn a_table_of_no_keys_still_has_one() {
        let table = ZipfTable::new(0, ZIPF_EXPONENT);
        assert_eq!(table.keys(), 1);
        assert_eq!(table.sample(0.7), 0);
    }

    #[test]
    fn each_operation_is_a_control_line() {
        assert_eq!(Op::Get(5).line(), "get k5");
        assert_eq!(Op::Fetch(6).line(), "fetch k6");
        assert_eq!(Op::Put(7, 8).line(), "put k7 v8");
        assert_eq!(Op::Del(9).line(), "del k9");
        assert_eq!(Op::Churn.line(), "churn 64");
    }

    #[test]
    fn the_operation_mix_holds_over_one_hundred_thousand_draws() {
        let table = ZipfTable::new(20_000, ZIPF_EXPONENT);
        let mut rng = StdRng::seed_from_u64(42);
        let mut counts = [0u32; 5];
        for n in 0..100_000 {
            let index = match pick_op(&mut rng, &table, n) {
                Op::Get(_) => 0,
                Op::Fetch(_) => 1,
                Op::Put(..) => 2,
                Op::Del(_) => 3,
                Op::Churn => 4,
            };
            counts[index] += 1;
        }
        for (count, want) in counts.iter().zip([0.55, 0.20, 0.15, 0.05, 0.05]) {
            let share = f64::from(*count) / 100_000.0;
            assert!((share - want).abs() < 0.01, "{share} vs {want}: {counts:?}");
        }
    }

    #[test]
    fn writes_pick_uniform_keys_and_every_key_is_in_range() {
        let table = ZipfTable::new(50, ZIPF_EXPONENT);
        let mut rng = StdRng::seed_from_u64(3);
        let mut written = [0u32; 50];
        for n in 0..50_000 {
            match pick_op(&mut rng, &table, n) {
                Op::Get(k) | Op::Fetch(k) | Op::Del(k) => assert!(k < 50),
                Op::Put(k, v) => {
                    assert!(k < 50);
                    assert_eq!(v, n);
                    written[usize::try_from(k).unwrap()] += 1;
                }
                Op::Churn => {}
            }
        }
        // About 7,500 puts over 50 keys: 150 each, give or take five sigma.
        let min = *written.iter().min().unwrap();
        let max = *written.iter().max().unwrap();
        assert!(min >= 90 && max <= 210, "{min}..{max}");
    }

    #[test]
    fn due_operations_carry_their_fraction() {
        let tick = Duration::from_millis(20);
        let (count, rest) = due(0.0, 1000.0, tick);
        assert_eq!(count, 20);
        assert!(rest.abs() < 1e-9);
        // 90 ops/s is 1.8 a tick: 1, then 2 (1.6), 1 (2.4 -> 2 rest .4)...
        let mut carry = 0.0;
        let mut total = 0;
        for _ in 0..50 {
            let (count, rest) = due(carry, 90.0, tick);
            carry = rest;
            total += count;
        }
        assert_eq!(total, 90);
        assert_eq!(due(0.0, 0.0, tick), (0, 0.0));
        assert_eq!(due(0.4, 10.0, Duration::ZERO), (0, 0.4));
    }

    /// A node that counts the lines it gets and answers `ok` to each.
    async fn counting_node() -> (SocketAddr, std::sync::Arc<std::sync::atomic::AtomicU64>) {
        use std::sync::atomic::{AtomicU64, Ordering};
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let seen = std::sync::Arc::new(AtomicU64::new(0));
        let counter = std::sync::Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let counter = std::sync::Arc::clone(&counter);
                tokio::spawn(async move {
                    let (reader, mut writer) = socket.into_split();
                    let mut lines = BufReader::new(reader).lines();
                    while let Ok(Some(_)) = lines.next_line().await {
                        counter.fetch_add(1, Ordering::Relaxed);
                        if writer.write_all(b"ok\n").await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (addr, seen)
    }

    async fn eventually(mut done: impl FnMut() -> bool) {
        for _ in 0..300 {
            if done() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        panic!("the condition did not hold within 7.5 s");
    }

    #[tokio::test]
    async fn the_load_sends_only_while_started_and_follows_a_node_that_returns() {
        use std::sync::atomic::Ordering;
        let (addr, seen) = counting_node().await;
        let (load, handle) = Load::spawn(500, 1000);
        assert!(!handle.is_running());
        handle.add("n1", addr, 0.0);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            seen.load(Ordering::Relaxed),
            0,
            "nothing flows before start"
        );

        handle.start();
        assert!(handle.is_running());
        eventually(|| seen.load(Ordering::Relaxed) > 50).await;

        handle.stop();
        assert!(!handle.is_running());
        tokio::time::sleep(Duration::from_millis(300)).await;
        let held = seen.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            seen.load(Ordering::Relaxed),
            held,
            "nothing flows after stop"
        );

        handle.start();
        eventually(|| seen.load(Ordering::Relaxed) > held + 50).await;

        // Removing the node ends its traffic.
        handle.remove("n1");
        tokio::time::sleep(Duration::from_millis(300)).await;
        let after = seen.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(seen.load(Ordering::Relaxed), after);
        load.shutdown();
    }

    #[tokio::test]
    async fn a_node_that_is_not_there_yet_is_retried_until_it_listens() {
        use std::sync::atomic::Ordering;
        // Reserve a port, free it, and bring the node up on it later.
        let reserved = {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            listener.local_addr().unwrap()
        };
        let (load, handle) = Load::spawn(500, 100);
        handle.add("n2", reserved, 1.3);
        handle.start();
        tokio::time::sleep(Duration::from_millis(700)).await;

        let listener = TcpListener::bind(reserved).await.unwrap();
        let seen = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let counter = std::sync::Arc::clone(&seen);
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let counter = std::sync::Arc::clone(&counter);
                tokio::spawn(async move {
                    let (reader, mut writer) = socket.into_split();
                    let mut lines = BufReader::new(reader).lines();
                    while let Ok(Some(_)) = lines.next_line().await {
                        counter.fetch_add(1, Ordering::Relaxed);
                        let _ = writer.write_all(b"ok\n").await;
                    }
                });
            }
        });
        eventually(|| seen.load(Ordering::Relaxed) > 20).await;
        load.shutdown();
    }

    #[tokio::test]
    async fn re_adding_a_label_replaces_its_connection() {
        use std::sync::atomic::Ordering;
        let (first, first_seen) = counting_node().await;
        let (second, second_seen) = counting_node().await;
        let (load, handle) = Load::spawn(500, 100);
        handle.start();
        handle.add("n1", first, 0.0);
        eventually(|| first_seen.load(Ordering::Relaxed) > 20).await;
        handle.add("n1", second, 0.0);
        eventually(|| second_seen.load(Ordering::Relaxed) > 20).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
        let held = first_seen.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert_eq!(
            first_seen.load(Ordering::Relaxed),
            held,
            "the old worker is gone"
        );
        load.shutdown();
    }
}
