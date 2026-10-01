//! Time series: a reset-aware counter rate and a fixed-size ring of samples.

use std::time::Instant;

/// The number of samples a chart keeps: three minutes at one sample a second.
pub const RING_LEN: usize = 180;

/// Turns a monotonic counter's samples into rates.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct CounterTrack {
    last: Option<(Instant, f64)>,
}

impl CounterTrack {
    /// A track that has seen no sample.
    #[must_use]
    pub const fn new() -> Self {
        Self { last: None }
    }

    /// Records a sample of `value` at `t` and returns the rate per second
    /// since the previous sample. The first sample has no rate. A sample at or
    /// before the previous time has none either and is dropped. A value below
    /// the previous one is a counter reset (the node restarted): it has no
    /// rate, and the track continues from it.
    pub fn observe(&mut self, t: Instant, value: f64) -> Option<f64> {
        let Some((last_t, last_value)) = self.last else {
            self.last = Some((t, value));
            return None;
        };
        let dt = t.checked_duration_since(last_t)?.as_secs_f64();
        if dt <= 0.0 {
            return None;
        }
        self.last = Some((t, value));
        (value >= last_value).then(|| (value - last_value) / dt)
    }

    /// The last sample's value.
    #[must_use]
    pub fn last_value(&self) -> Option<f64> {
        self.last.map(|(_, value)| value)
    }
}

/// The newest `N` samples of a series, oldest dropped first.
#[derive(Debug, Clone, PartialEq)]
pub struct Ring<const N: usize> {
    samples: Vec<f64>,
    /// Where the next sample goes once the ring is full: the oldest slot.
    head: usize,
}

impl<const N: usize> Default for Ring<N> {
    fn default() -> Self {
        Self::new()
    }
}

impl<const N: usize> Ring<N> {
    /// An empty ring.
    #[must_use]
    pub const fn new() -> Self {
        Self {
            samples: Vec::new(),
            head: 0,
        }
    }

    /// Appends `value`, replacing the oldest sample once `N` are held.
    pub fn push(&mut self, value: f64) {
        if N == 0 {
            return;
        }
        if self.samples.len() < N {
            self.samples.push(value);
        } else {
            self.samples[self.head] = value;
            self.head = (self.head + 1) % N;
        }
    }

    /// How many samples the ring holds.
    #[must_use]
    pub fn len(&self) -> usize {
        self.samples.len()
    }

    /// Whether the ring holds no sample.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.samples.is_empty()
    }

    /// The newest sample.
    #[must_use]
    pub fn last(&self) -> Option<f64> {
        if self.samples.is_empty() {
            return None;
        }
        let newest = (self.head + self.samples.len() - 1) % self.samples.len();
        Some(self.samples[newest])
    }

    /// The samples, oldest first.
    pub fn iter(&self) -> impl Iterator<Item = f64> + '_ {
        let (newer, older) = self.samples.split_at(self.head);
        older.iter().chain(newer).copied()
    }

    /// The samples, oldest first, as a vector.
    #[must_use]
    pub fn to_vec(&self) -> Vec<f64> {
        self.iter().collect()
    }

    /// The largest sample, or 0 for an empty ring.
    #[must_use]
    pub fn max(&self) -> f64 {
        self.iter().filter(|v| v.is_finite()).fold(0.0, f64::max)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    fn at(base: Instant, secs: u64) -> Instant {
        base + Duration::from_secs(secs)
    }

    #[test]
    fn the_first_sample_has_no_rate() {
        let base = Instant::now();
        let mut track = CounterTrack::new();
        assert_eq!(track.observe(base, 100.0), None);
        assert_eq!(track.last_value(), Some(100.0));
    }

    #[test]
    fn a_steady_counter_gives_a_steady_rate() {
        let base = Instant::now();
        let mut track = CounterTrack::new();
        track.observe(base, 100.0);
        assert_eq!(track.observe(at(base, 1), 150.0), Some(50.0));
        assert_eq!(track.observe(at(base, 2), 200.0), Some(50.0));
        assert_eq!(track.observe(at(base, 4), 300.0), Some(50.0));
        assert_eq!(track.observe(at(base, 5), 300.0), Some(0.0));
    }

    #[test]
    fn a_reset_has_no_rate_and_the_track_continues_from_it() {
        let base = Instant::now();
        let mut track = CounterTrack::new();
        track.observe(base, 1000.0);
        assert_eq!(track.observe(at(base, 1), 10.0), None);
        assert_eq!(track.observe(at(base, 2), 30.0), Some(20.0));
    }

    #[test]
    fn a_zero_interval_has_no_rate_and_keeps_the_previous_sample() {
        let base = Instant::now();
        let mut track = CounterTrack::new();
        track.observe(base, 10.0);
        assert_eq!(track.observe(base, 99.0), None);
        assert_eq!(track.last_value(), Some(10.0));
        assert_eq!(track.observe(at(base, 2), 30.0), Some(10.0));
    }

    #[test]
    fn a_sample_from_the_past_has_no_rate() {
        let base = Instant::now();
        let mut track = CounterTrack::new();
        track.observe(at(base, 5), 10.0);
        assert_eq!(track.observe(base, 20.0), None);
        assert_eq!(track.last_value(), Some(10.0));
    }

    #[test]
    fn a_ring_holds_samples_oldest_first_until_full() {
        let mut ring = Ring::<4>::new();
        assert!(ring.is_empty());
        assert_eq!(ring.last(), None);
        for v in [1.0, 2.0, 3.0] {
            ring.push(v);
        }
        assert_eq!(ring.len(), 3);
        assert_eq!(ring.to_vec(), [1.0, 2.0, 3.0]);
        assert_eq!(ring.last(), Some(3.0));
    }

    #[test]
    fn a_ring_wraps_and_drops_the_oldest() {
        let mut ring = Ring::<3>::new();
        for v in 1..=7 {
            ring.push(f64::from(v));
            let expected: Vec<f64> = ((v - 2).max(1)..=v).map(f64::from).collect();
            assert_eq!(ring.to_vec(), expected);
            assert_eq!(ring.last(), Some(f64::from(v)));
        }
        assert_eq!(ring.len(), 3);
    }

    #[test]
    fn a_ring_reports_its_maximum() {
        let mut ring = Ring::<3>::new();
        assert!(ring.max().abs() < f64::EPSILON);
        for v in [4.0, 9.0, f64::NAN, 2.0] {
            ring.push(v);
        }
        assert!((ring.max() - 9.0).abs() < f64::EPSILON);
    }

    #[test]
    fn a_zero_size_ring_stays_empty() {
        let mut ring = Ring::<0>::new();
        ring.push(1.0);
        assert!(ring.is_empty());
        assert_eq!(ring.last(), None);
    }

    #[test]
    fn the_default_ring_length_is_three_minutes_of_seconds() {
        assert_eq!(RING_LEN, 180);
        assert_eq!(Ring::<RING_LEN>::default().len(), 0);
    }
}
