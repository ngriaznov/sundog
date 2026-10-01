//! Motion as pure functions of time: exponential easing, tweens, pulses,
//! color blends, the spinner and the blink.

use std::time::Duration;

use super::theme::Rgb;

/// The easing time constant for bars and shares.
pub const TAU: Duration = Duration::from_millis(250);

/// A tween finishes within this distance of its target.
const SNAP: f64 = 1e-3;

/// Moves `cur` toward `target` by the fraction `1 - e^(-dt/tau)` of the gap.
/// The result lies between `cur` and `target` and never overshoots; a zero
/// `tau` jumps to `target`.
#[must_use]
pub fn ease(cur: f64, target: f64, dt: Duration, tau: Duration) -> f64 {
    if tau.is_zero() {
        return target;
    }
    let fraction = 1.0 - (-dt.as_secs_f64() / tau.as_secs_f64()).exp();
    cur + (target - cur) * fraction.clamp(0.0, 1.0)
}

/// A value that eases toward a target.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Tween {
    value: f64,
    target: f64,
}

impl Tween {
    /// A tween at rest on `value`.
    #[must_use]
    pub const fn new(value: f64) -> Self {
        Self {
            value,
            target: value,
        }
    }

    /// Sets the value the tween eases toward.
    pub fn set(&mut self, target: f64) {
        self.target = target;
    }

    /// Advances the tween by `dt` with time constant [`TAU`].
    pub fn step(&mut self, dt: Duration) {
        self.value = ease(self.value, self.target, dt, TAU);
        if (self.target - self.value).abs() < SNAP {
            self.value = self.target;
        }
    }

    /// The current value.
    #[must_use]
    pub const fn value(&self) -> f64 {
        self.value
    }

    /// Whether the tween has reached its target.
    #[must_use]
    pub fn is_done(&self) -> bool {
        (self.target - self.value).abs() < f64::EPSILON
    }
}

/// A flash intensity that falls linearly from 1 at `age` zero to 0 at `span`
/// and stays 0 after. A zero `span` is already finished.
#[must_use]
pub fn pulse(age: Duration, span: Duration) -> f64 {
    if span.is_zero() || age >= span {
        return 0.0;
    }
    1.0 - age.as_secs_f64() / span.as_secs_f64()
}

/// The color `t` of the way from `a` to `b`: `a` at 0, `b` at 1. `t` outside
/// 0 to 1 clamps.
#[must_use]
pub fn blend(a: Rgb, b: Rgb, t: f64) -> Rgb {
    let t = if t.is_nan() { 0.0 } else { t.clamp(0.0, 1.0) };
    let mix = |x: u8, y: u8| -> u8 {
        let value = f64::from(x) + (f64::from(y) - f64::from(x)) * t;
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "the value is rounded and lies between two u8 values"
        )]
        let byte = value.round() as u8;
        byte
    };
    Rgb(mix(a.0, b.0), mix(a.1, b.1), mix(a.2, b.2))
}

/// The spinner frames, one braille dot pattern per step.
pub const SPINNER: [char; 10] = ['⠋', '⠙', '⠹', '⠸', '⠼', '⠴', '⠦', '⠧', '⠇', '⠏'];

/// The spinner runs at this many frames per second.
pub const SPINNER_HZ: u128 = 10;

/// The spinner frame at `elapsed` since start.
#[must_use]
pub fn spinner_frame(elapsed: Duration) -> char {
    let step = elapsed.as_millis() * SPINNER_HZ / 1000;
    let len = SPINNER.len() as u128;
    SPINNER[usize::try_from(step % len).unwrap_or(0)]
}

/// Whether a 2 Hz blink is on at `elapsed`: on for a quarter second, off for
/// a quarter second.
#[must_use]
pub fn blink(elapsed: Duration) -> bool {
    (elapsed.as_millis() / 250).is_multiple_of(2)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: fn(u64) -> Duration = Duration::from_millis;

    #[test]
    fn ease_moves_toward_the_target_without_overshoot() {
        let mut cur = 0.0;
        for _ in 0..200 {
            let next = ease(cur, 10.0, MS(50), TAU);
            assert!(next >= cur && next <= 10.0);
            cur = next;
        }
        assert!((cur - 10.0).abs() < 1e-3);
    }

    #[test]
    fn ease_converges_downward_too() {
        let mut cur = 5.0;
        for _ in 0..200 {
            let next = ease(cur, -2.0, MS(50), TAU);
            assert!(next <= cur && next >= -2.0);
            cur = next;
        }
        assert!((cur + 2.0).abs() < 1e-3);
    }

    #[test]
    fn ease_takes_one_time_constant_to_close_63_percent() {
        let value = ease(0.0, 1.0, TAU, TAU);
        assert!((value - (1.0 - (-1.0f64).exp())).abs() < 1e-12);
    }

    #[test]
    fn ease_with_zero_tau_jumps_and_zero_dt_holds() {
        assert!((ease(1.0, 9.0, MS(10), Duration::ZERO) - 9.0).abs() < f64::EPSILON);
        assert!((ease(1.0, 9.0, Duration::ZERO, TAU) - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn tween_reaches_its_target_and_reports_done() {
        let mut tween = Tween::new(0.0);
        assert!(tween.is_done());
        tween.set(1.0);
        assert!(!tween.is_done());
        let mut steps = 0;
        while !tween.is_done() {
            tween.step(MS(50));
            steps += 1;
            assert!(tween.value() <= 1.0);
            assert!(steps < 1000);
        }
        assert!((tween.value() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn tween_finishes_within_a_second_of_a_unit_step() {
        let mut tween = Tween::new(0.0);
        tween.set(1.0);
        for _ in 0..20 {
            tween.step(MS(50));
        }
        assert!(tween.value() > 0.98);
    }

    #[test]
    fn pulse_falls_from_one_to_zero_at_its_span() {
        let span = MS(1500);
        assert!((pulse(Duration::ZERO, span) - 1.0).abs() < f64::EPSILON);
        assert!((pulse(MS(750), span) - 0.5).abs() < 1e-12);
        assert!(pulse(span, span).abs() < f64::EPSILON);
        assert!(pulse(MS(9000), span).abs() < f64::EPSILON);
        assert!(pulse(MS(1), Duration::ZERO).abs() < f64::EPSILON);
    }

    #[test]
    fn blend_returns_its_endpoints_and_the_midpoint() {
        let a = Rgb(0, 100, 200);
        let b = Rgb(100, 200, 0);
        assert_eq!(blend(a, b, 0.0), a);
        assert_eq!(blend(a, b, 1.0), b);
        assert_eq!(blend(a, b, 0.5), Rgb(50, 150, 100));
        assert_eq!(blend(a, b, -4.0), a);
        assert_eq!(blend(a, b, 7.0), b);
        assert_eq!(blend(a, b, f64::NAN), a);
    }

    #[test]
    fn spinner_cycles_at_ten_hertz() {
        assert_eq!(spinner_frame(Duration::ZERO), SPINNER[0]);
        assert_eq!(spinner_frame(MS(99)), SPINNER[0]);
        assert_eq!(spinner_frame(MS(100)), SPINNER[1]);
        assert_eq!(spinner_frame(MS(950)), SPINNER[9]);
        assert_eq!(spinner_frame(MS(1000)), SPINNER[0]);
    }

    #[test]
    fn blink_toggles_every_quarter_second() {
        assert!(blink(Duration::ZERO));
        assert!(blink(MS(249)));
        assert!(!blink(MS(250)));
        assert!(!blink(MS(499)));
        assert!(blink(MS(500)));
    }

    #[test]
    fn the_spinner_turns_ten_times_a_second() {
        assert_eq!(SPINNER_HZ, 10);
        assert_eq!(SPINNER.len(), 10);
    }
}
