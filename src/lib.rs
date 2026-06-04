//! Streaming processing of ternary signals.
//!
//! Provides sliding windows, running statistics, pattern detection,
//! and downsampling for ternary-valued data streams where each element
//! is in {-1, 0, +1}.

#![forbid(unsafe_code)]
#![no_std]

extern crate alloc;

use alloc::collections::VecDeque;
use alloc::vec::Vec;
use alloc::vec;

/// A ternary value: Negative (-1), Zero (0), or Positive (+1).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Ternary {
    Neg = -1,
    Zero = 0,
    Pos = 1,
}

impl Ternary {
    pub fn from_i8(v: i8) -> Option<Self> {
        match v {
            -1 => Some(Ternary::Neg),
            0 => Some(Ternary::Zero),
            1 => Some(Ternary::Pos),
            _ => None,
        }
    }

    pub fn to_i8(self) -> i8 {
        self as i8
    }
}

impl core::ops::Neg for Ternary {
    type Output = Self;
    fn neg(self) -> Self {
        match self {
            Ternary::Neg => Ternary::Pos,
            Ternary::Zero => Ternary::Zero,
            Ternary::Pos => Ternary::Neg,
        }
    }
}

// ── StreamWindow ─────────────────────────────────────────────────────

/// A sliding window over a ternary data stream.
#[derive(Clone, Debug)]
pub struct StreamWindow {
    buffer: VecDeque<Ternary>,
    capacity: usize,
}

impl StreamWindow {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: VecDeque::with_capacity(capacity),
            capacity,
        }
    }

    /// Push a new value; if the window is full, returns the evicted value.
    pub fn push(&mut self, val: Ternary) -> Option<Ternary> {
        let evicted = if self.buffer.len() == self.capacity {
            self.buffer.pop_front()
        } else {
            None
        };
        self.buffer.push_back(val);
        evicted
    }

    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn capacity(&self) -> usize {
        self.capacity
    }

    pub fn contents(&self) -> Vec<Ternary> {
        self.buffer.iter().copied().collect()
    }

    /// Sum of ternary values in the window as i64.
    pub fn sum(&self) -> i64 {
        self.buffer.iter().map(|t| t.to_i8() as i64).sum()
    }

    /// Majority vote: returns the most common value, or Zero on tie.
    pub fn majority(&self) -> Ternary {
        let mut counts = [0i32; 3]; // neg, zero, pos
        for &t in &self.buffer {
            match t {
                Ternary::Neg => counts[0] += 1,
                Ternary::Zero => counts[1] += 1,
                Ternary::Pos => counts[2] += 1,
            }
        }
        if counts[2] > counts[0] && counts[2] > counts[1] {
            Ternary::Pos
        } else if counts[0] > counts[2] && counts[0] > counts[1] {
            Ternary::Neg
        } else {
            Ternary::Zero
        }
    }

    pub fn clear(&mut self) {
        self.buffer.clear();
    }
}

// ── StreamAggregator ─────────────────────────────────────────────────

/// Running statistics over a ternary stream.
#[derive(Clone, Debug)]
pub struct StreamAggregator {
    count: u64,
    sum: i64,
    counts: [u64; 3], // neg, zero, pos
}

impl StreamAggregator {
    pub fn new() -> Self {
        Self {
            count: 0,
            sum: 0,
            counts: [0; 3],
        }
    }

    pub fn observe(&mut self, val: Ternary) {
        self.count += 1;
        self.sum += val.to_i8() as i64;
        match val {
            Ternary::Neg => self.counts[0] += 1,
            Ternary::Zero => self.counts[1] += 1,
            Ternary::Pos => self.counts[2] += 1,
        }
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    pub fn sum(&self) -> i64 {
        self.sum
    }

    /// Fixed-point mean scaled by 1000 (avoids floats).
    /// Returns None if no observations.
    pub fn mean_milli(&self) -> Option<i64> {
        if self.count == 0 {
            return None;
        }
        Some(self.sum * 1000 / self.count as i64)
    }

    pub fn count_neg(&self) -> u64 { self.counts[0] }
    pub fn count_zero(&self) -> u64 { self.counts[1] }
    pub fn count_pos(&self) -> u64 { self.counts[2] }

    pub fn mode(&self) -> Ternary {
        let [n, z, p] = self.counts;
        if p >= n && p >= z {
            Ternary::Pos
        } else if n >= p && n >= z {
            Ternary::Neg
        } else {
            Ternary::Zero
        }
    }

    /// Merge another aggregator into this one.
    pub fn merge(&mut self, other: &StreamAggregator) {
        self.count += other.count;
        self.sum += other.sum;
        for i in 0..3 {
            self.counts[i] += other.counts[i];
        }
    }
}

impl Default for StreamAggregator {
    fn default() -> Self {
        Self::new()
    }
}

// ── StreamDetector ───────────────────────────────────────────────────

/// Detect patterns in real-time ternary streams.
#[derive(Clone, Debug)]
pub struct StreamDetector {
    pattern: Vec<Ternary>,
    buffer: VecDeque<Ternary>,
}

impl StreamDetector {
    pub fn new(pattern: &[Ternary]) -> Self {
        Self {
            pattern: pattern.to_vec(),
            buffer: VecDeque::with_capacity(pattern.len()),
        }
    }

    /// Feed a new value. Returns `true` if the pattern was just matched.
    pub fn feed(&mut self, val: Ternary) -> bool {
        if self.pattern.is_empty() {
            return false;
        }
        if self.buffer.len() == self.pattern.len() {
            self.buffer.pop_front();
        }
        self.buffer.push_back(val);

        if self.buffer.len() < self.pattern.len() {
            return false;
        }
        self.buffer.iter().zip(self.pattern.iter()).all(|(&a, &b)| a == b)
    }

    /// Detect a run of `n` identical values.
    pub fn feed_run(&mut self, val: Ternary, run_length: usize) -> bool {
        self.buffer.push_back(val);
        if self.buffer.len() > run_length {
            self.buffer.pop_front();
        }
        if self.buffer.len() < run_length {
            return false;
        }
        self.buffer.iter().all(|&v| v == val)
    }

    pub fn reset(&mut self) {
        self.buffer.clear();
    }
}

/// Detect transitions (neg→pos, pos→neg, zero→non-zero, etc.).
#[derive(Clone, Debug)]
pub struct TransitionDetector {
    last: Option<Ternary>,
}

impl TransitionDetector {
    pub fn new() -> Self {
        Self { last: None }
    }

    /// Returns `true` if this value differs from the previous.
    pub fn feed(&mut self, val: Ternary) -> bool {
        let changed = self.last.map_or(false, |l| l != val);
        self.last = Some(val);
        changed
    }

    /// Returns the transition type: (from, to) or None on first value.
    pub fn feed_detailed(&mut self, val: Ternary) -> Option<(Ternary, Ternary)> {
        let result = self.last.map(|l| (l, val));
        self.last = Some(val);
        result.filter(|(a, b)| a != b)
    }

    pub fn reset(&mut self) {
        self.last = None;
    }
}

impl Default for TransitionDetector {
    fn default() -> Self {
        Self::new()
    }
}

// ── StreamSampler ────────────────────────────────────────────────────

/// Downsample a ternary stream by aggregating groups.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SampleMethod {
    /// Majority vote in each group.
    Majority,
    /// Sum: positive sum → Pos, negative → Neg, zero → Zero.
    Sum,
    /// Take the first element of each group.
    First,
    /// Take the last element of each group.
    Last,
}

/// Downsampler that collects `factor` values then outputs one.
#[derive(Clone, Debug)]
pub struct StreamSampler {
    factor: usize,
    method: SampleMethod,
    buffer: Vec<Ternary>,
}

impl StreamSampler {
    pub fn new(factor: usize, method: SampleMethod) -> Self {
        assert!(factor > 0, "sample factor must be > 0");
        Self {
            factor,
            method,
            buffer: Vec::with_capacity(factor),
        }
    }

    /// Push a value. Returns a downsampled value when the group is complete.
    pub fn push(&mut self, val: Ternary) -> Option<Ternary> {
        self.buffer.push(val);
        if self.buffer.len() < self.factor {
            return None;
        }
        let result = self.aggregate();
        self.buffer.clear();
        Some(result)
    }

    /// Flush any remaining partial group.
    pub fn flush(&mut self) -> Option<Ternary> {
        if self.buffer.is_empty() {
            return None;
        }
        let result = self.aggregate();
        self.buffer.clear();
        Some(result)
    }

    fn aggregate(&self) -> Ternary {
        match self.method {
            SampleMethod::First => self.buffer[0],
            SampleMethod::Last => *self.buffer.last().unwrap(),
            SampleMethod::Sum => {
                let s: i64 = self.buffer.iter().map(|t| t.to_i8() as i64).sum();
                if s > 0 {
                    Ternary::Pos
                } else if s < 0 {
                    Ternary::Neg
                } else {
                    Ternary::Zero
                }
            }
            SampleMethod::Majority => {
                let mut counts = [0i32; 3];
                for &t in &self.buffer {
                    match t {
                        Ternary::Neg => counts[0] += 1,
                        Ternary::Zero => counts[1] += 1,
                        Ternary::Pos => counts[2] += 1,
                    }
                }
                if counts[2] > counts[0] && counts[2] >= counts[1] {
                    Ternary::Pos
                } else if counts[0] > counts[2] && counts[0] >= counts[1] {
                    Ternary::Neg
                } else {
                    Ternary::Zero
                }
            }
        }
    }
}

/// Batch-downsample a slice of ternary values.
pub fn downsample(data: &[Ternary], factor: usize, method: SampleMethod) -> Vec<Ternary> {
    let mut sampler = StreamSampler::new(factor, method);
    let mut result = Vec::new();
    for &v in data {
        if let Some(v) = sampler.push(v) {
            result.push(v);
        }
    }
    if let Some(v) = sampler.flush() {
        result.push(v);
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::vec;

    fn t(v: i8) -> Ternary {
        Ternary::from_i8(v).unwrap()
    }

    // ── Ternary basics ──
    #[test]
    fn ternary_from_i8() {
        assert_eq!(Ternary::from_i8(-1), Some(Ternary::Neg));
        assert_eq!(Ternary::from_i8(0), Some(Ternary::Zero));
        assert_eq!(Ternary::from_i8(1), Some(Ternary::Pos));
        assert_eq!(Ternary::from_i8(2), None);
    }

    #[test]
    fn ternary_neg() {
        assert_eq!(-Ternary::Neg, Ternary::Pos);
        assert_eq!(-Ternary::Pos, Ternary::Neg);
        assert_eq!(-Ternary::Zero, Ternary::Zero);
    }

    // ── StreamWindow ──
    #[test]
    fn window_slides() {
        let mut w = StreamWindow::new(3);
        assert!(w.push(t(1)).is_none());
        assert!(w.push(t(0)).is_none());
        assert!(w.push(t(-1)).is_none());
        assert_eq!(w.push(t(1)), Some(t(1)));
        assert_eq!(w.contents(), vec![t(0), t(-1), t(1)]);
    }

    #[test]
    fn window_sum() {
        let mut w = StreamWindow::new(5);
        for v in [1, -1, 1, 0, -1] {
            w.push(t(v));
        }
        assert_eq!(w.sum(), 0);
    }

    #[test]
    fn window_majority() {
        let mut w = StreamWindow::new(5);
        for v in [1, 1, 1, 0, -1] {
            w.push(t(v));
        }
        assert_eq!(w.majority(), Ternary::Pos);
    }

    #[test]
    fn window_majority_tie() {
        let mut w = StreamWindow::new(2);
        w.push(t(1));
        w.push(t(-1));
        assert_eq!(w.majority(), Ternary::Zero);
    }

    #[test]
    fn window_clear() {
        let mut w = StreamWindow::new(3);
        w.push(t(1));
        w.clear();
        assert!(w.is_empty());
    }

    // ── StreamAggregator ──
    #[test]
    fn aggregator_basic() {
        let mut agg = StreamAggregator::new();
        agg.observe(t(1));
        agg.observe(t(-1));
        agg.observe(t(1));
        assert_eq!(agg.count(), 3);
        assert_eq!(agg.sum(), 1);
        assert_eq!(agg.count_pos(), 2);
        assert_eq!(agg.count_neg(), 1);
        assert_eq!(agg.count_zero(), 0);
    }

    #[test]
    fn aggregator_mean_milli() {
        let mut agg = StreamAggregator::new();
        assert!(agg.mean_milli().is_none());
        agg.observe(t(1));
        agg.observe(t(1));
        assert_eq!(agg.mean_milli(), Some(1000));
    }

    #[test]
    fn aggregator_mode() {
        let mut agg = StreamAggregator::new();
        for _ in 0..5 { agg.observe(t(-1)); }
        for _ in 0..3 { agg.observe(t(1)); }
        assert_eq!(agg.mode(), Ternary::Neg);
    }

    #[test]
    fn aggregator_merge() {
        let mut a = StreamAggregator::new();
        a.observe(t(1));
        let mut b = StreamAggregator::new();
        b.observe(t(-1));
        b.observe(t(1));
        a.merge(&b);
        assert_eq!(a.count(), 3);
        assert_eq!(a.sum(), 1);
    }

    // ── StreamDetector ──
    #[test]
    fn detector_pattern_match() {
        let mut d = StreamDetector::new(&[t(1), t(0), t(-1)]);
        assert!(!d.feed(t(1)));
        assert!(!d.feed(t(0)));
        assert!(d.feed(t(-1)));
    }

    #[test]
    fn detector_no_match() {
        let mut d = StreamDetector::new(&[t(1), t(1)]);
        assert!(!d.feed(t(1)));
        assert!(!d.feed(t(0)));
    }

    #[test]
    fn detector_run() {
        let mut d = StreamDetector::new(&[]);
        // Build up a run of 3 identical values
        assert!(!d.feed_run(t(1), 3));
        assert!(!d.feed_run(t(1), 3));
        assert!(d.feed_run(t(1), 3));
    }

    // ── TransitionDetector ──
    #[test]
    fn transition_detect() {
        let mut td = TransitionDetector::new();
        assert!(!td.feed(t(1))); // first, no transition
        assert!(td.feed(t(0)));  // 1→0
        assert!(!td.feed(t(0))); // same
        assert!(td.feed(t(-1))); // 0→-1
    }

    #[test]
    fn transition_detailed() {
        let mut td = TransitionDetector::new();
        assert!(td.feed_detailed(t(1)).is_none());
        assert_eq!(td.feed_detailed(t(-1)), Some((t(1), t(-1))));
    }

    // ── StreamSampler ──
    #[test]
    fn sampler_sum_method() {
        let mut s = StreamSampler::new(2, SampleMethod::Sum);
        assert_eq!(s.push(t(1)), None);
        assert_eq!(s.push(t(-1)), Some(Ternary::Zero));
        assert_eq!(s.push(t(1)), None);
        assert_eq!(s.flush(), Some(Ternary::Pos));
    }

    #[test]
    fn sampler_majority_method() {
        let mut s = StreamSampler::new(3, SampleMethod::Majority);
        assert_eq!(s.push(t(1)), None);
        assert_eq!(s.push(t(1)), None);
        assert_eq!(s.push(t(-1)), Some(Ternary::Pos));
    }

    #[test]
    fn sampler_first_method() {
        let mut s = StreamSampler::new(3, SampleMethod::First);
        assert_eq!(s.push(t(-1)), None);
        assert_eq!(s.push(t(1)), None);
        assert_eq!(s.push(t(0)), Some(Ternary::Neg));
    }

    #[test]
    fn sampler_last_method() {
        let mut s = StreamSampler::new(2, SampleMethod::Last);
        assert_eq!(s.push(t(-1)), None);
        assert_eq!(s.push(t(1)), Some(Ternary::Pos));
    }

    #[test]
    fn downsample_batch() {
        let data: Vec<Ternary> = [1, 0, -1, 1, 0, -1].iter().map(|&v| t(v)).collect();
        let result = downsample(&data, 2, SampleMethod::Sum);
        assert_eq!(result, vec![Ternary::Pos, Ternary::Zero, Ternary::Neg]);
    }
}
