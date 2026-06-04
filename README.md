# ternary-streaming

Streaming processing of ternary signals — sliding windows, running statistics, pattern detection, and downsampling.

## Why This Exists

Ternary data often arrives as a stream: sensor readings (low/normal/high), market ticks (down/flat/up), or opinion flows (negative/neutral/positive). You can't load it all into memory, and you need real-time answers: "Has the pattern [+1, 0, −1] appeared?" or "What's the running majority over the last 50 samples?" This crate provides `no_std`-compatible streaming primitives — sliding windows, aggregators, pattern detectors, transition detectors, and downsamplers — that operate element-by-element with O(1) memory per item. No allocations in the hot path (beyond the configured window size).

## Core Concepts

- **`Ternary`** — Three-valued enum: `Neg` (−1), `Zero` (0), `Pos` (+1). Supports `Neg` operator for negation.
- **`StreamWindow`** — A fixed-capacity sliding window using a `VecDeque`. Push new values; old values are evicted automatically.
- **`StreamAggregator`** — Running statistics: count, sum, mean (fixed-point ×1000), mode, per-value counts. Mergeable for parallel processing.
- **`StreamDetector`** — Pattern matching in a stream: feed values one at a time, get a boolean when the pattern completes.
- **`TransitionDetector`** — Detects state changes (and their type) as values arrive.
- **`StreamSampler`** — Downsample a stream by aggregating groups (majority, sum, first, last).

## Quick Start

```toml
# Cargo.toml
[dependencies]
ternary-streaming = "0.1"
```

```rust
use ternary_streaming::*;

fn main() {
    // Sliding window of size 5
    let mut window = StreamWindow::new(5);
    for v in [1, -1, 1, 0, -1, 1] {
        let evicted = window.push(Ternary::from_i8(v).unwrap());
        println!("Window: {:?}, sum={}, majority={:?}",
            window.contents(), window.sum(), window.majority());
    }

    // Running aggregator
    let mut agg = StreamAggregator::new();
    for v in [1, -1, 1, 0, 1] {
        agg.observe(Ternary::from_i8(v).unwrap());
    }
    println!("Count: {}, Sum: {}, Mode: {:?}, Mean(milli): {:?}",
        agg.count(), agg.sum(), agg.mode(), agg.mean_milli());

    // Pattern detector: match [Pos, Zero, Neg]
    let mut detector = StreamDetector::new(&[
        Ternary::Pos, Ternary::Zero, Ternary::Neg
    ]);
    for v in [1, 0, -1] {
        if detector.feed(Ternary::from_i8(v).unwrap()) {
            println!("Pattern detected!");
        }
    }

    // Downsample by factor of 3 using majority vote
    let data: Vec<Ternary> = [1, 1, -1, 0, -1, -1].iter()
        .map(|&v| Ternary::from_i8(v).unwrap()).collect();
    let downsampled = downsample(&data, 3, SampleMethod::Majority);
    println!("Downsampled: {:?}", downsampled);
}
```

## API Overview

### Sliding Window (`StreamWindow`)
- `StreamWindow::new(capacity)` — Create fixed-size window
- `window.push(val)` — Add value; returns evicted value if full
- `window.sum()` / `window.majority()` / `window.contents()` — Query

### Running Statistics (`StreamAggregator`)
- `StreamAggregator::new()` — Create aggregator
- `agg.observe(val)` — Feed a value
- `agg.count()` / `agg.sum()` / `agg.mean_milli()` — Statistics
- `agg.count_neg()` / `agg.count_zero()` / `agg.count_pos()` — Per-value counts
- `agg.mode()` — Most frequent value
- `agg.merge(&other)` — Combine two aggregators

### Pattern Detection (`StreamDetector`)
- `StreamDetector::new(pattern)` — Watch for a specific pattern
- `detector.feed(val)` — Returns `true` when pattern just matched
- `detector.feed_run(val, run_length)` — Detect runs of identical values
- `detector.reset()` — Clear buffer

### Transition Detection (`TransitionDetector`)
- `TransitionDetector::new()` — Watch for state changes
- `td.feed(val)` — Returns `true` if value differs from previous
- `td.feed_detailed(val)` — Returns `Option<(from, to)>` for transitions

### Downsampling (`StreamSampler`)
- `StreamSampler::new(factor, method)` — Create downsampler
- `sampler.push(val)` — Returns `Some(downsampled)` when group is complete
- `sampler.flush()` — Output remaining partial group
- `downsample(data, factor, method)` — Batch downsampling
- `SampleMethod::Majority` / `Sum` / `First` / `Last` — Aggregation methods

## How It Works

**`StreamWindow`** uses a `VecDeque` ring buffer. Pushing when full dequeues the front element. Sum and majority are computed on-demand by iterating the buffer (O(window size)).

**`StreamAggregator`** maintains running totals: count, sum, and per-value counters. Mean is computed as a fixed-point value (sum × 1000 / count) to avoid floating point. Aggregators can be merged by adding their counters, enabling parallel stream processing.

**`StreamDetector`** keeps a buffer equal to the pattern length. Each new value evicts the oldest if full, then checks if the buffer exactly matches the pattern. This gives O(pattern length) per element.

**`StreamSampler`** collects `factor` values, then applies the chosen aggregation method. `Sum` adds ternary values and maps positive→Pos, negative→Neg, zero→Zero. `Majority` counts occurrences and returns the most common. `First`/`Last` are trivial.

**`no_std`** compatibility is achieved by using only `alloc` (for `Vec` and `VecDeque`) and `core`. No floating-point operations in the hot path.

## Use Cases

1. **Real-time sensor monitoring** — Detect anomalies in ternary sensor streams (low/normal/high) using sliding windows and pattern detectors.
2. **Financial tick processing** — Downsample high-frequency market direction signals for longer-term analysis.
3. **Sentiment stream analysis** — Track running statistics and detect sentiment transitions in social media feeds.
4. **Embedded systems** — Process ternary sensor data on microcontrollers with no OS and limited memory (`no_std` compatible).

## Known Limitations

- **StreamWindow recomputes `sum()` and `majority()` from scratch on every call**: Both methods iterate the entire `VecDeque` buffer rather than maintaining running totals. For a window of size W, each query is O(W). If you call `sum()` on every push, total processing becomes O(N×W) instead of O(N).

- **StreamDetector only matches contiguous patterns**: `StreamDetector::feed()` checks if the last `pattern.len()` values exactly match the pattern. It cannot detect patterns with wildcards, optional elements, or gaps. Partial matches that are interrupted by one wrong value are discarded entirely — no fuzzy matching.

- **StreamAggregator uses fixed-point mean (×1000)**: `mean_milli()` computes `sum × 1000 / count`, which truncates toward zero. For very long streams (count > 2¹⁹), the `i64` sum can overflow if the stream is biased toward Pos (+1). A stream of all Pos values overflows at ~9.2 × 10¹⁸ elements.

- **Downsampling with `SampleMethod::Sum` loses magnitude**: The Sum method adds ternary values and maps positive→Pos, negative→Neg, zero→Zero. A group of `[Pos, Neg]` sums to 0 → Zero, which is the same result as `[Zero, Zero]`. The magnitude information is discarded.

- **`StreamSampler::flush()` returns a partial group**: If the input length isn't divisible by the downsample factor, `flush()` emits a potentially unrepresentative partial group. No warning is provided that this last sample has fewer contributing values.

- **TransitionDetector doesn't debounce**: `feed_detailed()` fires on every value change. For noisy ternary streams that oscillate rapidly (e.g., Pos, Neg, Pos, Neg), this produces a transition event for every element. No hysteresis or holdoff mechanism is available.

## Ecosystem

- [`ternary-regex`](https://github.com/user/ternary-regex) — Full regex engine for ternary sequences
- [`ternary-signals`](https://github.com/user/ternary-signals) — Fourier analysis and signal processing for ternary data
- [`ternary-markov`](https://github.com/user/ternary-markov) — Markov chains on ternary state spaces

## License

MIT
