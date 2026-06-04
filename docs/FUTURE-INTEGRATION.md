# Future Integration: ternary-streaming

## Current State
Provides streaming processing for ternary signal sequences: sliding windows (`StreamWindow` with configurable size), running statistics (mean, mode, balance), pattern detection, and downsampling. `no_std` compatible with `VecDeque`-backed buffers.

## Integration Opportunities

### With ternary-cell (Real-Time Room Events)
Every ternary-cell tick cycle emits ternary signals (prediction error, surprise, vibe state). ternary-streaming's `StreamWindow` processes this tick stream in real time: a sliding window over the last N ticks computes running statistics that feed back into the cell's predict phase. The running balance (ratio of Pos to Neg) becomes a room-level health metric — positive balance = room is learning, negative balance = room is degrading.

### With ternary-signals (Frequency Analysis)
Streaming windows feed into ternary-signals' fixed-point DFT. A `StreamWindow` collecting 64 ticks provides the input vector for spectral analysis. The dominant frequency reveals the room's oscillation pattern — if cells cycle between Pos and Neg every ~8 ticks, the DFT peak at frequency 1/8 signals a coherent oscillation that might need damping.

### With ternary-replay (Event Reconstruction)
ternary-replay records experiment histories. ternary-streaming processes those histories in playback — the same `StreamWindow` that processes live ticks can replay recorded ticks, enabling "what would the running statistics have been?" queries. The `downsample()` function compresses long recordings into summary windows for visualization.

## Potential in Mature Systems
In room-as-codespace, each Codespace room produces a continuous stream of ternary events. PLATO's room registry subscribes to these streams using ternary-streaming windows. A monitoring dashboard shows running statistics for all active rooms. When a room's stream balance drops below threshold, PLATO triggers the room's GC phase early. When pattern detection finds a repeating sequence, PLATO recognizes a stable equilibrium and can suspend the Codespace to save compute.

## Cross-Pollination Ideas
- **ternary-noise**: Inject noise into the stream (via `NoisyEnvironment`) and measure how running statistics degrade — a streaming robustness test.
- **ternary-anomaly**: Anomaly detection on the streaming window — flag when the current window's statistics diverge significantly from historical baselines.
- **ternary-kalman**: Kalman filter predictions as a streaming operator — each new observation updates the filter, and the window tracks prediction error over time.

## Dependencies for Next Steps
- Define `RoomEventStream` trait wrapping `StreamWindow` in ternary-cell
- Add `StreamWindow::to_dft_input()` adapter for ternary-signals integration
- Implement stream serialization for ternary-protocol wire format
- Benchmark window operations on ESP32 (must handle tick-rate frequencies)
