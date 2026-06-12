# CLAUDE.md

## Working style (important)
- **Think first, then code.** Analyze the problem and explain the root cause before making changes.
- **Don't code too fast.** Avoid jumping straight to edits or patches.
- **Ask for confirmation before ANY file change and before ANY git commit. After committing, push immediately without asking.**
- Prefer diagnosing over patching — verify the actual cause (logs, processes, config) before proposing a fix.

## Project notes
- Rust project; `cargo build` builds the whole workspace cleanly. The main binary
  is `order_trade_machine`; build/run a single one with `cargo build --bin <name>`.
  Use `RUSTFLAGS=-Awarnings` to silence the remaining non-blocking warnings.
- IB connectivity: `connect_ib(is_test, msg)` in `src/ib.rs`. `is_test=true` -> port 4002 (paper gateway), `false` -> 4001 (live).
- Option-chain download entry point: `test_download_spy_opt_chain` in `src/bin/ib_download.rs`, run via `cargo run --bin ib_download -- opt-chain SPY`.
- Planned: a feature-gated Ratatui terminal dashboard (`--tui`, `--features tui`) that replaces the old
  egui `ib_gui` price window. Full implementation plan lives in `refactor_tui.md` — read it before starting
  that work.

## Studies
Research findings live in `studies/<theme>/SUMMARY.md`. The index is `studies/README.md` — read it first before starting a new study to avoid duplication. Analysis scripts (Python, standalone) live in `analysis/`. When a study concludes, add a one-line row to `studies/README.md`.

**After each study run, copy Claude's terminal analysis summary verbatim into the top of
`SUMMARY.md` or `analysis.md` (above the detailed sections).** The narrative Claude writes
at the end of a run is high-quality and should be preserved as-is, not paraphrased.

## Regression test
- After changing the state machine / trade manager / signals, run the `random_walk`
  backtest and confirm the total PnL is unchanged. With `backtest_config.toml` set to
  `stra_config_tomls = ["random_walk.toml"]` and dates `2025-04-01 08:00` → `2025-06-01 08:00`:
  `RUST_LOG=info RUSTFLAGS=-Awarnings cargo run --bin order_trade_machine` should end with
  **`TOTAL PNL -343.20`** (i.e. `-343.1999999999948`). A different value means a regression.


# Rust Project Context & Guidelines

## 1. Project Commands
Always use these exact commands for building, testing, and formatting. Do not assume or guess arguments.

- **Build Project:** `cargo build`
- **Build Release:** `cargo build --release`
- **Run Unit Tests:** `cargo test`
- **Run Specific Test:** `cargo test <test_name>`
- **Run Linter:** `cargo clippy --all-targets --all-features -- -D warnings`
- **Format Code:** `cargo fmt --all`
- **Verify CI:** `cargo fmt --all --check`
- **Update Dependencies:** `cargo update`

## 2. Rust Architecture & Conventions
Follow these strict architectural guidelines to respect Rust's ownership and type system rules.

### Ownership & Lifetimes
- **Prefer Cloning over Complex Lifetimes:** Prefer simple ownership models over complex lifetime hierarchies. Use cloning when it significantly simplifies code and the performance impact is negligible.Avoid unnecessary cloning in hot paths or large data structures.
- **Semantically Lift Logic:** When modifying or translating legacy logic, do not force raw pointer patterns. Lift structural workflows into native Rust ownership enforcements.
- **Smart Pointers:** Use `Box<T>` for dynamic sizing / trait objects. Use `Arc<T>` for cross-thread data sharing; do not use `Rc<T>` if any thread safety is required.

### Error Handling
- **No Panics in Libraries:** Never use `.unwrap()`, `.expect()`, or `panic!()` in library or production-ready backend code. Use the `Result<T, E>` enum explicitly.
- **Error Propagation:** Use the `?` operator. Prefer leveraging crates like `thiserror` for internal, structured library errors, and `anyhow` for top-level binary applications.
- **Contextualizing Errors:** Always provide context using `.context()` when bubbles up errors in binaries.

### Async & Concurrency
- **Runtime Choice:** Use Tokio (`#[tokio::main]`) for general networking and async applications.
- **Locking Granularity:** Keep `tokio::sync::Mutex` or standard `Mutex` guards as short-lived as possible. Never drop a mutex guard late, or hold it across an `.await` boundary unless using Tokio's specialized mutex.

## 3. Code Style & Safety Guidelines

### Unsafe Code Blocked
- Do NOT introduce the `unsafe` keyword unless explicitly instructed by a human engineer. 
- Prioritize keeping operations minimal, fully encapsulated, and abstracting behaviors safely behind public APIs.

### Idiomatic Expressions
- **Use Standard Lints:** Adhere stringently to Clippy suggestions. Code will fail CI if any Clippy warning triggers (`-D warnings`).
- **Matching over Conditioning:** Favor `match` statements and `if let` blocks over nested or highly boolean `if/else` checks for handling enums or `Option/Result`.
- **Iterators:** Prefer functional iterator chains (`.map()`, `.filter()`, `.collect()`) over imperative `for` loops where legibility is preserved.

## 4. Testing & Validation Rules
- **Unit Tests:** Place inline unit tests inside a `mod tests` block at the bottom of the file being developed, marked with `#[cfg(test)]`.
- **Integration Tests:** Put multi-module integration tests in the `/tests` folder.
- **Mocking:** Use standard traits for mocking boundaries instead of heavy reflective framework macros wherever possible.
