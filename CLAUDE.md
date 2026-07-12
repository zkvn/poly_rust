# CLAUDE.md

## Working style (important)
- **Think first, then code.** Analyze the problem and explain the root cause before making changes.
- **Don't code too fast.** Avoid jumping straight to edits or patches.
- **Ask for confirmation before ANY file change and before ANY git commit. Push by default: once a commit is made, push it immediately without asking.**
- **Exception — Markdown docs (`.md`): always commit and push these automatically, without asking first.** This covers audit/plan docs (`trader/doc/*.md`), study summaries, and README updates. The pushed doc/diff *is* the review mechanism — don't hold it back waiting for in-chat confirmation.
- **Exception — big/thorough code changes (`.rs`, `.py`, `.toml`, scripts): always commit and push automatically too, without asking first, once the change is actually verified** (relevant tests pass, `cargo fmt --all --check` / `cargo clippy -- -D warnings` clean for Rust, and — for anything with a runtime-visible effect like a regenerated report — the output was actually inspected, not just assumed correct). Confirmed 2026-07-12 after the daily-recon BT-reconciliation column work (new Rust CSV column + Python columns + 22 new tests, full suite green, two reports regenerated and read through before pushing). A quick throwaway one-off edit that hasn't been run/tested yet still isn't covered by this — that's judgment, not a loophole: if it's genuinely unverified, ask first.
- **Flag deferred/skipped items in README's `## TODO` section, one line each.** Any time work surfaces something important that isn't being fixed right now — a pre-existing bug found but out of scope, a fix deliberately skipped (e.g. a risk/tradeoff not worth taking in the moment), a known gap discovered mid-task — add a one-line dated entry to README's `## TODO` section (not just buried in a commit message or a "known incidents" writeup) so it stays tracked and doesn't get lost. Example: the 2026-07-08 `price_feed` clippy errors and the skipped `rust-toolchain.toml` pin, both found while fixing an unrelated `cargo fmt` issue.
- **Once a TODO item is done, move it — don't leave it struck through in place.** A `~~...~~` **Fixed**/**Done** TODO bullet should be deleted from `## TODO` and, if not already represented there, added as an entry under "Trading engine — known incidents" (or the sibling `price_feed` incidents living in that same section). Skip adding a new entry if the incident section already covers the same fix (check by date/topic first) — just delete the stale TODO bullet in that case.
- **Incident-section entries: short, with a link, not a full inline writeup.** New entries under "Trading engine — known incidents" should be a `###` heading (topic + date + fixed/added) followed by 2-5 sentences of root cause/fix — ideally closer to a one-liner — plus `Full writeup: <path>` pointing at the detailed doc in `trader/doc/` or `price_feed/doc/`. The doc holds the full narrative (root cause, fix steps, verification, follow-ups); the README entry is an index pointer, not a copy. Existing older entries (pre-2026-07-12) are longer/inline and can stay as-is — this convention applies going forward, don't mass-rewrite history for its own sake.
- Prefer diagnosing over patching — verify the actual cause (logs, processes, config) before proposing a fix.

## Project notes
- Rust project (Polymarket CLOB price recorder — streams live order-book/price data,
  writes daily Parquet). `cargo build` builds it; main binary is `price_feed`
  (`cargo run --bin price_feed -- collect --nats-url <url>`).
  Use `RUSTFLAGS=-Awarnings` to silence non-blocking warnings.
- See the top-level `README.md` for data-file layout, the hourly-seal parquet-integrity
  design, and Oracle cross-compile/deploy notes — read it before touching collector internals.

## Studies
Research findings live in `studies/<theme>/SUMMARY.md`. The index is `studies/README.md` — read it first before starting a new study to avoid duplication. Analysis scripts (Python, standalone) live in `analysis/`. When a study concludes, add a one-line row to `studies/README.md`.

**After each study run, copy Claude's terminal analysis summary verbatim into the top of
`SUMMARY.md` or `analysis.md` (above the detailed sections).** The narrative Claude writes
at the end of a run is high-quality and should be preserved as-is, not paraphrased.


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
