# Plan — replace maker-entry with an aggressive taker entry (5u reversal, paper)

Status: **executable plan, implementing now.** Root-caused against the currently-live
`strategy_20260720_24h.toml` run (deployed 2026-07-20 14:15 HKT, maker-entry, fail-closed
p(up) gate per `plan_stale_data_gate_2026-07-20.md`).

## 1. Problem

~24h into the current paper window, local-synced `trader/live_logs/paper_trades_*.csv` show
**1 total trade** (DOGE) across all 6 assets. Zero for BTC/ETH/SOL/BNB/XRP.

Root cause: the maker-entry mechanism (`plan_unwind_5u_maker_2026-07-19.md` §2.2) rests a GTC
BUY at the real best bid and only fills on a **trade-through** (`PAPER_TRADE_THROUGH = 0.01`
past the quote, not touch — `execution.rs::PaperExecutor::on_price`), then gets pulled at
`T-15s` before cycle end or the moment the reversal signal that justified it stops holding
(`Worker::check_maker_quote_cancel`). A quote that never trades through in that narrow window
produces **zero trade rows at all** — not a loss, not logged as a near-miss beyond
`paper_quotes_*.csv`'s cancel-reason row. Compounding this (correctly, by design, not a bug):
the 2026-07-20 fail-closed p(up) gate now also blocks on any stale/missing indicator reading
instead of failing open, which is strictly more entries blocked than before. Between "few
quotes ever trade through" and "some of those that would have are now also gated on
freshness," the combined effect is a strategy that essentially never fires.

This is a mechanism problem, not a parameter problem — no amount of further `delta_pct_rev`/
`reversal` re-tuning fixes a fill-rate problem rooted in requiring a trade-through on a quote
that gets pulled after ~1-2 minutes of resting at best. Per the user: stop trying to tune the
maker mechanism further, switch the entry itself to a marketable (taker) order that is
constructed to actually cross the spread and fill.

## 2. Design

### 2.1 Aggressive taker entry price

Reversal entries, `maker_entry = false`: instead of resting a GTC buy at the bid, submit a
marketable FAK buy (`Action::PlaceBuy`, the pre-existing non-maker path) priced to reliably
cross the touch:

```
entry_price = min(best_ask(side).unwrap_or(signal_price) + order_slippage, max_buy_price)
```

- `best_ask` is new (`LatestPolySignal::best_ask`, mirrors the existing `best_bid`): UP reads
  `up_ask` directly, DOWN derives `1 - up_bid` from the unified mint/merge book's
  complementary-token identity (same identity `best_bid` already uses the other way).
  Falls back to `signal_price` (the mid, `intent.token_price()`) when no real ask has been
  observed yet this run — same fallback posture `best_bid` already has for maker quotes.
- `order_slippage` (config: `0.05` in every existing `strategy_*.toml`, but **dead** — parsed
  into the TOML struct field list nowhere; grep confirms zero references outside the file
  itself) is wired in for the first time. Its own doc comment ("FAK order slippage — covers
  normal 1-tick bid-ask spread without sweeping wide asks") already describes exactly this use.
- Capped at `max_buy_price` (0.95 for this run) — the hard ceiling, unchanged from today.
- Scoped to reversal only, mirroring `maker_entry`'s existing scope — high_prob/v_shape entries
  are untouched (`entry_price == signal_price`, same as before this change).

This price is both what gets submitted to `Action::PlaceBuy` and what the p(up) negative-edge
gate checks against (unchanged structurally from the maker path's own rationale in
`asbuilt_unwind_5u_maker_2026-07-19.md` §3-4: gate against the price that will actually be
paid, not the passive mid — for an aggressive taker that price is *higher* than mid, so the
gate becomes correspondingly *stricter*, not looser).

Known, accepted gap (already tracked, not re-litigated here): README's TODO already flags that
`price_high_rev` only checks the pre-retry signal price, not the realized fill, for the
existing FAK retry ladder (`aggressive_entry_price` in `execution.rs`, live-money path only).
The same shape of gap exists here in the paper simulation sense — `entry_price`'s cap is
`max_buy_price`, checked once, up front; a live order's actual retry escalation (unaffected by
this change) can still land above the signal-time `price_high_rev` read. Not addressed by this
plan; this run stays paper (`paper_trade = true`), so no real capital is at risk from it.

### 2.2 Sizing: `trade_size_usdc` must clear the GTC exit floor

The exit take-profit leg is unchanged — a resting GTC limit sell, legal only at
`shares >= MIN_GTC_SHARES` (5.0). Maker-entry sized every quote at a fixed 5 shares directly;
the taker/FAK path is USDC-denominated (`shares = size_usdc / fill_price`), so
`trade_size_usdc` must be large enough that a fill at any plausible entry price still clears 5
shares. Worst case across all 6 assets: `price_high_rev = 0.9` (gate 5 blocks anything above
it) plus the aggressive premium up to `max_buy_price = 0.95` → 5 shares needs at most $4.75.
**`trade_size_usdc` default: `1.0 → 5.0`** (flat, comfortably covers $1.00 of headroom above
the worst case). This is the one other numeric change in the new config — every reversal
per-asset parameter (`delta_pct_rev`/`reversal`/`reversal_low_threshold`/`unwind_pnl_rev`/
`unwind_time_rev`/`sl_pnl_rev`/`sl_reversal`) stays exactly what the current 24h re-pick chose;
this plan does not re-tune them, only the entry mechanism and its sizing.

### 2.3 Take-profit: unchanged, already does what was asked

`Worker::finalize_entry_fill` (`worker.rs`) already places the take-profit exit as a resting
GTC limit sell — `Action::PlaceLimitSell{price: cost + unwind_pnl_rev, ...}` — priced off
`cost`, the **actual fill price** (`result.cost` from `ExecutionEngine::place`), not the signal
price, whenever the fill is `>= MIN_GTC_SHARES`. This is true today for both the maker and FAK
paths already (`via_maker_entry` only ever gated *notification* wording, never the mechanism)
— so "once confirmed, immediately submit a maker resting order based on executed price for
take-profit" requires no new logic, only the sizing fix in §2.2 so a taker fill reliably clears
the 5-share floor that makes this path reachable at all. Verified by reading
`finalize_entry_fill` (`worker.rs:1478-1534`) before writing this plan, not assumed.

### 2.4 Slippage logging: signal price vs. executed price

`pending_entry: Option<(Side, EntryType, f64)>` already captures the decision-time signal price
(`intent.token_price()`, the mid) at the moment `try_enter` fires — but `on_order_filled`
currently discards it (`let Some((side, entry_type, _intent_price)) = ...`). Wire it through
instead of dropping it:

- `Action::PlaceBuy` gains `signal_price: f64` (distinct from `price`, which after §2.1 is the
  aggressive submission ceiling, not the decision price) — the driver (`bin/live.rs`) computes
  `slippage = result.cost - signal_price` and shows it in both the console `[ORDER]` line and
  the "📋 Order placed" / "❗ Order REJECTED" Telegram messages.
- `HoldingData` and `TradeRecord` both gain `entry_signal_price: f64` (`#[serde(default)]` on
  the latter for back-compat with already-persisted state/CSV rows). Threaded through
  `finalize_entry_fill`'s three call sites: the FAK path passes the real signal price from
  `pending_entry`; the maker path (kept, just off by default in the new config — see §3) passes
  its own `quote_price` as both cost and signal price, i.e. zero slippage, which is correct by
  construction — a maker fill only ever happens at exactly its own resting price.
- CSV (`paper_trades_*.csv`/`live_trades_*.csv`): `CSV_HEADER`/`log_trade()` append two trailing
  columns, `entry_signal_price,entry_slippage` (the latter computed at write time as
  `token_price - entry_signal_price`) — same incremental-column precedent `entry_price_ts`
  and the four latency fields already established (old rows simply predate the new columns).
- The merged "🎯 ENTRY filled → EXIT quote resting" Telegram notification
  (`plan_stale_data_gate_2026-07-20.md` §2 item 2) currently only fires for `via_maker_entry`
  (the FAK path had no prior use for it, and wasn't the active config). Now that taker is the
  active path, drop that restriction — send it whenever the GTC exit sell actually goes live,
  labelled `(maker)`/`(taker)` from the same bool so both mechanisms stay visually
  distinguishable in the log.

### 2.5 What is deliberately *not* changing

- The p(up) negative-edge gate and its fail-closed staleness behavior
  (`plan_stale_data_gate_2026-07-20.md` §1) — untouched, still fires against the (now higher,
  aggressive) `entry_price`.
- Every reversal per-asset parameter from the current 24h re-pick (§2.2 above).
- `maker_entry` itself is not deleted — the flag, `MakerQuote`, `EnteringMaker`, and the whole
  resting-quote lifecycle stay in the code, just set `false` in the new config, in case a future
  maker-vs-taker comparison run is wanted. Dead code is not being introduced; this is an
  existing, tested, still-reachable code path.

## 3. Config

New `trader/config/strategy_20260721_taker.toml`: exact copy of `strategy_20260720_24h.toml`
(every reversal per-asset table, halt/gamma/indicator/pup-gate settings, v_shape/high_prob
inert blocks) except:
- `maker_entry = false`
- `[trade_size_usdc] default = 5.0` (was `1.0`, see §2.2)
- `meta.ts`/`meta.source` updated to describe this switch and link this plan doc.

`order_slippage = 0.05` is already present in this file (copied forward) — it simply starts
being read for the first time.

## 4. Test plan

- `cargo test` (full `trader` suite): new unit tests for `LatestPolySignal::best_ask` (UP direct,
  DOWN-derived, falls back to `None` when unobserved — mirrors `best_bid`'s existing tests);
  `try_enter`'s aggressive taker price calc (with a real observed ask, falling back to mid when
  none observed, capped at `max_buy_price`); `entry_signal_price`/slippage threading through
  `finalize_entry_fill` for both the FAK and maker-Matched-on-placement paths; CSV row shape
  (header + one written row parse to the same column count). Existing tests updated where the
  new `Action::PlaceBuy.signal_price` field and `AssetParams.order_slippage` field require it
  (compile-driven — every literal `Action::PlaceBuy`/`AssetParams` construction across
  `worker.rs`/`config.rs`/`backtest.rs`/`machine.rs`/`bin/live.rs`).
- `cargo fmt --all --check`, `cargo clippy --all-targets --all-features -- -D warnings`.
- Local soak: run `live --paper` locally against real feeds long enough to observe at least one
  full entry → fill → TP-resting → close cycle; read the actual rendered Telegram/console output
  (slippage line, TP-resting notification, CSV row) rather than assuming the format is right.

## 5. Rollout

1. This doc — pushed first.
2. Implement §2 in `trader/`, full local test pass (§4).
3. Generate `strategy_20260721_taker.toml` (§3).
4. Archive the current 24h run's `live_logs` (`paper_trades_*`/`paper_quotes_*`/
   `paper_pup_vetoes_*`/`live_state_*`/`paper_control_log.jsonl`) on Oracle to
   `live_logs/archive_paper_run_20260720_24h_maker/` — same precedent as the two prior
   archivals (`archive_paper_run_20260719_mid_pricing/`, and 2026-07-20's discarded-48h note).
5. Deploy: `./scripts/deploy_trader.sh` (trader-only — no `price_feed` changes this round).
6. Post-deploy verification (within 15 min): clean restart (`journalctl -u trader-live`), first
   taker entry fires and fills, TP resting sell follows with a sane target price, Telegram shows
   the new slippage line, fresh CSVs accruing rows with `entry_signal_price` populated.
7. Let it run; a follow-up evaluation doc (same shape as the 48h plan's §2.7) is out of scope for
   this plan and will be written once enough trades have accrued to say something meaningful.
