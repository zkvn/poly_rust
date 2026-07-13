//! FIFA World Cup market discovery — thin wrapper over `event_monitor`'s shared discovery/
//! monitoring core. See `event_monitor.rs`'s doc comment for why this is monitoring-only
//! (not run through `Machine`) and why subscriptions are batched per event, not per bucket.
//!
//! Unlike weather, World Cup event slugs are **static** (no per-day rotation) — configured
//! directly in `config/worldcup_events.toml` as a fixed list, gathered live from
//! `gamma-api.polymarket.com/public-search` on 2026-07-13 (62 active FIFA World Cup events,
//! filtered out unrelated "World Cup" results — cricket T20/U19 qualifiers and the Esports
//! World Cup, which share the search term but are different tournaments entirely). Includes
//! everything from the outright "World Cup Winner" (a negRisk group, one bucket per team —
//! Spain/England/France/Argentina still live as of this list; the rest already resolved to
//! 0/1 at semifinal stage) down to narrow prop bets (player goal counts, award winners,
//! record-broken markets). Refresh still re-fetches periodically in case Polymarket adds
//! markets to an existing event (e.g. a new stage-of-elimination outcome resolving).

use crate::event_monitor::{EventIdentity, EventSinks, run_event_supervisor};
use crate::market::SharedClients;

/// Entry point `main.rs` spawns once per configured event slug.
pub async fn run_event_supervisor_for(
    slug: String,
    clients: SharedClients,
    sinks: EventSinks,
    refresh_interval_secs: u64,
) {
    let identity = EventIdentity {
        log_key: format!("worldcup:{slug}"),
        snapshot_prefix: format!("worldcup:{slug}"),
        kind: "worldcup",
        display_name: slug.clone(),
    };
    let slug_fn = move || slug.clone();
    run_event_supervisor(identity, slug_fn, clients, sinks, refresh_interval_secs).await;
}
