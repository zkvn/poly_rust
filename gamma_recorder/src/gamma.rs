//! Gamma HTTP client: fetch-by-slug, bulk fetch-page, and the resolution-signal
//! logic shared by backfill and continuous mode.

use anyhow::{Context, Result, bail};
use serde::Deserialize;
use std::time::Duration;

use crate::db::Outcome;

/// The plain offset-paginated `/events` endpoint caps out around offset+limit <= 2100
/// (verified live 2026-07-15 — `offset too large, use /events/keyset for deeper
/// pagination`), which a full backfill blows through in well under a day's worth of
/// events. `/events/keyset` (cursor-based, `after_cursor`/`next_cursor`) has no such
/// cap and is used for both bulk paging and single-slug lookups.
const GAMMA_KEYSET_BASE: &str = "https://gamma-api.polymarket.com/events/keyset";

#[derive(Debug, Deserialize)]
pub struct GammaEvent {
    #[serde(default)]
    pub markets: Vec<GammaMarket>,
}

#[derive(Debug, Deserialize)]
struct KeysetResponse {
    #[serde(default)]
    events: Vec<GammaEvent>,
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GammaMarket {
    pub condition_id: Option<String>,
    pub slug: String,
    /// JSON-encoded string, e.g. `["Up", "Down"]` — Gamma quirk, not a real array field.
    #[serde(default)]
    pub outcomes: Option<String>,
    /// JSON-encoded string, e.g. `["0", "1"]`.
    #[serde(default)]
    pub outcome_prices: Option<String>,
    #[serde(default)]
    pub uma_resolution_status: Option<String>,
    #[serde(default)]
    pub closed_time: Option<String>,
    /// JSON-encoded string, e.g. `["905220...", "676718..."]`.
    #[serde(default)]
    pub clob_token_ids: Option<String>,
}

/// A decisive resolution signal extracted from a `GammaMarket`.
pub struct Signal {
    pub outcome: Outcome,
    pub up_token_id: Option<String>,
    pub down_token_id: Option<String>,
    pub resolved_at_ts: i64,
    pub resolved_at_is_estimated: bool,
}

/// Resolution check per plan doc §2: `umaResolutionStatus == "resolved"` **or**
/// `outcomePrices` containing a value >= 0.99 — HYPE observed live with a decisive
/// price but a `None` uma status, so gating on uma status alone would never resolve it.
/// Returns `Ok(None)` if the market isn't decisive yet (still `UNRESOLVED`).
pub fn resolution_signal(market: &GammaMarket, poll_time_ts: i64) -> Result<Option<Signal>> {
    let Some(outcomes_json) = &market.outcomes else {
        return Ok(None);
    };
    let Some(prices_json) = &market.outcome_prices else {
        return Ok(None);
    };
    let outcomes: Vec<String> = serde_json::from_str(outcomes_json)
        .with_context(|| format!("parsing outcomes for {}", market.slug))?;
    let prices: Vec<f64> = serde_json::from_str::<Vec<String>>(prices_json)
        .with_context(|| format!("parsing outcomePrices for {}", market.slug))?
        .iter()
        .map(|s| s.parse::<f64>())
        .collect::<Result<_, _>>()
        .with_context(|| format!("parsing outcomePrice values for {}", market.slug))?;
    if outcomes.len() != 2 || prices.len() != 2 {
        return Ok(None);
    }

    let uma_resolved = market.uma_resolution_status.as_deref() == Some("resolved");
    let price_decisive = prices.iter().any(|p| *p >= 0.99);
    if !uma_resolved && !price_decisive {
        return Ok(None);
    }

    let up_idx = outcomes.iter().position(|o| o.eq_ignore_ascii_case("up"));
    let down_idx = outcomes.iter().position(|o| o.eq_ignore_ascii_case("down"));
    let (Some(up_idx), Some(down_idx)) = (up_idx, down_idx) else {
        return Ok(None);
    };
    let outcome = if prices[up_idx] >= prices[down_idx] {
        Outcome::Up
    } else {
        Outcome::Down
    };

    let token_ids: Option<Vec<String>> = market
        .clob_token_ids
        .as_ref()
        .and_then(|s| serde_json::from_str(s).ok());
    let (up_token_id, down_token_id) = match &token_ids {
        Some(ids) if ids.len() == 2 => (Some(ids[up_idx].clone()), Some(ids[down_idx].clone())),
        _ => (None, None),
    };

    let (resolved_at_ts, resolved_at_is_estimated) = match parse_closed_time(&market.closed_time) {
        Some(ts) => (ts, false),
        None => (poll_time_ts, true),
    };

    Ok(Some(Signal {
        outcome,
        up_token_id,
        down_token_id,
        resolved_at_ts,
        resolved_at_is_estimated,
    }))
}

/// Gamma emits `closedTime` like `"2026-07-14 15:25:20+00"` — a 2-digit UTC offset
/// with no colon/minutes, which chrono's `%:z` doesn't accept as-is.
fn parse_closed_time(closed_time: &Option<String>) -> Option<i64> {
    let raw = closed_time.as_ref()?.trim();
    let normalized = normalize_offset(raw);
    chrono::DateTime::parse_from_str(&normalized, "%Y-%m-%d %H:%M:%S%:z")
        .ok()
        .map(|dt| dt.timestamp())
}

fn normalize_offset(s: &str) -> String {
    if let Some(sign_pos) = s.rfind(['+', '-']) {
        let (head, tail) = s.split_at(sign_pos);
        if tail.len() == 3 && tail[1..].chars().all(|c| c.is_ascii_digit()) {
            return format!("{head}{tail}:00");
        }
    }
    s.to_string()
}

/// Fetches one page of `/events/keyset` with the given query params (+ `after_cursor`
/// if this isn't the first page), retrying with exponential backoff (start 2s, double,
/// cap 60s, up to 6 attempts) on any transport error or non-2xx response, per plan doc
/// §6. Returns the page's events plus the cursor for the next page (`None` on the last
/// page).
pub async fn fetch_events_page(
    client: &reqwest::Client,
    query: &[(&str, String)],
    after_cursor: Option<&str>,
) -> Result<(Vec<GammaEvent>, Option<String>)> {
    let mut full_query: Vec<(&str, String)> = query.to_vec();
    if let Some(cursor) = after_cursor {
        full_query.push(("after_cursor", cursor.to_string()));
    }

    let mut backoff = Duration::from_secs(2);
    const MAX_ATTEMPTS: u32 = 6;
    let mut last_err = None;
    for attempt in 1..=MAX_ATTEMPTS {
        let resp = client
            .get(GAMMA_KEYSET_BASE)
            .query(&full_query)
            .send()
            .await;
        match resp {
            Ok(r) if r.status().is_success() => {
                let body: KeysetResponse = r
                    .json()
                    .await
                    .context("parsing Gamma /events/keyset response")?;
                return Ok((body.events, body.next_cursor));
            }
            Ok(r) => {
                last_err = Some(anyhow::anyhow!(
                    "Gamma /events/keyset returned HTTP {}",
                    r.status()
                ));
            }
            Err(e) => {
                last_err = Some(anyhow::Error::from(e));
            }
        }
        if attempt < MAX_ATTEMPTS {
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(60));
        }
    }
    bail!(
        "Gamma /events/keyset failed after {} attempts: {}",
        MAX_ATTEMPTS,
        last_err.map(|e| e.to_string()).unwrap_or_default()
    );
}

/// Fetches the single event for a slug (continuous-mode sweep). `None` if Gamma has
/// no event for this slug.
pub async fn fetch_by_slug(client: &reqwest::Client, slug: &str) -> Result<Option<GammaMarket>> {
    let query = [("slug", slug.to_string())];
    let (events, _) = fetch_events_page(client, &query, None).await?;
    Ok(events
        .into_iter()
        .next()
        .and_then(|e| e.markets.into_iter().next()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn market(
        outcomes: &str,
        prices: &str,
        uma: Option<&str>,
        closed_time: Option<&str>,
        token_ids: &str,
    ) -> GammaMarket {
        GammaMarket {
            condition_id: Some("0xabc".to_string()),
            slug: "btc-updown-5m-1700000000".to_string(),
            outcomes: Some(outcomes.to_string()),
            outcome_prices: Some(prices.to_string()),
            uma_resolution_status: uma.map(str::to_string),
            closed_time: closed_time.map(str::to_string),
            clob_token_ids: Some(token_ids.to_string()),
        }
    }

    #[test]
    fn resolved_via_uma_status() {
        let m = market(
            r#"["Up", "Down"]"#,
            r#"["1", "0"]"#,
            Some("resolved"),
            Some("2026-07-14 15:25:20+00"),
            r#"["up-tok", "down-tok"]"#,
        );
        let sig = resolution_signal(&m, 999).unwrap().unwrap();
        assert!(matches!(sig.outcome, Outcome::Up));
        assert!(!sig.resolved_at_is_estimated);
        assert_eq!(sig.up_token_id.as_deref(), Some("up-tok"));
        assert_eq!(sig.down_token_id.as_deref(), Some("down-tok"));
    }

    #[test]
    fn resolved_via_price_threshold_without_uma_status_hype_case() {
        let m = market(
            r#"["Up", "Down"]"#,
            r#"["0.0005", "0.9995"]"#,
            None,
            None,
            r#"["up-tok", "down-tok"]"#,
        );
        let sig = resolution_signal(&m, 12345).unwrap().unwrap();
        assert!(matches!(sig.outcome, Outcome::Down));
        assert!(sig.resolved_at_is_estimated);
        assert_eq!(sig.resolved_at_ts, 12345);
    }

    #[test]
    fn not_yet_decisive_returns_none() {
        let m = market(
            r#"["Up", "Down"]"#,
            r#"["0.45", "0.55"]"#,
            None,
            None,
            r#"["up-tok", "down-tok"]"#,
        );
        assert!(resolution_signal(&m, 0).unwrap().is_none());
    }

    #[test]
    fn parses_gamma_closed_time_offset_format() {
        let ts = parse_closed_time(&Some("2026-07-14 15:25:20+00".to_string())).unwrap();
        // 2026-07-14T15:25:20Z
        assert_eq!(ts, 1_784_042_720);
    }
}
