// Status/params display formatting — the read side of the control plane.
// Workers publish a lightweight StatusSnapshot; this module turns it into the
// text the Telegram task sends back (mirrors telegram_bot.py's `_status`).

use std::collections::BTreeMap;

#[derive(Debug, Clone, Default, PartialEq)]
pub struct AssetStatus {
    pub wins: u32,
    pub losses: u32,
    pub stoplosses: u32,
    pub unwinds: u32,
    pub halted: bool,
    pub last_trade: Option<String>, // e.g. "14:32:10 DOWN STOPLOSS -0.2235"
}

/// Render a `/status` reply from a fixed set of per-asset snapshots, sorted by
/// asset name for determinism (golden-testable).
pub fn format_status(snapshots: &BTreeMap<String, AssetStatus>) -> String {
    if snapshots.is_empty() {
        return "No status data yet.".to_string();
    }
    let mut lines = vec!["📊 <b>Status</b>".to_string()];
    for (asset, s) in snapshots {
        let light = if s.halted { "🛑" } else { "🟢" };
        let trades = s.wins + s.losses + s.stoplosses + s.unwinds;
        lines.push(format!(
            "{light} <b>{asset}</b>  W:{} L:{} SL:{} UW:{}  ({trades} trades)",
            s.wins, s.losses, s.stoplosses, s.unwinds
        ));
        if let Some(last) = &s.last_trade {
            lines.push(format!("    last: {last}"));
        }
    }
    lines.join("\n")
}

/// Render a `/params` reply for one asset's resolved parameters.
pub fn format_params(asset: &str, params: &crate::config::AssetParams) -> String {
    format!(
        "⚙️ <b>{asset}</b> params\n\
         strategies: {:?}\n\
         reversal: {} (low={}, start={}s)\n\
         price_high_rev: {}\n\
         delta_pct_rev: {}  delta_pct_hp: {}\n\
         sl_reversal: {}  sl_high_prob: {}\n\
         sl_pnl_rev: {}  unwind_pnl_rev: {}\n\
         sl_pnl_hp: {}  unwind_pnl_hp: {}\n\
         halt_rev: {}  halt_prob: {}\n\
         trade_size_usdc: {}",
        params.strategies,
        params.reversal,
        params.reversal_low_threshold,
        params.reversal_start_time,
        params.price_high_rev,
        params.delta_pct_rev,
        params.delta_pct_hp,
        params.sl_reversal,
        params.sl_high_prob,
        params.sl_pnl_rev,
        params.unwind_pnl_rev,
        params.sl_pnl_hp,
        params.unwind_pnl_hp,
        params.halt_rev,
        params.halt_prob,
        params.trade_size_usdc,
    )
}

pub const HELP_TEXT: &str = "\
/status           — W/L count + halt state per asset
/params <ASSET>   — show current resolved params
/set <k> <v>      — change a config param (default asset)
/set <k> <ASSET> <v> — change a per-asset config param
/halt [asset]     — suppress new entries (all assets or one)
/resume [asset]   — clear a halt
/reset_losses [asset] — zero the halt loss counter
/strategies [ASSET strat1,strat2] — show or set active strategies
/trade_assets [A,B,C] — show or set the traded asset list
/delta <hp|rev> [ASSET] <f> — set the delta_pct gate for a strategy
/help             — this message";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_status_message() {
        assert_eq!(format_status(&BTreeMap::new()), "No status data yet.");
    }

    #[test]
    fn status_golden_two_assets() {
        let mut snaps = BTreeMap::new();
        snaps.insert(
            "BTC".to_string(),
            AssetStatus {
                wins: 3,
                losses: 1,
                stoplosses: 2,
                unwinds: 1,
                halted: false,
                last_trade: Some("14:32:10 DOWN STOPLOSS -0.2235".to_string()),
            },
        );
        snaps.insert(
            "ETH".to_string(),
            AssetStatus {
                wins: 0,
                losses: 0,
                stoplosses: 0,
                unwinds: 0,
                halted: true,
                last_trade: None,
            },
        );

        let expected = "\
📊 <b>Status</b>
🟢 <b>BTC</b>  W:3 L:1 SL:2 UW:1  (7 trades)
    last: 14:32:10 DOWN STOPLOSS -0.2235
🛑 <b>ETH</b>  W:0 L:0 SL:0 UW:0  (0 trades)";

        assert_eq!(format_status(&snaps), expected);
    }

    #[test]
    fn status_sorted_by_asset_name() {
        let mut snaps = BTreeMap::new();
        snaps.insert("SOL".to_string(), AssetStatus::default());
        snaps.insert("BTC".to_string(), AssetStatus::default());
        let out = format_status(&snaps);
        let btc_pos = out.find("BTC").unwrap();
        let sol_pos = out.find("SOL").unwrap();
        assert!(btc_pos < sol_pos, "BTC should render before SOL");
    }

    #[test]
    fn help_text_lists_core_commands() {
        assert!(HELP_TEXT.contains("/halt"));
        assert!(HELP_TEXT.contains("/set"));
        assert!(HELP_TEXT.contains("/status"));
    }
}
