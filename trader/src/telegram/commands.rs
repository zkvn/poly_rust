// Command parsing — mirrors the surface in bot/telegram_bot.py `_handle` and
// bot/config.py `SETTABLE_PARAMS`. Pure function: text -> Command, no I/O.

use std::collections::HashSet;

/// Params settable via `/set <param> <value>` — must match bot/config.py::SETTABLE_PARAMS.
pub fn settable_params() -> HashSet<&'static str> {
    [
        "trade_size_usdc",
        "price_low",
        "price_high",
        "price_high_rev",
        "reversal",
        "halt_prob",
        "halt_rev",
        "halt_reset_hour_rev",
        "halt_reset_hour_hp",
        "sl_high_prob",
        "sl_reversal",
        "unwind_pnl_rev",
        "sl_pnl_rev",
        "unwind_pnl_hp",
        "sl_pnl_hp",
        "enter_when_time_left",
        "no_enter_when_time_left",
        "reversal_low_threshold",
        "min_cycle_remaining_at_load",
        "trade_assets",
        "delta_pct_hp",
        "delta_pct_rev",
    ]
    .into_iter()
    .collect()
}

/// Per-asset dict params: `/set <param> <ASSET> <value>` (4 args) vs `/set <param> <value>`
/// (3 args, sets "default"). Must match the `_DICT_PARAMS` set in telegram_bot.py.
fn dict_params() -> HashSet<&'static str> {
    [
        "halt_prob",
        "halt_rev",
        "halt_reset_hour_rev",
        "halt_reset_hour_hp",
        "sl_high_prob",
        "sl_reversal",
        "delta_pct_hp",
        "delta_pct_rev",
        "price_low",
        "price_high",
        "price_high_rev",
        "reversal",
        "reversal_low_threshold",
        "trade_size_usdc",
        "enter_when_time_left",
    ]
    .into_iter()
    .collect()
}

/// Resolve a param alias to its canonical name (mirrors `_PARAM_ALIASES`).
fn resolve_alias(param: &str) -> String {
    match param {
        "min_delta_hp" => "delta_pct_hp".to_string(),
        "min_delta_rev" => "delta_pct_rev".to_string(),
        "delta_hp" => "delta_pct_hp".to_string(),
        "delta_rev" => "delta_pct_rev".to_string(),
        other => other.to_string(),
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum DeltaStrat {
    HighProb,
    Reversal,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Command {
    TradeHist,
    Assets,
    TradeAssetsQuery,
    TradeAssetsSet(Vec<String>),
    StrategiesQuery,
    StrategiesSet {
        asset: String,
        strategies: Vec<String>,
    },
    Markets,
    DeltaQuery,
    DeltaSet {
        strat: DeltaStrat,
        key: String,
        value: String,
    },
    ReconToday {
        asset: Option<String>,
        date: Option<String>,
    },
    WarningAnalysis {
        date: Option<String>,
    },
    Sys,
    Last(usize),
    LiveLog(usize),
    SkippedLog(usize),
    Status,
    Params,
    Set {
        param: String,
        key: String,
        value: String,
    },
    ResetLosses {
        asset: String,
    }, // "" = all
    Halt {
        asset: String,
    }, // "" = all
    Resume {
        asset: String,
    }, // "" = all
    Help,
    /// Recognized-but-malformed usage, or an unrecognized command.
    Invalid(String),
}

/// Parse one incoming Telegram message text into a `Command`.
/// Returns `None` only for a blank/whitespace-only message (nothing to dispatch).
pub fn parse_command(text: &str) -> Option<Command> {
    let parts: Vec<&str> = text.split_whitespace().collect();
    if parts.is_empty() {
        return None;
    }
    let cmd = parts[0].to_lowercase();

    Some(match cmd.as_str() {
        "/trade_hist" => Command::TradeHist,
        "/assets" => Command::Assets,
        "/trade_assets" => {
            if parts.len() > 1 {
                let joined = parts[1..].join(" ");
                let list: Vec<String> = joined
                    .split(',')
                    .map(|s| s.trim().to_uppercase())
                    .filter(|s| !s.is_empty())
                    .collect();
                Command::TradeAssetsSet(list)
            } else {
                Command::TradeAssetsQuery
            }
        }
        "/strategies" => {
            if parts.len() == 1 {
                Command::StrategiesQuery
            } else if parts.len() < 3 {
                Command::Invalid("Usage: /strategies <ASSET> <strategy1[,strategy2]>".to_string())
            } else {
                let asset = parts[1].to_uppercase();
                let joined = parts[2..].join(" ");
                let names: Vec<String> = joined
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                let valid: HashSet<&str> = ["high_prob", "reversal"].into_iter().collect();
                let invalid: Vec<&String> = names
                    .iter()
                    .filter(|n| !valid.contains(n.as_str()))
                    .collect();
                if !invalid.is_empty() {
                    Command::Invalid(format!(
                        "Unknown strategies: {}",
                        invalid
                            .iter()
                            .map(|s| s.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    ))
                } else {
                    Command::StrategiesSet {
                        asset,
                        strategies: names,
                    }
                }
            }
        }
        "/markets" => Command::Markets,
        "/delta" => {
            if parts.len() == 1 {
                Command::DeltaQuery
            } else {
                let strat = match parts[1].to_lowercase().as_str() {
                    "hp" => Some(DeltaStrat::HighProb),
                    "rev" => Some(DeltaStrat::Reversal),
                    _ => None,
                };
                match strat {
                    None => Command::Invalid("Usage: /delta <hp|rev> [ASSET] <f>".to_string()),
                    Some(_) if parts.len() < 3 => {
                        Command::Invalid("Usage: /delta <hp|rev> [ASSET] <f>".to_string())
                    }
                    Some(strat) => {
                        let (key, value) = if parts.len() == 4 {
                            (parts[2].to_uppercase(), parts[3].to_string())
                        } else {
                            ("default".to_string(), parts[2].to_string())
                        };
                        Command::DeltaSet { strat, key, value }
                    }
                }
            }
        }
        "/recon_today" => {
            let asset = parts[1..]
                .iter()
                .find(|p| p.parse::<u64>().is_err())
                .map(|s| s.to_uppercase());
            let date = parts[1..]
                .iter()
                .find(|p| p.parse::<u64>().is_ok())
                .map(|s| s.to_string());
            Command::ReconToday { asset, date }
        }
        "/warning_analysis" => Command::WarningAnalysis {
            date: parts.get(1).map(|s| s.to_string()),
        },
        "/sys" => Command::Sys,
        c if c.starts_with("/last") => {
            let n = parts
                .get(1)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(20);
            Command::Last(n)
        }
        c if c.starts_with("/live_log") => {
            let n = parts
                .get(1)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(40);
            Command::LiveLog(n)
        }
        c if c.starts_with("/skipped_log") => {
            let n = parts
                .get(1)
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(40);
            Command::SkippedLog(n)
        }
        "/status" => Command::Status,
        "/params" => Command::Params,
        "/set" => {
            if parts.len() < 3 {
                Command::Invalid(format!(
                    "Usage: /set <param> <value>  or  /set <param> <ASSET> <value>\nAllowed: {}",
                    {
                        let mut v: Vec<_> = settable_params().into_iter().collect();
                        v.sort();
                        v.join(", ")
                    }
                ))
            } else {
                let param = resolve_alias(&parts[1].to_lowercase());
                if !settable_params().contains(param.as_str()) {
                    Command::Invalid(format!("Unknown param {param}"))
                } else if dict_params().contains(param.as_str()) && parts.len() == 4 {
                    Command::Set {
                        param,
                        key: parts[2].to_uppercase(),
                        value: parts[3].to_string(),
                    }
                } else {
                    Command::Set {
                        param,
                        key: "default".to_string(),
                        value: parts[2].to_string(),
                    }
                }
            }
        }
        "/reset_losses" => Command::ResetLosses {
            asset: parts.get(1).map(|s| s.to_uppercase()).unwrap_or_default(),
        },
        "/halt" => Command::Halt {
            asset: parts.get(1).map(|s| s.to_uppercase()).unwrap_or_default(),
        },
        "/resume" => Command::Resume {
            asset: parts.get(1).map(|s| s.to_uppercase()).unwrap_or_default(),
        },
        "/start" | "/help" => Command::Help,
        _ => Command::Invalid(format!("Unknown command: {cmd}")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_status_and_help() {
        assert_eq!(parse_command("/status"), Some(Command::Status));
        assert_eq!(parse_command("/help"), Some(Command::Help));
        assert_eq!(parse_command("/start"), Some(Command::Help));
    }

    #[test]
    fn parses_set_default_scalar() {
        assert_eq!(
            parse_command("/set trade_size_usdc 2.0"),
            Some(Command::Set {
                param: "trade_size_usdc".to_string(),
                key: "default".to_string(),
                value: "2.0".to_string()
            })
        );
    }

    #[test]
    fn parses_set_per_asset_dict_param() {
        assert_eq!(
            parse_command("/set halt_rev BTC 3"),
            Some(Command::Set {
                param: "halt_rev".to_string(),
                key: "BTC".to_string(),
                value: "3".to_string()
            })
        );
    }

    #[test]
    fn set_resolves_aliases() {
        assert_eq!(
            parse_command("/set delta_rev BTC 0.0005"),
            Some(Command::Set {
                param: "delta_pct_rev".to_string(),
                key: "BTC".to_string(),
                value: "0.0005".to_string()
            })
        );
    }

    #[test]
    fn set_rejects_unknown_param() {
        match parse_command("/set not_a_real_param 1").unwrap() {
            Command::Invalid(_) => {}
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn set_rejects_too_few_args() {
        match parse_command("/set trade_size_usdc").unwrap() {
            Command::Invalid(_) => {}
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn parses_halt_resume_scoped_and_global() {
        assert_eq!(
            parse_command("/halt"),
            Some(Command::Halt {
                asset: "".to_string()
            })
        );
        assert_eq!(
            parse_command("/halt btc"),
            Some(Command::Halt {
                asset: "BTC".to_string()
            })
        );
        assert_eq!(
            parse_command("/resume ETH"),
            Some(Command::Resume {
                asset: "ETH".to_string()
            })
        );
    }

    #[test]
    fn parses_reset_losses() {
        assert_eq!(
            parse_command("/reset_losses"),
            Some(Command::ResetLosses {
                asset: "".to_string()
            })
        );
        assert_eq!(
            parse_command("/reset_losses doge"),
            Some(Command::ResetLosses {
                asset: "DOGE".to_string()
            })
        );
    }

    #[test]
    fn parses_strategies_query_and_set() {
        assert_eq!(parse_command("/strategies"), Some(Command::StrategiesQuery));
        assert_eq!(
            parse_command("/strategies BTC high_prob,reversal"),
            Some(Command::StrategiesSet {
                asset: "BTC".to_string(),
                strategies: vec!["high_prob".to_string(), "reversal".to_string()],
            })
        );
    }

    #[test]
    fn strategies_set_rejects_invalid_strategy_name() {
        match parse_command("/strategies BTC not_real").unwrap() {
            Command::Invalid(_) => {}
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn parses_delta_default_and_scoped() {
        assert_eq!(
            parse_command("/delta hp 0.0004"),
            Some(Command::DeltaSet {
                strat: DeltaStrat::HighProb,
                key: "default".to_string(),
                value: "0.0004".to_string()
            })
        );
        assert_eq!(
            parse_command("/delta rev BTC 0.0005"),
            Some(Command::DeltaSet {
                strat: DeltaStrat::Reversal,
                key: "BTC".to_string(),
                value: "0.0005".to_string()
            })
        );
    }

    #[test]
    fn parses_trade_assets_query_and_set() {
        assert_eq!(
            parse_command("/trade_assets"),
            Some(Command::TradeAssetsQuery)
        );
        assert_eq!(
            parse_command("/trade_assets BTC, ETH, doge"),
            Some(Command::TradeAssetsSet(vec![
                "BTC".to_string(),
                "ETH".to_string(),
                "DOGE".to_string()
            ]))
        );
    }

    #[test]
    fn unknown_command_is_invalid() {
        match parse_command("/frobnicate").unwrap() {
            Command::Invalid(_) => {}
            other => panic!("expected Invalid, got {other:?}"),
        }
    }

    #[test]
    fn blank_message_is_none() {
        assert_eq!(parse_command("   "), None);
        assert_eq!(parse_command(""), None);
    }

    #[test]
    fn command_is_case_insensitive() {
        assert_eq!(parse_command("/STATUS"), Some(Command::Status));
        assert_eq!(
            parse_command("/HALT"),
            Some(Command::Halt {
                asset: "".to_string()
            })
        );
    }
}
