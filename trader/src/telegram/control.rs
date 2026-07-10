// Control channel — Command -> ControlMsg routed to workers over mpsc.
//
// Mirrors the JSON dicts telegram_bot.py puts on `command_queue`
// ({"action": "set_param", ...} etc). The Telegram task owns no worker state;
// it only produces ControlMsgs. Workers apply them via the `ControlTarget` trait.

use anyhow::Result;

use crate::telegram::commands::{Command, DeltaStrat};

#[derive(Debug, Clone, PartialEq)]
pub enum ControlMsg {
    SetParam {
        param: String,
        key: String,
        value: String,
    },
    SetStrategies {
        asset: String,
        strategies: Vec<String>,
    },
    ResetLosses {
        asset: String,
    }, // "" = all
    Halt {
        asset: String,
        strategy: Option<String>,
    }, // asset "" = all; strategy None = all strategies for the asset
    Resume {
        asset: String,
        strategy: Option<String>,
    }, // asset "" = all; strategy None = all strategies for the asset
}

/// Convert a parsed `Command` into a `ControlMsg`, if that command mutates state.
/// Display-only commands (Status/Params/Help/...) have no control effect and
/// return `None` — the Telegram task renders them directly instead.
pub fn command_to_control(cmd: &Command) -> Option<ControlMsg> {
    match cmd {
        Command::Set { param, key, value } => Some(ControlMsg::SetParam {
            param: param.clone(),
            key: key.clone(),
            value: value.clone(),
        }),
        Command::DeltaSet { strat, key, value } => {
            let param = match strat {
                DeltaStrat::HighProb => "delta_pct_hp",
                DeltaStrat::Reversal => "delta_pct_rev",
            };
            Some(ControlMsg::SetParam {
                param: param.to_string(),
                key: key.clone(),
                value: value.clone(),
            })
        }
        Command::TradeAssetsSet(assets) => Some(ControlMsg::SetParam {
            param: "trade_assets".to_string(),
            key: "default".to_string(),
            value: assets.join(","),
        }),
        Command::StrategiesSet { asset, strategies } => Some(ControlMsg::SetStrategies {
            asset: asset.clone(),
            strategies: strategies.clone(),
        }),
        Command::ResetLosses { asset } => Some(ControlMsg::ResetLosses {
            asset: asset.clone(),
        }),
        Command::Halt { asset, strategy } => Some(ControlMsg::Halt {
            asset: asset.clone(),
            strategy: strategy.clone(),
        }),
        Command::Resume { asset, strategy } => Some(ControlMsg::Resume {
            asset: asset.clone(),
            strategy: strategy.clone(),
        }),
        _ => None,
    }
}

/// The mutation boundary a worker exposes to the control plane. A live worker
/// implements this against its `Arc<RwLock<AssetParams>>` + strategy handles +
/// per-strategy `entry_suppressed` flag (§8: halt is a no-entry gate, not a
/// state transition — `halt`/`resume` here must only set/clear that flag).
pub trait ControlTarget {
    fn set_param(&mut self, param: &str, key: &str, value: &str) -> Result<()>;
    fn set_strategies(&mut self, asset: &str, strategies: &[String]);
    fn reset_losses(&mut self, asset: &str);
    fn halt(&mut self, asset: &str, strategy: Option<&str>);
    fn resume(&mut self, asset: &str, strategy: Option<&str>);
}

/// Apply a `ControlMsg` to a target. Returns an error only for a malformed
/// `set_param` (e.g. non-numeric value) — routing errors, not command-parse
/// errors (those are rejected earlier by `parse_command`).
pub fn apply_control(target: &mut impl ControlTarget, msg: &ControlMsg) -> Result<()> {
    match msg {
        ControlMsg::SetParam { param, key, value } => target.set_param(param, key, value)?,
        ControlMsg::SetStrategies { asset, strategies } => target.set_strategies(asset, strategies),
        ControlMsg::ResetLosses { asset } => target.reset_losses(asset),
        ControlMsg::Halt { asset, strategy } => target.halt(asset, strategy.as_deref()),
        ControlMsg::Resume { asset, strategy } => target.resume(asset, strategy.as_deref()),
    }
    Ok(())
}

/// Convenience: parse `value` as f64, mirroring the Python `/set` numeric coercion.
pub fn parse_f64(value: &str) -> Result<f64> {
    value
        .parse::<f64>()
        .map_err(|_| anyhow::anyhow!("not a number: {value}"))
}

/// Convenience: parse `value` as i64 (for halt counters / hour params).
pub fn parse_i64(value: &str) -> Result<i64> {
    value
        .parse::<i64>()
        .map_err(|_| anyhow::anyhow!("not an integer: {value}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[derive(Default)]
    struct MockWorker {
        params: HashMap<(String, String), String>, // (param, key) -> value
        strategies: HashMap<String, Vec<String>>,
        losses_reset: Vec<String>,
        suppressed: std::collections::HashSet<String>,
    }

    impl ControlTarget for MockWorker {
        fn set_param(&mut self, param: &str, key: &str, value: &str) -> Result<()> {
            // Mirror Python: numeric params must parse; reject garbage.
            if param != "trade_assets" {
                parse_f64(value).map_err(|_| anyhow::anyhow!("bad value for {param}: {value}"))?;
            }
            self.params
                .insert((param.to_string(), key.to_string()), value.to_string());
            Ok(())
        }
        fn set_strategies(&mut self, asset: &str, strategies: &[String]) {
            self.strategies
                .insert(asset.to_string(), strategies.to_vec());
        }
        fn reset_losses(&mut self, asset: &str) {
            self.losses_reset.push(asset.to_string());
        }
        fn halt(&mut self, asset: &str, strategy: Option<&str>) {
            let key = match (asset.is_empty(), strategy) {
                (true, _) => "*".to_string(),
                (false, Some(s)) => format!("{asset}:{s}"),
                (false, None) => asset.to_string(),
            };
            self.suppressed.insert(key);
        }
        fn resume(&mut self, asset: &str, strategy: Option<&str>) {
            if asset.is_empty() {
                self.suppressed.clear();
            } else {
                match strategy {
                    Some(s) => {
                        self.suppressed.remove(&format!("{asset}:{s}"));
                    }
                    None => {
                        self.suppressed.remove(asset);
                    }
                }
            }
        }
    }

    #[test]
    fn set_param_reversal_mutates_target() {
        let cmd = crate::telegram::commands::parse_command("/set reversal BTC 0.6").unwrap();
        let msg = command_to_control(&cmd).expect("set_param produces a ControlMsg");
        let mut w = MockWorker::default();
        apply_control(&mut w, &msg).expect("apply succeeds");
        assert_eq!(
            w.params.get(&("reversal".to_string(), "BTC".to_string())),
            Some(&"0.6".to_string())
        );
    }

    #[test]
    fn set_param_rejects_non_numeric_value() {
        let msg = ControlMsg::SetParam {
            param: "reversal".to_string(),
            key: "BTC".to_string(),
            value: "not_a_number".to_string(),
        };
        let mut w = MockWorker::default();
        assert!(apply_control(&mut w, &msg).is_err());
    }

    #[test]
    fn halt_is_no_entry_gate_and_resume_clears_it() {
        let mut w = MockWorker::default();
        apply_control(
            &mut w,
            &ControlMsg::Halt {
                asset: "BTC".to_string(),
                strategy: None,
            },
        )
        .unwrap();
        assert!(w.suppressed.contains("BTC"));
        apply_control(
            &mut w,
            &ControlMsg::Resume {
                asset: "BTC".to_string(),
                strategy: None,
            },
        )
        .unwrap();
        assert!(!w.suppressed.contains("BTC"));
    }

    #[test]
    fn global_halt_and_resume() {
        let mut w = MockWorker::default();
        apply_control(
            &mut w,
            &ControlMsg::Halt {
                asset: "".to_string(),
                strategy: None,
            },
        )
        .unwrap();
        assert!(w.suppressed.contains("*"));
        apply_control(
            &mut w,
            &ControlMsg::Resume {
                asset: "".to_string(),
                strategy: None,
            },
        )
        .unwrap();
        assert!(w.suppressed.is_empty());
    }

    #[test]
    fn strategy_scoped_halt_leaves_other_strategy_running() {
        let cmd = crate::telegram::commands::parse_command("/halt eth high_prob").unwrap();
        let msg = command_to_control(&cmd).unwrap();
        let mut w = MockWorker::default();
        apply_control(&mut w, &msg).unwrap();
        assert!(w.suppressed.contains("ETH:high_prob"));
        assert!(!w.suppressed.contains("ETH:reversal"));

        let resume_cmd = crate::telegram::commands::parse_command("/resume eth high_prob").unwrap();
        let resume_msg = command_to_control(&resume_cmd).unwrap();
        apply_control(&mut w, &resume_msg).unwrap();
        assert!(!w.suppressed.contains("ETH:high_prob"));
    }

    #[test]
    fn set_strategies_routes_correctly() {
        let cmd =
            crate::telegram::commands::parse_command("/strategies ETH high_prob,reversal").unwrap();
        let msg = command_to_control(&cmd).unwrap();
        let mut w = MockWorker::default();
        apply_control(&mut w, &msg).unwrap();
        assert_eq!(
            w.strategies.get("ETH"),
            Some(&vec!["high_prob".to_string(), "reversal".to_string()])
        );
    }

    #[test]
    fn reset_losses_routes_correctly() {
        let cmd = crate::telegram::commands::parse_command("/reset_losses DOGE").unwrap();
        let msg = command_to_control(&cmd).unwrap();
        let mut w = MockWorker::default();
        apply_control(&mut w, &msg).unwrap();
        assert_eq!(w.losses_reset, vec!["DOGE".to_string()]);
    }

    #[test]
    fn display_only_commands_produce_no_control_msg() {
        assert_eq!(command_to_control(&Command::Status), None);
        assert_eq!(command_to_control(&Command::Params), None);
        assert_eq!(command_to_control(&Command::Help), None);
        assert_eq!(command_to_control(&Command::Invalid("x".to_string())), None);
    }

    #[test]
    fn delta_set_maps_to_correct_param_name() {
        let cmd = crate::telegram::commands::parse_command("/delta rev BTC 0.0005").unwrap();
        let msg = command_to_control(&cmd).unwrap();
        assert_eq!(
            msg,
            ControlMsg::SetParam {
                param: "delta_pct_rev".to_string(),
                key: "BTC".to_string(),
                value: "0.0005".to_string()
            }
        );
    }

    #[test]
    fn trade_assets_set_joins_into_default_key() {
        let cmd = Command::TradeAssetsSet(vec!["BTC".to_string(), "ETH".to_string()]);
        let msg = command_to_control(&cmd).unwrap();
        assert_eq!(
            msg,
            ControlMsg::SetParam {
                param: "trade_assets".to_string(),
                key: "default".to_string(),
                value: "BTC,ETH".to_string()
            }
        );
    }
}
