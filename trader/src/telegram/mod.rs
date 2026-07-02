// Telegram control plane — bot task, long-poll loop, auth.
//
// NOTE: `TelegramBot::run_loop` makes real HTTP calls to api.telegram.org and
// requires a bot token + chat id (secrets, not committed). It is not invoked
// anywhere in this crate's tests or binaries yet — wiring it into a live
// worker is an A3/I-phase integration step that needs the user's own bot
// token and explicit go-ahead to run against the real Telegram API.

pub mod commands;
pub mod control;
pub mod render;

use anyhow::{Context, Result};
use serde::Deserialize;

use commands::{parse_command, Command};
use control::{command_to_control, ControlMsg};

#[derive(Debug, Clone)]
pub struct AuthConfig {
    pub token: String,
    pub chat_id: i64,
    /// 0 = accept any user in the allowed chat.
    pub user_id: i64,
}

impl AuthConfig {
    /// `chat_id == 0` means "not yet configured" (discovery mode) — every
    /// message is rejected until a real chat id is set, mirroring
    /// telegram_bot.py's `if not CHAT_ID: discovery mode`.
    pub fn is_authorized(&self, chat_id: i64, from_id: i64) -> bool {
        if self.chat_id == 0 {
            return false;
        }
        if chat_id != self.chat_id {
            return false;
        }
        if self.user_id != 0 && from_id != 0 && from_id != self.user_id {
            return false;
        }
        true
    }
}

/// The outcome of dispatching one incoming message: either a control mutation
/// to forward to a worker, or text to send back to the chat (or both — an
/// unrecognized/malformed command still gets a reply and produces no control
/// message).
pub struct Dispatch {
    pub control: Option<ControlMsg>,
    pub reply: Option<String>,
}

/// Pure dispatch: parse `text`, decide the ControlMsg (if any) and the reply
/// text (if any). No I/O — testable without a network or a real worker.
pub fn dispatch(text: &str) -> Option<Dispatch> {
    let cmd = parse_command(text)?;
    let control = command_to_control(&cmd);
    let reply = match &cmd {
        Command::Help => Some(render::HELP_TEXT.to_string()),
        Command::Invalid(msg) => Some(msg.clone()),
        Command::Set { param, key, value } => Some(format!(
            "✅ Sent: set {param}[{key}] = {value}"
        )),
        Command::Halt { asset } => Some(format!(
            "🛑 Sent halt for {}",
            if asset.is_empty() { "all assets".to_string() } else { asset.clone() }
        )),
        Command::Resume { asset } => Some(format!(
            "▶️ Sent resume for {}",
            if asset.is_empty() { "all assets".to_string() } else { asset.clone() }
        )),
        Command::ResetLosses { asset } => Some(format!(
            "🔄 reset_losses sent for {}",
            if asset.is_empty() { "all assets".to_string() } else { asset.clone() }
        )),
        Command::StrategiesSet { asset, strategies } => Some(format!(
            "✅ Sent: {asset} strategies = {}", strategies.join(", ")
        )),
        Command::TradeAssetsSet(assets) => Some(format!(
            "✅ Sent: trade_assets = {}", assets.join(",")
        )),
        Command::DeltaSet { key, value, .. } => Some(format!(
            "✅ Sent: set delta[{key}] = {value}"
        )),
        // Display-only / reporting commands: the caller (worker/render layer)
        // fills in the actual reply from live state; no canned text here.
        _ => None,
    };
    Some(Dispatch { control, reply })
}

// ── Live network layer (untested here — needs a real bot token) ─────────────

#[derive(Debug, Deserialize)]
struct GetUpdatesResponse {
    result: Vec<Update>,
}

#[derive(Debug, Deserialize)]
struct Update {
    update_id: i64,
    message: Option<Message>,
}

#[derive(Debug, Deserialize)]
struct Message {
    text: Option<String>,
    chat: Chat,
    from: Option<From>,
}

#[derive(Debug, Deserialize)]
struct Chat {
    id: i64,
}

#[derive(Debug, Deserialize)]
struct From {
    id: i64,
}

pub struct IncomingMessage {
    pub text: String,
    pub chat_id: i64,
    pub from_id: i64,
}

pub struct TelegramBot {
    auth: AuthConfig,
    http: reqwest::Client,
    api_base: String,
    offset: i64,
}

impl TelegramBot {
    pub fn new(auth: AuthConfig) -> Result<Self> {
        let http = reqwest::Client::builder().build().context("http client")?;
        let api_base = format!("https://api.telegram.org/bot{}", auth.token);
        Ok(Self { auth, http, api_base, offset: 0 })
    }

    pub async fn send(&self, text: &str) -> Result<()> {
        if self.auth.chat_id == 0 {
            return Ok(());
        }
        self.http
            .post(format!("{}/sendMessage", self.api_base))
            .json(&serde_json::json!({
                "chat_id": self.auth.chat_id,
                "text": text,
                "parse_mode": "HTML",
            }))
            .send()
            .await
            .context("sendMessage")?;
        Ok(())
    }

    /// One long-poll cycle (30s server-side timeout). Returns authorized
    /// messages only — unauthorized senders are silently dropped, matching
    /// telegram_bot.py's `_handle` behavior.
    pub async fn poll_once(&mut self) -> Result<Vec<IncomingMessage>> {
        let resp: GetUpdatesResponse = self
            .http
            .get(format!("{}/getUpdates", self.api_base))
            .query(&[("offset", self.offset.to_string()), ("timeout", "30".to_string())])
            .timeout(std::time::Duration::from_secs(35))
            .send()
            .await
            .context("getUpdates")?
            .json()
            .await
            .context("getUpdates json")?;

        let mut out = Vec::new();
        for update in resp.result {
            self.offset = update.update_id + 1;
            let Some(msg) = update.message else { continue };
            let Some(text) = msg.text else { continue };
            let chat_id = msg.chat.id;
            let from_id = msg.from.map(|f| f.id).unwrap_or(0);
            if self.auth.is_authorized(chat_id, from_id) {
                out.push(IncomingMessage { text, chat_id, from_id });
            }
        }
        Ok(out)
    }

    /// Long-poll loop. Forwards each dispatched ControlMsg on `control_tx` and
    /// sends any reply text back to the chat. Runs until the channel closes or
    /// an unrecoverable HTTP error occurs.
    ///
    /// NOT invoked anywhere yet — requires a real `TELEGRAM_BOT_TOKEN` +
    /// `TELEGRAM_CHAT_ID` and the user's go-ahead before running live.
    pub async fn run_loop(&mut self, control_tx: tokio::sync::mpsc::UnboundedSender<ControlMsg>) -> Result<()> {
        loop {
            match self.poll_once().await {
                Ok(messages) => {
                    for m in messages {
                        if let Some(d) = dispatch(&m.text) {
                            if let Some(control) = d.control {
                                if control_tx.send(control).is_err() {
                                    return Ok(()); // receiver dropped — worker shut down
                                }
                            }
                            if let Some(reply) = d.reply {
                                let _ = self.send(&reply).await;
                            }
                        }
                    }
                }
                Err(e) => {
                    eprintln!("[telegram] poll error: {e:#}");
                    tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn auth() -> AuthConfig {
        AuthConfig { token: "dummy".to_string(), chat_id: 12345, user_id: 999 }
    }

    #[test]
    fn rejects_unknown_chat() {
        assert!(!auth().is_authorized(1, 999));
    }

    #[test]
    fn rejects_unauthorized_user_in_right_chat() {
        assert!(!auth().is_authorized(12345, 1));
    }

    #[test]
    fn accepts_right_chat_and_user() {
        assert!(auth().is_authorized(12345, 999));
    }

    #[test]
    fn accepts_right_chat_with_unknown_from_id_zero() {
        // from_id=0 happens for channel posts / some clients; Python treats
        // USER_ID=0 or from_id=0 as "don't filter on user".
        assert!(auth().is_authorized(12345, 0));
    }

    #[test]
    fn discovery_mode_rejects_everything_until_chat_id_configured() {
        let a = AuthConfig { token: "dummy".to_string(), chat_id: 0, user_id: 0 };
        assert!(!a.is_authorized(12345, 999));
    }

    #[test]
    fn dispatch_halt_produces_control_and_reply() {
        let d = dispatch("/halt BTC").unwrap();
        assert_eq!(d.control, Some(ControlMsg::Halt { asset: "BTC".to_string() }));
        assert!(d.reply.unwrap().contains("BTC"));
    }

    #[test]
    fn dispatch_status_produces_no_control_and_no_canned_reply() {
        let d = dispatch("/status").unwrap();
        assert_eq!(d.control, None);
        assert_eq!(d.reply, None);
    }

    #[test]
    fn dispatch_help_has_no_control() {
        let d = dispatch("/help").unwrap();
        assert_eq!(d.control, None);
        assert!(d.reply.unwrap().contains("/halt"));
    }

    #[test]
    fn dispatch_blank_message_is_none() {
        assert!(dispatch("   ").is_none());
    }
}
