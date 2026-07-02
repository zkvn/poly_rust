// Telegram live validation probe (plan §5/A3 follow-up).
//
// Standalone binary, deliberately separate from the worker: proves
// `telegram/mod.rs`'s `TelegramBot` round-trips against the real Telegram
// API with a real bot token before it gets wired into worker.rs.
//
//   cargo run --bin telegram_probe -- send "hello"
//   cargo run --bin telegram_probe -- poll

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};

use trader::telegram::{AuthConfig, TelegramBot};

#[derive(Parser, Debug)]
#[command(name = "telegram_probe", about = "Live Telegram bot validation probe")]
struct Args {
    #[arg(long, default_value = ".env")]
    env_file: String,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand, Debug)]
enum Cmd {
    /// Send a text message to TELEGRAM_CHAT_ID.
    Send {
        #[arg(default_value = "poly_rust telegram_probe: bot is alive.")]
        text: String,
    },
    /// One long-poll cycle (30s), print any authorized incoming messages.
    Poll,
}

fn auth_from_env() -> Result<AuthConfig> {
    let token = std::env::var("TELEGRAM_BOT_TOKEN").context("TELEGRAM_BOT_TOKEN not set in env file")?;
    let chat_id: i64 = std::env::var("TELEGRAM_CHAT_ID")
        .context("TELEGRAM_CHAT_ID not set in env file")?
        .parse()
        .context("TELEGRAM_CHAT_ID is not a valid i64")?;
    Ok(AuthConfig { token, chat_id, user_id: 0 })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    dotenvy::from_path(&args.env_file).with_context(|| format!("load {}", args.env_file))?;

    let auth = auth_from_env()?;
    let mut bot = TelegramBot::new(auth)?;

    match args.cmd {
        Cmd::Send { text } => {
            bot.send(&text).await?;
            println!("sent to chat_id from TELEGRAM_CHAT_ID: {text:?}");
        }
        Cmd::Poll => {
            println!("polling once (up to 30s)...");
            let messages = bot.poll_once().await?;
            if messages.is_empty() {
                println!("no authorized messages received");
            }
            for m in messages {
                println!("from={} chat={} text={:?}", m.from_id, m.chat_id, m.text);
            }
        }
    }

    Ok(())
}
