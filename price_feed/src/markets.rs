use std::io::stdout;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::StreamExt as _;
use ratatui::{
    backend::CrosstermBackend,
    layout::Constraint,
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Cell, Row, Table},
    Terminal,
};
use tokio::sync::mpsc;

#[derive(Clone, Default)]
struct MarketData {
    asset: String,
    up_price: f64,
    down_price: f64,
    volume: f64,
    last_updated: String,
    error: Option<String>,
}

fn current_slot() -> u64 {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    (now / 300) * 300
}

fn secs_until_next_slot() -> u64 {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
    300 - (now % 300)
}

fn make_slug(asset: &str, slot: u64) -> String {
    format!("{}-updown-5m-{}", asset.to_lowercase(), slot)
}

async fn fetch_market(client: &reqwest::Client, asset: &str) -> Result<MarketData> {
    let slug = make_slug(asset, current_slot());
    let url = format!("https://gamma-api.polymarket.com/events?slug={slug}");

    let resp: serde_json::Value = client.get(&url).send().await?.json().await?;

    let event = resp
        .as_array()
        .and_then(|a| a.first())
        .ok_or_else(|| anyhow::anyhow!("no event for {slug}"))?;

    let market = event["markets"]
        .as_array()
        .and_then(|a| a.first())
        .ok_or_else(|| anyhow::anyhow!("no market in event"))?;

    let prices: Vec<f64> =
        serde_json::from_str(market["outcomePrices"].as_str().unwrap_or("[]"))?;
    let outcomes: Vec<String> =
        serde_json::from_str(market["outcomes"].as_str().unwrap_or("[]"))?;

    // volumeNum is already a float; fall back to the string "volume" field
    let volume = market["volumeNum"]
        .as_f64()
        .or_else(|| market["volume"].as_f64())
        .or_else(|| market["volume"].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(0.0);

    let mut up_price = 0.0f64;
    let mut down_price = 0.0f64;
    for (outcome, price) in outcomes.iter().zip(prices.iter()) {
        match outcome.to_lowercase().as_str() {
            "up" => up_price = *price,
            "down" => down_price = *price,
            _ => {}
        }
    }

    Ok(MarketData {
        asset: asset.to_uppercase(),
        up_price,
        down_price,
        volume,
        last_updated: chrono::Utc::now().format("%H:%M:%S").to_string(),
        error: None,
    })
}

pub async fn run(assets: Vec<String>) -> Result<()> {
    enable_raw_mode()?;
    execute!(stdout(), EnterAlternateScreen)?;

    let mut terminal = Terminal::new(CrosstermBackend::new(stdout()))?;
    let result = run_app(&mut terminal, assets).await;

    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen)?;
    terminal.show_cursor()?;

    result
}

async fn run_app(
    terminal: &mut Terminal<CrosstermBackend<std::io::Stdout>>,
    assets: Vec<String>,
) -> Result<()> {
    let (tx, mut rx) = mpsc::channel::<(usize, MarketData)>(64);

    let mut state: Vec<MarketData> = assets
        .iter()
        .map(|a| MarketData {
            asset: a.to_uppercase(),
            ..Default::default()
        })
        .collect();

    for (idx, asset) in assets.iter().enumerate() {
        let asset = asset.clone();
        let tx = tx.clone();
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            loop {
                let data = match fetch_market(&client, &asset).await {
                    Ok(d) => d,
                    Err(e) => MarketData {
                        asset: asset.to_uppercase(),
                        error: Some(e.to_string()),
                        ..Default::default()
                    },
                };
                let _ = tx.send((idx, data)).await;
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
        });
    }

    let mut events = EventStream::new();
    let mut redraw = tokio::time::interval(Duration::from_millis(500));

    loop {
        tokio::select! {
            _ = redraw.tick() => {
                let secs = secs_until_next_slot();
                terminal.draw(|f| draw(f, &state, secs))?;
            }
            Some(msg) = rx.recv() => {
                let (idx, data) = msg;
                if idx < state.len() {
                    state[idx] = data;
                }
            }
            Some(Ok(event)) = events.next() => {
                if let Event::Key(k) = event {
                    if k.kind == KeyEventKind::Press {
                        match k.code {
                            KeyCode::Char('q') => break,
                            KeyCode::Char('c') if k.modifiers.contains(KeyModifiers::CONTROL) => break,
                            _ => {}
                        }
                    }
                }
            }
        }
    }

    Ok(())
}

fn draw(f: &mut ratatui::Frame, state: &[MarketData], secs_left: u64) {
    use chrono::TimeZone as _;

    let slot = current_slot();
    let window_start = chrono::Utc
        .timestamp_opt(slot as i64, 0)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M UTC").to_string())
        .unwrap_or_else(|| "?".to_string());
    let mins = secs_left / 60;
    let secs = secs_left % 60;

    let title = format!(
        " Polymarket 5-min │ {window_start} │ rotates in {mins}m{secs:02}s │ q = quit "
    );

    let header = Row::new([
        Cell::from("Asset"),
        Cell::from("UP"),
        Cell::from("DOWN"),
        Cell::from("Volume"),
        Cell::from("Updated"),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED));

    let rows: Vec<Row> = state
        .iter()
        .map(|d| {
            if let Some(ref err) = d.error {
                Row::new([
                    Cell::from(d.asset.clone()),
                    Cell::from(format!("err: {err}"))
                        .style(Style::default().fg(Color::Red)),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                ])
            } else if d.last_updated.is_empty() {
                Row::new([
                    Cell::from(d.asset.clone()),
                    Cell::from("loading…").style(Style::default().fg(Color::DarkGray)),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                ])
            } else {
                let up_color = if d.up_price >= 0.5 { Color::Green } else { Color::Red };
                let dn_color = if d.down_price >= 0.5 { Color::Green } else { Color::Red };
                Row::new([
                    Cell::from(d.asset.clone()),
                    Cell::from(format!("{:.1}%", d.up_price * 100.0))
                        .style(Style::default().fg(up_color)),
                    Cell::from(format!("{:.1}%", d.down_price * 100.0))
                        .style(Style::default().fg(dn_color)),
                    Cell::from(format!("${:.0}", d.volume)),
                    Cell::from(d.last_updated.clone()),
                ])
            }
        })
        .collect();

    let widths = [
        Constraint::Length(6),
        Constraint::Length(8),
        Constraint::Length(8),
        Constraint::Length(10),
        Constraint::Length(10),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title))
        .column_spacing(2);

    f.render_widget(table, f.area());
}
