use std::io::stdout;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use chrono::{FixedOffset, TimeZone as _};
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

#[derive(Clone)]
struct MarketData {
    asset: String,
    up_price: f64,
    down_price: f64,
    volume: f64,
    last_updated: String, // HKT
    latency_ms: f64,      // -1 = waiting, >=0 = last fetch duration
    error: Option<String>,
}

impl Default for MarketData {
    fn default() -> Self {
        Self {
            asset: String::new(),
            up_price: 0.0,
            down_price: 0.0,
            volume: 0.0,
            last_updated: String::new(),
            latency_ms: -1.0,
            error: None,
        }
    }
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

fn hkt() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).unwrap()
}

async fn fetch_market(client: &reqwest::Client, asset: &str) -> Result<MarketData> {
    let slug = make_slug(asset, current_slot());
    let url = format!("https://gamma-api.polymarket.com/events?slug={slug}");

    let t0 = Instant::now();
    let resp: serde_json::Value = client.get(&url).send().await?.json().await?;
    let latency_ms = t0.elapsed().as_secs_f64() * 1000.0;

    let event = resp
        .as_array()
        .and_then(|a| a.first())
        .ok_or_else(|| anyhow::anyhow!("no event for {slug}"))?;

    let market = event["markets"]
        .as_array()
        .and_then(|a| a.first())
        .ok_or_else(|| anyhow::anyhow!("no market in event"))?;

    let price_strs: Vec<String> =
        serde_json::from_str(market["outcomePrices"].as_str().unwrap_or("[]"))?;
    let prices: Vec<f64> = price_strs.iter().map(|s| s.parse().unwrap_or(0.0)).collect();
    let outcomes: Vec<String> =
        serde_json::from_str(market["outcomes"].as_str().unwrap_or("[]"))?;

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

    let hkt = hkt();
    let last_updated = chrono::Utc::now()
        .with_timezone(&hkt)
        .format("%H:%M:%S HKT")
        .to_string();

    Ok(MarketData {
        asset: asset.to_uppercase(),
        up_price,
        down_price,
        volume,
        last_updated,
        latency_ms,
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
            let client = reqwest::Client::builder()
                .user_agent("Mozilla/5.0")
                .build()
                .expect("http client");
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

fn latency_cell(ms: f64) -> Cell<'static> {
    if ms < 0.0 {
        Cell::from("● wait").style(Style::default().fg(Color::DarkGray))
    } else if ms < 500.0 {
        Cell::from(format!("● ok {ms:.0}ms")).style(Style::default().fg(Color::Green))
    } else if ms < 2000.0 {
        Cell::from(format!("● slow {ms:.0}ms")).style(Style::default().fg(Color::Yellow))
    } else {
        Cell::from("● stale").style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD))
    }
}

fn draw(f: &mut ratatui::Frame, state: &[MarketData], secs_left: u64) {
    let slot = current_slot();
    let window_start = hkt()
        .timestamp_opt(slot as i64, 0)
        .single()
        .map(|dt| dt.format("%Y-%m-%d %H:%M HKT").to_string())
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
        Cell::from("Feed"),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED));

    let rows: Vec<Row> = state
        .iter()
        .map(|d| {
            if let Some(ref err) = d.error {
                Row::new([
                    Cell::from(d.asset.clone()),
                    Cell::from(format!("err: {err}")).style(Style::default().fg(Color::Red)),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from("● stale")
                        .style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
                ])
            } else if d.last_updated.is_empty() {
                Row::new([
                    Cell::from(d.asset.clone()),
                    Cell::from("loading…").style(Style::default().fg(Color::DarkGray)),
                    Cell::from(""),
                    Cell::from(""),
                    Cell::from(""),
                    latency_cell(-1.0),
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
                    latency_cell(d.latency_ms),
                ])
            }
        })
        .collect();

    let widths = [
        Constraint::Length(6),
        Constraint::Length(7),
        Constraint::Length(7),
        Constraint::Length(9),
        Constraint::Length(14),
        Constraint::Length(14),
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title))
        .column_spacing(2);

    f.render_widget(table, f.area());
}
