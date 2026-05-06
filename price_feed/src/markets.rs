use std::io::stdout;
use std::str::FromStr as _;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use chrono::{FixedOffset, TimeZone as _};
use crossterm::{
    event::{Event, EventStream, KeyCode, KeyEventKind, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use futures::StreamExt as _;
use polymarket_client_sdk_v2::clob::ws::Client as ClobWsClient;
use polymarket_client_sdk_v2::rtds::Client as RtdsClient;
use polymarket_client_sdk_v2::types::U256;
use ratatui::{
    backend::CrosstermBackend,
    layout::Constraint,
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Cell, Row, Table},
    Terminal,
};
use tokio::sync::mpsc;

const CL_SYMBOLS: &[(&str, &str)] = &[
    ("btc/usd", "BTC"),
    ("eth/usd", "ETH"),
    ("sol/usd", "SOL"),
    ("bnb/usd", "BNB"),
];

// Gamma metadata for one slot — fetched once per slot rotation
struct SlotMeta {
    slot: u64,
    up_token_id: String,
    volume: f64,
    fetched_at: Instant,
}

#[derive(Clone)]
struct MarketData {
    asset: String,
    up_price: f64,
    down_price: f64,
    volume: f64,
    last_updated: String,
    poly_latency_ms: f64,
    error: Option<String>,
    cl_price: Option<f64>,
    cl_latency_ms: f64,
}

impl Default for MarketData {
    fn default() -> Self {
        Self {
            asset: String::new(),
            up_price: 0.0,
            down_price: 0.0,
            volume: 0.0,
            last_updated: String::new(),
            poly_latency_ms: -1.0,
            error: None,
            cl_price: None,
            cl_latency_ms: -1.0,
        }
    }
}

fn now_secs() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs()
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

fn current_slot() -> u64 {
    let s = now_secs();
    (s / 300) * 300
}

fn secs_until_next_slot() -> u64 {
    300 - (now_secs() % 300)
}

fn make_slug(asset: &str, slot: u64) -> String {
    format!("{}-updown-5m-{}", asset.to_lowercase(), slot)
}

fn hkt() -> FixedOffset {
    FixedOffset::east_opt(8 * 3600).unwrap()
}

fn slot_label(slot: u64) -> String {
    let tz = hkt();
    match (
        tz.timestamp_opt(slot as i64, 0).single(),
        tz.timestamp_opt((slot + 300) as i64, 0).single(),
    ) {
        (Some(s), Some(e)) => format!("{}-{} HKT", s.format("%H:%M"), e.format("%H:%M")),
        _ => "?".to_string(),
    }
}

// Fetch gamma metadata (token IDs + volume) — called once per slot rotation
async fn fetch_meta(http: &reqwest::Client, asset: &str, slot: u64) -> Result<SlotMeta> {
    let slug = make_slug(asset, slot);
    let url = format!("https://gamma-api.polymarket.com/events?slug={slug}");
    let resp: serde_json::Value = http.get(&url).send().await?.json().await?;

    let event = resp.as_array().and_then(|a| a.first())
        .ok_or_else(|| anyhow::anyhow!("no event for {slug}"))?;
    let market = event["markets"].as_array().and_then(|a| a.first())
        .ok_or_else(|| anyhow::anyhow!("no market in event"))?;

    let token_ids: Vec<String> =
        serde_json::from_str(market["clobTokenIds"].as_str().unwrap_or("[]"))?;
    let outcomes: Vec<String> =
        serde_json::from_str(market["outcomes"].as_str().unwrap_or("[]"))?;

    let up_token_id = outcomes.iter().zip(token_ids.iter())
        .find(|(o, _)| o.to_lowercase() == "up")
        .map(|(_, tid)| tid.clone())
        .ok_or_else(|| anyhow::anyhow!("no Up token found"))?;

    let volume = market["volumeNum"].as_f64()
        .or_else(|| market["volume"].as_f64())
        .or_else(|| market["volume"].as_str().and_then(|s| s.parse().ok()))
        .unwrap_or(0.0);

    Ok(SlotMeta { slot, up_token_id, volume, fetched_at: Instant::now() })
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
    // (idx, up_midpoint, server_timestamp_ms) — from CLOB WS
    let (price_tx, mut price_rx) = mpsc::channel::<(usize, f64, i64)>(128);
    // (idx, volume) — from Gamma, refreshed each slot
    let (vol_tx, mut vol_rx) = mpsc::channel::<(usize, f64)>(16);
    let (cl_tx, mut cl_rx) = mpsc::channel::<(String, f64, i64)>(128);

    let mut state: Vec<MarketData> = assets
        .iter()
        .map(|a| MarketData {
            asset: a.to_uppercase(),
            error: Some("loading metadata…".to_string()),
            ..Default::default()
        })
        .collect();

    let clob_client = ClobWsClient::default();

    // One task per asset: fetches Gamma metadata and manages the CLOB WS subscription.
    // Prices arrive via the WS stream task spawned here; volume comes via vol_tx.
    for (idx, asset) in assets.iter().enumerate() {
        let clob = clob_client.clone();
        let price_tx = price_tx.clone();
        let vol_tx = vol_tx.clone();
        let asset = asset.clone();

        tokio::spawn(async move {
            let http = reqwest::Client::builder()
                .user_agent("Mozilla/5.0")
                .build()
                .expect("http client");

            let mut meta: Option<SlotMeta> = None;
            let mut current_token_id: Option<U256> = None;
            // Abort handle for the current WS stream task; replaced on each slot rotation.
            let mut stream_task: Option<tokio::task::JoinHandle<()>> = None;

            loop {
                let slot = current_slot();
                let stale = meta.as_ref().map(|m| {
                    m.slot != slot || m.fetched_at.elapsed() > Duration::from_secs(30)
                }).unwrap_or(true);

                if stale {
                    if let Ok(new_meta) = fetch_meta(&http, &asset, slot).await {
                        let _ = vol_tx.send((idx, new_meta.volume)).await;

                        if let Ok(new_id) = U256::from_str(&new_meta.up_token_id) {
                            if Some(new_id) != current_token_id {
                                // Abort old stream task and unsubscribe old token on slot rotation.
                                if let Some(old_task) = stream_task.take() {
                                    old_task.abort();
                                }
                                if let Some(old_id) = current_token_id {
                                    let _ = clob.unsubscribe_midpoints(&[old_id]);
                                }
                                if let Ok(stream) = clob.subscribe_midpoints(vec![new_id]) {
                                    let tx = price_tx.clone();
                                    stream_task = Some(tokio::spawn(async move {
                                        let mut s = Box::pin(stream);
                                        while let Some(Ok(update)) = s.next().await {
                                            let mid: f64 = update.midpoint
                                                .to_string()
                                                .parse()
                                                .unwrap_or(f64::NAN);
                                            if !mid.is_finite() { continue; }
                                            let _ = tx.send((idx, mid, update.timestamp)).await;
                                        }
                                    }));
                                }
                                current_token_id = Some(new_id);
                            }
                        }
                        meta = Some(new_meta);
                    }
                }

                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        });
    }

    // Chainlink RTDS — single shared task for all assets
    {
        let tx = cl_tx;
        tokio::spawn(async move {
            let client = RtdsClient::default();
            let Ok(stream) = client.subscribe_chainlink_prices(None) else { return };
            let mut stream = Box::pin(stream);
            while let Some(Ok(price)) = stream.next().await {
                let sym = price.symbol.to_lowercase();
                let Some(&(_, asset)) = CL_SYMBOLS.iter().find(|(s, _)| *s == sym) else {
                    continue;
                };
                let value: f64 = price.value.try_into().unwrap_or(f64::NAN);
                if !value.is_finite() { continue; }
                let _ = tx.send((asset.to_string(), value, price.timestamp)).await;
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
            Some((idx, mid, ts_ms)) = price_rx.recv() => {
                if idx < state.len() {
                    let d = &mut state[idx];
                    d.up_price = mid;
                    d.down_price = 1.0 - mid;
                    // Latency = time since the server emitted the book update
                    d.poly_latency_ms = (now_ms() - ts_ms).max(0) as f64;
                    d.last_updated = chrono::Utc::now()
                        .with_timezone(&hkt())
                        .format("%H:%M:%S HKT")
                        .to_string();
                    d.error = None;
                }
            }
            Some((idx, volume)) = vol_rx.recv() => {
                if idx < state.len() {
                    state[idx].volume = volume;
                }
            }
            Some((asset, value, ts_ms)) = cl_rx.recv() => {
                let latency_ms = (now_ms() - ts_ms).max(0) as f64;
                if let Some(d) = state.iter_mut().find(|d| d.asset == asset) {
                    d.cl_price = Some(value);
                    d.cl_latency_ms = latency_ms;
                }
            }
            Some(Ok(event)) = events.next() => {
                if let Event::Key(k) = event
                    && k.kind == KeyEventKind::Press {
                    match k.code {
                        KeyCode::Char('q') => break,
                        KeyCode::Char('c') if k.modifiers.contains(KeyModifiers::CONTROL) => break,
                        _ => {}
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
    let mins = secs_left / 60;
    let secs = secs_left % 60;

    let title = format!(
        " Polymarket 5-min │ {} │ rotates in {mins}m{secs:02}s │ q = quit ",
        slot_label(slot)
    );

    let header = Row::new([
        Cell::from("Slot"),
        Cell::from("Asset"),
        Cell::from("UP"),
        Cell::from("DOWN"),
        Cell::from("CL Price"),
        Cell::from("Volume"),
        Cell::from("Updated"),
        Cell::from("poly_ws"),
        Cell::from("cl_ws"),
    ])
    .style(Style::default().add_modifier(Modifier::BOLD | Modifier::UNDERLINED));

    let rows: Vec<Row> = state.iter().map(|d| {
        let slot_cell = Cell::from(slot_label(current_slot()));
        let cl_price_cell = match d.cl_price {
            Some(p) => Cell::from(format!("${p:.2}")),
            None => Cell::from("…").style(Style::default().fg(Color::DarkGray)),
        };

        if let Some(ref err) = d.error {
            Row::new([
                slot_cell,
                Cell::from(d.asset.clone()),
                Cell::from(err.as_str()).style(Style::default().fg(Color::Red)),
                Cell::from(""),
                cl_price_cell,
                Cell::from(""),
                Cell::from(""),
                Cell::from("● stale")
                    .style(Style::default().fg(Color::Red).add_modifier(Modifier::BOLD)),
                latency_cell(d.cl_latency_ms),
            ])
        } else if d.last_updated.is_empty() {
            Row::new([
                slot_cell,
                Cell::from(d.asset.clone()),
                Cell::from("loading…").style(Style::default().fg(Color::DarkGray)),
                Cell::from(""),
                cl_price_cell,
                Cell::from(""),
                Cell::from(""),
                latency_cell(-1.0),
                latency_cell(d.cl_latency_ms),
            ])
        } else {
            let up_color = if d.up_price >= 0.5 { Color::Green } else { Color::Red };
            let dn_color = if d.down_price >= 0.5 { Color::Green } else { Color::Red };
            Row::new([
                slot_cell,
                Cell::from(d.asset.clone()),
                Cell::from(format!("{:.1}%", d.up_price * 100.0))
                    .style(Style::default().fg(up_color)),
                Cell::from(format!("{:.1}%", d.down_price * 100.0))
                    .style(Style::default().fg(dn_color)),
                cl_price_cell,
                Cell::from(format!("${:.0}", d.volume)),
                Cell::from(d.last_updated.clone()),
                latency_cell(d.poly_latency_ms),
                latency_cell(d.cl_latency_ms),
            ])
        }
    }).collect();

    let widths = [
        Constraint::Length(15), // "13:50-13:55 HKT"
        Constraint::Length(5),  // Asset
        Constraint::Length(6),  // UP
        Constraint::Length(6),  // DOWN
        Constraint::Length(11), // CL Price
        Constraint::Length(8),  // Volume
        Constraint::Length(12), // Updated
        Constraint::Length(13), // poly_ws
        Constraint::Length(13), // cl_ws
    ];

    let table = Table::new(rows, widths)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title(title))
        .column_spacing(2);

    f.render_widget(table, f.area());
}
