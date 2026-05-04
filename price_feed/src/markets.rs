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
use polymarket_client_sdk_v2::rtds::Client as RtdsClient;
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

#[derive(Clone)]
struct MarketData {
    asset: String,
    up_price: f64,
    down_price: f64,
    volume: f64,
    last_updated: String,
    poly_latency_ms: f64,
    error: Option<String>,
    // chainlink fields — preserved across gamma API refreshes
    cl_price: Option<f64>,
    cl_latency_ms: f64, // now_ms - oracle_timestamp_ms; -1 = no data yet
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
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

fn current_slot() -> u64 {
    let s = now_secs();
    (s / 300) * 300
}

fn secs_until_next_slot() -> u64 {
    let s = now_secs();
    300 - (s % 300)
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

async fn fetch_market(client: &reqwest::Client, asset: &str) -> Result<MarketData> {
    let slug = make_slug(asset, current_slot());
    let url = format!("https://gamma-api.polymarket.com/events?slug={slug}");

    let t0 = Instant::now();
    let resp: serde_json::Value = client.get(&url).send().await?.json().await?;
    let poly_latency_ms = t0.elapsed().as_secs_f64() * 1000.0;

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

    let last_updated = chrono::Utc::now()
        .with_timezone(&hkt())
        .format("%H:%M:%S HKT")
        .to_string();

    Ok(MarketData {
        asset: asset.to_uppercase(),
        up_price,
        down_price,
        volume,
        last_updated,
        poly_latency_ms,
        error: None,
        // caller preserves cl fields from previous state
        cl_price: None,
        cl_latency_ms: -1.0,
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
    let (poly_tx, mut poly_rx) = mpsc::channel::<(usize, MarketData)>(64);
    // (asset_uppercase, price_f64, oracle_timestamp_ms)
    let (cl_tx, mut cl_rx) = mpsc::channel::<(String, f64, i64)>(128);

    let mut state: Vec<MarketData> = assets
        .iter()
        .map(|a| MarketData { asset: a.to_uppercase(), ..Default::default() })
        .collect();

    // Gamma polling — one task per asset
    for (idx, asset) in assets.iter().enumerate() {
        let asset = asset.clone();
        let tx = poly_tx.clone();
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

    // Chainlink RTDS — single task, all assets
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
                if !value.is_finite() {
                    continue;
                }
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
            Some((idx, mut data)) = poly_rx.recv() => {
                if idx < state.len() {
                    // preserve chainlink fields across gamma refresh
                    data.cl_price = state[idx].cl_price;
                    data.cl_latency_ms = state[idx].cl_latency_ms;
                    state[idx] = data;
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

    let rows: Vec<Row> = state
        .iter()
        .map(|d| {
            let slot_cell = Cell::from(slot_label(current_slot()));
            let cl_price_cell = match d.cl_price {
                Some(p) => Cell::from(format!("${p:.2}")),
                None => Cell::from("…").style(Style::default().fg(Color::DarkGray)),
            };

            if let Some(ref err) = d.error {
                Row::new([
                    slot_cell,
                    Cell::from(d.asset.clone()),
                    Cell::from(format!("err: {err}")).style(Style::default().fg(Color::Red)),
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
        })
        .collect();

    let widths = [
        Constraint::Length(15), // "13:50-13:55 HKT"
        Constraint::Length(5),  // Asset
        Constraint::Length(6),  // UP
        Constraint::Length(6),  // DOWN
        Constraint::Length(11), // CL Price "$94532.50"
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
