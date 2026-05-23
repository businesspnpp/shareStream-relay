//! shareStream-relay (multi-device)
//!
//! Architecture:
//!   * Multiple desktops can connect simultaneously, each identified by
//!     `device_id` in the URL: `WS /ingest/:device_id`.
//!   * On connect, the desktop's first message MUST be a text frame containing
//!     a JSON metadata blob (hostname, IPs, OS, battery, …).
//!   * The relay maintains an in-memory registry of devices and exposes
//!     `GET /devices` for the browser viewer to enumerate.
//!   * Browser viewers subscribe with `WS /live/:device_id` and receive that
//!     device's broadcast fan-out.
//!   * Each device's session is recorded to disk at `live_record_<id>.h264`
//!     and served via `GET /download/:device_id`.

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};

use axum::{
    body::Body,
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        Path, State,
    },
    http::{header, StatusCode},
    response::{Html, IntoResponse, Json, Response},
    routing::get,
    Router,
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    sync::{broadcast, Mutex, RwLock},
};
use tower_http::{cors::CorsLayer, trace::TraceLayer};

const BROADCAST_CAPACITY: usize = 256;
const RECORDING_DIR: &str = ".";

#[derive(Clone)]
struct DeviceEntry {
    /// Metadata blob from the desktop, served verbatim to viewers.
    metadata: serde_json::Value,
    /// Outgoing fan-out for live H.264 chunks.
    tx: broadcast::Sender<Bytes>,
    /// Active recording file for the current session.
    recording: Arc<Mutex<Option<File>>>,
    /// True while a desktop is actively pushing frames.
    online: bool,
    /// Unix-ms of last frame received.
    last_seen_ms: u64,
}

type Registry = Arc<RwLock<HashMap<String, DeviceEntry>>>;

#[derive(Clone)]
struct AppState {
    devices: Registry,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn recording_path(device_id: &str) -> String {
    let safe: String = device_id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect();
    format!("{}/live_record_{}.h264", RECORDING_DIR, safe)
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sharestream_relay=info,tower_http=info".into()),
        )
        .init();

    let state = AppState {
        devices: Arc::new(RwLock::new(HashMap::new())),
    };

    let app = Router::new()
        .route("/", get(index))
        .route("/health", get(|| async { "ok" }))
        .route("/devices", get(list_devices))
        .route("/ingest/:device_id", get(ws_ingest))
        .route("/live/:device_id", get(ws_live))
        .route("/download/:device_id", get(download_recording))
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    let port: u16 = std::env::var("PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(8080);
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    tracing::info!("relay listening on {addr}");

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await
        .unwrap();
}

// ---- Devices catalog ------------------------------------------------------

async fn list_devices(State(state): State<AppState>) -> Json<serde_json::Value> {
    let devices = state.devices.read().await;
    let arr: Vec<serde_json::Value> = devices
        .iter()
        .map(|(id, e)| {
            serde_json::json!({
                "device_id": id,
                "online": e.online,
                "viewer_count": e.tx.receiver_count(),
                "last_seen_ms": e.last_seen_ms,
                "metadata": e.metadata,
            })
        })
        .collect();
    Json(serde_json::json!({ "devices": arr }))
}

// ---- Ingest (desktop -> relay) -------------------------------------------

async fn ws_ingest(
    ws: WebSocketUpgrade,
    Path(device_id): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ingest(socket, state, device_id))
}

async fn handle_ingest(mut socket: WebSocket, state: AppState, device_id: String) {
    tracing::info!("ingest connected: {device_id}");

    let metadata: serde_json::Value = match socket.next().await {
        Some(Ok(Message::Text(t))) => serde_json::from_str(&t).unwrap_or_else(|e| {
            tracing::warn!("invalid metadata json from {device_id}: {e}");
            serde_json::json!({})
        }),
        _ => {
            tracing::warn!("ingest {device_id}: first frame was not text metadata");
            serde_json::json!({})
        }
    };

    let (tx, recording_arc) = {
        let mut devices = state.devices.write().await;
        let entry = devices.entry(device_id.clone()).or_insert_with(|| {
            let (tx, _) = broadcast::channel::<Bytes>(BROADCAST_CAPACITY);
            DeviceEntry {
                metadata: serde_json::json!({}),
                tx,
                recording: Arc::new(Mutex::new(None)),
                online: false,
                last_seen_ms: 0,
            }
        });
        entry.metadata = metadata;
        entry.online = true;
        entry.last_seen_ms = now_ms();
        (entry.tx.clone(), entry.recording.clone())
    };

    let path = recording_path(&device_id);
    match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&path)
        .await
    {
        Ok(f) => {
            *recording_arc.lock().await = Some(f);
        }
        Err(e) => tracing::error!("cannot open recording {path}: {e}"),
    }

    while let Some(msg) = socket.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("ingest {device_id} read error: {e}");
                break;
            }
        };

        match msg {
            Message::Binary(data) => {
                let bytes = Bytes::from(data);
                let _ = tx.send(bytes.clone());
                if let Some(file) = recording_arc.lock().await.as_mut() {
                    if let Err(e) = file.write_all(&bytes).await {
                        tracing::warn!("recording write failed: {e}");
                    }
                }
                if let Some(entry) = state.devices.write().await.get_mut(&device_id) {
                    entry.last_seen_ms = now_ms();
                }
            }
            Message::Text(_) => {
                // Allow late metadata updates (e.g. battery refresh).
            }
            Message::Close(_) => break,
            Message::Ping(p) => {
                let _ = socket.send(Message::Pong(p)).await;
            }
            _ => {}
        }
    }

    if let Some(mut file) = recording_arc.lock().await.take() {
        let _ = file.flush().await;
        let _ = file.sync_all().await;
    }
    if let Some(entry) = state.devices.write().await.get_mut(&device_id) {
        entry.online = false;
    }
    tracing::info!("ingest disconnected: {device_id}");
}

// ---- Live fan-out (relay -> browser viewers) ------------------------------

async fn ws_live(
    ws: WebSocketUpgrade,
    Path(device_id): Path<String>,
    State(state): State<AppState>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_live(socket, state, device_id))
}

async fn handle_live(socket: WebSocket, state: AppState, device_id: String) {
    let tx = match state.devices.read().await.get(&device_id) {
        Some(e) => e.tx.clone(),
        None => {
            tracing::info!("viewer wanted unknown device {device_id}");
            let mut s = socket;
            let _ = s.close().await;
            return;
        }
    };

    tracing::info!("viewer connected to {device_id}");
    let (mut sink, mut stream) = socket.split();
    let mut rx = tx.subscribe();

    let send_task = tokio::spawn(async move {
        loop {
            match rx.recv().await {
                Ok(bytes) => {
                    if sink.send(Message::Binary(bytes.to_vec())).await.is_err() {
                        break;
                    }
                }
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    tracing::warn!("viewer lagged, skipped {n} chunks");
                    continue;
                }
                Err(broadcast::error::RecvError::Closed) => break,
            }
        }
    });

    let recv_task = tokio::spawn(async move { while stream.next().await.is_some() {} });

    let _ = tokio::try_join!(send_task, recv_task);
    tracing::info!("viewer disconnected from {device_id}");
}

async fn index() -> Html<&'static str> {
    Html(
        "<!doctype html><meta charset=utf-8><title>shareStream-relay</title>\
         <h1>shareStream-relay (multi-device)</h1>\
         <ul>\
         <li>WS  /ingest/:device_id   &mdash; desktop pushes frames here</li>\
         <li>WS  /live/:device_id     &mdash; browser viewers subscribe here</li>\
         <li>GET /devices             &mdash; list of devices + metadata</li>\
         <li>GET /download/:device_id &mdash; raw Annex-B H.264 recording</li>\
         <li>GET /health</li>\
         </ul>",
    )
}

async fn download_recording(Path(device_id): Path<String>) -> Response {
    use tokio::io::AsyncReadExt;
    let path = recording_path(&device_id);
    let mut file = match tokio::fs::File::open(&path).await {
        Ok(f) => f,
        Err(_) => {
            return (
                StatusCode::NOT_FOUND,
                format!("No recording yet for {device_id}."),
            )
                .into_response();
        }
    };
    let mut buf = Vec::new();
    if let Err(e) = file.read_to_end(&mut buf).await {
        return (StatusCode::INTERNAL_SERVER_ERROR, format!("read failed: {e}")).into_response();
    }
    let disposition = format!("attachment; filename=\"live_record_{device_id}.h264\"");
    (
        [
            (header::CONTENT_TYPE, "video/h264".to_string()),
            (header::CONTENT_DISPOSITION, disposition),
        ],
        Body::from(buf),
    )
        .into_response()
}
