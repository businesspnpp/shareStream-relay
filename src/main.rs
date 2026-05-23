//! shareStream-relay
//!
//! Single-process WebSocket relay:
//!   * `GET  /ingest`            — desktop streamer connects here and pushes binary chunks.
//!   * `GET  /live`              — browser viewers connect here; receive a broadcast fan-out.
//!   * `GET  /download/live_record.mp4` — pulls the last completed recording.
//!   * `GET  /health`            — Render health probe.
//!
//! Architecture: one `tokio::sync::broadcast` channel per process. Ingest writes
//! each binary frame both to the broadcast (for live viewers) and appended to
//! `live_record.mp4` on disk (for post-session download). Viewers join late and
//! receive everything from the moment they connect onward; they tolerate lag
//! by dropping (broadcast::error::RecvError::Lagged is logged and skipped).

use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Router,
};
use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
    sync::{broadcast, Mutex},
};
use tower_http::{cors::CorsLayer, services::ServeFile, trace::TraceLayer};

const RECORDING_PATH: &str = "live_record.mp4";
const BROADCAST_CAPACITY: usize = 256;

#[derive(Clone)]
struct AppState {
    tx: broadcast::Sender<Bytes>,
    recording: Arc<Mutex<Option<File>>>,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "sharestream_relay=info,tower_http=info".into()),
        )
        .init();

    let (tx, _rx) = broadcast::channel::<Bytes>(BROADCAST_CAPACITY);
    let state = AppState {
        tx,
        recording: Arc::new(Mutex::new(None)),
    };

    let app = Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/ingest", get(ws_ingest))
        .route("/live", get(ws_live))
        .route_service(
            "/download/live_record.mp4",
            ServeFile::new(PathBuf::from(RECORDING_PATH)),
        )
        .layer(CorsLayer::permissive())
        .layer(TraceLayer::new_for_http())
        .with_state(state);

    // Render injects $PORT; default 8080 locally.
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

// ---- Ingest (desktop -> relay) -------------------------------------------

async fn ws_ingest(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_ingest(socket, state))
}

async fn handle_ingest(mut socket: WebSocket, state: AppState) {
    tracing::info!("ingest client connected");

    // Truncate + open the recording file for this session.
    let file = match OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(RECORDING_PATH)
        .await
    {
        Ok(f) => f,
        Err(e) => {
            tracing::error!("cannot open recording file: {e}");
            return;
        }
    };
    *state.recording.lock().await = Some(file);

    while let Some(msg) = socket.next().await {
        let msg = match msg {
            Ok(m) => m,
            Err(e) => {
                tracing::warn!("ingest read error: {e}");
                break;
            }
        };

        match msg {
            Message::Binary(data) => {
                let bytes = Bytes::from(data);

                // Fan out to live viewers (ignore if no subscribers).
                let _ = state.tx.send(bytes.clone());

                // Append to on-disk recording.
                if let Some(file) = state.recording.lock().await.as_mut() {
                    if let Err(e) = file.write_all(&bytes).await {
                        tracing::warn!("recording write failed: {e}");
                    }
                }
            }
            Message::Close(_) => break,
            Message::Ping(p) => {
                let _ = socket.send(Message::Pong(p)).await;
            }
            _ => {}
        }
    }

    // Flush recording on disconnect so /download serves a complete file.
    if let Some(mut file) = state.recording.lock().await.take() {
        let _ = file.flush().await;
        let _ = file.sync_all().await;
    }
    tracing::info!("ingest client disconnected; recording flushed");
}

// ---- Live fan-out (relay -> browser viewers) ------------------------------

async fn ws_live(ws: WebSocketUpgrade, State(state): State<AppState>) -> impl IntoResponse {
    ws.on_upgrade(|socket| handle_live(socket, state))
}

async fn handle_live(socket: WebSocket, state: AppState) {
    tracing::info!("viewer connected");
    let (mut sink, mut stream) = socket.split();
    let mut rx = state.tx.subscribe();

    // Outbound: forward broadcast chunks to this viewer.
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

    // Inbound: just drain (so close frames are handled).
    let recv_task = tokio::spawn(async move { while stream.next().await.is_some() {} });

    let _ = tokio::try_join!(send_task, recv_task);
    tracing::info!("viewer disconnected");
}

// Server-side health endpoint used by Render returns 200 from the closure above.
#[allow(dead_code)]
async fn _unused_status_helper() -> StatusCode {
    StatusCode::OK
}
