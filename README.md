# shareStream-relay

WebSocket fan-out + recording server for the [`shareStream`](https://github.com/businesspnpp/shareStream) desktop client and [`stream-UI`](https://github.com/businesspnpp/stream-UI) viewer.

## Endpoints

| Path                            | Who connects        | Purpose                                        |
| ------------------------------- | ------------------- | ---------------------------------------------- |
| `wss://.../ingest`              | Desktop streamer    | Receives binary H.264-in-fMP4 chunks.          |
| `wss://.../live`                | Browser viewers     | Fan-out broadcast of ingest chunks.            |
| `https://.../download/live_record.mp4` | Anyone       | Last completed recording (after disconnect).  |
| `https://.../health`            | Render              | Liveness probe.                                |

## Local run

```powershell
cargo run --release
# desktop:  SHARESTREAM_WSS=ws://127.0.0.1:8080/ingest cargo run --release  (in shareStream repo)
# viewer:   open stream-UI/index.html, paste ws://127.0.0.1:8080/live
```

## Deploy to Render

1. Push this repo to GitHub.
2. Render → New → Blueprint → pick this repo. `render.yaml` provisions everything.
3. Point the desktop client at `wss://<your-app>.onrender.com/ingest`.
4. Point the viewer at `wss://<your-app>.onrender.com/live`.
