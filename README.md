# RévisEdu — Realtime Server

WebSocket server for the RévisEdu classroom (chat, calls, games sync).

## Deploy on Render

1. Create a **Web Service** from this repo
2. **Build Command:** `npm install`
3. **Start Command:** `npm start`
4. **Health Check Path:** `/health`

Render sets `PORT` automatically. Your WebSocket URL will be:

```
wss://YOUR-SERVICE.onrender.com
```

Set that in the frontend as `VITE_WS_URL`.

## Local dev

```bash
npm install
npm start
```

Server runs on `http://localhost:5174` (health check at `/health`).
