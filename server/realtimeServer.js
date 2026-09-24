import { createServer } from 'http';
import { request as httpsRequest } from 'https';
import { request as httpRequest } from 'http';
import { readFileSync, writeFileSync, renameSync, mkdirSync, existsSync, copyFileSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer, WebSocket } from 'ws';

// Global error traps to guarantee zero server crashes
process.on('uncaughtException', (err) => {
  console.error('[RévisEdu Server CRITICAL] Uncaught exception:', err);
});
process.on('unhandledRejection', (reason, promise) => {
  console.error('[RévisEdu Server CRITICAL] Unhandled rejection at:', promise, 'reason:', reason);
});

const PORT = Number(process.env.PORT) || 5174;
const SERVER_START_TIME = Date.now();
const RENDER_SERVICE_URL = process.env.RENDER_EXTERNAL_URL || 'https://revisedu.onrender.com';

const clients = new Set();

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = join(__dirname, '..', 'data');
const DATA_FILE = join(DATA_DIR, 'store.json');
const DATA_TMP_FILE = join(DATA_DIR, 'store.json.tmp');
const DATA_BACKUP_FILE = join(DATA_DIR, 'store.backup.json');

/** @type {Map<string, { name: string; avatar?: string; bio?: string; id?: string; status?: string; lastSeen?: number }>} */
const registeredAccounts = new Map();
/** @type {Array<object>} */
let savedChannels = [];
/** @type {Array<object>} */
let savedMessages = [];
/** @type {Array<object>} */
let savedDirectMessages = [];
/** @type {Array<object>} */
let savedGroups = [];
/** @type {Array<object>} */
let savedLeaderboard = [];
/** @type {string|null} */
let currentAnnouncement = null;

// Initial Load
function loadStore() {
  try {
    if (!existsSync(DATA_FILE)) {
      if (existsSync(DATA_BACKUP_FILE)) {
        console.log('[RévisEdu Realtime] Restoring from backup file...');
        copyFileSync(DATA_BACKUP_FILE, DATA_FILE);
      } else {
        return;
      }
    }
    const raw = readFileSync(DATA_FILE, 'utf-8');
    const data = JSON.parse(raw);
    savedChannels = Array.isArray(data.channels) ? data.channels : [];
    savedMessages = Array.isArray(data.messages) ? data.messages : [];
    savedDirectMessages = Array.isArray(data.directMessages) ? data.directMessages : [];
    savedGroups = Array.isArray(data.groups) ? data.groups : [];
    savedLeaderboard = Array.isArray(data.leaderboard) ? data.leaderboard : [];
    currentAnnouncement = data.announcement ?? null;

    if (Array.isArray(data.accounts)) {
      for (const acc of data.accounts) upsertAccount(acc);
    }
    console.log(
      `[RévisEdu Realtime] Loaded ${savedMessages.length} msgs, ${savedDirectMessages.length} DMs, ${savedChannels.length} channels, ${registeredAccounts.size} accounts from disk`
    );
  } catch (err) {
    console.error('[RévisEdu Realtime] Failed to load store from disk:', err);
  }
}

// High Performance Debounced Store Persistence
let persistTimeout = null;
let isDirty = false;

function schedulePersistStore() {
  isDirty = true;
  if (!persistTimeout) {
    persistTimeout = setTimeout(() => {
      persistTimeout = null;
      flushStoreSync();
    }, 1000); // Batch writes every 1 second max
  }
}

function flushStoreSync() {
  if (!isDirty) return;
  isDirty = false;
  try {
    if (!existsSync(DATA_DIR)) mkdirSync(DATA_DIR, { recursive: true });
    const content = JSON.stringify(
      {
        accounts: getAccountsList(),
        channels: savedChannels,
        messages: savedMessages,
        directMessages: savedDirectMessages,
        groups: savedGroups,
        leaderboard: savedLeaderboard,
        announcement: currentAnnouncement,
        updatedAt: Date.now()
      },
      null,
      2
    );

    writeFileSync(DATA_TMP_FILE, content, 'utf-8');
    renameSync(DATA_TMP_FILE, DATA_FILE);

    // Save a backup occasionally
    try {
      copyFileSync(DATA_FILE, DATA_BACKUP_FILE);
    } catch {}
  } catch (err) {
    console.error('[RévisEdu Realtime] Failed to atomically persist store:', err);
  }
}

function upsertAccount(profile) {
  if (!profile?.name) return;
  const key = profile.name.toLowerCase().trim();
  const existing = registeredAccounts.get(key) || { name: profile.name };
  registeredAccounts.set(key, {
    ...existing,
    ...profile,
    name: profile.name,
    status: profile.status || 'online',
    lastSeen: Date.now()
  });
}

function getAccountsList() {
  return Array.from(registeredAccounts.values());
}

loadStore();

// Full state payload generator
function getCurrentState() {
  return {
    accounts: getAccountsList(),
    channels: savedChannels,
    messages: savedMessages,
    directMessages: savedDirectMessages,
    groups: savedGroups,
    leaderboard: savedLeaderboard,
    announcement: currentAnnouncement,
    serverTime: Date.now()
  };
}

// Safe broadcast to peers
function broadcastToPeers(senderWs, payloadString) {
  for (const client of clients) {
    if (client !== senderWs && client.readyState === WebSocket.OPEN) {
      try {
        client.send(payloadString);
      } catch (err) {
        console.warn('[RévisEdu Realtime] Peer send error, removing client:', err.message);
        try {
          client.terminate();
        } catch {}
        clients.delete(client);
      }
    }
  }
}

// -------------------------------------------------------------
// HTTP Server: Health, Keep-Alive, Rest Sync, and Status Dashboard
// -------------------------------------------------------------
const httpServer = createServer((req, res) => {
  // CORS Headers
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const url = req.url?.split('?')[0] || '';

  // 1. Healthcheck / Ping endpoint
  if (url === '/health' || url === '/ping' || url === '/api/keepalive') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(
      JSON.stringify({
        status: 'ok',
        service: 'revisedu-realtime',
        version: '3.0.0',
        uptimeSeconds: Math.floor((Date.now() - SERVER_START_TIME) / 1000),
        peers: clients.size,
        messages: savedMessages.length,
        directMessages: savedDirectMessages.length,
        channels: savedChannels.length,
        accounts: registeredAccounts.size,
        serverTime: Date.now()
      })
    );
    return;
  }

  // 2. HTML Live Status Dashboard
  if (url === '/' || url === '/status') {
    const uptimeSec = Math.floor((Date.now() - SERVER_START_TIME) / 1000);
    const hours = Math.floor(uptimeSec / 3600);
    const mins = Math.floor((uptimeSec % 3600) / 60);
    const secs = uptimeSec % 60;
    const memUsage = Math.round(process.memoryUsage().rss / 1024 / 1024);

    const onlinePeers = Array.from(clients)
      .map((c) => c.userProfile?.name || 'Visiteur')
      .slice(0, 20);

    const html = `<!DOCTYPE html>
<html lang="fr">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Serveur RévisEdu Temps Réel</title>
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; }
    body { background: #0f172a; color: #f8fafc; padding: 24px; display: flex; justify-content: center; }
    .container { max-width: 800px; width: 100%; }
    .header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 24px; padding-bottom: 16px; border-bottom: 1px solid #1e293b; }
    .title { font-size: 24px; font-weight: 700; color: #38bdf8; display: flex; align-items: center; gap: 10px; }
    .badge-live { background: #10b981; color: #022c22; font-size: 12px; font-weight: 800; padding: 4px 10px; border-radius: 9999px; text-transform: uppercase; animation: pulse 2s infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: 0.6; } }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 16px; margin-bottom: 24px; }
    .card { background: #1e293b; border-radius: 12px; padding: 20px; border: 1px solid #334155; }
    .card-label { font-size: 13px; color: #94a3b8; font-weight: 500; text-transform: uppercase; }
    .card-value { font-size: 28px; font-weight: 800; margin-top: 8px; color: #f1f5f9; }
    .users-section { background: #1e293b; border-radius: 12px; padding: 20px; border: 1px solid #334155; }
    .users-title { font-size: 16px; font-weight: 700; margin-bottom: 12px; color: #cbd5e1; }
    .tag { display: inline-block; background: #0369a1; color: #e0f2fe; padding: 4px 10px; border-radius: 6px; font-size: 13px; margin: 4px; }
    .footer { margin-top: 24px; text-align: center; color: #64748b; font-size: 13px; }
  </style>
  <script>
    setTimeout(() => { window.location.reload(); }, 10000);
  </script>
</head>
<body>
  <div class="container">
    <div class="header">
      <div class="title">🚀 Serveur RévisEdu Temps Réel v3.0</div>
      <div class="badge-live">● En Ligne</div>
    </div>
    <div class="grid">
      <div class="card">
        <div class="card-label">Élèves Connectés</div>
        <div class="card-value" style="color: #38bdf8;">${clients.size}</div>
      </div>
      <div class="card">
        <div class="card-label">Messages Stockés</div>
        <div class="card-value" style="color: #a78bfa;">${savedMessages.length + savedDirectMessages.length}</div>
      </div>
      <div class="card">
        <div class="card-label">Temps En Ligne</div>
        <div class="card-value" style="color: #34d399; font-size: 22px;">${hours}h ${mins}m ${secs}s</div>
      </div>
      <div class="card">
        <div class="card-label">Mémoire RAM</div>
        <div class="card-value" style="color: #fbbf24;">${memUsage} Mo</div>
      </div>
    </div>
    <div class="users-section">
      <div class="users-title">👤 Utilisateurs Actifs (${clients.size})</div>
      <div>
        ${onlinePeers.length > 0 ? onlinePeers.map((name) => `<span class="tag">${name}</span>`).join('') : '<span style="color:#64748b;">Aucun élève connecté pour le moment</span>'}
      </div>
    </div>
    <div class="footer">
      Actualisation automatique toutes les 10s • Auto Keep-Alive Actif • Zéro Crash Trap Actif
    </div>
  </div>
</body>
</html>`;

    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  // 3. REST API /api/sync fallback
  if (url === '/api/sync') {
    if (req.method === 'GET') {
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify(getCurrentState()));
      return;
    }

    if (req.method === 'POST') {
      let body = '';
      req.on('data', (chunk) => {
        body += chunk;
      });
      req.on('end', () => {
        try {
          const clientData = JSON.parse(body || '{}');
          let changed = false;

          if (Array.isArray(clientData.accounts)) {
            for (const acc of clientData.accounts) {
              upsertAccount(acc);
              changed = true;
            }
          }
          if (Array.isArray(clientData.messages)) {
            for (const m of clientData.messages) {
              if (m?.id && !savedMessages.some((existing) => existing.id === m.id)) {
                savedMessages.push(m);
                changed = true;
              }
            }
          }

          if (changed) schedulePersistStore();

          res.writeHead(200, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify(getCurrentState()));
        } catch (e) {
          res.writeHead(400, { 'Content-Type': 'application/json' });
          res.end(JSON.stringify({ error: 'Invalid JSON payload' }));
        }
      });
      return;
    }
  }

  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('Not found');
});

// -------------------------------------------------------------
// WebSocket Engine with Anti-Spam & Peer Association
// -------------------------------------------------------------
const wss = new WebSocketServer({
  server: httpServer,
  maxPayload: 10 * 1024 * 1024 // 10MB maximum payload protection
});

// Active Heartbeat every 25s to keep connections alive and kill dead sockets
const HEARTBEAT_INTERVAL = 25000;
const heartbeatTimer = setInterval(() => {
  for (const ws of clients) {
    if (ws.isAlive === false) {
      console.log('[RévisEdu Realtime] Terminating inactive dead socket');
      handleClientDisconnection(ws);
      continue;
    }

    ws.isAlive = false;
    try {
      ws.ping();
    } catch {
      handleClientDisconnection(ws);
    }
  }
}, HEARTBEAT_INTERVAL);

function handleClientDisconnection(ws) {
  if (!clients.has(ws)) return;
  clients.delete(ws);

  try {
    ws.terminate();
  } catch {}

  // If user profile was attached, notify peers immediately so UI updates
  if (ws.userProfile?.name) {
    console.log(`[RévisEdu Realtime] Peer ${ws.userProfile.name} disconnected. Remaining: ${clients.size}`);
    const key = ws.userProfile.name.toLowerCase().trim();
    const acc = registeredAccounts.get(key);
    if (acc) {
      acc.status = 'offline';
      acc.lastSeen = Date.now();
      schedulePersistStore();
    }

    // Broadcast instant offline event to close any active call/game
    const disconnectEvent = JSON.stringify({
      type: 'CALL_END',
      senderId: ws.userProfile.id || 'system',
      data: {
        reason: 'peer_disconnected',
        disconnectedUserName: ws.userProfile.name
      },
      timestamp: Date.now()
    });
    broadcastToPeers(ws, disconnectEvent);
  } else {
    console.log(`[RévisEdu Realtime] Client disconnected. Peers remaining: ${clients.size}`);
  }
}

wss.on('connection', (ws, req) => {
  ws.isAlive = true;
  ws.messageCount = 0;
  ws.lastMessageReset = Date.now();
  ws.userProfile = null;

  clients.add(ws);

  ws.on('pong', () => {
    ws.isAlive = true;
  });

  const clientIp = req.socket.remoteAddress;
  console.log(`[RévisEdu Realtime] Client connected from ${clientIp}. Total peers: ${clients.size}`);

  // Send full state to newly connected client
  try {
    ws.send(
      JSON.stringify({
        type: 'SYNC_STATE',
        senderId: 'server',
        data: getCurrentState(),
        timestamp: Date.now()
      })
    );
  } catch (err) {
    console.error('[RévisEdu Realtime] Failed to send SYNC_STATE:', err);
  }

  ws.on('message', (messageRaw) => {
    ws.isAlive = true;

    // Rate Limiting Protection (Max 120 messages per 5 seconds per socket)
    const now = Date.now();
    if (now - ws.lastMessageReset > 5000) {
      ws.messageCount = 0;
      ws.lastMessageReset = now;
    }
    ws.messageCount++;
    if (ws.messageCount > 120) {
      console.warn('[RévisEdu Realtime] Rate limit exceeded by client, dropping message');
      return;
    }

    try {
      const rawStr = messageRaw.toString();
      const payload = JSON.parse(rawStr);
      let shouldPersist = false;

      // Attach profile to socket for disconnect cleanup
      if (payload.data?.profile?.name) {
        ws.userProfile = payload.data.profile;
      }

      // Ping response from client app
      if (payload.type === 'PING') {
        ws.send(JSON.stringify({ type: 'PONG', timestamp: Date.now() }));
        return;
      }

      // Bidirectional state reconciliation from client
      if (payload.type === 'CLIENT_RECONCILE' && payload.data) {
        const { messages, accounts, channels, directMessages, groups, leaderboard } = payload.data;

        if (Array.isArray(accounts)) {
          for (const acc of accounts) upsertAccount(acc);
          shouldPersist = true;
        }

        if (Array.isArray(messages)) {
          for (const msg of messages) {
            if (msg?.id && !savedMessages.some((m) => m.id === msg.id)) {
              savedMessages.push(msg);
              shouldPersist = true;
            }
          }
          if (savedMessages.length > 5000) savedMessages = savedMessages.slice(-5000);
        }

        if (Array.isArray(directMessages)) {
          for (const dm of directMessages) {
            if (dm?.id && !savedDirectMessages.some((d) => d.id === dm.id)) {
              savedDirectMessages.push(dm);
              shouldPersist = true;
            }
          }
          if (savedDirectMessages.length > 2000) savedDirectMessages = savedDirectMessages.slice(-2000);
        }

        if (Array.isArray(channels)) {
          for (const ch of channels) {
            if (ch?.id && !savedChannels.some((c) => c.id === ch.id)) {
              savedChannels.push(ch);
              shouldPersist = true;
            }
          }
        }

        if (Array.isArray(groups)) {
          for (const grp of groups) {
            if (grp?.id && !savedGroups.some((g) => g.id === grp.id)) {
              savedGroups.push(grp);
              shouldPersist = true;
            }
          }
        }

        if (Array.isArray(leaderboard)) {
          savedLeaderboard = leaderboard;
          shouldPersist = true;
        }

        // Return updated state
        ws.send(
          JSON.stringify({
            type: 'SYNC_STATE',
            senderId: 'server',
            data: getCurrentState(),
            timestamp: Date.now()
          })
        );
      }

      // Public messages
      if (payload.type === 'MESSAGE_SENT' && payload.data) {
        if (!savedMessages.some((m) => m.id === payload.data.id)) {
          savedMessages.push(payload.data);
          if (savedMessages.length > 5000) savedMessages.shift();
          shouldPersist = true;
        }
      }

      if (payload.type === 'MESSAGE_DELETED' && payload.data?.messageId) {
        savedMessages = savedMessages.filter((m) => m.id !== payload.data.messageId);
        shouldPersist = true;
      }

      if (payload.type === 'MESSAGES_CLEAR_ALL') {
        savedMessages = [];
        shouldPersist = true;
      }

      // Direct Private Messages (DMs)
      if (payload.type === 'DM_SENT' && payload.data) {
        if (!savedDirectMessages.some((d) => d.id === payload.data.id)) {
          savedDirectMessages.push(payload.data);
          if (savedDirectMessages.length > 2000) savedDirectMessages.shift();
          shouldPersist = true;
        }
      }

      // Channels
      if (payload.type === 'CHANNEL_CREATED' && payload.data) {
        if (!savedChannels.some((c) => c.id === payload.data.id)) {
          savedChannels.push(payload.data);
          shouldPersist = true;
        }
      }

      if (payload.type === 'CHANNEL_DELETED' && payload.data?.channelId) {
        savedChannels = savedChannels.filter((c) => c.id !== payload.data.channelId);
        savedMessages = savedMessages.filter((m) => m.channelId !== payload.data.channelId);
        shouldPersist = true;
      }

      // Groups
      if (payload.type === 'GROUP_CREATED' && payload.data) {
        if (!savedGroups.some((g) => g.id === payload.data.id)) {
          savedGroups.push(payload.data);
          shouldPersist = true;
        }
      }

      if (payload.type === 'GROUP_DELETED' && payload.data?.groupId) {
        savedGroups = savedGroups.filter((g) => g.id !== payload.data.groupId);
        shouldPersist = true;
      }

      if (payload.type === 'GROUP_UPDATED' && payload.data) {
        savedGroups = savedGroups.map((g) => (g.id === payload.data.id ? payload.data : g));
        shouldPersist = true;
      }

      // Leaderboard
      if (payload.type === 'LEADERBOARD_UPDATE' && Array.isArray(payload.data?.entries)) {
        savedLeaderboard = payload.data.entries;
        shouldPersist = true;
      }

      // Profiles
      if (payload.type === 'PROFILE_UPDATED' && payload.data?.profile) {
        upsertAccount(payload.data.profile);
        ws.userProfile = payload.data.profile;
        shouldPersist = true;
      }

      if (payload.type === 'PRESENCE_PING' && payload.data?.profile) {
        upsertAccount(payload.data.profile);
        ws.userProfile = payload.data.profile;
        shouldPersist = true;
      }

      // Announcements
      if (payload.type === 'ADMIN_ANNOUNCEMENT') {
        const text = payload.data?.text?.trim();
        currentAnnouncement = text || null;
        shouldPersist = true;
      }

      if (shouldPersist) {
        schedulePersistStore();
      }

      // Broadcast to other peers safely
      broadcastToPeers(ws, rawStr);
    } catch (err) {
      console.error('[RévisEdu Realtime] Error processing message:', err);
    }
  });

  ws.on('close', () => {
    handleClientDisconnection(ws);
  });

  ws.on('error', (err) => {
    console.warn('[RévisEdu Realtime] Client error:', err.message);
    handleClientDisconnection(ws);
  });
});

// -------------------------------------------------------------
// Auto Keep-Alive for Render (Prevents Free Instance Sleeping)
// -------------------------------------------------------------
const KEEP_ALIVE_INTERVAL = 12 * 60 * 1000; // 12 minutes
setInterval(() => {
  try {
    const targetUrl = new URL(`${RENDER_SERVICE_URL}/health`);
    const reqModule = targetUrl.protocol === 'https:' ? httpsRequest : httpRequest;
    const keepAliveReq = reqModule(targetUrl, { method: 'GET', timeout: 10000 }, (res) => {
      console.log(`[RévisEdu Keep-Alive] Ping to ${targetUrl.href} succeeded (Status: ${res.statusCode})`);
    });
    keepAliveReq.on('error', (err) => {
      console.warn(`[RévisEdu Keep-Alive] Ping failed:`, err.message);
    });
    keepAliveReq.end();
  } catch (err) {
    console.warn(`[RévisEdu Keep-Alive] Error scheduling keep-alive:`, err.message);
  }
}, KEEP_ALIVE_INTERVAL);

// -------------------------------------------------------------
// Start Server & Graceful Shutdown
// -------------------------------------------------------------
httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`[RévisEdu Realtime v3.0] HTTP + WebSocket active on port ${PORT}`);
  console.log(`[RévisEdu Realtime] Dashboard: http://0.0.0.0:${PORT}/status`);
  console.log(`[RévisEdu Realtime] Health check: http://0.0.0.0:${PORT}/health`);
  console.log(`[RévisEdu Realtime] Sync endpoint: http://0.0.0.0:${PORT}/api/sync`);
  console.log(`[RévisEdu Realtime] Auto Keep-Alive active every 12 mins for ${RENDER_SERVICE_URL}`);
});

const cleanupAndExit = () => {
  console.log('[RévisEdu Realtime] Gracefully shutting down...');
  clearInterval(heartbeatTimer);
  flushStoreSync();
  for (const client of clients) {
    try {
      if (client.readyState === WebSocket.OPEN) client.close();
    } catch {}
  }
  wss.close();
  httpServer.close(() => {
    process.exit(0);
  });
};

process.on('SIGTERM', cleanupAndExit);
process.on('SIGINT', cleanupAndExit);
