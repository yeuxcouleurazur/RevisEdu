import { createServer } from 'http';
import { readFileSync, writeFileSync, renameSync, mkdirSync, existsSync } from 'fs';
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
const clients = new Set();

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = join(__dirname, '..', 'data');
const DATA_FILE = join(DATA_DIR, 'store.json');
const DATA_TMP_FILE = join(DATA_DIR, 'store.json.tmp');

/** @type {Map<string, { name: string; avatar?: string; bio?: string; id?: string }>} */
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

function loadStore() {
  try {
    if (!existsSync(DATA_FILE)) return;
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

// Atomic file write to completely eliminate file corruption
function persistStore() {
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

// Robust broadcast that isolates socket errors
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

const httpServer = createServer((req, res) => {
  // CORS Headers for all requests
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');

  if (req.method === 'OPTIONS') {
    res.writeHead(204);
    res.end();
    return;
  }

  const url = req.url?.split('?')[0] || '';

  if (url === '/health' || url === '/' || url === '/api/keepalive') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(
      JSON.stringify({
        status: 'ok',
        service: 'revisedu-realtime',
        version: '2.5.0',
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

  // REST fallback for initial sync or when WebSockets are blocked
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

          // Merge accounts
          if (Array.isArray(clientData.accounts)) {
            for (const acc of clientData.accounts) {
              upsertAccount(acc);
              changed = true;
            }
          }

          // Merge messages
          if (Array.isArray(clientData.messages)) {
            for (const m of clientData.messages) {
              if (m?.id && !savedMessages.some((existing) => existing.id === m.id)) {
                savedMessages.push(m);
                changed = true;
              }
            }
          }

          if (changed) persistStore();

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

const wss = new WebSocketServer({ server: httpServer });

// Active Heartbeat every 25s to keep connections alive and kill zombies
const HEARTBEAT_INTERVAL = 25000;
const heartbeatTimer = setInterval(() => {
  for (const ws of clients) {
    if (ws.isAlive === false) {
      console.log('[RévisEdu Realtime] Terminating inactive dead socket');
      clients.delete(ws);
      try {
        ws.terminate();
      } catch {}
      continue;
    }

    ws.isAlive = false;
    try {
      ws.ping();
    } catch {
      clients.delete(ws);
    }
  }
}, HEARTBEAT_INTERVAL);

wss.on('connection', (ws, req) => {
  ws.isAlive = true;
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
    try {
      const rawStr = messageRaw.toString();
      const payload = JSON.parse(rawStr);
      let shouldPersist = false;

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
        shouldPersist = true;
      }

      if (payload.type === 'PRESENCE_PING' && payload.data?.profile) {
        upsertAccount(payload.data.profile);
        shouldPersist = true;
      }

      // Announcements
      if (payload.type === 'ADMIN_ANNOUNCEMENT') {
        const text = payload.data?.text?.trim();
        currentAnnouncement = text || null;
        shouldPersist = true;
      }

      if (shouldPersist) {
        persistStore();
      }

      // Broadcast to other peers safely
      broadcastToPeers(ws, rawStr);
    } catch (err) {
      console.error('[RévisEdu Realtime] Error processing message:', err);
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`[RévisEdu Realtime] Client disconnected. Peers remaining: ${clients.size}`);
  });

  ws.on('error', (err) => {
    console.warn('[RévisEdu Realtime] Client error:', err.message);
    clients.delete(ws);
    try {
      ws.terminate();
    } catch {}
  });
});

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`[RévisEdu Realtime v2.5] HTTP + WebSocket active on port ${PORT}`);
  console.log(`[RévisEdu Realtime] Health check: http://0.0.0.0:${PORT}/health`);
  console.log(`[RévisEdu Realtime] Sync endpoint: http://0.0.0.0:${PORT}/api/sync`);
});

const cleanupAndExit = () => {
  console.log('[RévisEdu Realtime] Gracefully shutting down...');
  clearInterval(heartbeatTimer);
  persistStore();
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
