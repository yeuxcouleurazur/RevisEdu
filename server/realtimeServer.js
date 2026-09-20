import { createServer } from 'http';
import { readFileSync, writeFileSync, mkdirSync, existsSync } from 'fs';
import { dirname, join } from 'path';
import { fileURLToPath } from 'url';
import { WebSocketServer, WebSocket } from 'ws';

const PORT = process.env.PORT || 5174;
const clients = new Set();

const __dirname = dirname(fileURLToPath(import.meta.url));
const DATA_DIR = join(__dirname, '..', 'data');
const DATA_FILE = join(DATA_DIR, 'store.json');

/** @type {Map<string, { name: string; avatar?: string; bio?: string; id?: string }>} */
const registeredAccounts = new Map();
/** @type {Array<object>} */
let savedChannels = [];
/** @type {Array<object>} */
let savedMessages = [];
/** @type {string|null} */
let currentAnnouncement = null;

function loadStore() {
  try {
    if (!existsSync(DATA_FILE)) return;
    const raw = readFileSync(DATA_FILE, 'utf-8');
    const data = JSON.parse(raw);
    savedChannels = data.channels || [];
    savedMessages = data.messages || [];
    currentAnnouncement = data.announcement ?? null;
    if (data.accounts?.length) {
      for (const acc of data.accounts) upsertAccount(acc);
    }
    console.log(`[RévisEdu Realtime] Loaded ${savedMessages.length} messages from disk`);
  } catch (err) {
    console.error('[RévisEdu Realtime] Failed to load store:', err);
  }
}

function persistStore() {
  try {
    if (!existsSync(DATA_DIR)) mkdirSync(DATA_DIR, { recursive: true });
    writeFileSync(
      DATA_FILE,
      JSON.stringify({
        accounts: getAccountsList(),
        channels: savedChannels,
        messages: savedMessages,
        announcement: currentAnnouncement
      }),
      'utf-8'
    );
  } catch (err) {
    console.error('[RévisEdu Realtime] Failed to persist store:', err);
  }
}

function upsertAccount(profile) {
  if (!profile?.name) return;
  const key = profile.name.toLowerCase().trim();
  const existing = registeredAccounts.get(key) || { name: profile.name };
  registeredAccounts.set(key, {
    ...existing,
    ...profile,
    name: profile.name
  });
}

function getAccountsList() {
  return Array.from(registeredAccounts.values());
}

loadStore();

const httpServer = createServer((req, res) => {
  if (req.url === '/health' || req.url === '/') {
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({
      status: 'ok',
      service: 'revisedu-realtime',
      peers: clients.size,
      messages: savedMessages.length,
      accounts: registeredAccounts.size
    }));
    return;
  }
  res.writeHead(404);
  res.end('Not found');
});

const wss = new WebSocketServer({ server: httpServer });

httpServer.listen(PORT, '0.0.0.0', () => {
  console.log(`[RévisEdu Realtime] HTTP + WebSocket on port ${PORT}`);
  console.log(`[RévisEdu Realtime] Health check: http://0.0.0.0:${PORT}/health`);
});

wss.on('connection', (ws, req) => {
  clients.add(ws);
  const clientIp = req.socket.remoteAddress;
  console.log(`[RévisEdu Realtime] Client connected from ${clientIp}. Peers: ${clients.size}`);

  ws.send(JSON.stringify({
    type: 'SYNC_STATE',
    senderId: 'server',
    data: {
      accounts: getAccountsList(),
      channels: savedChannels,
      messages: savedMessages,
      announcement: currentAnnouncement
    },
    timestamp: Date.now()
  }));

  ws.on('message', (messageRaw) => {
    try {
      const payload = JSON.parse(messageRaw.toString());
      let shouldPersist = false;

      if (payload.type === 'MESSAGE_SENT' && payload.data) {
        if (!savedMessages.some((m) => m.id === payload.data.id)) {
          savedMessages.push(payload.data);
          if (savedMessages.length > 2000) savedMessages.shift();
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

      if (payload.type === 'PROFILE_UPDATED' && payload.data?.profile) {
        upsertAccount(payload.data.profile);
        shouldPersist = true;
      }

      if (payload.type === 'PRESENCE_PING' && payload.data?.profile) {
        upsertAccount(payload.data.profile);
        shouldPersist = true;
      }

      if (payload.type === 'ADMIN_ANNOUNCEMENT') {
        const text = payload.data?.text?.trim();
        currentAnnouncement = text || null;
        shouldPersist = true;
      }

      if (shouldPersist) persistStore();

      for (const client of clients) {
        if (client !== ws && client.readyState === WebSocket.OPEN) {
          client.send(messageRaw.toString());
        }
      }
    } catch (err) {
      console.error('[RévisEdu Realtime] Error processing message:', err);
    }
  });

  ws.on('close', () => {
    clients.delete(ws);
    console.log(`[RévisEdu Realtime] Client disconnected. Peers: ${clients.size}`);
  });

  ws.on('error', (err) => {
    console.error('[RévisEdu Realtime] Client error:', err);
    clients.delete(ws);
  });
});

process.on('SIGTERM', () => {
  console.log('[RévisEdu Realtime] Shutting down...');
  persistStore();
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) client.close();
  }
  wss.close();
  httpServer.close();
});
