import { createServer } from 'http';
import { WebSocketServer, WebSocket } from 'ws';

const PORT = process.env.PORT || 5174;
const clients = new Set();

/** @type {Map<string, { name: string; avatar?: string; bio?: string; id?: string }>} */
const registeredAccounts = new Map();
/** @type {Array<object>} */
let savedChannels = [];
/** @type {Array<object>} */
let savedMessages = [];
/** @type {string|null} */
let currentAnnouncement = null;

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

      if (payload.type === 'MESSAGE_SENT' && payload.data) {
        savedMessages.push(payload.data);
        if (savedMessages.length > 500) savedMessages.shift();
      }

      if (payload.type === 'CHANNEL_CREATED' && payload.data) {
        savedChannels.push(payload.data);
      }

      if (payload.type === 'PROFILE_UPDATED' && payload.data?.profile) {
        upsertAccount(payload.data.profile);
      }

      if (payload.type === 'PRESENCE_PING' && payload.data?.profile) {
        upsertAccount(payload.data.profile);
      }

      if (payload.type === 'ADMIN_ANNOUNCEMENT' && payload.data?.text) {
        currentAnnouncement = payload.data.text;
      }

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
  for (const client of clients) {
    if (client.readyState === WebSocket.OPEN) client.close();
  }
  wss.close();
  httpServer.close();
});
