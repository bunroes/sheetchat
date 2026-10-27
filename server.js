const express = require('express');
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const TTL_MS = 48 * 60 * 60 * 1000; // 메시지 보관 시간: 48시간
const MAX_TEXT = 2000;
const MAX_NAME = 20;

// ---- 저장소 (간단한 JSON 파일) ----
let db = { rooms: {} }; // rooms[id] = { id, title, createdAt, messages: [] }
try {
  db = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8'));
} catch {}

let saveTimer = null;
function save() {
  clearTimeout(saveTimer);
  saveTimer = setTimeout(() => {
    fs.writeFile(DATA_FILE, JSON.stringify(db), () => {});
  }, 500);
}

function prune() {
  const cutoff = Date.now() - TTL_MS;
  for (const room of Object.values(db.rooms)) {
    room.messages = room.messages.filter((m) => m.ts > cutoff);
  }
  save();
}
prune();
setInterval(prune, 5 * 60 * 1000);

const newId = () => crypto.randomBytes(9).toString('base64url');
const clean = (s, max) => String(s ?? '').trim().slice(0, max);

// ---- HTTP ----
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

app.post('/api/rooms', (req, res) => {
  const id = newId();
  const title = clean(req.body?.title, 60) || '제목 없는 스프레드시트';
  db.rooms[id] = { id, title, createdAt: Date.now(), messages: [] };
  save();
  res.json({ id, title });
});

app.get('/api/rooms/:id', (req, res) => {
  const room = db.rooms[req.params.id];
  if (!room) return res.status(404).json({ error: 'not found' });
  res.json({ id: room.id, title: room.title });
});

// 초대 링크: /d/<roomId>
app.get('/d/:id', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ---- 실시간 ----
const server = http.createServer(app);
const io = new Server(server);

io.on('connection', (socket) => {
  socket.on('join', (roomId, ack) => {
    const room = db.rooms[roomId];
    if (!room) return ack?.({ error: 'not found' });
    socket.join(roomId);
    const cutoff = Date.now() - TTL_MS;
    ack?.({ title: room.title, messages: room.messages.filter((m) => m.ts > cutoff) });
  });

  socket.on('leave', (roomId) => socket.leave(roomId));

  socket.on('message', ({ roomId, name, color, text } = {}) => {
    const room = db.rooms[roomId];
    text = clean(text, MAX_TEXT);
    name = clean(name, MAX_NAME) || '익명';
    color = /^#[0-9a-fA-F]{6}$/.test(color) ? color : '#1a73e8';
    if (!room || !text || !socket.rooms.has(roomId)) return;
    const msg = { id: newId(), name, color, text, ts: Date.now() };
    room.messages.push(msg);
    save();
    io.to(roomId).emit('message', { roomId, msg });
  });

  socket.on('rename', ({ roomId, title } = {}) => {
    const room = db.rooms[roomId];
    title = clean(title, 60);
    if (!room || !title || !socket.rooms.has(roomId)) return;
    room.title = title;
    save();
    io.to(roomId).emit('renamed', { roomId, title });
  });
});

server.listen(PORT, () => console.log(`http://localhost:${PORT}`));
