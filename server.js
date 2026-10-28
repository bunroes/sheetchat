const express = require('express');
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;
const DATA_FILE = process.env.DATA_FILE || path.join(__dirname, 'data.json');
const MAX_TEXT = 2000;
const MAX_NAME = 20;
// 방마다 고를 수 있는 메시지 보관 기간 (시간). 0 = 계속 보관
const TTL_CHOICES = [48, 168, 720, 0];
const DEFAULT_TTL = 48;

const newId = () => crypto.randomBytes(9).toString('base64url');
const clean = (s, max) => String(s ?? '').trim().slice(0, max);
const cutoffFor = (ttl) => (ttl ? Date.now() - ttl * 3600 * 1000 : 0);

// ---- 저장소 ----
// DATABASE_URL이 있으면 Postgres(서버가 재시작돼도 남음), 없으면 JSON 파일.
function fileStore() {
  let db = { rooms: {} }; // rooms[id] = { id, title, ttl, createdAt, messages: [] }
  try { db = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); } catch {}
  let timer = null;
  const save = () => {
    clearTimeout(timer);
    timer = setTimeout(() => fs.writeFile(DATA_FILE, JSON.stringify(db), () => {}), 500);
  };
  const info = (r) => r && { id: r.id, title: r.title, ttl: r.ttl ?? DEFAULT_TTL };
  return {
    async init() {},
    async createRoom(title) {
      const id = newId();
      db.rooms[id] = { id, title, ttl: DEFAULT_TTL, createdAt: Date.now(), messages: [] };
      save();
      return info(db.rooms[id]);
    },
    async getRoom(id) { return info(db.rooms[id]); },
    async messages(id) {
      const r = db.rooms[id];
      const cutoff = cutoffFor(r.ttl ?? DEFAULT_TTL);
      return r.messages.filter((m) => m.ts > cutoff);
    },
    async addMessage(roomId, msg) { db.rooms[roomId].messages.push(msg); save(); },
    async updateRoom(id, fields) { Object.assign(db.rooms[id], fields); save(); },
    async prune() {
      for (const r of Object.values(db.rooms)) {
        const cutoff = cutoffFor(r.ttl ?? DEFAULT_TTL);
        r.messages = r.messages.filter((m) => m.ts > cutoff);
      }
      save();
    },
  };
}

function pgStore(url) {
  const { Pool } = require('pg');
  const local = /localhost|127\.0\.0\.1/.test(url);
  const pool = new Pool({ connectionString: url, ssl: local ? false : { rejectUnauthorized: false } });
  const q = (sql, params) => pool.query(sql, params);
  const info = (r) => r && { id: r.id, title: r.title, ttl: r.ttl };
  return {
    async init() {
      await q(`CREATE TABLE IF NOT EXISTS rooms (
        id TEXT PRIMARY KEY, title TEXT NOT NULL, ttl INT NOT NULL DEFAULT ${DEFAULT_TTL}, created_at BIGINT NOT NULL)`);
      await q(`CREATE TABLE IF NOT EXISTS messages (
        id TEXT PRIMARY KEY, room_id TEXT NOT NULL REFERENCES rooms(id) ON DELETE CASCADE,
        name TEXT NOT NULL, color TEXT NOT NULL, text TEXT NOT NULL, cid TEXT, ts BIGINT NOT NULL)`);
      await q('CREATE INDEX IF NOT EXISTS messages_room_ts ON messages (room_id, ts)');
    },
    async createRoom(title) {
      const id = newId();
      await q('INSERT INTO rooms (id, title, created_at) VALUES ($1, $2, $3)', [id, title, Date.now()]);
      return { id, title, ttl: DEFAULT_TTL };
    },
    async getRoom(id) {
      const { rows } = await q('SELECT id, title, ttl FROM rooms WHERE id = $1', [id]);
      return info(rows[0]);
    },
    async messages(id) {
      const room = await this.getRoom(id);
      const { rows } = await q(
        'SELECT id, name, color, text, cid, ts FROM messages WHERE room_id = $1 AND ts > $2 ORDER BY ts',
        [id, cutoffFor(room.ttl)]);
      return rows.map((m) => ({ ...m, ts: Number(m.ts) }));
    },
    async addMessage(roomId, m) {
      await q('INSERT INTO messages (id, room_id, name, color, text, cid, ts) VALUES ($1,$2,$3,$4,$5,$6,$7)',
        [m.id, roomId, m.name, m.color, m.text, m.cid, m.ts]);
    },
    async updateRoom(id, { title, ttl }) {
      if (title !== undefined) await q('UPDATE rooms SET title = $2 WHERE id = $1', [id, title]);
      if (ttl !== undefined) await q('UPDATE rooms SET ttl = $2 WHERE id = $1', [id, ttl]);
    },
    async prune() {
      await q(`DELETE FROM messages m USING rooms r
        WHERE m.room_id = r.id AND r.ttl > 0 AND m.ts < $1 - r.ttl::bigint * 3600000`, [Date.now()]);
    },
  };
}

const store = process.env.DATABASE_URL ? pgStore(process.env.DATABASE_URL) : fileStore();

// ---- HTTP ----
const app = express();
app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// express 4는 async 오류를 잡지 못하므로 직접 넘겨줌
const route = (fn) => (req, res, next) => fn(req, res).catch(next);

app.post('/api/rooms', route(async (req, res) => {
  const title = clean(req.body?.title, 60) || '제목 없는 스프레드시트';
  res.json(await store.createRoom(title));
}));

app.get('/api/rooms/:id', route(async (req, res) => {
  const room = await store.getRoom(req.params.id);
  if (!room) return res.status(404).json({ error: 'not found' });
  res.json(room);
}));

// 초대 링크: /d/<roomId>
app.get('/d/:id', (req, res) => res.sendFile(path.join(__dirname, 'public', 'index.html')));

// ---- 실시간 ----
const server = http.createServer(app);
const io = new Server(server);

// 소켓 핸들러에서 DB 오류가 나도 서버가 죽지 않게
const safe = (fn) => (...args) => fn(...args).catch((e) => console.error(e));

io.on('connection', (socket) => {
  socket.on('join', safe(async (roomId, ack) => {
    const room = await store.getRoom(String(roomId));
    if (!room) return ack?.({ error: 'not found' });
    socket.join(room.id);
    ack?.({ title: room.title, ttl: room.ttl, messages: await store.messages(room.id) });
  }));

  socket.on('leave', (roomId) => socket.leave(roomId));

  socket.on('message', safe(async ({ roomId, name, color, text, cid } = {}) => {
    text = clean(text, MAX_TEXT);
    name = clean(name, MAX_NAME) || '익명';
    color = /^#[0-9a-fA-F]{6}$/.test(color) ? color : '#1a73e8';
    if (!text || !socket.rooms.has(roomId)) return;
    const msg = { id: newId(), name, color, text, cid: clean(cid, 32), ts: Date.now() };
    await store.addMessage(roomId, msg);
    io.to(roomId).emit('message', { roomId, msg });
  }));

  socket.on('rename', safe(async ({ roomId, title } = {}) => {
    title = clean(title, 60);
    if (!title || !socket.rooms.has(roomId)) return;
    await store.updateRoom(roomId, { title });
    io.to(roomId).emit('renamed', { roomId, title });
  }));

  socket.on('ttl', safe(async ({ roomId, ttl } = {}) => {
    if (!TTL_CHOICES.includes(ttl) || !socket.rooms.has(roomId)) return;
    await store.updateRoom(roomId, { ttl });
    io.to(roomId).emit('ttl', { roomId, ttl });
  }));
});

store.init().then(async () => {
  await store.prune();
  setInterval(safe(() => store.prune()), 5 * 60 * 1000);
  server.listen(PORT, () => console.log(`http://localhost:${PORT}` + (process.env.DATABASE_URL ? ' (Postgres)' : ' (data.json)')));
});
