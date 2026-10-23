'use strict';
/* Haunted Mansion: HTTP (отдаёт index.html) + WebSocket (комнаты и presence).
   Комната = код из латиницы и цифр, пустой код = общая комната.
   Клиент шлёт {t:'p', d:{...}}: частичное обновление своего presence.
   Сервер рассылает остальным {t:'p', id, d}, при входе шлёт {t:'hello', id, peers}, при выходе {t:'leave', id}. */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const INDEX = path.join(__dirname, 'Haunted_Mansion.html');
const MAX_ROOM = 30;          // игроков в одной комнате
const MAX_ROOMS = 500;        // комнат на весь сервер
const MAX_KEYS_PER_MSG = 40;
const MAX_KEYS_TOTAL = 60;
const BAD_KEYS = new Set(['__proto__', 'constructor', 'prototype']);

const server = http.createServer((req, res) => {
  const u = (req.url || '/').split('?')[0];
  if (u === '/' || u === '/index.html' || u === '/Haunted_Mansion.html') {
    fs.readFile(INDEX, (e, b) => {
      if (e) { res.writeHead(500); res.end('Haunted_Mansion.html not found'); return; }
      res.writeHead(200, {
        'Content-Type': 'text/html; charset=utf-8',
        'Cache-Control': 'no-cache',
        'X-Content-Type-Options': 'nosniff'
      });
      res.end(b);
    });
    return;
  }
  if (u === '/healthz') { res.writeHead(200, { 'Content-Type': 'text/plain' }); res.end('ok'); return; }
  res.writeHead(404, { 'Content-Type': 'text/plain' });
  res.end('not found');
});

const wss = new WebSocketServer({ noServer: true, maxPayload: 4096 });
const rooms = new Map(); // code -> Map(id -> ws)

server.on('upgrade', (req, socket, head) => {
  let url;
  try { url = new URL(req.url, 'http://localhost'); } catch (e) { socket.destroy(); return; }
  if (url.pathname !== '/ws') { socket.destroy(); return; }
  wss.handleUpgrade(req, socket, head, ws => wss.emit('connection', ws, req, url));
});

const cleanRoom = v => String(v || '').toLowerCase().replace(/[^a-z0-9_.-]/g, '').replace(/^[^a-z0-9]+/, '').slice(0, 40);
const cleanId = v => { const s = String(v || '').toLowerCase().replace(/[^a-z0-9]/g, '').slice(0, 24); return s.length >= 6 ? s : ''; };
const rid = () => crypto.randomBytes(8).toString('hex');

function send(ws, o) {
  if (ws.readyState === 1) { try { ws.send(JSON.stringify(o)); } catch (e) { /* ignore */ } }
}
function broadcast(room, o, exceptId) {
  const s = JSON.stringify(o);
  room.forEach((w, id) => { if (id !== exceptId && w.readyState === 1) { try { w.send(s); } catch (e) { /* ignore */ } } });
}

wss.on('connection', (ws, req, url) => {
  const code = cleanRoom(url.searchParams.get('room'));
  const id = cleanId(url.searchParams.get('id')) || rid();
  let room = rooms.get(code);
  if (!room) {
    if (rooms.size >= MAX_ROOMS) { ws.close(1013, 'server busy'); return; }
    room = new Map();
    rooms.set(code, room);
  }
  const old = room.get(id);
  if (!old && room.size >= (code.indexOf('mkhub') === 0 ? 400 : MAX_ROOM)) { ws.close(1013, 'room full'); if (!room.size) rooms.delete(code); return; }

  ws.pid = id;
  ws.pres = old ? old.pres : {};   // переподключение с тем же id сохраняет состояние
  ws.alive = true;
  ws.tokens = 80;
  ws.last = Date.now();
  if (old) { try { old.terminate(); } catch (e) { /* ignore */ } }
  room.set(id, ws);

  const peers = [];
  room.forEach((w, k) => { if (k !== id) peers.push({ id: k, pres: w.pres }); });
  send(ws, { t: 'hello', id, peers });

  ws.on('pong', () => { ws.alive = true; });
  ws.on('error', () => {});
  ws.on('message', (data, isBinary) => {
    if (isBinary) return;
    const now = Date.now();
    ws.tokens = Math.min(80, ws.tokens + (now - ws.last) * 0.06); // ~60 сообщений в секунду
    ws.last = now;
    if (ws.tokens < 1) return;
    ws.tokens -= 1;
    let m;
    try { m = JSON.parse(data.toString()); } catch (e) { return; }
    if (!m || m.t !== 'p' || !m.d || typeof m.d !== 'object' || Array.isArray(m.d)) return;
    const keys = Object.keys(m.d);
    if (!keys.length || keys.length > MAX_KEYS_PER_MSG) return;
    const clean = {};
    for (const k of keys) {
      if (BAD_KEYS.has(k) || k.length > 30) continue;
      clean[k] = m.d[k];
    }
    const ck = Object.keys(clean);
    if (!ck.length) return;
    const merged = Object.assign({}, ws.pres, clean);
    if (Object.keys(merged).length > MAX_KEYS_TOTAL) return;
    ws.pres = merged;
    broadcast(room, { t: 'p', id, d: clean }, id);
  });
  ws.on('close', () => {
    if (room.get(id) === ws) {
      room.delete(id);
      broadcast(room, { t: 'leave', id }, null);
      if (!room.size && rooms.get(code) === room) rooms.delete(code);
    }
  });
});

/* пинг раз в 25 секунд: Render и браузеры не рвут простаивающие соединения, мёртвые закрываются */
setInterval(() => {
  wss.clients.forEach(ws => {
    if (ws.alive === false) { try { ws.terminate(); } catch (e) { /* ignore */ } return; }
    ws.alive = false;
    try { ws.ping(); } catch (e) { /* ignore */ }
  });
}, 25000);

server.listen(PORT, '0.0.0.0', () => console.log('Haunted Mansion server on port ' + PORT));
