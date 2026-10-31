'use strict';
/* Haunted Mansion: HTTP (отдаёт index.html) + WebSocket (комнаты и presence) + парк игровых серверов.
   Парк серверов (по принципу облачных игровых серверов, как у Roblox):
   - для каждой карты (place) работают экземпляры сервера (job), в каждом до 12 игроков;
   - GET /api/join?place=X  : матчмейкинг, выдаёт самый заполненный сервер со свободным местом или запускает новый;
   - GET /api/servers?place=X: список запущенных серверов (игроки, возраст);
   - GET /api/new?place=X   : запустить новый сервер вручную;
   - GET /api/stats         : состояние парка (серверы, игроки, аптайм);
   - пустой сервер автоматически останавливается через 30 секунд, как и в облаке.
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

/* ===== парк серверов ===== */
const PLACES_OK = new Set(['mansion', 'cross', 'lava', 'past', 'sword', 'comm', 'mask1', 'mask2', 'mask3', 'ost', 'base', 'cd', 'np']);
const MAX_PLAYERS = 12;                                   // игроков на сервер
const MAX_INSTANCES = 200;                                // серверов на весь парк
const IDLE_MS = Number(process.env.FLEET_IDLE_MS) || 30000; // пустой сервер выключается через это время
const RES_MS = 20000;                                     // бронь места для игрока, который подключается
const started = Date.now();
const fleet = new Map();                                  // ключ комнаты -> сервер
const createLog = new Map();                              // ip -> времена запуска серверов
const rooms = new Map();                                  // code -> Map(id -> ws)
const liveCount = i => { const r = rooms.get(i.key); return r ? r.size : 0; };
const resCount = i => { const t = Date.now(); i.res = i.res.filter(x => t - x < RES_MS); return i.res.length; };
const loadOf = i => liveCount(i) + resCount(i);
function spawnInstance(place) {
  if (fleet.size >= MAX_INSTANCES) return null;
  let job;
  do { job = crypto.randomBytes(3).toString('hex'); } while (fleet.has('srv-' + place + '-' + job));
  const i = { key: 'srv-' + place + '-' + job, place, job, max: MAX_PLAYERS, created: Date.now(), emptySince: Date.now(), res: [] };
  fleet.set(i.key, i);
  console.log('[fleet] start ' + i.key + ' (' + fleet.size + ' servers)');
  return i;
}
function allocate(place) {
  let best = null;
  fleet.forEach(i => { if (i.place !== place) return; const l = loadOf(i); if (l >= i.max - 1) return; if (!best || l > loadOf(best)) best = i; });
  if (!best) fleet.forEach(i => { if (!best && i.place === place && loadOf(i) < i.max) best = i; });
  if (!best) best = spawnInstance(place);
  if (best) best.res.push(Date.now());
  return best;
}
const info = i => ({ key: i.key, job: i.job, place: i.place, n: liveCount(i), max: i.max, age: Math.round((Date.now() - i.created) / 1000) });
function canCreate(ip) {
  const t = Date.now(), a = (createLog.get(ip) || []).filter(x => t - x < 60000);
  if (a.length >= 10) { createLog.set(ip, a); return false; }
  a.push(t); createLog.set(ip, a); return true;
}
setInterval(() => {                                       // «диспетчер»: останавливает пустые серверы
  const t = Date.now();
  fleet.forEach((i, k) => {
    if (liveCount(i) > 0 || resCount(i) > 0) { i.emptySince = t; return; }
    if (t - i.emptySince > IDLE_MS) { fleet.delete(k); rooms.delete(k); console.log('[fleet] stop ' + k + ' (' + fleet.size + ' servers)'); }
  });
  createLog.forEach((a, ip) => { if (!a.some(x => t - x < 60000)) createLog.delete(ip); });
}, 5000);
function api(u, url, req, res) {
  const out = (code, o) => { res.writeHead(code, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' }); res.end(JSON.stringify(o)); };
  const place = String(url.searchParams.get('place') || '');
  const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  if (u === '/api/stats') {
    let players = 0; rooms.forEach(r => { players += r.size; });
    const by = {}; fleet.forEach(i => { const o = by[i.place] || (by[i.place] = { servers: 0, players: 0 }); o.servers++; o.players += liveCount(i); });
    return out(200, { uptime: Math.round((Date.now() - started) / 1000), servers: fleet.size, players, byPlace: by });
  }
  if (!PLACES_OK.has(place)) return out(400, { error: 'bad place' });
  if (u === '/api/servers') {
    const l = []; fleet.forEach(i => { if (i.place === place) l.push(info(i)); });
    l.sort((a, b) => b.n - a.n);
    return out(200, l);
  }
  if (u === '/api/join') {
    const before = fleet.size, i = allocate(place);
    if (i && fleet.size > before && !canCreate(ip)) { fleet.delete(i.key); return out(429, { error: 'too many servers started' }); }
    if (!i) return out(503, { error: 'no capacity' });
    return out(200, info(i));
  }
  if (u === '/api/new') {
    if (!canCreate(ip)) return out(429, { error: 'too many servers started' });
    const i = spawnInstance(place);
    if (!i) return out(503, { error: 'no capacity' });
    i.res.push(Date.now());
    return out(200, info(i));
  }
  return out(404, { error: 'not found' });
}

const server = http.createServer((req, res) => {
  const u = (req.url || '/').split('?')[0];
  if (u.indexOf('/api/') === 0) { let url; try { url = new URL(req.url, 'http://localhost'); } catch (e) { res.writeHead(400); res.end(); return; } api(u, url, req, res); return; }
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
  const inst = code.indexOf('srv-') === 0 ? fleet.get(code) : null;
  if (code.indexOf('srv-') === 0 && !inst) { ws.close(1008, 'no such server'); return; }
  let room = rooms.get(code);
  if (!room) {
    if (rooms.size >= MAX_ROOMS) { ws.close(1013, 'server busy'); return; }
    room = new Map();
    rooms.set(code, room);
  }
  const old = room.get(id);
  if (!old && room.size >= (inst ? inst.max : (code.indexOf('mkhub') === 0 ? 400 : MAX_ROOM))) { ws.close(1013, 'room full'); if (!room.size) rooms.delete(code); return; }

  if (inst && !old && inst.res.length) inst.res.shift();
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
