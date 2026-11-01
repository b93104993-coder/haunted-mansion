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

/* ===== владелец и баны ===== */
const OWNER_SALT = 'ugM7x2';
const OWNER_HASH = process.env.OWNER_HASH || 'a06ea89da3f02f6769843b8dce9ca1a3890a5d73d76f3e365df18cfcbc6b25e0'; // sha256(salt:пароль); можно заменить переменной окружения
const OWNER_TOKEN = crypto.createHmac('sha256', crypto.randomBytes(24)).update('owner').digest('hex'); // действует до перезапуска сервера
const BAN_MS = 60 * 60 * 1000;
const banId = new Map(), banIp = new Map(), authLog = new Map();
/* постоянные данные: администраторы (токены), правки каталога, заблокированные плейсы. Файл лежит рядом; на бесплатном Render диск стирается при перезапуске */
const DATA_FILE = path.join(process.env.DATA_DIR || __dirname, 'hm_data.json');
let DATA = { adm: {}, cat: { set: {}, del: {} }, blk: [] };
try { const d = JSON.parse(fs.readFileSync(DATA_FILE, 'utf8')); if (d && typeof d === 'object') DATA = Object.assign(DATA, d); } catch (e) { /* первый запуск */ }
const saveData = () => { try { fs.writeFileSync(DATA_FILE, JSON.stringify(DATA)); } catch (e) { /* ignore */ } };
const catMsg = () => ({ t: 'cat', cat: DATA.cat, blk: DATA.blk });
const sameStr = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };
const ownerPw = pw => sameStr(crypto.createHash('sha256').update(OWNER_SALT + ':' + String(pw).slice(0, 100)).digest('hex'), OWNER_HASH);
const isBanned = (id, ip) => { const t = Date.now(); for (const [m, k] of [[banId, id], [banIp, ip]]) { const e = m.get(k); if (e) { if (e > t) return true; m.delete(k); } } return false; };
const authOk = ip => { const t = Date.now(), a = (authLog.get(ip) || []).filter(x => t - x < 60000); a.push(t); authLog.set(ip, a); return a.length <= 6; };

/* ===== парк серверов ===== */
const PLACES_OK = new Set(['mansion', 'cross', 'lava', 'past', 'sword', 'comm', 'mask1', 'mask2', 'mask3', 'ost', 'base', 'cd', 'np', 'rm']);
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
  if (u === '/api/catalog') return out(200, { cat: DATA.cat, blk: DATA.blk });
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
  const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim();
  if (isBanned(id, ip)) { ws.close(4003, 'banned'); return; }
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
  ws.ip = ip;
  ws.owner = false;
  ws.pres = old ? old.pres : {};   // переподключение с тем же id сохраняет состояние
  ws.alive = true;
  ws.tokens = 80;
  ws.last = Date.now();
  if (old) { try { old.terminate(); } catch (e) { /* ignore */ } }
  room.set(id, ws);

  const peers = [];
  room.forEach((w, k) => { if (k !== id) peers.push({ id: k, pres: w.pres }); });
  send(ws, { t: 'hello', id, peers });
  send(ws, catMsg());

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
    const staff = ws.owner || ws.admin;
    if (m && m.t === 'auth') {
      if (m.atok !== undefined) {
        if (typeof m.atok === 'string' && DATA.adm[m.atok] && authOk(ws.ip)) { ws.admin = true; ws.atok = m.atok; send(ws, { t: 'admin', ok: 1 }); }
        else send(ws, { t: 'admin', ok: 0 });
      }
      if (m.tok !== undefined || m.pw !== undefined) {
        if (m.tok && sameStr(m.tok, OWNER_TOKEN)) { ws.owner = true; send(ws, { t: 'owner', ok: 1 }); }
        else if (m.pw !== undefined && authOk(ws.ip) && ownerPw(m.pw)) { ws.owner = true; send(ws, { t: 'owner', ok: 1, tok: OWNER_TOKEN }); }
        else send(ws, { t: 'owner', ok: 0 });
      }
      return;
    }
    if (m && m.t === 'ban' && staff) {
      const tg = room.get(cleanId(m.id));
      if (tg && tg !== ws && !tg.owner && !tg.admin) {
        const ms = Math.min(1440, Math.max(1, parseInt(m.mins, 10) || 60)) * 60000, exp = Date.now() + ms;
        banId.set(tg.pid, exp);
        if (tg.ip && tg.ip !== ws.ip) banIp.set(tg.ip, exp);
        broadcast(room, { t: 'banned', id: tg.pid, n: String(m.n || '').slice(0, 20) }, null);
        console.log('[ban] ' + tg.pid + ' by ' + (ws.owner ? 'owner' : 'admin'));
        setTimeout(() => { try { tg.close(4003, 'banned'); } catch (e) { /* ignore */ } }, 150);
      }
      return;
    }
    if (m && m.t === 'unban' && staff) { banId.clear(); banIp.clear(); return; }
    if (m && m.t === 'say' && staff) {
      const o = JSON.stringify({ t: 'announce', n: String(m.n || '').slice(0, 20), text: String(m.text || '').slice(0, 160) });
      wss.clients.forEach(c => { if (c.readyState === 1) { try { c.send(o); } catch (e) { /* ignore */ } } });
      return;
    }
    if (m && m.t === 'cat' && staff) {
      const kind = String(m.kind), id = String(m.id || '').replace(/[^a-z0-9_]/gi, '').slice(0, 12);
      if (['hat', 'tool', 'sword'].indexOf(kind) >= 0 && id) {
        const st = DATA.cat.set[kind] || (DATA.cat.set[kind] = {}), dl = DATA.cat.del[kind] || (DATA.cat.del[kind] = []);
        if (m.op === 'del') { delete st[id]; if (dl.indexOf(id) < 0) dl.push(id); }
        else if (m.op === 'add' || m.op === 'price') { st[id] = Math.min(9999, Math.max(0, parseInt(m.price, 10) || 0)); DATA.cat.del[kind] = dl.filter(x => x !== id); }
        saveData();
        const o = JSON.stringify(catMsg());
        wss.clients.forEach(c => { if (c.readyState === 1) { try { c.send(o); } catch (e) { /* ignore */ } } });
      }
      return;
    }
    if (m && m.t === 'blk' && staff) {
      const id = String(m.id || '').slice(0, 40);
      if (id.charAt(0) === 'u') {
        if (m.op === 'add' && DATA.blk.indexOf(id) < 0 && DATA.blk.length < 500) DATA.blk.push(id);
        if (m.op === 'del') DATA.blk = DATA.blk.filter(x => x !== id);
        saveData();
        const o = JSON.stringify(catMsg());
        wss.clients.forEach(c => { if (c.readyState === 1) { try { c.send(o); } catch (e) { /* ignore */ } } });
      }
      return;
    }
    if (m && m.t === 'grant' && ws.owner) {
      const tg = room.get(cleanId(m.id)), nm = String(m.n || '').slice(0, 20);
      if (tg && tg !== ws && !tg.owner && nm) {
        Object.keys(DATA.adm).forEach(k => { if (DATA.adm[k].n.toLowerCase() === nm.toLowerCase()) delete DATA.adm[k]; });
        const tok = crypto.randomBytes(24).toString('hex');
        DATA.adm[tok] = { n: nm, ts: Date.now() }; saveData();
        tg.admin = true; tg.atok = tok;
        send(tg, { t: 'admin', ok: 1, tok });
        send(ws, { t: 'info', text: 'Администратор выдан: ' + nm });
      }
      return;
    }
    if (m && m.t === 'revoke' && ws.owner) {
      const nm = String(m.n || '').toLowerCase(); let n = 0;
      Object.keys(DATA.adm).forEach(k => { if (DATA.adm[k].n.toLowerCase() === nm) { delete DATA.adm[k]; n++; wss.clients.forEach(c => { if (c.atok === k) { c.admin = false; c.atok = ''; send(c, { t: 'admin', ok: 0 }); } }); } });
      if (n) saveData();
      send(ws, { t: 'info', text: n ? 'Права сняты: ' + nm : 'Такого администратора нет: ' + nm });
      return;
    }
    if (m && m.t === 'admins' && ws.owner) {
      const l = Object.keys(DATA.adm).map(k => DATA.adm[k].n).join(', ');
      send(ws, { t: 'info', text: 'Администраторы: ' + (l || 'нет') });
      return;
    }
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
