'use strict';
/*
 * Сервер для игры Haunted Mansion (БЛОКС).
 *  - комнаты: игроки группируются по коду, presence пересылается всем в комнате, кроме отправителя;
 *  - форум: посты лежат в forum.json, при подключении игрок получает их все,
 *    новые посты рассылаются ВСЕМ подключённым (не только в комнате).
 *  - если рядом лежит файл Haunted_Mansion.html, сервер отдаёт его по адресу "/"
 *    и сам подставляет в игру адрес сервера (друзьям ничего вводить не нужно).
 */
const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const { WebSocketServer } = require('ws');

const PORT = parseInt(process.env.PORT, 10) || 7860; // Hugging Face сам задаёт PORT, по умолчанию 7860
const GAME_FILE = path.join(__dirname, 'Haunted_Mansion.html');

/* ---------- где хранить forum.json ---------- */
function pickDataDir() {
  if (process.env.DATA_DIR) return process.env.DATA_DIR;
  try { fs.accessSync('/data', fs.constants.W_OK); return '/data'; } catch (e) { /* нет постоянного диска */ }
  return path.join(__dirname, 'data');
}
const DATA_DIR = pickDataDir();
try { fs.mkdirSync(DATA_DIR, { recursive: true }); } catch (e) { console.error('Не могу создать папку', DATA_DIR, e.message); }
const FORUM_FILE = path.join(DATA_DIR, 'forum.json');
const log = (...a) => console.log(new Date().toISOString(), ...a);
if (!process.env.DATA_DIR && DATA_DIR !== '/data') {
  log('ВНИМАНИЕ: папка /data недоступна, форум сохраняется в ' + DATA_DIR +
    '. На Hugging Face без Persistent storage этот файл пропадёт при перезапуске Space.');
}

/* ---------- копия форума в GitHub Gist (чтобы форум не пропадал на бесплатных хостингах) ---------- */
const GH_TOKEN = process.env.GITHUB_TOKEN || '';
const GIST_ID = /^[A-Za-z0-9]{8,64}$/.test(process.env.GIST_ID || '') ? process.env.GIST_ID : '';
const GH_API = (process.env.GITHUB_API || 'https://api.github.com').replace(/\/+$/, '');
const GIST_RETRY_MS = parseInt(process.env.GIST_RETRY_MS, 10) || 30000;
const GIST_ON = !!(GH_TOKEN && GIST_ID);
let gistReady = false, gistDirty = false, gistBusy = false, gistTimer = null;
const sleep = ms => new Promise(r => setTimeout(r, ms));

async function gh(method, pathname, body) {
  const r = await fetch(GH_API + pathname, {
    method,
    headers: Object.assign({
      Authorization: 'Bearer ' + GH_TOKEN, Accept: 'application/vnd.github+json',
      'User-Agent': 'haunted-mansion-server', 'X-GitHub-Api-Version': '2022-11-28'
    }, body ? { 'Content-Type': 'application/json' } : {}),
    body: body ? JSON.stringify(body) : undefined,
    signal: AbortSignal.timeout(10000)
  });
  if (!r.ok) throw new Error('GitHub ответил ' + r.status);
  return r.json();
}
async function gistRead() {
  const g = await gh('GET', '/gists/' + GIST_ID);
  const f = g.files && g.files['forum.json'];
  if (!f) return '';
  if (f.truncated && f.raw_url) {
    const r = await fetch(f.raw_url, { headers: { Authorization: 'Bearer ' + GH_TOKEN, 'User-Agent': 'haunted-mansion-server' }, signal: AbortSignal.timeout(10000) });
    if (!r.ok) throw new Error('GitHub raw ' + r.status);
    return r.text();
  }
  return f.content || '';
}
function mergeForum(base, extra) { // добавляет в base то, чего там ещё нет
  for (const t of extra) {
    const b = base.find(x => x.id === t.id);
    if (!b) { base.push(t); continue; }
    for (const p of t.posts) if (!b.posts.some(q => q.id === p.id)) b.posts.push(p);
    b.posts.sort((x, y) => x.ts - y.ts);
  }
}
async function gistLoad(afterOutage) {
  const txt = await gistRead();
  let loaded = [];
  if (txt.trim()) {
    try { loaded = sanitizeThreads(JSON.parse(txt).threads); }
    catch (e) { throw new Error('forum.json в Gist повреждён, исправь его вручную'); } // не перезаписываем, чтобы не потерять данные
  }
  const wasEmpty = !loaded.length;
  if (wasEmpty) loaded = seedForum().threads;
  mergeForum(loaded, forum.threads); // сохраняем то, что успели написать, пока Gist был недоступен
  forum.threads = loaded;
  trimForum();
  gistReady = true;
  if (afterOutage || wasEmpty) { gistDirty = true; scheduleGist(); }
}
function gistRetry() {
  setTimeout(async () => {
    try { await gistLoad(true); log('Gist снова доступен, форум объединён'); saveNow(false); }
    catch (e) { log('Gist всё ещё недоступен: ' + e.message); gistRetry(); }
  }, GIST_RETRY_MS).unref();
}
function scheduleGist(delay) {
  if (!GIST_ON || gistTimer) return;
  gistTimer = setTimeout(async () => { gistTimer = null; await flushGist(); }, delay || 4000);
}
async function flushGist() {
  if (!gistReady || gistBusy || !gistDirty) return;
  gistBusy = true; gistDirty = false;
  let failed = false;
  try {
    await gh('PATCH', '/gists/' + GIST_ID, { files: { 'forum.json': { content: JSON.stringify({ threads: forum.threads }) } } });
    log('Форум сохранён в Gist');
  } catch (e) {
    failed = true; gistDirty = true;
    log('Не удалось сохранить в Gist: ' + e.message + '. Повторю позже.');
  }
  gistBusy = false;
  if (failed) scheduleGist(30000); else if (gistDirty) scheduleGist();
}
if (GIST_ON) log('Копия форума в GitHub Gist включена');
else log('Gist не настроен (переменные GITHUB_TOKEN и GIST_ID): форум хранится только в файле');

/* ---------- форум ---------- */
const CATS = new Set(['g', 'p', 'w', 'b']);
const ID_RE = /^[A-Za-z0-9_-]{1,24}$/;
const MAX_THREADS = 300, MAX_POSTS = 300;
const clean = (v, n) => (typeof v === 'string' ? v.replace(/[\u0000-\u001f\u007f]/g, ' ').trim().slice(0, n) : '');

function seedForum() {
  const d = Date.now() - 864e5;
  const mk = (id, c, t, x) => ({ id, c, t, a: 'Хранитель', ts: d, posts: [{ id: id + 'p', a: 'Хранитель', x, ts: d }] });
  return {
    threads: [
      mk('t1', 'g', 'Добро пожаловать в БЛОКС!', 'Здесь можно обсудить игру, плейсы и оружие. Пиши аккуратно и будь добр к другим игрокам.'),
      mk('t2', 'p', 'Как сделать свой плейс?', 'Оформи подписку Создателя в магазине (200 TIX), открой вкладку «Редактор» и нажми «Создать новый плейс». Код плейса можно отправить друзьям.'),
      mk('t3', 'w', 'Какой меч лучше?', 'Windforce сбрасывает врагов в бездну, Darkheart лечит, Ice Dagger замораживает. Что выбираешь ты?'),
      mk('t4', 'b', 'Баги и предложения', 'Пиши сюда, что сломалось или чего не хватает.')
    ]
  };
}

function sanitizeThreads(arr) {
  const out = [];
  if (!Array.isArray(arr)) return out;
  const now = Date.now();
  for (const t of arr.slice(0, MAX_THREADS)) {
    if (!t || !ID_RE.test(t.id) || !CATS.has(t.c) || !Array.isArray(t.posts)) continue;
    if (out.some(o => o.id === t.id)) continue;
    const posts = [];
    for (const p of t.posts.slice(0, MAX_POSTS)) {
      const x = clean(p && p.x, 400);
      if (!p || !ID_RE.test(p.id) || !x || posts.some(q => q.id === p.id)) continue;
      posts.push({ id: p.id, a: clean(p.a, 14) || 'Игрок', x, ts: Math.min(+p.ts || now, now) });
    }
    if (!posts.length) continue;
    out.push({ id: t.id, c: t.c, t: clean(t.t, 60) || 'Тема', a: clean(t.a, 14) || 'Игрок', ts: Math.min(+t.ts || now, now), posts });
  }
  return out;
}

let forum = { threads: [] };
let saveTimer = null;

function saveNow(sync) {
  const data = JSON.stringify({ threads: forum.threads });
  const tmp = FORUM_FILE + '.tmp';
  if (sync) {
    try { fs.writeFileSync(tmp, data); fs.renameSync(tmp, FORUM_FILE); } catch (e) { console.error('Не удалось сохранить форум:', e.message); }
    return;
  }
  fs.writeFile(tmp, data, err => {
    if (err) return console.error('Не удалось сохранить форум:', err.message);
    fs.rename(tmp, FORUM_FILE, e => { if (e) console.error('Не удалось сохранить форум:', e.message); });
  });
}
function markDirty() { // сохраняем не чаще, чем раз в 1,5 секунды (в Gist ещё реже)
  if (GIST_ON) { gistDirty = true; scheduleGist(); }
  if (saveTimer) return;
  saveTimer = setTimeout(() => { saveTimer = null; saveNow(false); }, 1500);
}
function loadLocal() {
  try {
    const d = JSON.parse(fs.readFileSync(FORUM_FILE, 'utf8'));
    forum = { threads: sanitizeThreads(d && d.threads) };
    trimForum();
    log('Форум загружен из файла: тем ' + forum.threads.length);
  } catch (e) {
    if (e.code !== 'ENOENT') {
      log('forum.json повреждён, переименовываю и начинаю заново');
      try { fs.renameSync(FORUM_FILE, FORUM_FILE + '.bad-' + Date.now()); } catch (_) { /* ignore */ }
    }
    forum = seedForum();
    saveNow(true);
    log('Создан новый форум');
  }
}
async function loadForum() {
  if (GIST_ON) {
    forum = { threads: [] };
    try {
      await gistLoad(false);
      log('Форум загружен из Gist: тем ' + forum.threads.length);
      saveNow(true);
      return;
    } catch (e) {
      log('Gist недоступен при запуске: ' + e.message + '. Пробую снова позже, пока работаю с локальной копией.');
      gistRetry();
    }
  }
  loadLocal();
}
const MAX_TOTAL_POSTS = 800; // чтобы файл форума оставался небольшим
function trimForum() {
  const last = t => t.posts[t.posts.length - 1].ts;
  const total = () => forum.threads.reduce((n, t) => n + t.posts.length, 0);
  if (forum.threads.length <= MAX_THREADS && total() <= MAX_TOTAL_POSTS) return;
  forum.threads.sort((a, b) => last(b) - last(a));
  while (forum.threads.length > MAX_THREADS || (total() > MAX_TOTAL_POSTS && forum.threads.length > 1)) forum.threads.pop();
}

/* ---------- клиенты и комнаты ---------- */
const clients = new Set();
const rooms = new Map(); // код комнаты -> Set клиентов
const perIp = new Map();
const MAX_CLIENTS = 400, MAX_PER_IP = 30;

function send(c, obj) {
  const ws = c.ws;
  if (ws.readyState !== 1 || ws.bufferedAmount > 1e6) return;
  try { ws.send(JSON.stringify(obj)); } catch (e) { /* ignore */ }
}
function broadcastRoom(set, except, obj) {
  if (!set) return;
  const raw = JSON.stringify(obj);
  for (const c of set) if (c !== except && c.ws.readyState === 1 && c.ws.bufferedAmount < 1e6) { try { c.ws.send(raw); } catch (e) { /* ignore */ } }
}
function broadcastAll(except, obj) {
  const raw = JSON.stringify(obj);
  for (const c of clients) if (c !== except && c.ws.readyState === 1 && c.ws.bufferedAmount < 1e6) { try { c.ws.send(raw); } catch (e) { /* ignore */ } }
}
const normRoom = r => String(r == null ? '' : r).toLowerCase().replace(/[^a-z0-9_.-]/g, '').slice(0, 48) || 'lobby';

function cleanPres(d) {
  if (!d || typeof d !== 'object' || Array.isArray(d)) return null;
  const keys = Object.keys(d);
  if (keys.length > 40) return null;
  const out = {};
  for (const k of keys) {
    if (k === '__proto__' || k === 'constructor' || k === 'prototype' || k.length > 12) continue;
    out[k] = d[k];
  }
  if (JSON.stringify(out).length > 2048) return null;
  return out;
}
function mergePres(c, d) {
  for (const k of Object.keys(d)) {
    if (!(k in c.pres) && Object.keys(c.pres).length >= 40) continue;
    c.pres[k] = d[k];
  }
}

function leaveRoom(c) {
  if (!c.room) return;
  const set = rooms.get(c.room);
  if (set) {
    set.delete(c);
    broadcastRoom(set, null, { t: 'left', peer: c.id });
    if (!set.size) rooms.delete(c.room); // пустая комната удаляется
  }
  c.room = null;
}
function joinRoom(c, code, pres) {
  leaveRoom(c);
  if (pres) mergePres(c, pres);
  c.room = code;
  let set = rooms.get(code);
  if (!set) { set = new Set(); rooms.set(code, set); }
  set.add(c);
  const peers = [];
  for (const o of set) if (o !== c) peers.push({ peer: o.id, presence: o.pres });
  send(c, { t: 'welcome', id: c.id, room: code, peers });
  if (Object.keys(c.pres).length) broadcastRoom(set, c, { t: 'pres', peer: c.id, d: c.pres });
}

/* ---------- форум: действия игроков ---------- */
function forumAction(c, m) {
  const now = Date.now();
  c.fposts = c.fposts.filter(t => now - t < 30000);
  if (c.fposts.length >= 6) return send(c, { t: 'forum', action: 'error', reason: 'slow' });
  const a = clean(m.a, 14) || 'Игрок', x = clean(m.x, 400);
  if (!ID_RE.test(m.id) || !x) return;
  if (m.action === 'post') {
    const ti = clean(m.ti, 60);
    if (!ti || !CATS.has(m.c) || forum.threads.some(t => t.id === m.id)) return;
    c.fposts.push(now);
    forum.threads.push({ id: m.id, c: m.c, t: ti, a, ts: now, posts: [{ id: m.id + 'p', a, x, ts: now }] });
    trimForum(); markDirty();
    broadcastAll(c, { t: 'forum', action: 'post', id: m.id, c: m.c, ti, a, x, ts: now });
  } else if (m.action === 'reply') {
    const th = forum.threads.find(t => t.id === m.th);
    if (!th || th.posts.length >= MAX_POSTS || th.posts.some(p => p.id === m.id)) return;
    c.fposts.push(now);
    th.posts.push({ id: m.id, a, x, ts: now });
    markDirty();
    broadcastAll(c, { t: 'forum', action: 'reply', id: m.id, th: th.id, a, x, ts: now });
  }
}
function sendForumSnapshot(c) { // все посты, кусками, чтобы сообщения не были огромными
  const all = forum.threads;
  for (let i = 0; i < all.length || i === 0; i += 40) send(c, { t: 'forum', action: 'all', threads: all.slice(i, i + 40) });
}

/* ---------- HTTP: здоровье и (по желанию) сама игра ---------- */
function hostOf(req) {
  const h = String(req.headers['x-forwarded-host'] || req.headers.host || '').split(',')[0].trim();
  return /^[A-Za-z0-9.\-:]+$/.test(h) ? h : '';
}
function protoOf(req) {
  const p = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim();
  return p || (req.socket.encrypted ? 'https' : 'http');
}
const server = http.createServer((req, res) => {
  const url = (req.url || '/').split('?')[0];
  if (url === '/health') {
    res.writeHead(200, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
    return res.end(JSON.stringify({ ok: true, players: clients.size, rooms: rooms.size, threads: forum.threads.length, gist: GIST_ON ? (gistReady ? 'ok' : 'wait') : 'off' }));
  }
  if ((url === '/' || url === '/index.html' || url === '/Haunted_Mansion.html') && fs.existsSync(GAME_FILE)) {
    fs.readFile(GAME_FILE, 'utf8', (err, html) => {
      if (err) { res.writeHead(500); return res.end('error'); }
      const host = hostOf(req);
      if (host) { // подставляем адрес этого сервера, если игрок ещё не задал свой
        const ws = (protoOf(req) === 'https' ? 'wss://' : 'ws://') + host;
        const inj = '<script>try{if(!localStorage.getItem("hm_server"))localStorage.setItem("hm_server","' + ws + '")}catch(e){}</script>';
        html = html.includes('</head>') ? html.replace('</head>', inj + '</head>') : inj + html;
      }
      res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'no-cache' });
      res.end(html);
    });
    return;
  }
  res.writeHead(200, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Haunted Mansion server is running. Откройте игру и введите адрес этого сервера в меню: вкладка «Комната».');
});

/* ---------- WebSocket ---------- */
const wss = new WebSocketServer({ server, maxPayload: 16 * 1024, perMessageDeflate: false });

wss.on('connection', (ws, req) => {
  const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '?').split(',')[0].trim();
  if (clients.size >= MAX_CLIENTS || (perIp.get(ip) || 0) >= MAX_PER_IP) { ws.close(1013, 'busy'); return; }
  perIp.set(ip, (perIp.get(ip) || 0) + 1);
  const c = { ws, id: crypto.randomBytes(5).toString('hex'), room: null, pres: {}, alive: true, tokens: 120, last: Date.now(), fposts: [], ip };
  clients.add(c);

  ws.on('pong', () => { c.alive = true; });
  ws.on('error', () => { /* ошибки сокета не должны ронять сервер */ });
  ws.on('close', () => {
    leaveRoom(c);
    clients.delete(c);
    const n = (perIp.get(ip) || 1) - 1;
    if (n <= 0) perIp.delete(ip); else perIp.set(ip, n);
  });
  ws.on('message', (raw, isBinary) => {
    if (isBinary) return;
    const now = Date.now();
    c.tokens = Math.min(120, c.tokens + (now - c.last) * 0.08); // примерно 80 сообщений в секунду
    c.last = now;
    if (c.tokens < 1) return;
    c.tokens--;
    let m;
    try { m = JSON.parse(raw.toString()); } catch (e) { return; }
    if (!m || typeof m !== 'object') return;
    if (m.t === 'join') {
      joinRoom(c, normRoom(m.room), cleanPres(m.pres));
    } else if (m.t === 'pres') {
      const d = cleanPres(m.d);
      if (!d || !c.room) return;
      mergePres(c, d);
      broadcastRoom(rooms.get(c.room), c, { t: 'pres', peer: c.id, d });
    } else if (m.t === 'forum') {
      forumAction(c, m);
    } else if (m.t === 'ping') {
      send(c, { t: 'pong' });
    }
  });

  sendForumSnapshot(c); // сразу отдаём все посты форума
});

/* убираем «мёртвые» соединения */
setInterval(() => {
  for (const c of clients) {
    if (!c.alive) { c.ws.terminate(); continue; }
    c.alive = false;
    try { c.ws.ping(); } catch (e) { /* ignore */ }
  }
}, 30000).unref();

/* ---------- запуск и аккуратная остановка ---------- */
loadForum().then(() => {
  server.listen(PORT, '0.0.0.0', () => log('Сервер слушает порт ' + PORT + ', файл форума: ' + FORUM_FILE));
});

let stopping = false;
async function shutdown() {
  if (stopping) return;
  stopping = true;
  if (saveTimer) clearTimeout(saveTimer);
  saveNow(true);
  if (GIST_ON && gistReady) { // успеваем отправить последние посты в Gist
    if (gistTimer) { clearTimeout(gistTimer); gistTimer = null; }
    const until = Date.now() + 6000;
    while (gistBusy && Date.now() < until) await sleep(100);
    try { await Promise.race([flushGist(), sleep(Math.max(500, until - Date.now()))]); } catch (e) { /* ignore */ }
  }
  process.exit(0);
}
process.on('SIGTERM', shutdown);
process.on('SIGINT', shutdown);
