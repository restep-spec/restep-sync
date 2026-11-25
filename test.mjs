// Tests for restep-sync. From restep/:  node tools/restep-sync/test.mjs
// Starts a copy of the reference server with sync (ref-sync-server.mjs) on port 8860 and a
// proxy on 8861 that the client goes through: the proxy can cut a response (to test retry without
// duplicating) or answer itself (to test Retry-After). For "offline" the real server is stopped
// and started again (the epoch changes, so its earlier state gives 410).
import { spawn } from 'node:child_process';
import { createServer, request } from 'node:http';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildSyncServer } from './ref-sync-server.mjs';
import { openRestepSync, fileStorage, memoryStorage } from './restep-sync.mjs';

const SERVER_PORT = 8860, PROXY_PORT = 8861, OTHER_PORT = 8862; // OTHER: another origin, for §4.3
const SERVER = `http://localhost:${SERVER_PORT}`, API = `http://localhost:${PROXY_PORT}/`;

// ── Utilidades ──
let passed = 0, failed = 0;
const results = [];
async function check(name, fn) {
  try { await fn(); passed++; results.push(`ok    ${name}`); }
  catch (e) { failed++; results.push(`FAIL  ${name}\n        ${e.stack?.split('\n').slice(0, 3).join('\n        ')}`); }
  console.log(results[results.length - 1]);
}
function assert(cond, msg) { if (!cond) throw new Error(msg); }
function eq(a, b, msg) { if (JSON.stringify(a) !== JSON.stringify(b)) throw new Error(`${msg}: ${JSON.stringify(a)} ≠ ${JSON.stringify(b)}`); }
const wait = (ms) => new Promise((r) => setTimeout(r, ms));

async function api(method, path, body, headers = {}) {
  const res = await fetch(SERVER + path, { method, headers: { 'content-type': 'application/json', ...headers },
    body: body === undefined ? undefined : JSON.stringify(body) });
  const text = await res.text();
  return { status: res.status, headers: res.headers, body: text ? JSON.parse(text) : undefined };
}
async function serverItems(coll) {
  const items = [];
  for (let next = `/${coll}?limit=50`; next;) { const r = await api('GET', next); items.push(...r.body.items); next = r.body.next; }
  return items.map(({ _etag, ...it }) => it).sort((a, b) => a.id.localeCompare(b.id));
}
const localItems = (db, coll) => db.list(coll).sort((a, b) => a.id.localeCompare(b.id));
async function sameAsServer(db, coll) { eq(localItems(db, coll), await serverItems(coll), `${coll}: local copy differs from the server's`); }

// ── Draft 7, against an API simulated with the fetch option (no server needed) ──
// fake(discovery, handler) records every request and answers with handler(method, url, headers) or a
// small collection of books with state, :changes and filter[id][in].
function fake(discovery, { books = [{ id: '1', title: 'Uno', _etag: '"a1"' }, { id: '2', title: 'Dos', _etag: '"a2"' }], handler, changes } = {}) {
  const seen = [];
  const fetchFn = async (url, { method = 'GET', headers = {} } = {}) => {
    const u = new URL(url);
    seen.push({ method, path: u.pathname, query: Object.fromEntries(u.searchParams), headers });
    const out = handler?.(method, u, headers);
    const json = (status, body, h = {}) => new Response(body === undefined ? null : JSON.stringify(body), { status, headers: { 'content-type': 'application/json', ...h } });
    if (out) return json(...out);
    if (u.pathname === '/') return json(200, discovery);
    if (u.pathname === '/books:changes') return json(200, changes ?? { since: u.searchParams.get('since'), state: 's2', has_more: false, created: [], updated: [], destroyed: [] });
    if (u.pathname === '/books') {
      const ids = u.searchParams.get('filter[id][in]')?.split(',');
      return json(200, { items: books.filter((b) => !ids || ids.includes(b.id)), next: null, state: 's1' });
    }
    return json(404, { type: 'about:blank', title: 'Not Found', status: 404, code: 'not_found' });
  };
  return { fetchFn, seen };
}
const D7 = (books = {}, root = {}) => ({ restep: '1.0-draft.7', profiles: ['core', 'query', 'writes', 'sync'], idempotency: { ttl: 86400 }, ...root,
  collections: { books: { path: '/books', filter: { id: { type: 'string', ops: ['eq', 'in'] } }, sync: {}, ...books } } });
const openFake = (f, extra = {}) => openRestepSync({ url: 'http://api.test/', collections: ['books'], interval: 0, autoPush: false, fetch: f.fetchFn, ...extra });
async function rejects(promise, text) {
  let msg = '';
  try { await promise; } catch (e) { msg = e.message; }
  assert(msg.includes(text), `expected an error with «${text}», got «${msg}»`);
}

console.log('draft 7, simulated API');
await check('draft 7: sync is used with "1.0-draft.7"', async () => {
  const f = fake(D7());
  const db = await openFake(f);
  eq(db.mode('books'), 'sync', 'mode');
  await db.sync();
  await db.sync();
  assert(f.seen.some((s) => s.path === '/books:changes'), 'it never called :changes');
  await db.close();
});
await check('draft 7: the traversal and the reload by id turn off every default filter (§5.2, §10)', async () => {
  const named = { active: { description: 'Not archived.', default: true }, featured: { description: 'Featured.' }, search: { type: 'string', description: 'Text.', default: true } };
  const f = fake(D7({ named_filters: named }), { changes: { since: 's1', state: 's2', has_more: false, created: ['2'], updated: [], destroyed: [] } });
  const db = await openFake(f);
  await db.sync(); // traversal
  await db.sync(); // :changes, then reload of 2 by id
  const list = f.seen.filter((s) => s.path === '/books');
  eq(list.length, 2, 'requests to /books');
  eq(list[0].query, { 'filter[active]': 'false' }, 'traversal');
  eq(list[1].query, { 'filter[id][in]': '2', 'filter[active]': 'false' }, 'reload by id');
  await db.close();
});
await check('draft 7: no copy of a collection without GET or with a required filter (§4.3, §6.1)', async () => {
  await rejects(openFake(fake(D7({ methods: ['POST'] }))), 'does not support GET');
  await rejects(openFake(fake(D7({ filter: { id: { type: 'string', ops: ['eq', 'in'] }, isbn: { type: 'string', ops: ['eq'], required: true } } }))), 'requires the filter isbn');
  await rejects(openFake(fake(D7({ named_filters: { shelf: { type: 'string', description: 'x', required: true } } }))), 'requires the filter shelf');
});
await check('draft 7: the copy is in one language; changing it traverses again (§10)', async () => {
  const f = fake(D7());
  const storage = memoryStorage();
  let db = await openFake(f, { language: 'es-es', storage });
  await db.sync();
  assert(f.seen.filter((s) => s.path === '/books').every((s) => s.headers['accept-language'] === 'es-es'), 'Accept-Language not sent');
  await db.sync();
  eq(f.seen.at(-1).path, '/books:changes', 'second sync');
  await db.setLanguage('en');
  await db.sync();
  eq([f.seen.at(-1).path, f.seen.at(-1).headers['accept-language']], ['/books', 'en'], 'after setLanguage');
  await db.close();
  // Opening a stored copy with another language also traverses again.
  db = await openFake(f, { language: 'fr', storage });
  await db.sync();
  eq(f.seen.at(-1).path, '/books', 'reopened in another language');
  await db.close();
});
await check('draft 7: a localized field is not refused locally; the server decides (§3, §5.1)', async () => {
  const f = fake(D7({ localized: { title: { source: 'title_i18n' } } }));
  const db = await openFake(f);
  await db.sync();
  await db.update('books', '1', { title: 'Uno' });
  await db.sync({ pull: false });
  assert(f.seen.some((s) => s.method === 'PATCH' && s.path === '/books/1'), 'the PATCH with the localized field was not sent');
  await db.close();
});
await check('draft 7: core only after a 2xx without restep or a 404, never after another error (§4.2)', async () => {
  await rejects(openFake(fake(null, { handler: (m, u) => u.pathname === '/' && [400, { status: 400, code: 'bad_request' }] })), 'answered 400');
  await rejects(openFake(fake(null, { handler: (m, u) => u.pathname === '/' && [405, { status: 405, code: 'method_not_allowed' }] })), 'answered 405');
  const db = await openFake(fake(null, { handler: (m, u) => u.pathname === '/' && [404, { status: 404, code: 'not_found' }] }));
  eq(db.mode('books'), 'reload', 'core only');
  await db.close();
});
await check('draft 7: no automatic repeat with the key after idempotency.ttl (§5.7)', async () => {
  let posts = 0;
  const f = fake(D7({}, { idempotency: { ttl: 3600 } }), { handler: (m) => m === 'POST' && (posts++, [500, { status: 500, code: 'internal' }]) });
  const db = await openFake(f);
  await db.sync();
  await db.create('books', { title: 'Tres' });
  await db.sync({ pull: false });
  eq(posts, 1, 'first attempt');
  await db.sync({ pull: false });
  eq(posts, 2, 'repeated within the ttl, with the key');
  db.data.queue[0].firstAt = Date.now() - 3601 * 1000;
  const report = await db.sync({ pull: false });
  eq(posts, 2, 'not repeated past the ttl');
  assert(report.uncertain && db.pending()[0].uncertain, 'the create should be uncertain');
  const keys = f.seen.filter((s) => s.method === 'POST').map((s) => s.headers['idempotency-key']);
  assert(keys[0] && keys[0] === keys[1], 'the same key within the ttl');
  await db.sync({ pull: false, retryUncertain: true });
  eq(posts, 3, 'sent again when the application decides');
  const last = f.seen.filter((s) => s.method === 'POST').at(-1).headers['idempotency-key'];
  assert(last !== keys[0], 'a new key after the ttl');
  await db.close();
});
await check('draft 7: with reusable_ids, an id in created is a new item and keeps no old ETag (§5.1, §10)', async () => {
  const books = [{ id: '1', title: 'Uno', _etag: '"a1"' }];
  const f = fake(D7({ id_from: 'client', reusable_ids: true }), { books, changes: { since: 's1', state: 's2', has_more: false, created: ['1'], updated: [], destroyed: [] } });
  const db = await openFake(f);
  await db.sync();
  books[0] = { id: '1', title: 'Uno' }; // the same data, another incarnation, and no _etag in the list
  const changes = [];
  db.on('change', (c) => changes.push(c));
  await db.sync();
  eq(changes.at(-1)?.created, ['1'], 'reported as created');
  eq(db.data.collections.books.items['1'].etag, null, 'the old ETag is not kept');
  await db.close();
});
await check('draft 7: an id of created or updated that the reload does not return leaves the copy; it goes on from the state (§10)', async () => {
  let hide = false;
  const f = fake(D7(), {
    changes: { since: 's1', state: 's2', has_more: false, created: ['3'], updated: ['2'], destroyed: [] },
    handler: (m, u) => (hide && u.pathname === '/books' && u.searchParams.has('filter[id][in]') ? [200, { items: [], next: null, state: 's2' }] : null),
  });
  const db = await openFake(f);
  await db.sync();
  eq(db.list('books').map((b) => b.id).sort(), ['1', '2'], 'initial copy');
  hide = true;
  const changes = [];
  db.on('change', (c) => changes.push(c));
  await db.sync();
  eq(db.list('books').map((b) => b.id), ['1'], 'the id that did not come back is removed; the unknown one is not added');
  eq(changes.at(-1)?.destroyed, ['2'], 'reported as destroyed');
  eq(db.data.collections.books.state, 's2', 'the state of :changes is kept');
  await db.sync();
  eq(f.seen.filter((s) => s.path === '/books:changes').at(-1).query.since, 's2', 'the next :changes starts from that state');
  await db.close();
});
await check('draft 7: a replayed create without ETag, _etag or Content-Location is not taken as the copy; the item is read (§7.1)', async () => {
  const books = [{ id: '1', title: 'Uno', _etag: '"a1"' }];
  const f = fake(D7(), {
    books,
    handler: (m, u) => {
      if (m === 'POST') return [201, { id: '9', title: 'Stored' }, { location: '/books/9', 'idempotent-replayed': 'true' }];
      if (m === 'GET' && u.pathname === '/books/9') return [200, { id: '9', title: 'Current', _etag: '"c9"' }, { etag: '"c9"' }];
      return null;
    },
  });
  const db = await openFake(f);
  await db.sync();
  await db.create('books', { title: 'Stored' });
  await db.sync({ pull: false });
  eq(db.get('books', '9')?.title, 'Current', 'the copy is the item read, not the replayed body');
  eq(db.data.collections.books.items['9'].etag, '"c9"', 'its ETag');
  assert(f.seen.some((s) => s.method === 'GET' && s.path === '/books/9'), 'the item was read');
  await db.close();
});
await check('council round 3: a replayed 201 without a body carries no ETag; the item is read at Location (§7.3)', async () => {
  const f = fake(D7(), {
    handler: (m, u) => {
      if (m === 'POST') return [201, undefined, { location: '/books/9', 'idempotent-replayed': 'true' }];
      if (m === 'GET' && u.pathname === '/books/9') return [200, { id: '9', title: 'Current', _etag: '"c9"' }, { etag: '"c9"' }];
      return null;
    },
  });
  const db = await openFake(f);
  await db.sync();
  await db.create('books', { title: 'Stored' });
  await db.sync({ pull: false });
  eq(db.get('books', '9')?.title, 'Current', 'the copy is the item read at Location');
  eq(db.data.collections.books.items['9'].etag, '"c9"', 'with the ETag of that read');
  assert(f.seen.some((s) => s.method === 'GET' && s.path === '/books/9'), 'the item was read');
  await db.close();
});
await check('draft 7: a replayed create that carries its ETag keeps the body as the copy (§7.1)', async () => {
  const f = fake(D7(), {
    handler: (m) => (m === 'POST' ? [201, { id: '9', title: 'Current', _etag: '"c9"' }, { location: '/books/9', etag: '"c9"', 'idempotent-replayed': 'true' }] : null),
  });
  const db = await openFake(f);
  await db.sync();
  await db.create('books', { title: 'Current' });
  await db.sync({ pull: false });
  eq(db.get('books', '9')?.title, 'Current', 'the body is the copy');
  assert(!f.seen.some((s) => s.method === 'GET' && s.path === '/books/9'), 'no extra read');
  await db.close();
});

// ── Servidor y proxy ──
const serverFile = await buildSyncServer();
let server;
async function startServer() {
  server = spawn(process.execPath, [serverFile], { env: { ...process.env, PORT: String(SERVER_PORT), SYNC_PAGE: '2' }, stdio: ['ignore', 'pipe', 'inherit'] });
  await new Promise((resolve, reject) => {
    server.stdout.on('data', (d) => { if (String(d).includes('http://')) resolve(); });
    server.on('exit', (c) => reject(new Error(`the server exited (${c})`)));
  });
}
async function stopServer() {
  if (!server || server.exitCode !== null) return;
  const done = new Promise((r) => server.once('exit', r));
  server.kill();
  await done;
}

// rewrite(req, status, body) may return another JSON body (to simulate other discovery documents).
const proxy = { requests: [], dropNext: null, injectNext: null, rewrite: null, headers: [] };
const proxyServer = createServer((req, res) => {
  proxy.requests.push(`${req.method} ${req.url}`);
  proxy.headers.push({ line: `${req.method} ${req.url}`, headers: req.headers });
  if (proxy.injectNext) {
    const { status, headers, body } = proxy.injectNext; proxy.injectNext = null;
    res.writeHead(status, { 'content-type': 'application/problem+json', ...headers });
    return res.end(JSON.stringify(body));
  }
  const drop = proxy.dropNext?.(req); if (drop) proxy.dropNext = null;
  const up = request({ host: 'localhost', port: SERVER_PORT, path: req.url, method: req.method, headers: req.headers }, (ur) => {
    const chunks = [];
    ur.on('data', (c) => chunks.push(c));
    ur.on('end', () => {
      if (req.method === 'POST') proxy.lastPost = { status: ur.statusCode, headers: ur.headers };
      if (drop) return req.socket.destroy(); // the server did the work, but the response is lost
      let body = Buffer.concat(chunks);
      if (proxy.rewrite && body.length && /json/.test(ur.headers['content-type'] ?? '')) {
        const out = proxy.rewrite(req, ur.statusCode, JSON.parse(body));
        if (out !== undefined) {
          body = Buffer.from(JSON.stringify(out));
          const { 'content-length': _, etag: __, ...rest } = ur.headers;
          return res.writeHead(ur.statusCode, rest), res.end(body);
        }
      }
      res.writeHead(ur.statusCode, ur.headers); res.end(body);
    });
  });
  up.on('error', () => req.socket.destroy()); // server stopped: for the client, offline
  req.pipe(up);
});
await new Promise((r) => proxyServer.listen(PROXY_PORT, r));
// Another origin: records the headers it receives and answers an empty page.
const other = { seen: [] };
const otherServer = createServer((req, res) => {
  other.seen.push(req.headers);
  res.writeHead(200, { 'content-type': 'application/json' });
  res.end(JSON.stringify({ items: [], next: null, state: 'x' }));
});
await new Promise((r) => otherServer.listen(OTHER_PORT, r));
// A modified discovery document: f receives a copy and changes it.
const onRoot = (f) => (req, status, body) => {
  if (req.method !== 'GET' || req.url !== '/' || status !== 200) return undefined;
  const doc = structuredClone(body);
  return f(doc) ?? doc;
};

const dir = await mkdtemp(join(tmpdir(), 'restep-sync-test-'));
const file = join(dir, 'replica.json');
let db;

try {
  await startServer();
  const events = { change: [], conflict: [], error: [] };
  const open = async (opts = {}) => {
    const d = await openRestepSync({ url: API, collections: ['books', 'categories'], storage: fileStorage(file),
      interval: 0, autoPush: false, retry: { base: 50, max: 200 }, ...opts });
    for (const k of Object.keys(events)) d.on(k, (e) => events[k].push(e));
    return d;
  };
  db = await open();

  await check('initial sync: the local copy matches the server', async () => {
    const r = await db.sync();
    assert(r.online && r.errors.length === 0, `report: ${JSON.stringify(r)}`);
    eq(db.list('books').length, 7, 'number of books');
    await sameAsServer(db, 'books');
    await sameAsServer(db, 'categories');
    const saved = JSON.parse(await readFile(file, 'utf8'));
    assert(/^[0-9a-f]{8}\.\d+$/.test(saved.collections.books.state), 'the state is saved in the file');
  });

  await check('remote changes: created, updated and deleted, with has_more (SYNC_PAGE=2)', async () => {
    const created = await api('POST', '/books', { title: 'Remoto', price: '9.99' });
    await api('PATCH', '/books/7', { price: '17.00' });
    await api('PATCH', '/books/21', { rating: 4.9 });
    await api('DELETE', '/books/15');
    events.change.length = 0;
    proxy.requests.length = 0;
    const r = await db.sync();
    eq(r.pulled.books, { created: 1, updated: 2, destroyed: 1 }, 'change counts');
    assert(proxy.requests.filter((x) => x.includes('books:changes')).length >= 2, 'followed has_more');
    assert(proxy.requests.every((x) => !x.startsWith('GET /books?limit')), 'did not reload the whole collection');
    eq(db.get('books', created.body.id)?.title, 'Remoto', 'the created item is local');
    eq(db.get('books', '15'), null, 'the deleted item is gone');
    eq(db.get('books', '7').price, '17.00', 'the updated item is current');
    assert(events.change.some((e) => e.source === 'remote' && e.destroyed.includes('15')), 'change event for the deletion');
    await sameAsServer(db, 'books');
  });

  await check('change in another collection through inheritance (11.6): deleting a category changes its children', async () => {
    await api('DELETE', '/categories/2'); // 4 inherits from 2: it loses the link and its tax_rate changes
    const r = await db.sync();
    eq(r.pulled.categories, { created: 0, updated: 1, destroyed: 1 }, 'categories changes');
    await sameAsServer(db, 'categories');
  });

  await check('optimistic writes: create, update and delete are visible locally before being sent', async () => {
    const item = await db.create('books', { title: 'Optimista', price: '5.00' });
    assert(item.id.startsWith('local-'), 'provisional local id');
    await db.update('books', item.id, { price: '6.00' });
    await db.update('books', '12', { price: '15.50' });
    await db.update('books', '12', { year: 2024 }); // two changes in a row on the same item
    eq(db.get('books', item.id).price, '6.00', 'update on a pending create');
    eq(db.pending().length, 4, 'four pending operations');
    const r = await db.sync();
    eq([r.pushed, r.pending, r.conflicts], [4, 0, 0], 'pushed, pending and conflicts');
    const real = db.resolveId('books', item.id);
    assert(!real.startsWith('local-'), 'the provisional id points to the final one');
    eq(db.get('books', item.id)?.id, real, 'get with the provisional id still works');
    eq((await api('GET', `/books/${real}`)).body.price, '6.00', 'the server has the update of the created item');
    const twelve = (await api('GET', '/books/12')).body;
    eq([twelve.price, twelve.year], ['15.50', 2024], 'the two consecutive updates arrive without conflict');
    await sameAsServer(db, 'books');
  });

  await check('412 conflict, server wins: the local change is undone', async () => {
    await db.update('books', '12', { price: '1.00' }, { onConflict: 'server' });
    const before = await api('GET', '/books/12');
    await api('PATCH', '/books/12', { title: 'El último ferry (2.ª ed.)' }, { 'if-match': before.headers.get('etag') });
    events.conflict.length = 0;
    proxy.requests.length = 0;
    const r = await db.sync();
    eq(r.conflicts, 1, 'one conflict');
    assert(proxy.requests.includes('PATCH /books/12'), 'the PATCH reached the server with If-Match');
    eq(events.conflict[0]?.resolution, 'server', 'resolution');
    const now = (await api('GET', '/books/12')).body;
    eq([now.title, now.price], ['El último ferry (2.ª ed.)', '15.50'], 'the server has not changed');
    eq(db.get('books', '12').price, '15.50', 'the local copy reverts to the server version');
    await sameAsServer(db, 'books');
  });

  await check('412 conflict, client wins: resent with the new ETag', async () => {
    await db.update('books', '12', { price: '2.00' }, { onConflict: 'client' });
    await api('PATCH', '/books/12', { rating: 3.3 });
    events.conflict.length = 0;
    const r = await db.sync();
    eq([r.conflicts, r.pushed, r.pending], [1, 1, 0], 'conflicts, pushed, pending');
    const now = (await api('GET', '/books/12')).body;
    eq([now.price, now.rating], ['2.00', 3.3], 'the local price wins and the remote rating is kept');
    await sameAsServer(db, 'books');
  });

  await check('conflict with a user function: custom merge', async () => {
    await db.update('books', '30', { title: 'Cuaderno de faros (local)' }, {
      onConflict: ({ server, local, base }) => {
        assert(server.price === '99.00' && base.price === '12.00' && local.title === 'Cuaderno de faros (local)', 'function arguments');
        return { title: `${local.title} + remoto` };
      },
    });
    await api('PATCH', '/books/30', { price: '99.00' }); // the listing's _etag is no longer valid: 412 and it is re-read
    const r = await db.sync();
    eq(r.conflicts, 1, 'one conflict');
    const now = (await api('GET', '/books/30')).body;
    eq([now.title, now.price], ['Cuaderno de faros (local) + remoto', '99.00'], 'merge applied');
    await sameAsServer(db, 'books');
  });

  await check('_etag from the listing (7.2): writes with If-Match without reading the item first', async () => {
    const listed = (await api('GET', '/books?filter[id][in]=21')).body.items[0];
    assert(typeof listed?._etag === 'string', 'the server puts _etag in the listing');
    await db.sync();
    await db.update('books', '21', { rating: 4.2 });
    proxy.requests.length = 0;
    const r = await db.sync();
    eq([r.pushed, r.conflicts], [1, 0], 'pushed without conflict');
    const i = proxy.requests.indexOf('PATCH /books/21');
    assert(i >= 0 && !proxy.requests.slice(0, i).includes('GET /books/21'), `does not reread first: ${proxy.requests.join(', ')}`);
    assert(!('_etag' in db.get('books', '21')), '_etag is not part of the local data');
    await sameAsServer(db, 'books');
  });

  await check('delete against a changed item: server wins and the item comes back', async () => {
    await db.delete('books', '23');
    eq(db.get('books', '23'), null, 'deleted locally');
    await api('PATCH', '/books/23', { price: '22.00' });
    await db.sync();
    eq(db.get('books', '23')?.price, '22.00', 'it is back, with the server version');
    await sameAsServer(db, 'books');
  });

  await check('retry without duplicating: the POST response is lost and it is repeated with the same Idempotency-Key', async () => {
    const item = await db.create('books', { title: 'Respuesta perdida', price: '3.00' });
    proxy.dropNext = (req) => req.method === 'POST';
    const r1 = await db.sync();
    eq([r1.online, r1.pending], [false, 1], 'first attempt: no response, still pending');
    eq(proxy.lastPost.status, 201, 'the server did create it');
    eq(db.pending()[0].attempts, 1, 'one attempt counted');
    const r2 = await db.sync();
    eq([r2.pushed, r2.pending], [1, 0], 'second attempt');
    eq(proxy.lastPost.headers['idempotent-replayed'], 'true', 'the server replays the stored response');
    const all = await serverItems('books');
    eq(all.filter((b) => b.title === 'Respuesta perdida').length, 1, 'a single book on the server');
    eq(db.list('books', { filter: { title: 'Respuesta perdida' } }).length, 1, 'a single book locally');
    assert(!db.resolveId('books', item.id).startsWith('local-'), 'final id');
    await sameAsServer(db, 'books');
  });

  await check('Retry-After: a 503 from the server postpones the retry even if sync is requested', async () => {
    await db.create('books', { title: 'Paciencia', price: '4.00' });
    proxy.injectNext = { status: 503, headers: { 'retry-after': '1' }, body: { status: 503, code: 'unavailable', title: 'No disponible' } };
    const r1 = await db.sync();
    eq(r1.pending, 1, 'still pending');
    assert(r1.waitingUntil > Date.now() + 500, 'waits as long as Retry-After says');
    proxy.requests.length = 0;
    const r2 = await db.sync();
    eq([r2.pending, proxy.requests.filter((x) => x.startsWith('POST')).length], [1, 0], 'does not retry too early');
    await wait(1100);
    const r3 = await db.sync();
    eq([r3.pushed, r3.pending], [1, 0], 'once the time has passed, it is sent');
    eq((await serverItems('books')).filter((b) => b.title === 'Paciencia').length, 1, 'only one');
  });

  await check('409 request_in_progress: retried after Retry-After', async () => {
    await db.create('books', { title: 'En curso', price: '4.50' });
    proxy.injectNext = { status: 409, headers: { 'retry-after': '1' }, body: { status: 409, code: 'request_in_progress', title: 'Conflicto' } };
    const r1 = await db.sync();
    eq([r1.pending, r1.dropped], [1, 0], 'still pending');
    assert(r1.waitingUntil > Date.now() + 500, 'waits as long as Retry-After says');
    await wait(1100);
    const r2 = await db.sync();
    eq([r2.pushed, r2.pending], [1, 0], 'once the time has passed, it is sent');
    eq((await serverItems('books')).filter((b) => b.title === 'En curso').length, 1, 'only one');
  });

  await check('409 conflict: permanent, removed from the queue and undone', async () => {
    const item = await db.create('books', { title: 'Duplicado', price: '4.50' });
    events.error.length = 0;
    proxy.injectNext = { status: 409, headers: {}, body: { status: 409, code: 'conflict', title: 'Conflicto' } };
    const r = await db.sync();
    eq([r.dropped, r.pending], [1, 0], 'dropped without retrying');
    eq(events.error[0]?.error.problem.code, 'conflict', 'error event with the problem+json');
    eq(db.get('books', item.id), null, 'the optimistic item disappears');
  });

  await check('permanent error (422): removed from the queue, undone and reported', async () => {
    const item = await db.create('books', { title: 'Sin precio' });
    events.error.length = 0;
    const r = await db.sync();
    eq([r.dropped, r.pending], [1, 0], 'dropped');
    eq(events.error[0]?.error.status, 422, 'error event with the status');
    eq(events.error[0]?.error.problem.code, 'invalid', 'and the problem+json');
    eq(db.get('books', item.id), null, 'the optimistic item disappears');
  });

  let offlineTitle;
  await check('offline: queued writes survive closing and reopening from the file', async () => {
    await stopServer();
    offlineTitle = `Written offline ${Date.now()}`;
    const item = await db.create('books', { title: offlineTitle, price: '8.00' });
    await db.update('categories', '1', { name: 'Papelería y regalos' }); // untouched since the seed
    await db.delete('books', '50');
    const r = await db.sync();
    eq([r.online, r.pending], [false, 3], 'offline, three pending');
    await db.close();
    db = await open(); // no server: uses the saved discovery document
    eq(db.pending().length, 3, 'the queue is still there');
    eq(db.get('books', item.id)?.title, offlineTitle, 'the created item is still visible');
    eq(db.get('categories', '1').name, 'Papelería y regalos', 'the update is still visible');
    eq(db.get('books', '50'), null, 'the deleted item is still hidden');
  });

  await check('server back: the queue is sent, the old state gives 410 and it reloads', async () => {
    await startServer(); // seed data and a new epoch
    events.change.length = 0;
    const r = await db.sync();
    eq([r.online, r.pushed, r.pending, r.conflicts], [true, 3, 0, 0], 'pushed without conflict');
    eq(r.reset.sort(), ['books', 'categories'], 'reload on state_expired');
    assert(events.change.some((e) => e.reload), 'reload change event');
    const all = await serverItems('books');
    eq(all.filter((b) => b.title === offlineTitle).length, 1, 'the item created offline is there once');
    eq([(await api('GET', '/categories/1')).body.name, all.find((b) => b.id === '50')], ['Papelería y regalos', undefined], 'update and delete applied');
    await sameAsServer(db, 'books');
    await sameAsServer(db, 'categories');
  });

  await check('background: the interval brings a remote change and emits change', async () => {
    const bg = await openRestepSync({ url: API, collections: ['books'], storage: memoryStorage(), interval: 150 });
    try {
      await bg.sync();
      const got = new Promise((resolve) => bg.on('change', (e) => { if (e.source === 'remote' && e.updated.includes('21')) resolve(e); }));
      await api('PATCH', '/books/21', { price: '20.50' });
      await Promise.race([got, wait(3000).then(() => { throw new Error('no change arrived within 3 s'); })]);
      eq(bg.get('books', '21').price, '20.50', 'updated without calling sync');
      const created = await bg.create('books', { title: 'Automático', price: '1.50' }); // autoPush
      await wait(400);
      eq(bg.pending().length, 0, 'autoPush emptied the queue');
      assert(!bg.resolveId('books', created.id).startsWith('local-'), 'with a final id');
    } finally { await bg.close(); }
  });

  await check('collection without sync (§10): reloaded whole and never asks for :changes', async () => {
    eq((await api('GET', '/products:changes?since=x')).status, 404, ':changes of a collection without sync');
    const p = await openRestepSync({ url: API, collections: ['products', 'books'], interval: 0, autoPush: false });
    try {
      eq([p.mode('products'), p.mode('books')], ['reload', 'sync'], 'modes');
      proxy.requests.length = 0;
      await p.sync();
      await api('PATCH', '/products/2', { price: '7.00' });
      const r = await p.sync();
      eq(r.pulled.products.updated, 1, 'the change arrives by reloading');
      assert(!proxy.requests.some((x) => x.includes('products:changes')), `does not ask for :changes: ${proxy.requests.join(', ')}`);
      eq(p.get('products', '2').price, '7.00', 'price up to date');
    } finally { await p.close(); }
  });

  await check('unknown sync text (§1.2): with "1.0-draft.5" or "1.1" it does not use :changes', async () => {
    for (const version of ['1.0-draft.5', '1.1']) {
      proxy.rewrite = onRoot((d) => { d.restep = version; });
      const p = await openRestepSync({ url: API, collections: ['books'], interval: 0, autoPush: false });
      proxy.rewrite = null;
      try {
        eq(p.mode('books'), 'reload', `mode with ${version}`);
        proxy.requests.length = 0;
        await p.sync(); await p.sync();
        assert(!proxy.requests.some((x) => x.includes(':changes')), `${version}: does not ask for :changes`);
        await sameAsServer(p, 'books');
      } finally { await p.close(); }
    }
  });

  await check('another major version or malformed restep: not Restep 1, does not open (§1.1)', async () => {
    for (const version of ['2.0', '1.01', '1.0-beta.1']) {
      proxy.rewrite = onRoot((d) => { d.restep = version; });
      let msg = '';
      try { await openRestepSync({ url: API, collections: ['books'], interval: 0 }); } catch (e) { msg = e.message; }
      proxy.rewrite = null;
      assert(msg.includes('not Restep 1'), `${version}: ${msg}`);
    }
  });

  await check('no discovery (§4.2): core only, the collection at /<name>, reloading', async () => {
    proxy.rewrite = onRoot(() => ({ hello: 'I am not a discovery document' }));
    const p = await openRestepSync({ url: API, collections: ['books'], interval: 0, autoPush: false });
    try {
      eq(p.mode('books'), 'reload', 'mode');
      await p.sync();
      await sameAsServer(p, 'books');
      // Without writes announced there is no Idempotency-Key (§7.1).
      proxy.headers.length = 0;
      await p.create('books', { title: 'Solo core', price: '1.00' });
      await p.sync();
      const post = proxy.headers.find((x) => x.line === 'POST /books');
      assert(post && post.headers['idempotency-key'] === undefined, 'no Idempotency-Key');
      eq(post.headers['content-type'], 'application/json', 'Content-Type of the POST (§3)');
    } finally { proxy.rewrite = null; await p.close(); }
  });

  await check('a real subcollection of the reference (reviews, no sync): clear error on open, and its :changes gives 404', async () => {
    eq((await api('GET', '/books/7/reviews:changes?since=x')).status, 404, ':changes of a subcollection');
    let msg = '';
    try { await openRestepSync({ url: API, collections: ['reviews'], interval: 0 }); } catch (e) { msg = e.message; }
    assert(msg.includes('subcollection') && msg.includes('/books/{book_id}/reviews'), msg);
  });

  await check('subcollection with a template: clear error on open', async () => {
    proxy.rewrite = onRoot((d) => { d.collections.books.path = '/shelves/{shelf_id}/books'; d.collections.books.parent = 'shelves'; });
    let msg = '';
    try { await openRestepSync({ url: API, collections: ['books'], interval: 0 }); } catch (e) { msg = e.message; }
    proxy.rewrite = null;
    assert(msg.includes('subcollection'), msg);
  });

  await check('id_from (§5.1): with "client" the client chooses the id, no alias; with "server" it cannot', async () => {
    proxy.rewrite = onRoot((d) => { d.collections.categories.id_from = 'client'; });
    const p = await openRestepSync({ url: API, collections: ['categories', 'books'], interval: 0, autoPush: false });
    proxy.rewrite = null;
    try {
      const cat = await p.create('categories', { name: `Elegida ${Date.now()}` });
      assert(/^[0-9a-f-]{36}$/.test(cat.id), `a UUID: ${cat.id}`);
      proxy.headers.length = 0;
      const r = await p.sync();
      eq([r.pushed, r.pending], [1, 0], 'pushed');
      const post = proxy.headers.find((x) => x.line === 'POST /categories');
      assert(post, 'there is a POST');
      eq(p.resolveId('categories', cat.id), cat.id, 'no alias: the id is already final');
      eq((await api('GET', `/categories/${cat.id}`)).status, 200, 'the server has it with that id');
      let msg = '';
      try { await p.create('books', { id: 'mio', title: 'No', price: '1.00' }); } catch (e) { msg = e.message; }
      assert(msg.includes('does not let the client choose the id'), msg);
    } finally { await p.close(); }
  });

  await check('upload without create (§5.1): create refuses JSON before queuing it, also where */* would take it as a file', async () => {
    proxy.rewrite = onRoot((d) => { d.collections.categories.upload = { media_types: ['*/*'] }; delete d.collections.categories.schemas?.create; });
    const p = await openRestepSync({ url: API, collections: ['categories'], interval: 0, autoPush: false });
    proxy.rewrite = null;
    try {
      let msg = '';
      try { await p.create('categories', { name: 'No' }); } catch (e) { msg = e.message; }
      assert(msg.includes('takes only files'), msg);
      eq(p.pending().length, 0, 'nothing queued');
    } finally { await p.close(); }
  });

  await check('own id and lost response, no writes: the 409 taken is read and confirmed (§5.1)', async () => {
    proxy.rewrite = onRoot((d) => { d.profiles = d.profiles.filter((x) => x !== 'writes'); d.collections.categories.id_from = 'client'; });
    const p = await openRestepSync({ url: API, collections: ['categories'], interval: 0, autoPush: false });
    proxy.rewrite = null;
    try {
      const cat = await p.create('categories', { name: `Perdida ${Date.now()}` });
      proxy.dropNext = (req) => req.method === 'POST';
      proxy.headers.length = 0;
      const r1 = await p.sync();
      eq([r1.online, r1.pending, r1.uncertain], [false, 1, undefined], 'no response, pending and not uncertain');
      assert(proxy.headers.find((x) => x.line === 'POST /categories')?.headers['idempotency-key'] === undefined, 'no Idempotency-Key');
      const r2 = await p.sync();
      eq([r2.pushed, r2.pending, r2.dropped], [1, 0, 0], 'confirmed after the 409 taken');
      eq(p.get('categories', cat.id)?.name, cat.name, 'in the local copy');
      eq((await serverItems('categories')).filter((c) => c.name === cat.name).length, 1, 'only one on the server');
    } finally { await p.close(); }
  });

  await check('no writes and no own id: a create with a lost response is not repeated automatically (§5.7)', async () => {
    proxy.rewrite = onRoot((d) => { d.profiles = d.profiles.filter((x) => x !== 'writes'); });
    const p = await openRestepSync({ url: API, collections: ['books'], interval: 0, autoPush: false });
    proxy.rewrite = null;
    const errors = [];
    p.on('error', (e) => errors.push(e));
    try {
      const title = `Incierto ${Date.now()}`;
      await p.create('books', { title, price: '2.00' });
      proxy.dropNext = (req) => req.method === 'POST';
      const r1 = await p.sync();
      assert(r1.uncertain && p.pending()[0].uncertain, 'uncertain');
      assert(errors.some((e) => /unknown whether the create/.test(e.error.message)), 'error event');
      proxy.requests.length = 0;
      const r2 = await p.sync();
      eq([r2.pending, proxy.requests.filter((x) => x.startsWith('POST')).length], [1, 0], 'does not repeat it on its own');
      const r3 = await p.sync({ retryUncertain: true });
      eq([r3.pushed, r3.pending], [1, 0], 'repeats it when asked');
      eq((await serverItems('books')).filter((b) => b.title === title).length, 2, 'and then there are two: that is why it does not do it on its own');
    } finally { await p.close(); }
  });

  await check('discard(): removes an uncertain create and undoes the local change', async () => {
    proxy.rewrite = onRoot((d) => { d.profiles = d.profiles.filter((x) => x !== 'writes'); });
    const p = await openRestepSync({ url: API, collections: ['books'], interval: 0, autoPush: false });
    proxy.rewrite = null;
    try {
      const item = await p.create('books', { title: 'Descartado', price: '2.00' });
      proxy.dropNext = (req) => req.method === 'POST';
      await p.sync();
      const [op] = p.pending();
      eq(await p.discard(op.opId), true, 'dropped');
      eq([p.pending().length, p.get('books', item.id)], [0, null], 'no queue and no provisional item');
    } finally { await p.close(); }
  });

  await check('credentials only to the API origin, and links with another scheme (§3, §4.3)', async () => {
    proxy.rewrite = (req, status, body) => (req.method === 'GET' && req.url.startsWith('/books?limit=') && status === 200
      ? { ...body, next: `http://localhost:${OTHER_PORT}/books?cursor=ajeno` } : undefined);
    const p = await openRestepSync({ url: API, collections: ['books'], interval: 0, autoPush: false, token: 'secreto', headers: { 'x-tenant': 'uno' } });
    try {
      other.seen.length = 0;
      await p.sync();
      eq(other.seen.length, 1, 'followed next to the other origin');
      eq([other.seen[0].authorization, other.seen[0]['x-tenant']], [undefined, undefined], 'no token or extra headers');
      proxy.rewrite = (req, status, body) => (req.method === 'GET' && req.url.startsWith('/books?limit=') && status === 200
        ? { ...body, next: 'file:///etc/passwd' } : undefined);
      const fresh = await openRestepSync({ url: API, collections: ['books'], interval: 0, autoPush: false });
      const r = await fresh.sync();
      await fresh.close();
      assert(r.errors.some((e) => /scheme/.test(e.message)), `error for the scheme: ${r.errors.map((e) => e.message).join('; ')}`);
    } finally { proxy.rewrite = null; await p.close(); }
  });

  await check('collations (§6.1): does not sort or filter locally by a field with the server collation', async () => {
    let msg = '';
    try { db.list('categories', { sort: 'name' }); } catch (e) { msg = e.message; }
    assert(msg.includes("server's collation"), `sort: ${msg}`);
    msg = '';
    try { db.list('categories', { filter: { name: 'Libros' } }); } catch (e) { msg = e.message; }
    assert(msg.includes("server's collation"), `filter: ${msg}`);
    eq(db.list('categories', { sort: (a, b) => a.id.localeCompare(b.id) }).length > 0, true, 'with a function it does');
    const titles = db.list('books', { sort: 'title' }).map((b) => b.title);
    const byCodepoint = [...titles].sort((a, b) => { const x = [...a], y = [...b]; for (let i = 0; i < Math.min(x.length, y.length); i++) { const d = x[i].codePointAt(0) - y[i].codePointAt(0); if (d) return d; } return x.length - y.length; });
    eq(titles, byCodepoint, 'strings sort by code point');
  });

  await check('clear errors on open', async () => {
    let msg = '';
    try { await openRestepSync({ url: API, collections: ['nada'], interval: 0 }); } catch (e) { msg = e.message; }
    assert(msg.includes('does not declare the collection nada'), msg);
    msg = '';
    try { await openRestepSync({ url: 'http://localhost:8869/', collections: ['books'], interval: 0 }); } catch (e) { msg = e.message; }
    assert(msg.includes('no local copy of the discovery document'), msg);
  });
} finally {
  await db?.close();
  await stopServer();
  proxyServer.close();
  otherServer.close();
  await rm(dir, { recursive: true, force: true });
}

console.log(`\n${passed} passed, ${failed} failed`);
process.exit(failed ? 1 : 0);
