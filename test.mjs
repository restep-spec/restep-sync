// Pruebas de restep-sync. Desde restep/:  node tools/restep-sync/test.mjs
// Arranca una copia del servidor de referencia con sync (ref-sync-server.mjs) en el puerto 8860 y un
// proxy en el 8861 por el que pasa el cliente: el proxy puede cortar una respuesta (para probar el
// reintento sin duplicar) o contestar él (para probar Retry-After). Para «sin conexión» se para el
// servidor de verdad y se vuelve a arrancar (la época cambia, así que su state anterior da 410).
import { spawn } from 'node:child_process';
import { createServer, request } from 'node:http';
import { mkdtemp, rm, readFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { buildSyncServer } from './ref-sync-server.mjs';
import { openRestepSync, fileStorage, memoryStorage } from './restep-sync.mjs';

const SERVER_PORT = 8860, PROXY_PORT = 8861;
const SERVER = `http://localhost:${SERVER_PORT}`, API = `http://localhost:${PROXY_PORT}/`;

// ── Utilidades ──
let passed = 0, failed = 0;
const results = [];
async function check(name, fn) {
  try { await fn(); passed++; results.push(`ok    ${name}`); }
  catch (e) { failed++; results.push(`FALLO ${name}\n        ${e.stack?.split('\n').slice(0, 3).join('\n        ')}`); }
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
  return items.sort((a, b) => a.id.localeCompare(b.id));
}
const localItems = (db, coll) => db.list(coll).sort((a, b) => a.id.localeCompare(b.id));
async function sameAsServer(db, coll) { eq(localItems(db, coll), await serverItems(coll), `${coll}: copia local distinta de la del servidor`); }

// ── Servidor y proxy ──
const serverFile = await buildSyncServer();
let server;
async function startServer() {
  server = spawn(process.execPath, [serverFile], { env: { ...process.env, PORT: String(SERVER_PORT), SYNC_PAGE: '2' }, stdio: ['ignore', 'pipe', 'inherit'] });
  await new Promise((resolve, reject) => {
    server.stdout.on('data', (d) => { if (String(d).includes('http://')) resolve(); });
    server.on('exit', (c) => reject(new Error(`el servidor ha salido (${c})`)));
  });
}
async function stopServer() {
  if (!server || server.exitCode !== null) return;
  const done = new Promise((r) => server.once('exit', r));
  server.kill();
  await done;
}

const proxy = { requests: [], dropNext: null, injectNext: null };
const proxyServer = createServer((req, res) => {
  proxy.requests.push(`${req.method} ${req.url}`);
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
      if (drop) return req.socket.destroy(); // el servidor hizo el trabajo, pero la respuesta se pierde
      res.writeHead(ur.statusCode, ur.headers); res.end(Buffer.concat(chunks));
    });
  });
  up.on('error', () => req.socket.destroy()); // servidor parado: para el cliente, sin conexión
  req.pipe(up);
});
await new Promise((r) => proxyServer.listen(PROXY_PORT, r));

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

  await check('sincronización inicial: la copia local es la del servidor', async () => {
    const r = await db.sync();
    assert(r.online && r.errors.length === 0, `informe: ${JSON.stringify(r)}`);
    eq(db.list('books').length, 7, 'número de libros');
    await sameAsServer(db, 'books');
    await sameAsServer(db, 'categories');
    const saved = JSON.parse(await readFile(file, 'utf8'));
    assert(/^[0-9a-f]{8}\.\d+$/.test(saved.collections.books.state), 'el state se guarda en el archivo');
  });

  await check('cambios remotos: creados, modificados y borrados, con has_more (SYNC_PAGE=2)', async () => {
    const created = await api('POST', '/books', { title: 'Remoto', price: '9.99' });
    await api('PATCH', '/books/7', { price: '17.00' });
    await api('PATCH', '/books/21', { rating: 4.9 });
    await api('DELETE', '/books/15');
    events.change.length = 0;
    proxy.requests.length = 0;
    const r = await db.sync();
    eq(r.pulled.books, { created: 1, updated: 2, destroyed: 1 }, 'recuento de cambios');
    assert(proxy.requests.filter((x) => x.includes('books:changes')).length >= 2, 'ha seguido has_more');
    assert(proxy.requests.every((x) => !x.startsWith('GET /books?limit')), 'no ha recargado la colección entera');
    eq(db.get('books', created.body.id)?.title, 'Remoto', 'el creado está en local');
    eq(db.get('books', '15'), null, 'el borrado ya no está');
    eq(db.get('books', '7').price, '17.00', 'el modificado está al día');
    assert(events.change.some((e) => e.source === 'remote' && e.destroyed.includes('15')), 'evento change del borrado');
    await sameAsServer(db, 'books');
  });

  await check('cambio en otra colección por herencia (5.7): borrar una categoría cambia a sus hijas', async () => {
    await api('DELETE', '/categories/2'); // la 4 hereda de la 2: pierde el enlace y cambia su tax_rate
    const r = await db.sync();
    eq(r.pulled.categories, { created: 0, updated: 1, destroyed: 1 }, 'cambios de categories');
    await sameAsServer(db, 'categories');
  });

  await check('escritura optimista: create, update y delete se ven en local antes de enviarse', async () => {
    const item = await db.create('books', { title: 'Optimista', price: '5.00' });
    assert(item.id.startsWith('local-'), 'id local provisional');
    await db.update('books', item.id, { price: '6.00' });
    await db.update('books', '12', { price: '15.50' });
    await db.update('books', '12', { year: 2024 }); // dos cambios seguidos sobre el mismo elemento
    eq(db.get('books', item.id).price, '6.00', 'update sobre un creado pendiente');
    eq(db.pending().length, 4, 'cuatro operaciones pendientes');
    const r = await db.sync();
    eq([r.pushed, r.pending, r.conflicts], [4, 0, 0], 'enviadas, pendientes y conflictos');
    const real = db.resolveId('books', item.id);
    assert(!real.startsWith('local-'), 'el id provisional apunta al definitivo');
    eq(db.get('books', item.id)?.id, real, 'get con el id provisional sigue funcionando');
    eq((await api('GET', `/books/${real}`)).body.price, '6.00', 'el servidor tiene el update del creado');
    const twelve = (await api('GET', '/books/12')).body;
    eq([twelve.price, twelve.year], ['15.50', 2024], 'los dos updates seguidos llegan sin conflicto');
    await sameAsServer(db, 'books');
  });

  await check('conflicto 412, servidor gana: el cambio local se deshace', async () => {
    await db.update('books', '12', { price: '1.00' }, { onConflict: 'server' });
    const before = await api('GET', '/books/12');
    await api('PATCH', '/books/12', { title: 'El último ferry (2.ª ed.)' }, { 'if-match': before.headers.get('etag') });
    events.conflict.length = 0;
    proxy.requests.length = 0;
    const r = await db.sync();
    eq(r.conflicts, 1, 'un conflicto');
    assert(proxy.requests.includes('PATCH /books/12'), 'el PATCH ha llegado al servidor con If-Match');
    eq(events.conflict[0]?.resolution, 'server', 'resolución');
    const now = (await api('GET', '/books/12')).body;
    eq([now.title, now.price], ['El último ferry (2.ª ed.)', '15.50'], 'el servidor no ha cambiado');
    eq(db.get('books', '12').price, '15.50', 'la copia local vuelve a la del servidor');
    await sameAsServer(db, 'books');
  });

  await check('conflicto 412, cliente gana: se reenvía con la ETag nueva', async () => {
    await db.update('books', '12', { price: '2.00' }, { onConflict: 'client' });
    await api('PATCH', '/books/12', { rating: 3.3 });
    events.conflict.length = 0;
    const r = await db.sync();
    eq([r.conflicts, r.pushed, r.pending], [1, 1, 0], 'conflictos, enviadas, pendientes');
    const now = (await api('GET', '/books/12')).body;
    eq([now.price, now.rating], ['2.00', 3.3], 'gana el precio local y se conserva el rating remoto');
    await sameAsServer(db, 'books');
  });

  await check('conflicto con función del usuario: fusión propia', async () => {
    await db.update('books', '30', { title: 'Cuaderno de faros (local)' }, {
      onConflict: ({ server, local, base }) => {
        assert(server.price === '99.00' && base.price === '12.00' && local.title === 'Cuaderno de faros (local)', 'argumentos de la función');
        return { title: `${local.title} + remoto` };
      },
    });
    await api('PATCH', '/books/30', { price: '99.00' }); // sin ETag en la colección: se detecta al releer
    const r = await db.sync();
    eq(r.conflicts, 1, 'un conflicto');
    const now = (await api('GET', '/books/30')).body;
    eq([now.title, now.price], ['Cuaderno de faros (local) + remoto', '99.00'], 'fusión aplicada');
    await sameAsServer(db, 'books');
  });

  await check('delete contra un elemento cambiado: servidor gana y el elemento vuelve', async () => {
    await db.delete('books', '23');
    eq(db.get('books', '23'), null, 'borrado en local');
    await api('PATCH', '/books/23', { price: '22.00' });
    await db.sync();
    eq(db.get('books', '23')?.price, '22.00', 'ha vuelto, con la versión del servidor');
    await sameAsServer(db, 'books');
  });

  await check('reintento sin duplicar: se pierde la respuesta del POST y se repite con la misma Idempotency-Key', async () => {
    const item = await db.create('books', { title: 'Respuesta perdida', price: '3.00' });
    proxy.dropNext = (req) => req.method === 'POST';
    const r1 = await db.sync();
    eq([r1.online, r1.pending], [false, 1], 'primer intento: sin respuesta, sigue pendiente');
    eq(proxy.lastPost.status, 201, 'el servidor sí lo creó');
    eq(db.pending()[0].attempts, 1, 'un intento contado');
    const r2 = await db.sync();
    eq([r2.pushed, r2.pending], [1, 0], 'segundo intento');
    eq(proxy.lastPost.headers['idempotent-replayed'], 'true', 'el servidor repite la respuesta guardada');
    const all = await serverItems('books');
    eq(all.filter((b) => b.title === 'Respuesta perdida').length, 1, 'un solo libro en el servidor');
    eq(db.list('books', { filter: { title: 'Respuesta perdida' } }).length, 1, 'un solo libro en local');
    assert(!db.resolveId('books', item.id).startsWith('local-'), 'id definitivo');
    await sameAsServer(db, 'books');
  });

  await check('Retry-After: un 503 del servidor aplaza el reintento aunque se pida sync', async () => {
    await db.create('books', { title: 'Paciencia', price: '4.00' });
    proxy.injectNext = { status: 503, headers: { 'retry-after': '1' }, body: { status: 503, code: 'unavailable', title: 'No disponible' } };
    const r1 = await db.sync();
    eq(r1.pending, 1, 'sigue pendiente');
    assert(r1.waitingUntil > Date.now() + 500, 'espera lo que dice Retry-After');
    proxy.requests.length = 0;
    const r2 = await db.sync();
    eq([r2.pending, proxy.requests.filter((x) => x.startsWith('POST')).length], [1, 0], 'no reintenta antes de tiempo');
    await wait(1100);
    const r3 = await db.sync();
    eq([r3.pushed, r3.pending], [1, 0], 'pasado el plazo, se envía');
    eq((await serverItems('books')).filter((b) => b.title === 'Paciencia').length, 1, 'uno solo');
  });

  await check('error permanente (422): se quita de la cola, se deshace y se avisa', async () => {
    const item = await db.create('books', { title: 'Sin precio' });
    events.error.length = 0;
    const r = await db.sync();
    eq([r.dropped, r.pending], [1, 0], 'descartada');
    eq(events.error[0]?.error.status, 422, 'evento error con el estado');
    eq(events.error[0]?.error.problem.code, 'invalid', 'y el problem+json');
    eq(db.get('books', item.id), null, 'el elemento optimista desaparece');
  });

  let offlineTitle;
  await check('sin conexión: escrituras en cola, que sobreviven a cerrar y reabrir desde el archivo', async () => {
    await stopServer();
    offlineTitle = `Escrito sin red ${Date.now()}`;
    const item = await db.create('books', { title: offlineTitle, price: '8.00' });
    await db.update('categories', '1', { name: 'Papelería y regalos' }); // sin tocar desde la semilla
    await db.delete('books', '50');
    const r = await db.sync();
    eq([r.online, r.pending], [false, 3], 'sin conexión, tres pendientes');
    await db.close();
    db = await open(); // sin servidor: usa el descubrimiento guardado
    eq(db.pending().length, 3, 'la cola sigue ahí');
    eq(db.get('books', item.id)?.title, offlineTitle, 'el creado sigue visible');
    eq(db.get('categories', '1').name, 'Papelería y regalos', 'el update sigue visible');
    eq(db.get('books', '50'), null, 'el borrado sigue sin verse');
  });

  await check('vuelta del servidor: se envía la cola, el state antiguo da 410 y se recarga', async () => {
    await startServer(); // datos de la semilla y época nueva
    events.change.length = 0;
    const r = await db.sync();
    eq([r.online, r.pushed, r.pending, r.conflicts], [true, 3, 0, 0], 'enviadas sin conflicto');
    eq(r.reset.sort(), ['books', 'categories'], 'recarga por state_expired');
    assert(events.change.some((e) => e.reload), 'evento change de recarga');
    const all = await serverItems('books');
    eq(all.filter((b) => b.title === offlineTitle).length, 1, 'el creado sin red está una vez');
    eq([(await api('GET', '/categories/1')).body.name, all.find((b) => b.id === '50')], ['Papelería y regalos', undefined], 'update y delete aplicados');
    await sameAsServer(db, 'books');
    await sameAsServer(db, 'categories');
  });

  await check('segundo plano: el intervalo trae un cambio remoto y avisa con change', async () => {
    const bg = await openRestepSync({ url: API, collections: ['books'], storage: memoryStorage(), interval: 150 });
    try {
      await bg.sync();
      const got = new Promise((resolve) => bg.on('change', (e) => { if (e.source === 'remote' && e.updated.includes('21')) resolve(e); }));
      await api('PATCH', '/books/21', { price: '20.50' });
      await Promise.race([got, wait(3000).then(() => { throw new Error('no ha llegado el change en 3 s'); })]);
      eq(bg.get('books', '21').price, '20.50', 'actualizado sin llamar a sync');
      const created = await bg.create('books', { title: 'Automático', price: '1.50' }); // autoPush
      await wait(400);
      eq(bg.pending().length, 0, 'autoPush ha vaciado la cola');
      assert(!bg.resolveId('books', created.id).startsWith('local-'), 'con id definitivo');
    } finally { await bg.close(); }
  });

  await check('errores claros al abrir', async () => {
    let msg = '';
    try { await openRestepSync({ url: API, collections: ['nada'], interval: 0 }); } catch (e) { msg = e.message; }
    assert(msg.includes('no declara la colección nada'), msg);
    msg = '';
    try { await openRestepSync({ url: 'http://localhost:8869/', collections: ['books'], interval: 0 }); } catch (e) { msg = e.message; }
    assert(msg.includes('sin copia local del descubrimiento'), msg);
  });
} finally {
  await db?.close();
  await stopServer();
  proxyServer.close();
  await rm(dir, { recursive: true, force: true });
}

console.log(`\n${passed} pruebas correctas, ${failed} fallidas`);
process.exit(failed ? 1 : 0);
