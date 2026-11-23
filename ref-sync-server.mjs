#!/usr/bin/env node
// Copia del servidor de referencia (vendor/restep-server.mjs, borrador 6) con el perfil sync (§10)
// añadido, para probar restep-sync. El servidor de referencia no implementa sync: este archivo lee su código,
// le aplica unos parches de texto y escribe el resultado en un directorio temporal. No toca el original.
//
// Uso:
//   node tools/restep-sync/ref-sync-server.mjs            arranca la copia en PORT (por defecto 8860)
//   import { buildSyncServer } from './ref-sync-server.mjs'  devuelve la ruta del archivo generado
//
// Lo que añade:
// - "sync" en profiles y, solo en las colecciones de SYNCED (books y categories), sync.retention en el
//   descubrimiento: en el borrador 6 una colección solo está sincronizada si declara sync (§10). En las
//   demás (products), :changes da 404 y sus respuestas no llevan state.
// - state en cada respuesta de una colección sincronizada: "<época>.<secuencia>". La época cambia en cada arranque, así
//   que un estado de un arranque anterior da 410 state_expired (los datos vuelven a la semilla).
// - _etag en cada elemento de las respuestas de colección (§7.2), si la referencia no lo pone ya.
// - Todas las páginas de un recorrido llevan el state de la primera (va en el cursor). Con el orden
//   por defecto, el cursor lleva además el último id, así que borrar a mitad de recorrido no hace perder
//   otro elemento (paginación por clave).
// - GET /<colección>:changes?since=<state>, con has_more. SYNC_PAGE (variable de entorno, por defecto
//   100) fija cuántos cambios del registro entran en una respuesta; SYNC_RETENTION, los segundos que se
//   guardan (por defecto 604800).
// Los cambios se detectan comparando la ETag de cada elemento al principio de cada petición, así que
// también cuentan los que hace refresh() en los valores heredados (§11.6) o unlink() al borrar.
import { readFile, writeFile, mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
export const REFERENCE = join(here, 'vendor/restep-server.mjs');

const SYNC_CODE = `
// ── sync (§10), añadido por tools/restep-sync/ref-sync-server.mjs ────────────
const SYNCED = (process.env.SYNCED ?? 'books,categories').split(',').filter(Boolean);
const EPOCH = randomBytes(4).toString('hex');
const SYNC_PAGE = Number(process.env.SYNC_PAGE ?? 100);
const RETENTION = Number(process.env.SYNC_RETENTION ?? 604800);
let seq = 0, trimmed = 0;
const changeLog = []; // { seq, coll, id, kind, at }
const lastSeq = {};
const snapshots = new Map();
const stateOf = (n) => \`\${EPOCH}.\${lastSeq[n] ?? 0}\`;
let lastCursor = null;
function track() {
  for (const [n, c] of Object.entries(db)) {
    if (!SYNCED.includes(n) || !c.rows) continue; // solo lo que declara sync (una subcolección no tiene rows propio)
    const prev = snapshots.get(n);
    const now = new Map([...c.rows].map(([id, r]) => [id, etag(r)]));
    const log = (id, kind) => { seq++; lastSeq[n] = seq; changeLog.push({ seq, coll: n, id, kind, at: Date.now() }); };
    if (prev) {
      for (const [id, tg] of now) if (!prev.has(id)) log(id, 'created'); else if (prev.get(id) !== tg) log(id, 'updated');
      for (const id of prev.keys()) if (!now.has(id)) log(id, 'destroyed');
    }
    snapshots.set(n, now);
  }
  while (changeLog.length && changeLog[0].at < Date.now() - RETENTION * 1000) trimmed = changeLog.shift().seq;
}
function changes(n, params, req, res, method) {
  if (method !== 'GET' && method !== 'HEAD')
    throw new Problem(405, 'method_not_allowed', t('Only GET and HEAD.', 'Solo GET y HEAD.'), {}, { Allow: 'GET, HEAD' });
  for (const key of new Set(params.keys())) {
    if (key === 'since' || !isReserved(reservedName(key))) continue;
    throw unsupported(key, t(':changes covers the whole collection: only since.', ':changes abarca la colección entera: solo since.'));
  }
  if (params.getAll('since').length > 1) throw once('since');
  const since = params.get('since');
  if (since === null) throw bad('since', t('Send since with the state of your copy.', 'Envía since con el state de tu copia.'));
  const m = /^([0-9a-f]{8})\\.(\\d+)$/.exec(since);
  if (!m) throw bad('since', t('That is not a state issued by this server.', 'Ese no es un state emitido por este servidor.'));
  const from = Number(m[2]);
  if (m[1] !== EPOCH || from < trimmed)
    throw new Problem(410, 'state_expired', t('The changes since that state are no longer available: load the collection again.', 'Los cambios desde ese estado ya no están disponibles: vuelve a cargar la colección.'));
  if (from > seq) throw bad('since', t('That state is in the future.', 'Ese estado es del futuro.'));
  const entries = changeLog.filter((e) => e.coll === n && e.seq > from);
  const page = entries.slice(0, SYNC_PAGE), has_more = entries.length > SYNC_PAGE;
  // La lista la elige la visibilidad (10): se veía en since si su primer evento no es crearlo; borrado
  // al final va a destroyed (también si se creó entre medias); si no, updated o created.
  const seen = new Map();
  for (const e of page) seen.set(e.id, { first: seen.get(e.id)?.first ?? e.kind, last: e.kind });
  const kinds = new Map([...seen].map(([id, { first, last }]) =>
    [id, last === 'destroyed' ? 'destroyed' : first === 'created' ? 'created' : 'updated']));
  const of = (k) => [...kinds].filter(([, v]) => v === k).map(([id]) => id);
  const body = { since, state: has_more ? \`\${EPOCH}.\${page[page.length - 1].seq}\` : stateOf(n), has_more,
    created: of('created'), updated: of('updated'), destroyed: of('destroyed') };
  return send(res, 200, body, { 'Cache-Control': 'no-cache' }, method === 'HEAD');
}
`;

// [buscar, reemplazar]. Cada búsqueda tiene que aparecer exactamente una vez: si el servidor de
// referencia cambia y un parche ya no encaja, falla en voz alta en vez de generar algo a medias.
const PATCHES = [
  ["const PROFILES = ['core', 'query', 'writes', 'cache', 'relations'];", "const PROFILES = ['core', 'query', 'writes', 'cache', 'relations', 'sync'];"],
  ['let nextId = 100;\n', 'let nextId = 100;\n' + SYNC_CODE],
  ['const makeCursor = (o, print) => {\n  const payload = Buffer.from(JSON.stringify({ o, q: print })).toString(\'base64url\');',
    'const makeCursor = (o, print, extra = {}) => {\n  const payload = Buffer.from(JSON.stringify({ o, q: print, ...extra })).toString(\'base64url\');'],
  ["  if (!params.has('cursor')) return 0;", "  lastCursor = null;\n  if (!params.has('cursor')) return 0;"],
  ['  return c.o;\n}', '  lastCursor = c;\n  return c.o;\n}'],
  ['  const offset = readCursor(params, print);',
    "  let offset = readCursor(params, print);\n  const cur = lastCursor, collState = cur?.s ?? stateOf(nameOf(coll));\n" +
    "  if (cur?.k !== undefined && (params.get('sort') ?? 'id') === 'id') {\n" +
    "    const i = items.findIndex((r) => compare('string', r.id, cur.k) > 0);\n    offset = i < 0 ? items.length : i;\n  }"],
  ["    q.set('cursor', makeCursor(offset + limit, print));",
    "    q.set('cursor', makeCursor(offset + limit, print, { s: collState, k: page[page.length - 1].id }));"],
  // §7.2: en una colección sincronizada, cada elemento lleva su _etag en todas las páginas. Si la
  // referencia ya lo pone (lo hace desde el borrador 5), se deja; si no, se añade.
  [/^ {4}items: (.+), next,$/m, (_, items) =>
    `    items: ${items.includes('_etag') ? items : items.replace('pick(r, fields)', '({ ...pick(r, fields), _etag: etag(r) })')}, next,\n` +
    '    ...(SYNCED.includes(nameOf(coll)) && { state: collState }),'],
  ['  refresh(); // 11.6', '  refresh(); track(); // 11.6'],
  ['refresh();\nconst port', 'refresh(); track();\nconst port'],
  ['  if (action !== undefined) return handleAction(req, res, { url, params, coll, collPath, id, action });',
    // §10: solo una colección sincronizada responde :changes; en las demás (también en una subcolección), la
    // referencia lo trata como una acción que no declara: 404.
    "  if (action === 'changes' && id === undefined && !coll.base && SYNCED.includes(nameOf(coll))) return changes(nameOf(coll), params, req, res, method);\n" +
    '  if (action !== undefined) return handleAction(req, res, { url, params, coll, collPath, id, action });'],
  ['      path: c.path ?? `/${n}`, ', '      path: c.path ?? `/${n}`, ...(SYNCED.includes(n) && { sync: { retention: RETENTION } }), '],
];

export async function buildSyncServer(source = REFERENCE) {
  let code = await readFile(source, 'utf8');
  for (const [find, replace] of PATCHES) {
    const n = typeof find === 'string' ? code.split(find).length - 1 : (code.match(new RegExp(find.source, `${find.flags}g`)) ?? []).length;
    if (n !== 1) throw new Error(`ref-sync-server: el parche no encaja (${n} apariciones) en ${source}:\n${find}`);
    code = typeof find === 'string' ? code.replace(find, () => replace) : code.replace(find, replace);
  }
  const dir = await mkdtemp(join(tmpdir(), 'restep-sync-server-'));
  const file = join(dir, 'restep-sync-server.mjs');
  await writeFile(file, code);
  return file;
}

if (import.meta.url === pathToFileURL(process.argv[1] ?? '').href) {
  const file = await buildSyncServer();
  process.env.PORT ??= '8860';
  await import(pathToFileURL(file).href);
}
