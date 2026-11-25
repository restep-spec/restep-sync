#!/usr/bin/env node
// Copy of the reference server (vendor/restep-server.mjs, draft 7) with the sync profile (§10)
// added, to test restep-sync. The reference server does not implement sync: this file reads its code,
// applies some text patches to it and writes the result to a temporary directory. It does not touch the original.
//
// Usage:
//   node tools/restep-sync/ref-sync-server.mjs            starts the copy on PORT (default 8860)
//   import { buildSyncServer } from './ref-sync-server.mjs'  returns the path of the generated file
//
// What it adds:
// - "sync" in profiles and, only in the SYNCED collections (books and categories), sync.retention in
//   discovery: since draft 6 a collection is synced only if it declares sync (§10). In the
//   others (products), :changes gives 404 and their responses carry no state.
// - state in every response of a synced collection: "<epoch>.<sequence>". The epoch changes on every start, so
//   a state from an earlier start gives 410 state_expired (the data goes back to the seed).
// - _etag on every item of collection responses (§7.2), if the reference does not already put it.
// - Every page of a traversal carries the state of the first (it goes in the cursor). With the default
//   order, the cursor also carries the last id, so deleting in the middle of a traversal does not make
//   another item get lost (keyset pagination).
// - GET /<collection>:changes?since=<state>, with has_more. SYNC_PAGE (environment variable, default
//   100) sets how many log changes fit in a response; SYNC_RETENTION, the seconds they are
//   kept (default 604800).
// Changes are detected by comparing each item's ETag at the start of every request, so the ones made by
// refresh() on inherited values (§11.6) or by unlink() on delete count too.
import { readFile, writeFile, mkdtemp } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const here = dirname(fileURLToPath(import.meta.url));
export const REFERENCE = join(here, 'vendor/restep-server.mjs');

const SYNC_CODE = `
// ── sync (§10), added by tools/restep-sync/ref-sync-server.mjs ───────────────
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
    if (!SYNCED.includes(n) || !c.rows) continue; // only what declares sync (a subcollection has no rows of its own)
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
  // The list is chosen by visibility (10): it was seen at since if its first event is not creating it; deleted
  // at the end goes to destroyed (also if it was created in between); otherwise updated or created.
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

// [find, replace]. Each search has to appear exactly once: if the reference server
// changes and a patch no longer fits, it fails loudly instead of generating something half-done.
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
  // §7.2: in a synced collection, every item carries its _etag on all pages. If the
  // reference already puts it (it does since draft 5), it is left; otherwise it is added.
  [/^ {4}items: (.+), next,$/m, (_, items) =>
    `    items: ${items.includes('_etag') ? items : items.replace('pick(r, fields)', '({ ...pick(r, fields), _etag: etag(r) })')}, next,\n` +
    '    ...(SYNCED.includes(nameOf(coll)) && { state: collState }),'],
  ['  refresh(); // 11.6', '  refresh(); track(); // 11.6'],
  ['refresh();\nconst port', 'refresh(); track();\nconst port'],
  ['  if (action !== undefined) return handleAction(req, res, { url, params, coll, collPath, id, action });',
    // §10: only a synced collection answers :changes; in the others (also in a subcollection), the
    // reference treats it as an action it does not declare: 404.
    "  if (action === 'changes' && id === undefined && !coll.base && SYNCED.includes(nameOf(coll))) return changes(nameOf(coll), params, req, res, method);\n" +
    '  if (action !== undefined) return handleAction(req, res, { url, params, coll, collPath, id, action });'],
  ['      path: c.path ?? `/${n}`, ', '      path: c.path ?? `/${n}`, ...(SYNCED.includes(n) && { sync: { retention: RETENTION } }), '],
];

export async function buildSyncServer(source = REFERENCE) {
  let code = await readFile(source, 'utf8');
  for (const [find, replace] of PATCHES) {
    const n = typeof find === 'string' ? code.split(find).length - 1 : (code.match(new RegExp(find.source, `${find.flags}g`)) ?? []).length;
    if (n !== 1) throw new Error(`ref-sync-server: the patch does not fit (${n} occurrences) in ${source}:\n${find}`);
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
