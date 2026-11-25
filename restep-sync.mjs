// restep-sync · offline-first client for Restep 1 APIs (https://restep.wubytes.com/1/, draft 7).
// Keeps a local replica of one or more collections up to date with the sync profile (§10) and stores
// writes in a queue that is sent when there is a connection, with the guarantees of writes (§7):
// a stable Idempotency-Key on every create and If-Match on every update and delete.
// No dependencies. Works in Node 18+ and in the browser (fileStorage only in Node; indexedDBStorage
// only in the browser).
//
//   import { openRestepSync, fileStorage } from './restep-sync.mjs';
//   const db = await openRestepSync({ url: 'http://localhost:8080/', collections: ['books'],
//     storage: fileStorage('./replica.json') });
//   db.list('books', { filter: { category: 'poesia' } });
//   await db.update('books', '7', { price: '15.00' });   // optimistic: the local view changes right away
//   await db.sync();                                      // sends the queue and pulls the changes
//
// Model: per collection, the copy confirmed by the server is stored (data and ETag of each item)
// and the state from §9. Pending writes go in a FIFO queue; what list() and get() return is the
// confirmed copy with the queue applied on top. That way a conflict or a permanent error only has
// to remove the operation from the queue to undo the optimistic change.

const VERSION = 1; // of the format saved in the store
// §1.2: sync is experimental, so it is only used with an API that declares a version whose sync text
// this client implements; with any other, collections are reloaded in full.
export const KNOWN_TEXTS = ['1.0-draft.7'];
// §1.1: the grammar of restep, with no leading zeros.
const RESTEP_VERSION = /^(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-(draft|rc)\.(0|[1-9]\d*))?$/;
const TRANSIENT = new Set([408, 425, 429, 500, 502, 503, 504]);
// §5.7: 429 and 503 (and 425, which the server gives without processing) only reach requests with no
// effect; the others say nothing about the effect, so a create is only repeated when repeating it is safe.
const NO_EFFECT = new Set([425, 429, 503]);
// A 409 is only retried with code request_in_progress (the same request is still running, §7.1),
// honouring Retry-After; any other 409 (conflict: a duplicate, an impossible transition) is permanent.
const IN_PROGRESS = 'request_in_progress';
// §5.7: without idempotency.ttl, a key lives 24 hours.
const DEFAULT_TTL = 86400;
const MAX_CONFLICT_ROUNDS = 3;

// ── Stores ────────────────────────────────────────────────────────────────────
// A store is { read(): Promise<object|null>, write(object): Promise<void> }. It keeps the whole replica
// as a single JSON object: simple and enough for collections of thousands of items.

export function memoryStorage() {
  let data = null;
  return {
    async read() { return data === null ? null : structuredClone(data); },
    async write(value) { data = structuredClone(value); },
  };
}

// JSON file (Node). Writes to a temporary file and renames it, so a half-written file is never left.
export function fileStorage(path) {
  const fs = () => import('node:fs/promises');
  return {
    async read() {
      try { return JSON.parse(await (await fs()).readFile(path, 'utf8')); } catch (e) {
        if (e.code === 'ENOENT') return null;
        throw new Error(`restep-sync: cannot read ${path}: ${e.message}`);
      }
    },
    async write(value) {
      const { writeFile, rename } = await fs();
      const tmp = `${path}.${process.pid}.${Math.random().toString(36).slice(2)}.tmp`;
      await writeFile(tmp, JSON.stringify(value));
      await rename(tmp, path);
    },
  };
}

// IndexedDB (browser). A single record with the whole replica, under key in the object store `store`
// of database dbName. Thin on purpose: not tested in this version (the tests run in Node, with no
// browser).
export function indexedDBStorage(dbName = 'restep-sync', { store = 'replicas', key = 'default' } = {}) {
  let opening;
  const open = () => (opening ??= new Promise((resolve, reject) => {
    const req = indexedDB.open(dbName, 1);
    req.onupgradeneeded = () => req.result.createObjectStore(store);
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error);
  }));
  const run = async (mode, fn) => {
    const idb = await open();
    return new Promise((resolve, reject) => {
      const tx = idb.transaction(store, mode);
      const req = fn(tx.objectStore(store));
      tx.oncomplete = () => resolve(req.result);
      tx.onerror = tx.onabort = () => reject(tx.error);
    });
  };
  return {
    async read() { return (await run('readonly', (s) => s.get(key))) ?? null; },
    async write(value) { await run('readwrite', (s) => s.put(value, key)); },
  };
}

// ── Utilities ─────────────────────────────────────────────────────────────────
const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
function mergePatch(target, patch) { // RFC 7396
  if (!isObject(patch)) return patch;
  const out = isObject(target) ? { ...target } : {};
  for (const [k, v] of Object.entries(patch)) {
    if (v === null) delete out[k]; else out[k] = mergePatch(out[k], v);
  }
  return out;
}
// Equality as JSON values, ignoring member order and _etag (which is not part of the item).
const canonical = (v) => (Array.isArray(v) ? `[${v.map(canonical).join(',')}]`
  : isObject(v) ? `{${Object.keys(v).filter((k) => k !== '_etag').sort().map((k) => `${JSON.stringify(k)}:${canonical(v[k])}`).join(',')}}`
  : JSON.stringify(v));
const sameData = (a, b) => canonical(a) === canonical(b);
function codepoints(a, b) { // §6.1: by code points, not by UTF-16 units or by language
  const x = [...a], y = [...b];
  for (let i = 0; i < Math.min(x.length, y.length); i++) {
    const d = x[i].codePointAt(0) - y[i].codePointAt(0);
    if (d) return d;
  }
  return x.length - y.length;
}
function linkTargets(header, rel) {
  const out = [];
  for (const m of (header ?? '').matchAll(/<([^>]*)>([^,<]*)/g)) {
    const rels = m[2].match(/rel\s*=\s*("([^"]*)"|[^;\s]+)/i);
    if (rels && (rels[2] ?? rels[1]).split(/\s+/).includes(rel)) out.push(m[1]);
  }
  return out;
}
const clone = (v) => (v === undefined ? undefined : structuredClone(v));
// What is stored of an item: its data without _etag (which is not part of the item, §7.2) and its ETag,
// the one from the _etag the listing brings or, if missing, the one known some other way.
function entry(data, etag = null) {
  const { _etag, ...rest } = data;
  return { data: rest, etag: typeof _etag === 'string' ? _etag : etag };
}

let nodeCrypto;
async function uuid() {
  if (globalThis.crypto?.randomUUID) return globalThis.crypto.randomUUID();
  nodeCrypto ??= await import('node:crypto');
  return nodeCrypto.randomUUID();
}

// Retry-After: seconds or an HTTP date. Returns the instant in ms, or null.
function retryAfter(headers) {
  const v = headers.get('retry-after');
  if (!v) return null;
  if (/^\d+$/.test(v.trim())) return Date.now() + Number(v) * 1000;
  const t = Date.parse(v);
  return Number.isNaN(t) ? null : t;
}

class NetworkError extends Error {}
export class RestepError extends Error {
  constructor(status, problem, message) {
    super(message ?? `${status}${problem?.code ? ` ${problem.code}` : ''}${problem?.detail ? `: ${problem.detail}` : ''}`);
    this.status = status;
    this.problem = problem ?? null;
  }
}

// ── Client ────────────────────────────────────────────────────────────────────
export async function openRestepSync(options) {
  const db = new RestepSync(options);
  await db._open();
  return db;
}

class RestepSync {
  constructor({ url, collections, storage = memoryStorage(), interval = 30000, onConflict = 'server',
    token, headers = {}, fetch: fetchFn = globalThis.fetch, timeout = 15000, retry = {}, autoPush = true, language = null } = {}) {
    if (!url) throw new Error('restep-sync: missing url, the API root.');
    if (!Array.isArray(collections) || !collections.length) throw new Error('restep-sync: collections must be a list of collection names.');
    this.root = new URL(String(url).endsWith('/') ? url : `${url}/`);
    this.names = collections;
    this.storage = storage;
    this.interval = interval;
    this.onConflict = onConflict;
    this.token = token;
    this.extraHeaders = headers;
    this.fetchFn = fetchFn;
    this.timeout = timeout;
    this.retry = { base: 1000, max: 60000, ...retry };
    this.autoPush = autoPush;
    // §10: a copy is in one language, the one asked for with Accept-Language (null: the API's default).
    this.language = language ?? null;
    this.listeners = new Map();
    this.handlers = new Map(); // opId → conflict function (functions are not saved in the store)
    this.views = new Map();
    this.chain = Promise.resolve();
    this.saving = Promise.resolve();
    this.holdUntil = 0; // Retry-After from a read: nothing is requested again before then
    this.closed = false;
  }

  // ── Start-up ──
  async _open() {
    const saved = await this.storage.read();
    this.data = saved?.version === VERSION && saved.root === this.root.href ? saved
      : { version: VERSION, root: this.root.href, discovery: null, collections: {}, queue: [], aliases: {} };
    try {
      this.data.discovery = await this._discover();
    } catch (e) {
      if (!(e instanceof NetworkError)) throw e;
      if (!this.data.discovery) throw new Error(`restep-sync: no connection to ${this.root.href} and no local copy of the discovery document.`);
    }
    const d = this.data.discovery;
    // §4.2: without a discovery document, core only; collections are at /<name>.
    const core = d.restep === undefined;
    const profiles = core ? ['core'] : d.profiles ?? [];
    const knownText = KNOWN_TEXTS.includes(d.restep);
    this.writes = profiles.includes('writes');
    this.anonymousRefused = !core && isObject(d.idempotency) && d.idempotency.anonymous === false;
    // §5.7: a key may be repeated only within idempotency.ttl of its first attempt.
    this.ttl = !core && Number.isInteger(d.idempotency?.ttl) && d.idempotency.ttl > 0 ? d.idempotency.ttl : DEFAULT_TTL;
    for (const name of this.names) {
      const c = core ? { path: `/${name}` } : d.collections?.[name];
      if (!isObject(c)) throw new Error(`restep-sync: the API does not declare the collection ${name}.`);
      const path = typeof c.path === 'string' ? c.path : `/${name}`;
      if (/[{}]/.test(path)) throw new Error(`restep-sync: ${name} is a subcollection (${path}); this client only replicates collections without a template.`);
      // §4.3 and §5.2: a collection without GET cannot be listed, and one with a required filter has no
      // request for a full copy (§6.1): neither can be copied.
      if (Array.isArray(c.methods) && !c.methods.includes('GET'))
        throw new Error(`restep-sync: ${name} does not support GET (§4.3), so it cannot be copied.`);
      const required = [...Object.entries(isObject(c.filter) ? c.filter : {}), ...Object.entries(isObject(c.named_filters) ? c.named_filters : {})]
        .filter(([, f]) => isObject(f) && f.required === true).map(([k]) => k);
      if (required.length) throw new Error(`restep-sync: ${name} requires the filter ${required.join(', ')} (§6.1), so it has no request for a full copy (§5.2).`);
      // §10: a collection is synced if it declares sync with the advertised profile; only then does it answer :changes.
      const synced = profiles.includes('sync') && isObject(c.sync);
      if (synced && knownText && !c.filter?.id?.ops?.includes('in'))
        throw new Error(`restep-sync: ${name} declares sync but not filter id with in, which sync needs (§10).`);
      const local = this.data.collections[name] ??= { state: null, items: {} };
      local.path = path;
      // Without sync (or with a sync text this client does not know, §1.2), every synchronisation reloads the whole collection.
      local.mode = synced && knownText ? 'sync' : 'reload';
      local.why = local.mode === 'sync' ? null : !synced ? 'does not declare sync' : `the sync text of ${d.restep} is not one this client implements`;
      local.maxLimit = Number.isInteger(c.max_limit) ? c.max_limit : null;
      local.idFrom = c.id_from ?? 'server';
      // §5.1: a collection with upload and without schemas.create takes only files: a JSON POST there
      // is no creation (where its media_types admit application/json, the JSON would be stored as a file).
      local.onlyUploads = isObject(c.upload) && (c.schemas?.create === undefined || c.schemas?.create === null);
      local.collations = isObject(c.collations) ? c.collations : {};
      // §5.2 and §10: a copy turns off every default filter (a switch declared "default": true).
      local.defaults = Object.entries(isObject(c.named_filters) ? c.named_filters : {})
        .filter(([, f]) => isObject(f) && f.default === true && f.type === undefined).map(([k]) => k);
      // §5.1: in a collection with reusable_ids an id can come back as another item.
      local.reusableIds = c.reusable_ids === true;
      // §10: a copy made in another language is traversed again before it counts as current.
      if (local.language !== undefined && local.language !== this.language) local.state = null;
      local.language = this.language;
    }
    await this._save();
    if (this.interval > 0) {
      this.timer = setInterval(() => { this.sync({ background: true }).catch(() => {}); }, this.interval);
      this.timer.unref?.();
    }
    if (this.autoPush && this.data.queue.length) this._kick();
  }

  // GET the root; if it is not a discovery document, the first service-desc that is (§4.2).
  // Returns the document, or {} if there is none (core only).
  async _discover() {
    const r = await this._request('GET', '');
    if (TRANSIENT.has(r.status) && this.data.discovery) throw new NetworkError(`${r.status}`);
    const isDiscovery = (x) => x.status >= 200 && x.status < 300 && isObject(x.body) && typeof x.body.restep === 'string';
    let doc = isDiscovery(r) ? r.body : null;
    if (!doc) {
      for (const target of linkTargets(r.headers.get('link'), 'service-desc')) {
        const t = await this._request('GET', new URL(target, r.url).href).catch((e) => { if (e instanceof NetworkError) throw e; return null; });
        if (t && isDiscovery(t)) { doc = t.body; break; }
      }
    }
    if (doc) {
      // §1.1: a restep that does not follow the grammar, or of another major version, is not a Restep 1
      // document, and core only cannot be assumed with it either.
      const v = RESTEP_VERSION.exec(doc.restep);
      if (!v || v[1] !== '1') throw new Error(`restep-sync: the API declares "restep": ${JSON.stringify(doc.restep)}, which is not Restep 1 (§1.1).`);
      return doc;
    }
    // §4.2: only a 2xx without restep, or a 404, means core only; any other error says nothing.
    if (!(r.status >= 200 && r.status < 300) && r.status !== 404) throw new RestepError(r.status, r.body, `restep-sync: the API root answered ${r.status}.`);
    return {}; // §4.2: no document, core only
  }

  // 'sync' or 'reload': how each collection is fetched.
  mode(name) { return this._coll(name).mode; }

  // §10: changing language makes every copy stale; the next sync traverses each collection again.
  async setLanguage(language) {
    this.language = language ?? null;
    for (const name of this.names) {
      const c = this._coll(name);
      if (c.language !== this.language) { c.state = null; c.language = this.language; }
    }
    await this._save();
  }

  close() {
    this.closed = true;
    clearInterval(this.timer);
    return this.chain.then(() => this.saving);
  }

  // ── Events: change, conflict, error ──
  on(event, fn) {
    if (!this.listeners.has(event)) this.listeners.set(event, new Set());
    this.listeners.get(event).add(fn);
    return () => this.off(event, fn);
  }
  off(event, fn) { this.listeners.get(event)?.delete(fn); }
  _emit(event, payload) {
    for (const fn of this.listeners.get(event) ?? []) {
      try { fn(payload); } catch (e) { if (event !== 'error') this._emit('error', { error: e, during: event }); }
    }
  }

  // ── Local reads ──
  _coll(name) {
    const c = this.data.collections[name];
    if (!c) throw new Error(`restep-sync: ${name} is not one of the open collections (${this.names.join(', ')}).`);
    return c;
  }
  _view(name) {
    let view = this.views.get(name);
    if (view) return view;
    view = new Map(Object.entries(this._coll(name).items).map(([id, r]) => [id, r.data]));
    for (const op of this.data.queue) {
      if (op.collection !== name) continue;
      if (op.type === 'create') view.set(op.id, { ...op.body, id: op.id });
      else if (op.type === 'update') { if (view.has(op.id)) view.set(op.id, { ...mergePatch(view.get(op.id), op.patch), id: op.id }); }
      else view.delete(op.id);
    }
    this.views.set(name, view);
    return view;
  }
  _changed() { this.views.clear(); }
  resolveId(name, id) {
    this._coll(name);
    return this.data.aliases[`${name}/${id}`] ?? id;
  }

  // filter: object of equalities ({ category: 'poesia' }) or function. sort: 'field', '-field', a list
  // of them or a comparison function. limit and offset trim the result.
  // Strings are sorted by code points (§6.1). A field with another collation ("server" or one that is
  // not known) is neither sorted nor compared locally: §6.1 forbids reproducing that equality or order.
  list(name, { filter, sort, limit, offset = 0 } = {}) {
    let items = [...this._view(name).values()];
    const collations = this._coll(name).collations ?? {};
    const local = (k, what) => {
      if (collations[k] !== undefined && collations[k] !== 'codepoint')
        throw new Error(`restep-sync: ${name}.${k} is compared with the server's collation (${JSON.stringify(collations[k])}): it cannot be ${what} locally (§6.1); pass a function.`);
    };
    if (typeof filter === 'function') items = items.filter(filter);
    else if (isObject(filter)) {
      for (const [k, v] of Object.entries(filter)) if (typeof v === 'string') local(k, 'filtered by');
      items = items.filter((it) => Object.entries(filter).every(([k, v]) => sameData(it[k], v)));
    }
    if (typeof sort === 'function') items.sort(sort);
    else if (sort) {
      const keys = (Array.isArray(sort) ? sort : String(sort).split(',')).map((s) => [s.replace(/^-/, ''), s.startsWith('-') ? -1 : 1]);
      for (const [k] of keys) local(k, 'sorted by');
      items.sort((a, b) => {
        for (const [k, dir] of keys) {
          const x = a[k], y = b[k];
          if (x == null || y == null) { if (x == null && y == null) continue; return x == null ? 1 : -1; }
          const d = typeof x === 'number' && typeof y === 'number' ? x - y : codepoints(String(x), String(y));
          if (d) return d * dir;
        }
        return 0;
      });
    }
    return clone(items.slice(offset, limit === undefined ? undefined : offset + limit));
  }
  get(name, id) { return clone(this._view(name).get(this.resolveId(name, id))) ?? null; }
  pending() {
    return this.data.queue.map(({ opId, type, collection, id, body, patch, attempts, lastError, nextAt, retryAt, uncertain }) =>
      clone({ opId, type, collection, id, body, patch, attempts, lastError, nextAt, retryAt, ...(uncertain && { uncertain }) }));
  }

  // ── Optimistic writes ──
  // §5.1: with id_from "client", the client chooses the id (a UUID v4, or the id in the body); with "either"
  // only if the body brings one; with "server", undeclared or an unknown value, it is never sent.
  async create(name, body) {
    const c = this._coll(name);
    if (!isObject(body)) throw new Error('restep-sync: create needs an object.');
    if (c.onlyUploads) throw new Error(`restep-sync: ${name} declares upload without create: it takes only files, which this client does not send, not JSON (§5.1).`);
    const { id: chosen, ...rest } = body;
    const own = c.idFrom === 'client' || c.idFrom === 'either';
    if (chosen !== undefined && !own) throw new Error(`restep-sync: ${name} does not let the client choose the id (id_from ${JSON.stringify(c.idFrom)}, §5.1).`);
    if (chosen !== undefined && (typeof chosen !== 'string' || !/^[A-Za-z0-9._~-]+$/.test(chosen) || chosen === '.' || chosen === '..'))
      throw new Error(`restep-sync: ${JSON.stringify(chosen)} is not a valid id (§3).`);
    if (chosen !== undefined && this._view(name).has(chosen)) throw new Error(`restep-sync: there is already a ${name} ${chosen}.`);
    const clientId = chosen ?? (c.idFrom === 'client' ? await uuid() : null);
    const id = clientId ?? `local-${await uuid()}`;
    const op = { opId: await uuid(), type: 'create', collection: name, id, body: clone(rest), key: await uuid(), attempts: 0,
      ...(clientId && { clientId: true }) };
    return this._enqueue(op, { created: [id] }, id);
  }
  async update(name, id, patch, { onConflict } = {}) {
    id = this.resolveId(name, id);
    if (!this._view(name).has(id)) throw new Error(`restep-sync: there is no ${name} ${id} in the local copy.`);
    if (!isObject(patch)) throw new Error('restep-sync: update needs an object (Merge Patch).');
    const op = { opId: await uuid(), type: 'update', collection: name, id, patch: clone(patch), base: this._base(name, id), attempts: 0 };
    this._strategy(op, onConflict);
    return this._enqueue(op, { updated: [id] }, id);
  }
  async delete(name, id, { onConflict } = {}) {
    id = this.resolveId(name, id);
    if (!this._view(name).has(id)) throw new Error(`restep-sync: there is no ${name} ${id} in the local copy.`);
    const op = { opId: await uuid(), type: 'delete', collection: name, id, base: this._base(name, id), attempts: 0 };
    this._strategy(op, onConflict);
    await this._enqueue(op, { destroyed: [id] });
  }
  // The version the change was made on: the confirmed one, unless another operation is pending on the
  // same item; then it is fixed when that one finishes (null).
  _base(name, id) {
    if (this.data.queue.some((o) => o.collection === name && o.id === id)) return null;
    const r = this._coll(name).items[id];
    return r ? clone(r) : null;
  }
  _strategy(op, onConflict) {
    if (typeof onConflict === 'function') this.handlers.set(op.opId, onConflict);
    else if (onConflict) op.onConflict = onConflict;
  }
  async _enqueue(op, change, id) {
    this.data.queue.push(op);
    this._changed();
    await this._save();
    this._emit('change', { collection: op.collection, source: 'local', created: [], updated: [], destroyed: [], ...change });
    if (this.autoPush) this._kick();
    return id === undefined ? undefined : this.get(op.collection, id);
  }
  _kick() { setTimeout(() => { if (!this.closed) this.sync({ pull: false }).catch(() => {}); }, 0); }

  _save() {
    const snapshot = clone(this.data);
    this.saving = this.saving.then(() => this.storage.write(snapshot)).catch((e) => this._emit('error', { error: e, during: 'save' }));
    return this.saving;
  }

  // ── HTTP ──
  async _request(method, path, { body, headers = {}, json } = {}) {
    // Paths are relative to the API root (§2); './' stops «books:changes» from being read as a scheme.
    // A link (next, Location) already arrives resolved as an absolute URL.
    const url = /^[a-z][a-z0-9+.-]*:/i.test(path) ? new URL(path) : new URL(`./${path.replace(/^\//, '')}`, this.root);
    // §3: a link with a scheme other than https or the root's is not followed.
    if (url.protocol !== 'https:' && url.protocol !== this.root.protocol)
      throw new RestepError(0, null, `restep-sync: not following ${url.href}: its scheme is neither https nor the API's (§3).`);
    // §4.3: the API's credentials (the token and the extra headers) only go to its origin.
    const mine = url.origin === this.root.origin;
    const h = { accept: 'application/json', ...(this.language && { 'accept-language': this.language }), ...(mine && this.extraHeaders), ...headers };
    const token = mine && (typeof this.token === 'function' ? await this.token() : this.token);
    if (token) h.authorization = `Bearer ${token}`;
    if (body !== undefined) h['content-type'] ??= json ?? 'application/json';
    let res, text;
    try {
      res = await this.fetchFn(url, { method, headers: h, body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(this.timeout) });
      text = await res.text();
    } catch (e) {
      throw new NetworkError(`restep-sync: no connection to ${url.origin} (${e.cause?.code ?? e.name}).`);
    }
    let parsed;
    try { parsed = text ? JSON.parse(text) : undefined; } catch { parsed = undefined; }
    return { status: res.status, headers: res.headers, body: parsed, url };
  }

  // ── Synchronisation ──
  // Sends the queue and then pulls each collection's changes. It is serialised: if one is already
  // running, this one starts when it finishes. Without a connection it does not throw: it returns the
  // report with online: false. background: true honours the wait between retries; on demand it retries
  // at once, except what the server asked for with Retry-After. pull: false only sends the queue.
  // retryUncertain: true sends again a create whose outcome is unknown and that is not safe to repeat.
  sync({ background = false, pull = true, retryUncertain = false } = {}) {
    const run = this.chain.then(() => this._syncOnce({ background, pull, retryUncertain }));
    this.chain = run.catch(() => {});
    return run;
  }

  async _syncOnce({ background, pull, retryUncertain }) {
    const report = { online: true, pushed: 0, dropped: 0, conflicts: 0, pending: 0, pulled: {}, reset: [], errors: [] };
    if (this.closed) return { ...report, pending: this.data.queue.length };
    try {
      await this._push(report, { force: !background, retryUncertain });
      if (pull && report.online) {
        if (Date.now() < this.holdUntil) report.waitingUntil = this.holdUntil;
        else for (const name of this.names) await this._pull(name, report);
      }
    } catch (e) {
      if (e instanceof NetworkError) report.online = false;
      else if (e instanceof RestepError && TRANSIENT.has(e.status)) report.waitingUntil = this.holdUntil;
      else { report.errors.push(e); this._emit('error', { error: e, during: 'sync' }); }
    }
    report.pending = this.data.queue.length;
    return report;
  }

  // ── Sending the queue ──
  async _push(report, { force, retryUncertain }) {
    while (this.data.queue.length && !this.closed) {
      const op = this.data.queue[0];
      const now = Date.now();
      if (op.uncertain && !retryUncertain) { report.uncertain = op.opId; return; } // the queue waits for a decision
      if (op.retryAt > now) { report.waitingUntil = op.retryAt; return; } // the server asked for it
      if (!force && op.nextAt > now) { report.waitingUntil = op.nextAt; return; }
      // §5.7: past idempotency.ttl a create whose outcome is unknown is not repeated automatically.
      if (op.type === 'create' && op.attempts > 0 && !op.uncertain && !retryUncertain && !this._repeatable(op)) {
        this._uncertain(op, report, `its Idempotency-Key is older than idempotency.ttl (${this.ttl} s)`);
        await this._save();
        return;
      }
      // Sent again by decision of the application: a new key, since the old one may be forgotten.
      if (op.uncertain && op.type === 'create') { op.key = await uuid(); delete op.firstAt; }
      delete op.uncertain;
      let result;
      try {
        result = await this._send(op, report);
      } catch (e) {
        if (!(e instanceof NetworkError)) throw e;
        this._schedule(op, null, e.message);
        if (op.type === 'create' && !this._repeatable(op)) this._uncertain(op, report, e.message);
        await this._save();
        report.online = false;
        return;
      }
      if (result === 'retry') { report.waitingUntil = op.retryAt ?? op.nextAt; return; }
    }
  }

  // §5.7: a POST not known to have had an effect is only repeated if it carries an Idempotency-Key and
  // the API advertises writes, or if it creates with a client-chosen id, which cannot be created twice.
  _keys() { return this.writes && !(this.anonymousRefused && !this._credentials()); }
  _credentials() {
    const names = Object.keys(this.extraHeaders).map((k) => k.toLowerCase());
    return !!this.token || names.includes('authorization') || names.includes('cookie');
  }
  // §5.1: in a collection with reusable_ids a client-chosen id protects nothing once its item is deleted.
  // §5.7: the key protects only within idempotency.ttl of the first attempt.
  _repeatable(op) {
    if (op.clientId && !this._coll(op.collection).reusableIds) return true;
    return this._keys() && (op.firstAt === undefined || Date.now() - op.firstAt < this.ttl * 1000);
  }
  _uncertain(op, report, message) {
    op.uncertain = true;
    report.uncertain = op.opId;
    const error = new RestepError(0, null, `restep-sync: it is unknown whether the create of ${op.collection} ${op.id} arrived (${message}) and repeating it could duplicate it; use sync({ retryUncertain: true }) or discard().`);
    report.errors.push(error);
    this._emit('error', { error, operation: clone(op) });
  }
  // Removes an operation from the queue (for example an uncertain create) and undoes its local change.
  async discard(opId) {
    const op = this.data.queue.find((o) => o.opId === opId);
    if (!op) return false;
    this._drop(op, true);
    await this._save();
    this._emit('change', { collection: op.collection, source: 'rollback', created: [],
      updated: op.type === 'create' ? [] : [op.id], destroyed: op.type === 'create' ? [op.id] : [] });
    return true;
  }

  _schedule(op, res, message) {
    op.attempts = (op.attempts ?? 0) + 1;
    op.lastError = message;
    const delay = Math.min(this.retry.max, this.retry.base * 2 ** (op.attempts - 1)) * (0.5 + Math.random() / 2);
    op.nextAt = Date.now() + delay;
    op.retryAt = res ? retryAfter(res.headers) : null;
  }

  async _send(op, report) {
    const c = this._coll(op.collection);
    for (let round = 0; ; round++) {
      const res = op.type === 'create' ? await this._sendCreate(op, c) : await this._sendChange(op, c);
      if (res.status >= 200 && res.status < 300) return this._confirm(op, c, res, report);
      // §5.1: a create with its own id that is repeated after the response was lost gets 409 taken; the
      // item is read to find out whether the first attempt arrived.
      if (op.type === 'create' && op.clientId && op.attempts > 0 && res.status === 409
        && (res.body?.errors ?? []).some((e) => e?.pointer === '/id' && e?.code === 'taken')) {
        const read = await this._read(c, op.id);
        if (read) return this._confirm(op, c, { status: 200, body: { ...read.data, _etag: read.etag }, headers: new Headers(read.etag ? { etag: read.etag } : {}) }, report);
      }
      if (TRANSIENT.has(res.status) || (res.status === 409 && res.body?.code === IN_PROGRESS)) {
        this._schedule(op, res, `${res.status} ${res.body?.code ?? ''}`.trim());
        if (op.type === 'create' && !NO_EFFECT.has(res.status) && !this._repeatable(op)) this._uncertain(op, report, `${res.status}`);
        await this._save();
        return 'retry';
      }
      if (op.type !== 'create' && (res.status === 412 || res.status === 404 || res.conflict)) {
        if (res.status === 404 && op.type === 'delete') return this._confirm(op, c, res, report); // it was already gone
        const current = res.status === 404 ? null : res.current ?? await this._read(c, op.id);
        if (current && round < MAX_CONFLICT_ROUNDS) {
          const again = await this._resolve(op, c, current, report);
          if (again) continue;
          return 'done';
        }
        await this._resolve(op, c, current, report, 'server');
        return 'done';
      }
      // An error that retrying will not fix: the operation is removed and its local change undone.
      const error = new RestepError(res.status, res.body);
      this._drop(op, true);
      report.dropped++;
      report.errors.push(error);
      await this._save();
      this._emit('error', { error, operation: clone(op) });
      this._emit('change', { collection: op.collection, source: 'rollback', created: [],
        updated: op.type === 'update' || op.type === 'delete' ? [op.id] : [], destroyed: op.type === 'create' ? [op.id] : [] });
      return 'done';
    }
  }

  _sendCreate(op, c) {
    op.firstAt ??= Date.now();
    const body = op.clientId ? { ...op.body, id: op.id } : op.body;
    return this._request('POST', c.path, { body, headers: this._keys() ? { 'idempotency-key': `"${op.key}"` } : {} });
  }

  // update and delete with If-Match. The ETag of the starting version is the _etag the listing brought
  // (§7.2). Only if it is missing (a server that does not set it) is the item read: if it is still the
  // same as the version the user saw, its ETag is used; if not, it is a conflict, without writing.
  async _sendChange(op, c) {
    op.base ??= clone(c.items[op.id] ?? null);
    if (!op.base) return { status: 404 };
    if (!op.base.etag) {
      const current = await this._read(c, op.id);
      if (!current) return { status: 404 };
      // §5.1: with reusable_ids the same data may be another incarnation; only an ETag tells them apart.
      if (!sameData(current.data, op.base.data) || c.reusableIds) return { status: 412, conflict: true, current };
      op.base.etag = current.etag;
    }
    const path = `${c.path}/${encodeURIComponent(op.id)}`;
    const headers = { 'if-match': op.base.etag };
    return op.type === 'update'
      ? this._request('PATCH', path, { body: op.patch, headers, json: 'application/merge-patch+json' })
      : this._request('DELETE', path, { headers });
  }

  async _read(c, id) {
    const r = await this._request('GET', `${c.path}/${encodeURIComponent(id)}`);
    if (r.status === 404 || r.status === 410) return null;
    if (r.status !== 200) throw new RestepError(r.status, r.body);
    const { _etag, ...data } = r.body ?? {};
    return { data, etag: r.headers.get('etag') ?? _etag ?? null };
  }

  async _confirm(op, c, res, report) {
    const q = this.data.queue;
    q.splice(q.indexOf(op), 1);
    this.handlers.delete(op.opId);
    let id = op.id;
    const change = { collection: op.collection, source: 'push', created: [], updated: [], destroyed: [] };
    if (op.type === 'delete') {
      delete c.items[id];
      change.destroyed.push(id);
    } else {
      let data = res.body, etag = res.headers?.get('etag') ?? null;
      // §7.1: a replay whose body is not the item's current representation carries no ETag, _etag or
      // Content-Location; such a body is not taken as the new copy, and the item is read instead.
      let replayedId = null;
      if (isObject(data) && res.headers?.get('idempotent-replayed') === 'true' && !etag
        && typeof data._etag !== 'string' && !res.headers.get('content-location')) {
        replayedId = typeof data.id === 'string' ? data.id : null;
        data = null;
      }
      if (op.type === 'create') {
        if (!isObject(data)) { // Prefer: return=minimal, a server that does not return the body, or a stale replay
          const loc = res.headers.get('location');
          id = loc ? decodeURIComponent(new URL(loc, res.url ?? this.root).pathname.split('/').pop()) : replayedId;
          const read = id && await this._read(c, id);
          if (read) ({ data, etag } = read);
        }
        id = data?.id ?? id;
        if (id !== op.id) { // a client-chosen id is already the final one
          this.data.aliases[`${op.collection}/${op.id}`] = id;
          for (const o of q) if (o.collection === op.collection && o.id === op.id) o.id = id;
          change.replaced = { [op.id]: id };
        }
        change.created.push(id);
      } else {
        if (!isObject(data)) { const read = await this._read(c, id); if (read) ({ data, etag } = read); }
        change.updated.push(id);
      }
      if (isObject(data)) c.items[id] = entry(data, etag);
    }
    // The next operation on the same item starts from the version that was just confirmed.
    const next = q.find((o) => o.collection === op.collection && o.id === id);
    if (next) next.base = clone(c.items[id] ?? null);
    report.pushed++;
    this._changed();
    await this._save();
    this._emit('change', change);
    return 'done';
  }

  // Returns true if the operation has to be sent again (the client wins, or a merge).
  async _resolve(op, c, current, report, forced) {
    report.conflicts++;
    const strategy = forced ?? this.handlers.get(op.opId) ?? op.onConflict ?? this.onConflict;
    const base = op.base?.data ?? null;
    const local = op.type === 'update' && current ? mergePatch(current.data, op.patch) : null;
    let decision = strategy;
    if (typeof strategy === 'function') {
      decision = await strategy({ collection: op.collection, id: op.id, type: op.type, base: clone(base),
        server: clone(current?.data ?? null), local: clone(local), patch: clone(op.patch) });
    }
    if (!current) decision = 'server'; // it no longer exists on the server: nothing to win
    const resolution = isObject(decision) && op.type === 'update' ? 'merge' : decision === 'client' ? 'client' : 'server';
    this._emit('conflict', { collection: op.collection, id: op.id, type: op.type, resolution,
      base: clone(base), server: clone(current?.data ?? null), local: clone(local) });
    if (current) c.items[op.id] = clone(current); else delete c.items[op.id];
    if (resolution === 'server') {
      this._drop(op, !current);
      this._changed();
      await this._save();
      this._emit('change', { collection: op.collection, source: 'conflict', created: [],
        updated: current ? [op.id] : [], destroyed: current ? [] : [op.id] });
      return false;
    }
    if (resolution === 'merge') op.patch = clone(decision);
    op.base = clone(current);
    this._changed();
    await this._save();
    return true;
  }

  // Removes an operation from the queue. With dependents, also the later ones on the same item
  // (a create that fails takes its update and delete with it); otherwise the next one starts from the confirmed copy.
  _drop(op, dependents) {
    const q = this.data.queue;
    q.splice(q.indexOf(op), 1);
    this.handlers.delete(op.opId);
    const same = (o) => o.collection === op.collection && o.id === op.id;
    if (dependents || op.type === 'create') {
      for (const o of q.filter(same)) { q.splice(q.indexOf(o), 1); this.handlers.delete(o.opId); }
    } else {
      const next = q.find(same);
      if (next) next.base = clone(this._coll(op.collection).items[op.id] ?? null);
    }
    this._changed();
  }

  // ── Receiving changes (§10) ──
  async _pull(name, report) {
    const c = this._coll(name);
    if (c.mode === 'reload' || !c.state) return this._reload(name, report);
    for (;;) {
      const r = await this._request('GET', `${c.path}:changes?since=${encodeURIComponent(c.state)}`);
      // 410: the server can no longer compute the changes. A since it does not recognise (another start,
      // another version) is treated the same way: the collection is reloaded.
      if (r.status === 410 || (r.status === 400 && r.body?.code === 'invalid_parameter' && r.body?.parameter === 'since')) {
        report.reset.push(name); return this._reload(name, report);
      }
      this._check(r);
      const { created = [], updated = [], destroyed = [], state, has_more } = r.body ?? {};
      if (typeof state !== 'string') throw new RestepError(r.status, null, `restep-sync: ${name}:changes does not return state.`);
      const wanted = [...created, ...updated];
      const fetched = await this._fetchIds(c, wanted);
      const change = { collection: name, source: 'remote', created: [], updated: [], destroyed: [] };
      for (const id of wanted) {
        if (fetched.has(id)) {
          const data = fetched.get(id);
          const old = c.items[id];
          // §5.1 and §10: with reusable_ids, an id in created is another item, never the one kept.
          const reborn = c.reusableIds && created.includes(id);
          (old && !reborn ? change.updated : change.created).push(id);
          c.items[id] = entry(data, old && !c.reusableIds && sameData(old.data, data) ? old.etag : null);
        } else if (c.items[id]) { delete c.items[id]; change.destroyed.push(id); } // no longer visible
      }
      for (const id of destroyed) if (c.items[id]) { delete c.items[id]; change.destroyed.push(id); }
      c.state = state; // only after applying the changes (§10)
      this._changed();
      await this._save();
      const pulled = report.pulled[name] ??= { created: 0, updated: 0, destroyed: 0 };
      for (const k of ['created', 'updated', 'destroyed']) pulled[k] += change[k].length;
      if (change.created.length + change.updated.length + change.destroyed.length) this._emit('change', change);
      if (!has_more) return;
    }
  }

  _check(r) {
    if (r.status === 200) return;
    if (r.status === 429 || r.status === 503) this.holdUntil = retryAfter(r.headers) ?? Date.now() + this.retry.base;
    throw new RestepError(r.status, r.body);
  }

  // Requests items by id with filter[id][in] (§10), in chunks that fit in a page. limit only with a
  // declared max_limit (§6.4); URLSearchParams writes a literal + as %2B (§5.2).
  async _fetchIds(c, ids) {
    const out = new Map();
    const size = Math.min(c.maxLimit ?? 50, 50);
    for (let i = 0; i < ids.length; i += size) {
      const chunk = ids.slice(i, i + size);
      // §10: filter[id][in] as the only filter, with every default filter turned off.
      const q = new URLSearchParams({ 'filter[id][in]': chunk.join(','), ...this._defaultsOff(c), ...(c.maxLimit && { limit: String(chunk.length) }) });
      for (const it of await this._walk(`${c.path}?${q}`)) out.set(it.id, it);
    }
    return out;
  }

  // Follows next until null (§5.2), as is: it is an absolute path or a URL, resolved against the request's
  // (§3). Returns the items and the state of the first page.
  async _walk(first, onFirst) {
    const items = [];
    for (let next = first, page = 0; next; page++) {
      const r = await this._request('GET', next);
      this._check(r);
      if (!Array.isArray(r.body?.items)) throw new RestepError(r.status, null, `restep-sync: ${next} is not a collection.`);
      if (page === 0) onFirst?.(r.body);
      items.push(...r.body.items);
      next = typeof r.body.next === 'string' ? new URL(r.body.next, r.url).href : null;
    }
    return items;
  }

  // §5.2 and §6.1: filter[name]=false turns a default filter off; a full copy turns off every one.
  _defaultsOff(c) { return Object.fromEntries((c.defaults ?? []).map((k) => [`filter[${k}]`, 'false'])); }

  async _reload(name, report) {
    const c = this._coll(name);
    let state = null;
    const params = new URLSearchParams({ ...this._defaultsOff(c), ...(c.maxLimit && { limit: String(c.maxLimit) }) });
    const q = params.toString() ? `?${params}` : '';
    const items = await this._walk(`${c.path}${q}`, (b) => { state = b.state ?? null; });
    if (c.mode === 'sync' && typeof state !== 'string')
      throw new RestepError(200, null, `restep-sync: ${name} does not return state although it declares sync (§10).`);
    const before = c.items;
    c.items = {};
    const change = { collection: name, source: 'remote', reload: true, created: [], updated: [], destroyed: [] };
    for (const data of items) {
      const old = before[data.id];
      c.items[data.id] = entry(data, old && !c.reusableIds && sameData(old.data, data) ? old.etag : null);
      if (!old) change.created.push(data.id);
      else if (!sameData(before[data.id].data, data)) change.updated.push(data.id);
    }
    for (const id of Object.keys(before)) if (!c.items[id]) change.destroyed.push(id);
    c.state = state;
    this._changed();
    await this._save();
    report.pulled[name] = { created: change.created.length, updated: change.updated.length, destroyed: change.destroyed.length };
    if (change.created.length + change.updated.length + change.destroyed.length) this._emit('change', change);
  }
}
