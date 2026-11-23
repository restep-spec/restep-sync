// restep-sync · cliente offline-first para APIs Restep 1 (https://restep.wubytes.com/1/, borrador 6).
// Mantiene una réplica local de una o varias colecciones al día con el perfil sync (§10) y guarda las
// escrituras en una cola que se envía cuando hay conexión, con las garantías de writes (§7):
// Idempotency-Key estable en cada create e If-Match en cada update y delete.
// Sin dependencias. Funciona en Node 18+ y en el navegador (fileStorage solo en Node; indexedDBStorage
// solo en el navegador).
//
//   import { openRestepSync, fileStorage } from './restep-sync.mjs';
//   const db = await openRestepSync({ url: 'http://localhost:8080/', collections: ['books'],
//     storage: fileStorage('./replica.json') });
//   db.list('books', { filter: { category: 'poesia' } });
//   await db.update('books', '7', { price: '15.00' });   // optimista: la vista local cambia ya
//   await db.sync();                                      // envía la cola y trae los cambios
//
// Modelo: por colección se guarda la copia confirmada por el servidor (datos y ETag de cada elemento)
// y el state de §9. Las escrituras pendientes van en una cola FIFO; lo que devuelven list() y get()
// es la copia confirmada con la cola aplicada encima. Así, un conflicto o un error permanente solo
// tienen que quitar la operación de la cola para deshacer el cambio optimista.

const VERSION = 1; // del formato guardado en el almacén
// §1.2: sync es experimental, así que solo se usa con una API que declare una versión cuyo texto de
// sync implementa este cliente; con cualquier otra, las colecciones se recargan enteras.
export const KNOWN_TEXTS = ['1.0-draft.6'];
// §1.1: la gramática de restep, sin ceros a la izquierda.
const RESTEP_VERSION = /^(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-(draft|rc)\.(0|[1-9]\d*))?$/;
const TRANSIENT = new Set([408, 425, 429, 500, 502, 503, 504]);
// §5.7: 429 y 503 (y 425, que el servidor da sin procesar) solo llegan a peticiones sin efecto; los
// demás no dicen nada del efecto, así que un create solo se repite si es seguro repetirlo.
const NO_EFFECT = new Set([425, 429, 503]);
// Un 409 solo se reintenta con code request_in_progress (la misma petición sigue en curso, §7.1),
// respetando Retry-After; cualquier otro 409 (conflict: un duplicado, una transición imposible) es permanente.
const IN_PROGRESS = 'request_in_progress';
const MAX_CONFLICT_ROUNDS = 3;

// ── Almacenes ─────────────────────────────────────────────────────────────────
// Un almacén es { read(): Promise<objeto|null>, write(objeto): Promise<void> }. Guarda la réplica
// entera como un único objeto JSON: sencillo y suficiente para colecciones de miles de elementos.

export function memoryStorage() {
  let data = null;
  return {
    async read() { return data === null ? null : structuredClone(data); },
    async write(value) { data = structuredClone(value); },
  };
}

// Archivo JSON (Node). Escribe en un temporal y lo renombra, para no dejar nunca un archivo a medias.
export function fileStorage(path) {
  const fs = () => import('node:fs/promises');
  return {
    async read() {
      try { return JSON.parse(await (await fs()).readFile(path, 'utf8')); } catch (e) {
        if (e.code === 'ENOENT') return null;
        throw new Error(`restep-sync: no se puede leer ${path}: ${e.message}`);
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

// IndexedDB (navegador). Un solo registro con la réplica entera, en la clave key del almacén de
// objetos store de la base dbName. Delgada a propósito: no está probada en esta versión (las pruebas
// corren en Node, sin navegador).
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

// ── Utilidades ────────────────────────────────────────────────────────────────
const isObject = (v) => v !== null && typeof v === 'object' && !Array.isArray(v);
function mergePatch(target, patch) { // RFC 7396
  if (!isObject(patch)) return patch;
  const out = isObject(target) ? { ...target } : {};
  for (const [k, v] of Object.entries(patch)) {
    if (v === null) delete out[k]; else out[k] = mergePatch(out[k], v);
  }
  return out;
}
// Igualdad como valores JSON, sin contar el orden de los miembros ni _etag (que no es del elemento).
const canonical = (v) => (Array.isArray(v) ? `[${v.map(canonical).join(',')}]`
  : isObject(v) ? `{${Object.keys(v).filter((k) => k !== '_etag').sort().map((k) => `${JSON.stringify(k)}:${canonical(v[k])}`).join(',')}}`
  : JSON.stringify(v));
const sameData = (a, b) => canonical(a) === canonical(b);
function codepoints(a, b) { // §6.1: por puntos de código, no por unidades UTF-16 ni por idioma
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
// Lo que se guarda de un elemento: sus datos sin _etag (que no es del elemento, §7.2) y su ETag, la del
// _etag que trae el listado o, si falta, la que se conozca por otra vía.
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

// Retry-After: segundos o fecha HTTP. Devuelve el instante en ms, o null.
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

// ── Cliente ───────────────────────────────────────────────────────────────────
export async function openRestepSync(options) {
  const db = new RestepSync(options);
  await db._open();
  return db;
}

class RestepSync {
  constructor({ url, collections, storage = memoryStorage(), interval = 30000, onConflict = 'server',
    token, headers = {}, fetch: fetchFn = globalThis.fetch, timeout = 15000, retry = {}, autoPush = true } = {}) {
    if (!url) throw new Error('restep-sync: falta url, la raíz de la API.');
    if (!Array.isArray(collections) || !collections.length) throw new Error('restep-sync: collections tiene que ser una lista de nombres de colección.');
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
    this.listeners = new Map();
    this.handlers = new Map(); // opId → función de conflicto (las funciones no se guardan en el almacén)
    this.views = new Map();
    this.chain = Promise.resolve();
    this.saving = Promise.resolve();
    this.holdUntil = 0; // Retry-After de una lectura: no se vuelve a pedir nada antes
    this.closed = false;
  }

  // ── Arranque ──
  async _open() {
    const saved = await this.storage.read();
    this.data = saved?.version === VERSION && saved.root === this.root.href ? saved
      : { version: VERSION, root: this.root.href, discovery: null, collections: {}, queue: [], aliases: {} };
    try {
      this.data.discovery = await this._discover();
    } catch (e) {
      if (!(e instanceof NetworkError)) throw e;
      if (!this.data.discovery) throw new Error(`restep-sync: sin conexión con ${this.root.href} y sin copia local del descubrimiento.`);
    }
    const d = this.data.discovery;
    // §4.2: sin documento de descubrimiento, solo core; las colecciones están en /<nombre>.
    const core = d.restep === undefined;
    const profiles = core ? ['core'] : d.profiles ?? [];
    const knownText = KNOWN_TEXTS.includes(d.restep);
    this.writes = profiles.includes('writes');
    this.anonymousRefused = !core && isObject(d.idempotency) && d.idempotency.anonymous === false;
    for (const name of this.names) {
      const c = core ? { path: `/${name}` } : d.collections?.[name];
      if (!isObject(c)) throw new Error(`restep-sync: la API no declara la colección ${name}.`);
      const path = typeof c.path === 'string' ? c.path : `/${name}`;
      if (/[{}]/.test(path)) throw new Error(`restep-sync: ${name} es una subcolección (${path}); este cliente solo replica colecciones sin plantilla.`);
      // §10: una colección está sincronizada si declara sync con el perfil anunciado; solo entonces responde :changes.
      const synced = profiles.includes('sync') && isObject(c.sync);
      if (synced && knownText && !c.filter?.id?.ops?.includes('in'))
        throw new Error(`restep-sync: ${name} declara sync pero no filter id con in, que sync necesita (§10).`);
      const local = this.data.collections[name] ??= { state: null, items: {} };
      local.path = path;
      // Sin sync (o con un texto de sync que este cliente no conoce, §1.2), cada sincronización recarga la colección entera.
      local.mode = synced && knownText ? 'sync' : 'reload';
      local.why = local.mode === 'sync' ? null : !synced ? 'no declara sync' : `el texto de sync de ${d.restep} no es uno que este cliente implemente`;
      local.maxLimit = Number.isInteger(c.max_limit) ? c.max_limit : null;
      local.idFrom = c.id_from ?? 'server';
      local.collations = isObject(c.collations) ? c.collations : {};
    }
    await this._save();
    if (this.interval > 0) {
      this.timer = setInterval(() => { this.sync({ background: true }).catch(() => {}); }, this.interval);
      this.timer.unref?.();
    }
    if (this.autoPush && this.data.queue.length) this._kick();
  }

  // GET a la raíz; si no es un documento de descubrimiento, el primer service-desc que lo sea (§4.2).
  // Devuelve el documento, o {} si no hay ninguno (solo core).
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
      // §1.1: un restep que no sigue la gramática, o de otra versión mayor, no es un documento de Restep 1,
      // y con él tampoco se puede suponer solo core.
      const v = RESTEP_VERSION.exec(doc.restep);
      if (!v || v[1] !== '1') throw new Error(`restep-sync: la API declara "restep": ${JSON.stringify(doc.restep)}, que no es Restep 1 (§1.1).`);
      return doc;
    }
    if (r.status === 401 || r.status === 403 || r.status >= 500) throw new RestepError(r.status, r.body, `restep-sync: la raíz de la API respondió ${r.status}.`);
    return {}; // §4.2: sin documento, solo core
  }

  // 'sync' o 'reload': cómo se trae cada colección.
  mode(name) { return this._coll(name).mode; }

  close() {
    this.closed = true;
    clearInterval(this.timer);
    return this.chain.then(() => this.saving);
  }

  // ── Eventos: change, conflict, error ──
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

  // ── Lectura local ──
  _coll(name) {
    const c = this.data.collections[name];
    if (!c) throw new Error(`restep-sync: ${name} no es una de las colecciones abiertas (${this.names.join(', ')}).`);
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

  // filter: objeto de igualdades ({ category: 'poesia' }) o función. sort: 'campo', '-campo', lista
  // de ellos o función de comparación. limit y offset recortan el resultado.
  // Los textos se ordenan por puntos de código (§6.1). Un campo con otra colación ("server" o una que
  // no se conoce) no se ordena ni se compara en local: §6.1 prohíbe reproducir esa igualdad o ese orden.
  list(name, { filter, sort, limit, offset = 0 } = {}) {
    let items = [...this._view(name).values()];
    const collations = this._coll(name).collations ?? {};
    const local = (k, what) => {
      if (collations[k] !== undefined && collations[k] !== 'codepoint')
        throw new Error(`restep-sync: ${name}.${k} se compara con la colación del servidor (${JSON.stringify(collations[k])}): no se puede ${what} en local (§6.1); pasa una función.`);
    };
    if (typeof filter === 'function') items = items.filter(filter);
    else if (isObject(filter)) {
      for (const [k, v] of Object.entries(filter)) if (typeof v === 'string') local(k, 'filtrar por él');
      items = items.filter((it) => Object.entries(filter).every(([k, v]) => sameData(it[k], v)));
    }
    if (typeof sort === 'function') items.sort(sort);
    else if (sort) {
      const keys = (Array.isArray(sort) ? sort : String(sort).split(',')).map((s) => [s.replace(/^-/, ''), s.startsWith('-') ? -1 : 1]);
      for (const [k] of keys) local(k, 'ordenar por él');
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

  // ── Escritura optimista ──
  // §5.1: con id_from "client", el id lo elige el cliente (un UUID v4, o el id del cuerpo); con "either"
  // solo si el cuerpo lo trae; con "server", sin declarar o un valor que no se conoce, nunca se envía.
  async create(name, body) {
    const c = this._coll(name);
    if (!isObject(body)) throw new Error('restep-sync: create necesita un objeto.');
    const { id: chosen, ...rest } = body;
    const own = c.idFrom === 'client' || c.idFrom === 'either';
    if (chosen !== undefined && !own) throw new Error(`restep-sync: ${name} no deja elegir el id (id_from ${JSON.stringify(c.idFrom)}, §5.1).`);
    if (chosen !== undefined && (typeof chosen !== 'string' || !/^[A-Za-z0-9._~-]+$/.test(chosen) || chosen === '.' || chosen === '..'))
      throw new Error(`restep-sync: ${JSON.stringify(chosen)} no es un id válido (§3).`);
    if (chosen !== undefined && this._view(name).has(chosen)) throw new Error(`restep-sync: ya hay un ${name} ${chosen}.`);
    const clientId = chosen ?? (c.idFrom === 'client' ? await uuid() : null);
    const id = clientId ?? `local-${await uuid()}`;
    const op = { opId: await uuid(), type: 'create', collection: name, id, body: clone(rest), key: await uuid(), attempts: 0,
      ...(clientId && { clientId: true }) };
    return this._enqueue(op, { created: [id] }, id);
  }
  async update(name, id, patch, { onConflict } = {}) {
    id = this.resolveId(name, id);
    if (!this._view(name).has(id)) throw new Error(`restep-sync: no hay ningún ${name} ${id} en la copia local.`);
    if (!isObject(patch)) throw new Error('restep-sync: update necesita un objeto (Merge Patch).');
    const op = { opId: await uuid(), type: 'update', collection: name, id, patch: clone(patch), base: this._base(name, id), attempts: 0 };
    this._strategy(op, onConflict);
    return this._enqueue(op, { updated: [id] }, id);
  }
  async delete(name, id, { onConflict } = {}) {
    id = this.resolveId(name, id);
    if (!this._view(name).has(id)) throw new Error(`restep-sync: no hay ningún ${name} ${id} en la copia local.`);
    const op = { opId: await uuid(), type: 'delete', collection: name, id, base: this._base(name, id), attempts: 0 };
    this._strategy(op, onConflict);
    await this._enqueue(op, { destroyed: [id] });
  }
  // La versión sobre la que se hizo el cambio: la confirmada, salvo que haya otra operación pendiente
  // sobre el mismo elemento; entonces se fija cuando esa termine (null).
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
    // Las rutas son relativas a la raíz de la API (§2); './' evita que «books:changes» se lea como esquema.
    // Un enlace (next, Location) llega ya resuelto como URL absoluta.
    const url = /^[a-z][a-z0-9+.-]*:/i.test(path) ? new URL(path) : new URL(`./${path.replace(/^\//, '')}`, this.root);
    // §3: no se sigue un enlace con otro esquema que https o el de la raíz.
    if (url.protocol !== 'https:' && url.protocol !== this.root.protocol)
      throw new RestepError(0, null, `restep-sync: no se sigue ${url.href}: su esquema no es https ni el de la API (§3).`);
    // §4.3: las credenciales de la API (el token y las cabeceras extra) solo van a su origen.
    const mine = url.origin === this.root.origin;
    const h = { accept: 'application/json', ...(mine && this.extraHeaders), ...headers };
    const token = mine && (typeof this.token === 'function' ? await this.token() : this.token);
    if (token) h.authorization = `Bearer ${token}`;
    if (body !== undefined) h['content-type'] ??= json ?? 'application/json';
    let res, text;
    try {
      res = await this.fetchFn(url, { method, headers: h, body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(this.timeout) });
      text = await res.text();
    } catch (e) {
      throw new NetworkError(`restep-sync: sin conexión con ${url.origin} (${e.cause?.code ?? e.name}).`);
    }
    let parsed;
    try { parsed = text ? JSON.parse(text) : undefined; } catch { parsed = undefined; }
    return { status: res.status, headers: res.headers, body: parsed, url };
  }

  // ── Sincronización ──
  // Envía la cola y luego trae los cambios de cada colección. Se serializa: si ya hay una en marcha,
  // esta empieza cuando termine. Sin conexión no lanza: devuelve el informe con online: false.
  // background: true respeta la espera entre reintentos; bajo demanda se reintenta ya, salvo lo que el
  // servidor haya pedido con Retry-After. pull: false solo envía la cola.
  // retryUncertain: true vuelve a enviar un create cuyo resultado no se sabe y que no es seguro repetir.
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

  // ── Envío de la cola ──
  async _push(report, { force, retryUncertain }) {
    while (this.data.queue.length && !this.closed) {
      const op = this.data.queue[0];
      const now = Date.now();
      if (op.uncertain && !retryUncertain) { report.uncertain = op.opId; return; } // la cola espera a que se decida
      if (op.retryAt > now) { report.waitingUntil = op.retryAt; return; } // lo pidió el servidor
      if (!force && op.nextAt > now) { report.waitingUntil = op.nextAt; return; }
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

  // §5.7: un POST que no se sabe si tuvo efecto solo se repite solo si lleva Idempotency-Key y la API
  // anuncia writes, o si crea con un id elegido por el cliente, que no se puede crear dos veces.
  _keys() { return this.writes && !(this.anonymousRefused && !this._credentials()); }
  _credentials() {
    const names = Object.keys(this.extraHeaders).map((k) => k.toLowerCase());
    return !!this.token || names.includes('authorization') || names.includes('cookie');
  }
  _repeatable(op) { return !!op.clientId || this._keys(); }
  _uncertain(op, report, message) {
    op.uncertain = true;
    report.uncertain = op.opId;
    const error = new RestepError(0, null, `restep-sync: no se sabe si el create de ${op.collection} ${op.id} llegó (${message}) y repetirlo podría duplicarlo; usa sync({ retryUncertain: true }) o discard().`);
    report.errors.push(error);
    this._emit('error', { error, operation: clone(op) });
  }
  // Quita de la cola una operación (por ejemplo un create incierto) y deshace su cambio local.
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
      // §5.1: un create con id propio que se repite tras perder la respuesta da 409 taken; se lee el
      // elemento para saber si el primer intento llegó.
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
        if (res.status === 404 && op.type === 'delete') return this._confirm(op, c, res, report); // ya no estaba
        const current = res.status === 404 ? null : res.current ?? await this._read(c, op.id);
        if (current && round < MAX_CONFLICT_ROUNDS) {
          const again = await this._resolve(op, c, current, report);
          if (again) continue;
          return 'done';
        }
        await this._resolve(op, c, current, report, 'server');
        return 'done';
      }
      // Un error que reintentar no va a arreglar: se quita la operación y se deshace el cambio local.
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
    const body = op.clientId ? { ...op.body, id: op.id } : op.body;
    return this._request('POST', c.path, { body, headers: this._keys() ? { 'idempotency-key': `"${op.key}"` } : {} });
  }

  // update y delete con If-Match. La ETag de la versión de partida es el _etag que trajo el listado
  // (§7.2). Solo si falta (un servidor que no lo pone), se lee el elemento: si sigue igual que la
  // versión que vio el usuario, se usa su ETag; si no, es un conflicto, sin llegar a escribir.
  async _sendChange(op, c) {
    op.base ??= clone(c.items[op.id] ?? null);
    if (!op.base) return { status: 404 };
    if (!op.base.etag) {
      const current = await this._read(c, op.id);
      if (!current) return { status: 404 };
      if (!sameData(current.data, op.base.data)) return { status: 412, conflict: true, current };
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
      if (op.type === 'create') {
        if (!isObject(data)) { // Prefer: return=minimal o un servidor que no devuelve el cuerpo
          const loc = res.headers.get('location');
          id = loc ? decodeURIComponent(new URL(loc, res.url ?? this.root).pathname.split('/').pop()) : null;
          const read = id && await this._read(c, id);
          if (read) ({ data, etag } = read);
        }
        id = data?.id ?? id;
        if (id !== op.id) { // un id elegido por el cliente ya es el definitivo
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
    // La siguiente operación sobre el mismo elemento parte de la versión que acaba de confirmarse.
    const next = q.find((o) => o.collection === op.collection && o.id === id);
    if (next) next.base = clone(c.items[id] ?? null);
    report.pushed++;
    this._changed();
    await this._save();
    this._emit('change', change);
    return 'done';
  }

  // Devuelve true si hay que volver a enviar la operación (gana el cliente o una fusión).
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
    if (!current) decision = 'server'; // ya no existe en el servidor: no hay nada que ganar
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

  // Quita una operación de la cola. Con dependents, también las posteriores sobre el mismo elemento
  // (un create que falla se lleva sus update y delete); si no, la siguiente parte de la copia confirmada.
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

  // ── Recepción de cambios (§10) ──
  async _pull(name, report) {
    const c = this._coll(name);
    if (c.mode === 'reload' || !c.state) return this._reload(name, report);
    for (;;) {
      const r = await this._request('GET', `${c.path}:changes?since=${encodeURIComponent(c.state)}`);
      // 410: el servidor ya no puede calcular los cambios. Un since que no reconoce (otro arranque, otra
      // versión) se trata igual: se recarga la colección.
      if (r.status === 410 || (r.status === 400 && r.body?.code === 'invalid_parameter' && r.body?.parameter === 'since')) {
        report.reset.push(name); return this._reload(name, report);
      }
      this._check(r);
      const { created = [], updated = [], destroyed = [], state, has_more } = r.body ?? {};
      if (typeof state !== 'string') throw new RestepError(r.status, null, `restep-sync: ${name}:changes no devuelve state.`);
      const wanted = [...created, ...updated];
      const fetched = await this._fetchIds(c, wanted);
      const change = { collection: name, source: 'remote', created: [], updated: [], destroyed: [] };
      for (const id of wanted) {
        if (fetched.has(id)) {
          const data = fetched.get(id);
          const old = c.items[id];
          (old ? change.updated : change.created).push(id);
          c.items[id] = entry(data, old && sameData(old.data, data) ? old.etag : null);
        } else if (c.items[id]) { delete c.items[id]; change.destroyed.push(id); } // ya no se ve
      }
      for (const id of destroyed) if (c.items[id]) { delete c.items[id]; change.destroyed.push(id); }
      c.state = state; // solo después de aplicar los cambios (§10)
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

  // Pide los elementos por id con filter[id][in] (§10), en trozos que caben en una página. limit solo
  // con max_limit declarado (§6.4); URLSearchParams escribe un + literal como %2B (§5.2).
  async _fetchIds(c, ids) {
    const out = new Map();
    const size = Math.min(c.maxLimit ?? 50, 50);
    for (let i = 0; i < ids.length; i += size) {
      const chunk = ids.slice(i, i + size);
      const q = new URLSearchParams({ 'filter[id][in]': chunk.join(','), ...(c.maxLimit && { limit: String(chunk.length) }) });
      for (const it of await this._walk(`${c.path}?${q}`)) out.set(it.id, it);
    }
    return out;
  }

  // Sigue next hasta null (§5.2), tal cual: es una ruta absoluta o una URL, que se resuelve contra la
  // de la petición (§3). Devuelve los elementos y el state de la primera página.
  async _walk(first, onFirst) {
    const items = [];
    for (let next = first, page = 0; next; page++) {
      const r = await this._request('GET', next);
      this._check(r);
      if (!Array.isArray(r.body?.items)) throw new RestepError(r.status, null, `restep-sync: ${next} no es una colección.`);
      if (page === 0) onFirst?.(r.body);
      items.push(...r.body.items);
      next = typeof r.body.next === 'string' ? new URL(r.body.next, r.url).href : null;
    }
    return items;
  }

  async _reload(name, report) {
    const c = this._coll(name);
    let state = null;
    const q = c.maxLimit ? `?limit=${c.maxLimit}` : '';
    const items = await this._walk(`${c.path}${q}`, (b) => { state = b.state ?? null; });
    if (c.mode === 'sync' && typeof state !== 'string')
      throw new RestepError(200, null, `restep-sync: ${name} no devuelve state aunque declara sync (§10).`);
    const before = c.items;
    c.items = {};
    const change = { collection: name, source: 'remote', reload: true, created: [], updated: [], destroyed: [] };
    for (const data of items) {
      const old = before[data.id];
      c.items[data.id] = entry(data, old && sameData(old.data, data) ? old.etag : null);
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
