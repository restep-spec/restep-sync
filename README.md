# restep-sync (prototype)

Offline-first client library for [Restep 1](https://restep.wubytes.com/1/) APIs
(draft 6): it keeps a local replica of one or more collections up to date with the `sync` profile
(§10) and stores writes in a queue that is sent when there is a connection, with the guarantees of
`writes` (§7). A single module, Node 18+ or browser, no dependencies.

`sync` is experimental in draft 6 (§1.2): this client only uses it with an API that declares
`"restep": "1.0-draft.6"`, the only `sync` text it implements (`KNOWN_TEXTS`). With any other
version of Restep 1 it reloads whole collections.

```bash
npm install restep-sync
```

## Usage

```js
import { openRestepSync, fileStorage } from './restep-sync.mjs';

const db = await openRestepSync({
  url: 'http://localhost:8080/',
  collections: ['books', 'categories'],
  storage: fileStorage('./replica.json'),   // or memoryStorage(), or indexedDBStorage() in the browser
});

db.on('change', (e) => console.log(e.collection, e.source, e.created, e.updated, e.destroyed));
db.on('conflict', (e) => console.log('conflict', e.id, e.resolution));
db.on('error', (e) => console.error(e.error.message));

db.list('books', { filter: { category: 'poesia' }, sort: '-price' });   // local, no network
db.get('books', '7');

const created = await db.create('books', { title: 'New', price: '9.90' }); // provisional id "local-…"
await db.update('books', '7', { price: '15.00' });                         // Merge Patch
await db.delete('books', '50');
db.pending();                     // queued operations
const report = await db.sync();   // sends the queue and pulls changes
db.resolveId('books', created.id); // final id, once the server has created it
db.mode('books');                 // 'sync' (with :changes) or 'reload' (full reload)
await db.discard(opId);           // removes an operation from the queue and undoes its local change
await db.close();
```

To try it by hand against an API with `sync` (the reference server does not implement it, see
below):

```bash
node tools/restep-sync/ref-sync-server.mjs     # copy of the reference server with sync, on :8860
```

## `openRestepSync` options

| Option | Default | What it does |
|---|---|---|
| `url` | (required) | API root, where the discovery document is |
| `collections` | (required) | Names of the collections to replicate |
| `storage` | `memoryStorage()` | Where the replica is stored (see "Storage") |
| `interval` | `30000` | Milliseconds between background syncs; `0` turns them off |
| `autoPush` | `true` | After each local write, tries to send the queue right away |
| `onConflict` | `'server'` | `'server'`, `'client'` or a function (see "Conflicts") |
| `token` | — | String or function (async too) that gives the token for `Authorization: Bearer` |
| `headers` | `{}` | Extra headers on every request (for example `Accept-Language`) |
| `retry` | `{ base: 1000, max: 60000 }` | Exponential backoff between retries, in ms, with some jitter |
| `timeout` | `15000` | Maximum time per request, in ms |
| `fetch` | `globalThis.fetch` | To inject another implementation |

## How it works

- **Startup.** Reads the discovery document (§4.2): the root or, if it is not one, the first
  `rel="service-desc"` that leads to one. A `restep` that does not follow the grammar of §1.1, or
  is from another major version, is not Restep 1: it does not open. Without a discovery document
  it assumes `core` only, with each collection at `/<name>`. Checks that each collection exists
  and is not a subcollection (with a template in `path`). A collection syncs with `:changes` only
  if it declares `sync` with the profile announced (§10) and the version is one whose `sync` text
  it knows (§1.2); it then requires `filter` on `id` with `in`. The rest are reloaded in full on
  every sync (more expensive, but it works); `db.mode(name)` says which. Offline, it uses the
  stored discovery document; if there is none, it fails with a clear message.
- **Local model.** For each collection it stores the copy confirmed by the server (data and ETag
  of each item) and its `state`. Writes go into a FIFO queue. `list()` and `get()` return the
  confirmed copy with the queue applied on top, so undoing an optimistic change means removing
  its operation from the queue.
- **Initial sync.** `GET /collection?limit=<max_limit>` (without `limit` if it does not declare
  `max_limit`, §6.4), following `next` as is until `null`, resolved against the request URL, with
  the `state` from the first page (§10).
- **Incremental.** `GET /collection:changes?since=<state>`; created and updated items are fetched
  with `filter[id][in]=…` in page-sized chunks; destroyed ones are removed. An id from `created`
  or `updated` that does not come back in that request is taken as deleted (no longer visible).
  With `has_more: true` it repeats with the intermediate `state`. The `state` is stored only after
  applying each response. A `410 state_expired`, or an `invalid_parameter` on `since` (a `state`
  the server no longer recognises), triggers a reload of the collection (the write queue is
  kept).
- **Sending the queue.** Before pulling changes, in order:
  - `create`: `POST` with `Content-Type: application/json` and an `Idempotency-Key` generated
    when queuing and stored with the operation, so retries (also after restarting the app) do not
    duplicate. The key is only sent if the API announces `writes`, and not without credentials
    when `idempotency.anonymous` is `false` (§7.1). Once confirmed, the provisional id becomes
    the final one in the queue and in `resolveId()`.
  - `id_from` (§5.1): with `"client"`, the client picks the id (a UUID v4) and there is no
    provisional id; with `"client"` or `"either"`, `create()` also accepts an `id` in the body.
    With `"server"`, undeclared or with an unknown value, an `id` in the body is an error and is
    never sent. If a `create` with its own id is repeated after losing the response and gets a
    `409` with `taken` on `/id`, the item is read and, if it exists, taken as created.
  - `update`: `PATCH` with `application/merge-patch+json` and `If-Match` with the ETag of the
    version the change was made on: the `_etag` that item carried in the listing (§7.2), so it
    does not read it first. Only if the listing has no `_etag` (a server that does not set it)
    does it read the item first: if it is still the version the user saw, it uses its ETag; if
    not, it is a conflict without ever writing. The `_etag` is not part of the data `list()` and
    `get()` return. Two changes in a row on the same
    item do not clash with each other: the second starts from the version the first confirmed.
  - `delete`: `DELETE` with `If-Match`; a `404` counts as done.
- **Retries.** Offline, `408`, `425`, `429` and `5xx` are retried with exponential backoff, except
  a `create` that is not safe to repeat (§5.7): if the API does not announce `writes` (or the key
  cannot be sent) and the id is not client-chosen, after losing the response or on a `408`,
  `500`, `502` or `504` the operation becomes **uncertain**: it is reported with `error`, the
  report includes `uncertain` with its `opId`, and the queue waits until
  `sync({ retryUncertain: true })` (which may duplicate) or `discard(opId)` is called. `429` and
  `503` never reach a request with an effect (§5.7), so those are retried; if the server sends
  `Retry-After`, it does not retry earlier, not even with an on-demand `sync()` (which does skip
  the exponential backoff). A `409` with `code` `request_in_progress` (the same request still in
  progress, §7.1) is retried too, with the same `Idempotency-Key` and not before its
  `Retry-After`. Any other `4xx`, including a `409 conflict`, is permanent: the operation is
  removed (along with those that depend on it), the local change is undone and `error` is emitted
  with the `problem+json`.
- **`sync()`** is serialised (if one is running, it waits) and does not throw for lack of
  network: it returns a report `{ online, pushed, dropped, conflicts, pending, pulled, reset,
  errors, waitingUntil, uncertain }`.
- **Origins and links.** The token and the `headers` only go to the API's origin (§4.3); a `next`
  or a `Location` with a scheme other than `https` or the root's is not followed (§3).
- **Local filters and sorting.** `list()` sorts strings by code point (§6.1). A field the
  collection declares in `collations` with a value other than `"codepoint"` is not sorted or
  filtered locally with the object or string form, because §6.1 forbids reproducing that
  equality or ordering: it gives an error; with a function, the decision is up to whoever writes
  it.

## Conflicts

A `412` (or a different version on re-read) calls the operation's strategy: the one passed to the
call (`db.update(c, id, patch, { onConflict })`), or otherwise the one from `openRestepSync`.

- `'server'`: the local change is discarded and the local copy takes the server's version.
- `'client'`: the same change is resent with the new ETag (in an `update`, only the patch's
  fields, so other remote changes are kept).
- A function `({ collection, id, type, base, server, local, patch }) => decision`, which may be
  async. `base` is the version the user saw, `server` the current one and `local` the server's
  with the patch applied. It returns `'server'`, `'client'` or, in an `update`, an object that is
  sent as a Merge Patch instead of the original.

If the item no longer exists on the server, the server wins. After 3 rounds of conflict on the
same operation, it wins too. `conflict` is always emitted with the `resolution` applied.

## Events

- `change`: `{ collection, source, created, updated, destroyed }`. `source` is `local` (an
  optimistic write), `push` (the server has confirmed it; a `create` also carries
  `replaced: { provisionalId: finalId }`), `remote` (changes pulled; with `reload: true` if the
  collection was reloaded), `conflict` or `rollback` (a local change was undone).
- `conflict`: `{ collection, id, type, resolution, base, server, local }`.
- `error`: `{ error, operation? }`; `error.status` and `error.problem` if it came from the server.

`on()` returns a function to unsubscribe.

## Storage

A store is `{ read(): Promise<object | null>, write(object): Promise<void> }` and keeps the whole
replica (discovery document, collections, queue) as a single JSON object. That is enough for
collections of thousands of items; beyond that it would need a per-item store.

- `memoryStorage()`: in memory, for tests.
- `fileStorage(path)`: JSON file in Node; writes to a temporary file and renames it.
- `indexedDBStorage(dbName, { store, key })`: one record in IndexedDB. **Untested**: the tests
  run in Node, with no browser. It is deliberately thin (open, `get`, `put`).

Conflict functions passed on each call live in memory: if the app restarts with pending
operations, those use the general strategy.

## Tests

```bash
node tools/restep-sync/test.mjs      # from restep/
```

The reference server (`vendor/restep-server.mjs`, draft 6) **does not implement
`sync`**, so `ref-sync-server.mjs` generates a copy in a temporary directory with a few text
patches (failing loudly if any no longer applies) that add: `sync` in `profiles` and
`sync.retention` only on `books` and `categories` (variable `SYNCED`); `_etag` on each item of a
collection response if the reference does not set it already (§7.2); `state` on every response of
a synced collection, the same on every page of a traversal and with keyset pagination so no items
are lost (§10); `/{collection}:changes` with `has_more` (`SYNC_PAGE`, 100 by default),
`410 state_expired` for states from another run or older than the retention, and `404` on a
collection that does not declare `sync` (`products`). The original is left untouched.

The test starts that copy on port 8860 with `SYNC_PAGE=2`, a proxy on 8861 that the client goes
through, which can drop the response to a `POST`, answer a `503` with `Retry-After` or change the
discovery document, and a server on 8862 acting as another origin.
It covers: initial sync, remote changes with `has_more`, changes through inheritance (§11.6) in
another collection, optimistic writes (including an `update` on a pending `create` and two
`update`s in a row), conflict with each strategy (a real `412` and a different version on re-read),
`delete` against a changed item, retry without duplicating with `Idempotent-Replayed`,
`Retry-After`, writing with the listing's `_etag` without re-reading, `409 request_in_progress`
retried after `Retry-After`, a permanent `409 conflict`, a permanent `422` error, offline writes
that survive closing and reopening from the file, a server stopped and restarted (`410` and
reload), background sync with `autoPush`, and errors on open. And the draft 6 items: a collection
without `sync` is reloaded and never requests `:changes` (which gives `404`); with
`"1.0-draft.5"` or `"1.1"` `sync` is not used; `"2.0"`, `"1.01"` or `"1.0-beta.1"` do not open;
without a discovery document, `core` only; a subcollection gives an error; `id_from` `"client"`
with no alias and the `409 taken` after losing the response; without `writes`, an uncertain
`create` that is not repeated on its own, `retryUncertain` and `discard()`; credentials only to
the origin and links with another scheme; and collations in `list()`.

Note on draft 4: there, the same request still in progress gave `409 conflict`, and this client
retried it a few times on a `create`. Since draft 5 that case is `409 request_in_progress`, and a
`409 conflict` is treated as permanent, so with a server following draft 4 it stops retrying its
`409`s: the operation is removed from the queue and reported with `error`.

## Limits

- No notification channel (Server-Sent Events from `events`, §10): it syncs on an interval or on
  demand. So it does not name subcollections by their path in events either.
- It does not replicate subcollections (`path` with a template and `parent`, like
  `/books/{book_id}/reviews` in the reference server), nor singletons: opening one gives a clear
  error. Replicating one would mean choosing the parent items (one, several or all) and a `state`
  for each, and §10 only defines `:changes` for collections that declare `sync`; no subcollection
  in the reference server declares it.
- It syncs whole collections, as §10 requires; `list()` filters are local (equality or a
  function) and there are no indexes.
- The queue is strictly FIFO: an operation waiting to be retried blocks the ones after it, also
  those of other collections.
- If the response to a `create` is lost and changes are pulled in the background before the
  retry, the item may show up twice (the provisional one and the server's) until the retry
  confirms the id. On demand this does not happen: if sending fails on the network, no changes
  are pulled in that round.
- No `batch` or `actions`. `update` always sends Merge Patch.
- If the API does not announce `writes`, it does not send `Idempotency-Key` (§7.1) and `create`s
  without their own id may become uncertain (above). It still sends `If-Match` with whatever ETag
  it knows, but without `writes` it cannot rely on it.
- It does not interpret `_inherited` or any other `_` member: it stores them as data.
- The reference server keeps idempotency keys in memory: if it restarts after creating and
  before the retry arrives, that retry creates another item. A real server has to store them
  durably (§7.1).

## Questions about the spec

1. **ETag of synced items.** Settled in draft 5: in a collection that answers `:changes`, every
   item that accepts `PATCH` or `DELETE` carries its `_etag` on every page (§7.2).
2. **Ids with commas.** `filter[id][in]=a,b` does not define how to escape a comma inside an id,
   so a collection with such ids cannot be synced. In draft 6 an `id` only contains unreserved
   characters (§3), so it can no longer contain commas: settled.
3. **An id from `created`/`updated` that does not come back when requested.** It may have been
   deleted between `:changes` and the request, or have stopped being visible. This client removes
   it from the copy and does nothing more; the spec does not say what to do, although the next
   `:changes` should bring it in `destroyed`.
4. **`state` of a filtered response.** Settled in draft 6: a filtered response carries the same
   `state` as the whole collection (§10). This client still only uses the one from `:changes` or
   from a full load.
5. **Ambiguous `409`.** Settled in draft 5: the same request still in progress gives `409
   request_in_progress` (with `Retry-After`), distinct from `409 conflict`.
6. **Pending writes after a `410`.** The spec says that after `410 state_expired` the client
   "reloads the collection", but not what it does with its pending writes based on versions it
   can no longer check. This client keeps them and sends them with their `If-Match`; if the item
   changed, the conflict kicks in.
7. **`sync` without `writes`.** §1.2 makes `sync` require `query` but not `writes`. With draft 6,
   §5.7 says when a `POST` can be repeated without `writes` (a client-chosen id); outside that, an
   offline client that creates needs `writes`.
8. **Which `sync` text a client knows (§1.2).** This client implements the one from
   `1.0-draft.6`. When `1.0` is published, will its `sync` be the same text? Until that is known,
   an API declaring `"1.0"` is reloaded in full. It is also unclear whether a `state` stored under
   one draft is valid after the server declares another: this client tries it and, if it gets
   `410` or `invalid_parameter` on `since`, reloads.
9. **`invalid_parameter` on `since`.** §10 gives `410` when the server can no longer compute the
   changes, but a `state` it did not issue (or from another run) is also a bad `since` (§6.4 says
   so for cursors). This client treats both the same: it reloads.
10. **When is it "known" that a create with its own id arrived?** §5.1 says that after a
    `409 taken` the client reads the item to find out whether its first attempt arrived, but
    reading it only says it exists. With a UUID v4 a collision is negligible, and this client
    takes it as its own if it had already tried to send it.

## Code and issues

Code and issues: <https://github.com/restep-spec/restep-sync>. That repository is generated from the Restep
site, where the tool is developed alongside the [specification](https://restep.wubytes.com/1/) and the reference
server: `vendor/` and `schema/` are copies from the site so the tests run on their own.

MIT licensed.
