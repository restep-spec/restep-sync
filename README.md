# restep-sync (prototipo)

Biblioteca cliente offline-first para APIs [Restep 1](https://restep.wubytes.com/es/1/):
mantiene una réplica local de una o varias colecciones al día con el perfil `sync` (§9) y
guarda las escrituras en una cola que se envía cuando hay conexión, con las garantías de
`writes` (§7). Un solo módulo, Node 18+ o navegador, sin dependencias.

## Uso

```js
import { openRestepSync, fileStorage } from './restep-sync.mjs';

const db = await openRestepSync({
  url: 'http://localhost:8080/',
  collections: ['books', 'categories'],
  storage: fileStorage('./replica.json'),   // o memoryStorage(), o indexedDBStorage() en el navegador
});

db.on('change', (e) => console.log(e.collection, e.source, e.created, e.updated, e.destroyed));
db.on('conflict', (e) => console.log('conflicto', e.id, e.resolution));
db.on('error', (e) => console.error(e.error.message));

db.list('books', { filter: { category: 'poesia' }, sort: '-price' });   // local, sin red
db.get('books', '7');

const nuevo = await db.create('books', { title: 'Nuevo', price: '9.90' }); // id provisional "local-…"
await db.update('books', '7', { price: '15.00' });                         // Merge Patch
await db.delete('books', '50');
db.pending();                     // operaciones en cola
const informe = await db.sync();  // envía la cola y trae los cambios
db.resolveId('books', nuevo.id);  // id definitivo, cuando el servidor lo ha creado
await db.close();
```

Para probarla a mano contra una API con `sync` (el servidor de referencia no lo implementa, ver
abajo):

```bash
node tools/restep-sync/ref-sync-server.mjs     # copia del servidor de referencia con sync, en :8860
```

## Opciones de `openRestepSync`

| Opción | Por defecto | Qué hace |
|---|---|---|
| `url` | (obligatoria) | Raíz de la API, donde está el descubrimiento |
| `collections` | (obligatoria) | Nombres de las colecciones que se replican |
| `storage` | `memoryStorage()` | Dónde se guarda la réplica (ver «Almacenes») |
| `interval` | `30000` | Milisegundos entre sincronizaciones en segundo plano; `0` las desactiva |
| `autoPush` | `true` | Tras cada escritura local, intenta enviar la cola enseguida |
| `onConflict` | `'server'` | `'server'`, `'client'` o una función (ver «Conflictos») |
| `token` | — | Cadena o función (también asíncrona) que da el token para `Authorization: Bearer` |
| `headers` | `{}` | Cabeceras extra en cada petición (por ejemplo `Accept-Language`) |
| `retry` | `{ base: 1000, max: 60000 }` | Espera exponencial entre reintentos, en ms, con algo de azar |
| `timeout` | `15000` | Tiempo máximo de cada petición, en ms |
| `fetch` | `globalThis.fetch` | Para inyectar otra implementación |

## Cómo funciona

- **Arranque.** Lee el descubrimiento (§4.2) y comprueba que cada colección existe y, si la
  API anuncia `sync`, que declara `filter` sobre `id` con `in`. Sin conexión, usa el
  descubrimiento guardado; si no hay ninguno, falla con un mensaje claro. Si la API no
  anuncia `sync`, cada sincronización recarga la colección entera (más caro, pero funciona).
- **Modelo local.** Por colección se guarda la copia confirmada por el servidor (datos y ETag
  de cada elemento) y su `state`. Las escrituras van en una cola FIFO. `list()` y `get()`
  devuelven la copia confirmada con la cola aplicada encima, así que deshacer un cambio
  optimista es quitar su operación de la cola.
- **Sincronización inicial.** `GET /colección?limit=<max_limit>` siguiendo `next` hasta
  `null`, con el `state` de la primera página (§9).
- **Incremental.** `GET /colección:changes?since=<state>`; los creados y modificados se piden
  con `filter[id][in]=…` en trozos de una página; los borrados se quitan. Un id de `created`
  o `updated` que no vuelve en esa petición se da por borrado (ya no se ve). Con
  `has_more: true` repite con el `state` intermedio. El `state` se guarda solo después de
  aplicar cada respuesta. Un `410 state_expired` hace recargar la colección (la cola de
  escrituras se conserva).
- **Envío de la cola.** Antes de traer cambios, en orden:
  - `create`: `POST` con una `Idempotency-Key` que se genera al encolar y se guarda con la
    operación, así que los reintentos (también tras reiniciar la aplicación) no duplican. Al
    confirmarse, el id provisional pasa a ser el definitivo en la cola y en `resolveId()`.
  - `update`: `PATCH` con `application/merge-patch+json` e `If-Match` con la ETag de la versión
    sobre la que se hizo el cambio. Si la colección no trae `_etag` (el servidor de referencia
    no lo trae), lee el elemento antes: si sigue igual que la versión que vio el usuario, usa
    su ETag; si no, es un conflicto sin llegar a escribir. Dos cambios seguidos sobre el mismo
    elemento no chocan entre sí: el segundo parte de la versión que confirmó el primero.
  - `delete`: `DELETE` con `If-Match`; un `404` cuenta como hecho.
- **Reintentos.** Sin conexión, `408`, `425`, `429` y `5xx` se reintentan con espera
  exponencial; si el servidor manda `Retry-After`, no se reintenta antes, ni siquiera con
  `sync()` bajo demanda (que sí se salta la espera exponencial). Un `409` en un `create` se
  reintenta hasta 5 veces (puede ser la misma petición aún en curso, §7.1). Cualquier otro
  `4xx` es permanente: se quita la operación (y las que dependen de ella), se deshace el cambio
  local y se emite `error` con el `problem+json`.
- **`sync()`** se serializa (si hay una en marcha, espera) y no lanza por falta de red:
  devuelve un informe `{ online, pushed, dropped, conflicts, pending, pulled, reset, errors,
  waitingUntil }`.

## Conflictos

Un `412` (o una versión distinta al releer) llama a la estrategia de la operación: la de la
llamada (`db.update(c, id, patch, { onConflict })`), o si no, la de `openRestepSync`.

- `'server'`: se descarta el cambio local y la copia local toma la versión del servidor.
- `'client'`: se reenvía el mismo cambio con la ETag nueva (en un `update`, solo los campos del
  parche, así que los demás cambios remotos se conservan).
- Una función `({ collection, id, type, base, server, local, patch }) => decisión`, que puede
  ser asíncrona. `base` es la versión que vio el usuario, `server` la actual y `local` la del
  servidor con el parche aplicado. Devuelve `'server'`, `'client'` o, en un `update`, un objeto
  que se envía como Merge Patch en lugar del original.

Si el elemento ya no existe en el servidor, gana el servidor. Tras 3 rondas de conflicto sobre
la misma operación, también. Siempre se emite `conflict` con la `resolution` aplicada.

## Eventos

- `change`: `{ collection, source, created, updated, destroyed }`. `source` es `local` (una
  escritura optimista), `push` (el servidor la ha confirmado; un `create` trae además
  `replaced: { idProvisional: idDefinitivo }`), `remote` (cambios traídos; con `reload: true`
  si se recargó la colección), `conflict` o `rollback` (se deshizo un cambio local).
- `conflict`: `{ collection, id, type, resolution, base, server, local }`.
- `error`: `{ error, operation? }`; `error.status` y `error.problem` si vino del servidor.

`on()` devuelve una función para darse de baja.

## Almacenes

Un almacén es `{ read(): Promise<objeto | null>, write(objeto): Promise<void> }` y guarda la
réplica entera (descubrimiento, colecciones, cola) como un solo objeto JSON. Basta para
colecciones de miles de elementos; para más haría falta un almacén por elemento.

- `memoryStorage()`: en memoria, para pruebas.
- `fileStorage(ruta)`: archivo JSON en Node; escribe en un temporal y lo renombra.
- `indexedDBStorage(nombreBase, { store, key })`: un registro en IndexedDB. **Sin probar**: las
  pruebas corren en Node, sin navegador. Es deliberadamente delgado (abrir, `get`, `put`).

Las funciones de conflicto pasadas en cada llamada viven en memoria: si la aplicación se
reinicia con operaciones pendientes, esas usan la estrategia general.

## Pruebas

```bash
node tools/restep-sync/test.mjs      # desde restep/
```

El servidor de referencia (`vendor/restep-server.mjs`) **no implementa `sync`**, así que
`ref-sync-server.mjs` genera una copia en un directorio temporal con unos parches de texto
(falla en voz alta si alguno ya no encaja) que añaden: `sync` en `profiles` y `sync.retention`
en cada colección; `state` en cada respuesta de colección, igual en todas las páginas de un
recorrido y con paginación por clave para no perder elementos (§9); `/{colección}:changes` con
`has_more` (`SYNC_PAGE`, por defecto 100) y `410 state_expired` para estados de otro arranque o
más antiguos que la retención. El original no se toca.

La prueba arranca esa copia en el puerto 8860 con `SYNC_PAGE=2` y un proxy en el 8861 por el que
pasa el cliente, que puede perder la respuesta de un `POST` o contestar un `503` con
`Retry-After`. Cubre: sincronización inicial, cambios remotos con `has_more`, cambios por
herencia (§5.7) en otra colección, escrituras optimistas (también un `update` sobre un `create`
pendiente y dos `update` seguidos), conflicto con cada estrategia (`412` real y versión distinta
al releer), `delete` contra un elemento cambiado, reintento sin duplicar con
`Idempotent-Replayed`, `Retry-After`, error permanente `422`, escrituras sin conexión que
sobreviven a cerrar y reabrir desde el archivo, servidor parado y vuelto a arrancar (`410` y
recarga), sincronización en segundo plano con `autoPush`, y errores al abrir.

## Límites

- Sin canal de avisos (Server-Sent Events de §9): sincroniza por intervalo o bajo demanda.
- Sincroniza colecciones enteras, como manda §9; los filtros de `list()` son locales (igualdad
  o función) y no hay índices.
- La cola es estrictamente FIFO: una operación que espera reintento bloquea las siguientes,
  también las de otras colecciones.
- Si se pierde la respuesta de un `create` y en segundo plano se traen cambios antes del
  reintento, el elemento puede verse dos veces (el provisional y el del servidor) hasta que el
  reintento confirma el id. Bajo demanda no pasa: si el envío falla por la red, no se traen
  cambios en esa ronda.
- No hay `batch` ni `actions`. `update` siempre manda Merge Patch.
- Si la API no anuncia `writes`, el cliente manda igualmente `Idempotency-Key` e `If-Match`,
  pero no puede contar con ellos (§7.1).
- El servidor de referencia guarda las claves de idempotencia en memoria: si se reinicia
  después de crear y antes de que llegue el reintento, ese reintento crea otro elemento. Un
  servidor real tiene que guardarlas de forma duradera (§7.1).

## Dudas sobre la spec

1. **ETag de los elementos sincronizados.** §9 trae los cambios con `filter[id][in]`, pero
   `_etag` en los elementos de una colección es solo PUEDE (§7.2). Sin él, un cliente offline
   no tiene ETag para `If-Match` y tiene que releer cada elemento antes de escribir (o
   escribir sin `If-Match`). ¿Debería `sync` exigir (o recomendar) `_etag` en las colecciones
   sincronizadas?
2. **Ids con comas.** `filter[id][in]=a,b` no define cómo escapar una coma dentro de un id, así
   que una colección con ids así no se puede sincronizar. Tampoco se dice nada del límite de
   longitud de la URL con muchos ids (este cliente pide de página en página).
3. **Id de `created`/`updated` que no vuelve al pedirlo.** Puede haberse borrado entre
   `:changes` y la petición, o haber dejado de ser visible. Este cliente lo quita de la copia
   y no avanza nada más; la spec no dice qué hacer, aunque el siguiente `:changes` debería
   traerlo en `destroyed`.
4. **`state` de una respuesta filtrada.** §9 exige `state` en las respuestas de colección y dice
   que refleja la colección. No queda claro si la respuesta a `filter[id][in]` lleva el
   `state` de la colección entera ni si el cliente puede usarlo; este cliente lo ignora y solo
   usa el de `:changes` o el de una carga completa.
5. **`409` ambiguo.** «La misma petición aún en curso con la misma `Idempotency-Key`» (§7.1,
   reintentable) y «un duplicado o una transición imposible» (permanente) comparten
   `409 conflict`. Un cliente genérico no los distingue sin un `reason`; un código propio para
   el primero (por ejemplo `request_in_progress`) lo resolvería.
6. **Escrituras pendientes tras un `410`.** La spec dice que tras `410 state_expired` el cliente
   «vuelve a cargar la colección», pero no qué hace con sus escrituras pendientes basadas en
   versiones que ya no puede comprobar. Este cliente las conserva y las envía con su
   `If-Match`; si el elemento cambió, salta el conflicto.
7. **`sync` sin `writes`.** §1.2 hace que `sync` requiera `query` pero no `writes`. Un cliente
   offline que escribe necesita `Idempotency-Key` e `If-Match`; quizá valga la pena decir que
   un cliente que escribe offline debe exigir `writes` además de `sync`.

## Código y avisos

Código y avisos: <https://github.com/restep-spec/restep-sync>. Ese repositorio se genera desde la web de
Restep, donde la herramienta se desarrolla junto a la [especificación](https://restep.wubytes.com/1/) y el servidor de
referencia: `vendor/` y `schema/` son copias de la web para que las pruebas funcionen solas.

Licencia MIT.
