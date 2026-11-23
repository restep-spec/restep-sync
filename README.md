# restep-sync (prototipo)

Biblioteca cliente offline-first para APIs [Restep 1](https://restep.wubytes.com/es/1/)
(borrador 6): mantiene una réplica local de una o varias colecciones al día con el perfil `sync`
(§10) y guarda las escrituras en una cola que se envía cuando hay conexión, con las garantías de
`writes` (§7). Un solo módulo, Node 18+ o navegador, sin dependencias.

`sync` es experimental en el borrador 6 (§1.2): este cliente solo lo usa con una API que declare
`"restep": "1.0-draft.6"`, el único texto de `sync` que implementa (`KNOWN_TEXTS`). Con
cualquier otra versión de Restep 1 recarga las colecciones enteras.

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
db.mode('books');                 // 'sync' (con :changes) o 'reload' (recarga entera)
await db.discard(opId);           // quita una operación de la cola y deshace su cambio local
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

- **Arranque.** Lee el descubrimiento (§4.2): la raíz o, si no lo es, el primer
  `rel="service-desc"` que lleve a uno. Un `restep` que no sigue la gramática de §1.1 o de otra
  versión mayor no es Restep 1: no abre. Sin documento de descubrimiento supone solo `core`, con
  cada colección en `/<nombre>`. Comprueba que cada colección existe y que no es una
  subcolección (con plantilla en `path`). Una colección se sincroniza con `:changes` solo si
  declara `sync` con el perfil anunciado (§10) y la versión es una cuyo texto de `sync` conoce
  (§1.2); entonces exige `filter` sobre `id` con `in`. Las demás se recargan enteras en cada
  sincronización (más caro, pero funciona); `db.mode(nombre)` dice cuál. Sin conexión, usa el
  descubrimiento guardado; si no hay ninguno, falla con un mensaje claro.
- **Modelo local.** Por colección se guarda la copia confirmada por el servidor (datos y ETag
  de cada elemento) y su `state`. Las escrituras van en una cola FIFO. `list()` y `get()`
  devuelven la copia confirmada con la cola aplicada encima, así que deshacer un cambio
  optimista es quitar su operación de la cola.
- **Sincronización inicial.** `GET /colección?limit=<max_limit>` (sin `limit` si no declara
  `max_limit`, §6.4) siguiendo `next` hasta `null` tal cual, resuelto contra la URL de la
  petición, con el `state` de la primera página (§10).
- **Incremental.** `GET /colección:changes?since=<state>`; los creados y modificados se piden
  con `filter[id][in]=…` en trozos de una página; los borrados se quitan. Un id de `created`
  o `updated` que no vuelve en esa petición se da por borrado (ya no se ve). Con
  `has_more: true` repite con el `state` intermedio. El `state` se guarda solo después de
  aplicar cada respuesta. Un `410 state_expired`, o un `invalid_parameter` sobre `since` (un
  `state` que el servidor ya no reconoce), hace recargar la colección (la cola de escrituras se
  conserva).
- **Envío de la cola.** Antes de traer cambios, en orden:
  - `create`: `POST` con `Content-Type: application/json` y una `Idempotency-Key` que se genera
    al encolar y se guarda con la operación, así que los reintentos (también tras reiniciar la
    aplicación) no duplican. La clave solo se envía si la API anuncia `writes`, y no sin
    credenciales cuando `idempotency.anonymous` es `false` (§7.1). Al confirmarse, el id
    provisional pasa a ser el definitivo en la cola y en `resolveId()`.
  - `id_from` (§5.1): con `"client"`, el cliente elige el id (un UUID v4) y no hay id
    provisional; con `"client"` o `"either"`, `create()` acepta también un `id` en el cuerpo.
    Con `"server"`, sin declarar o con un valor desconocido, un `id` en el cuerpo es un error y
    nunca se envía. Si un `create` con id propio se repite tras perder la respuesta y da `409`
    con `taken` en `/id`, se lee el elemento y, si existe, se da por creado.
  - `update`: `PATCH` con `application/merge-patch+json` e `If-Match` con la ETag de la versión
    sobre la que se hizo el cambio: el `_etag` que trajo ese elemento en el listado (§7.2), así
    que no lo lee antes. Solo si el listado no trae `_etag` (un servidor que no lo pone), lee el
    elemento antes: si sigue igual que la versión que vio el usuario, usa su ETag; si no, es un
    conflicto sin llegar a escribir. El `_etag` no forma parte de los datos que devuelven
    `list()` y `get()`. Dos cambios seguidos sobre el mismo
    elemento no chocan entre sí: el segundo parte de la versión que confirmó el primero.
  - `delete`: `DELETE` con `If-Match`; un `404` cuenta como hecho.
- **Reintentos.** Sin conexión, `408`, `425`, `429` y `5xx` se reintentan con espera
  exponencial, salvo un `create` que no es seguro repetir (§5.7): si la API no anuncia `writes`
  (o no se puede mandar la clave) y el id no lo elige el cliente, tras perder la respuesta o
  con un `408`, `500`, `502` o `504` la operación queda **incierta**: se avisa con `error`, el
  informe trae `uncertain` con su `opId`, y la cola espera hasta que se pida
  `sync({ retryUncertain: true })` (que puede duplicar) o `discard(opId)`. `429` y `503` nunca
  llegan a una petición con efecto (§5.7), así que esos sí se reintentan; si el servidor manda `Retry-After`, no se reintenta antes, ni siquiera con
  `sync()` bajo demanda (que sí se salta la espera exponencial). Un `409` con `code`
  `request_in_progress` (la misma petición aún en curso, §7.1) también se reintenta, con la
  misma `Idempotency-Key` y no antes de su `Retry-After`. Cualquier otro `4xx`, también un `409
  conflict`, es permanente: se quita la operación (y las que dependen de ella), se deshace el cambio
  local y se emite `error` con el `problem+json`.
- **`sync()`** se serializa (si hay una en marcha, espera) y no lanza por falta de red:
  devuelve un informe `{ online, pushed, dropped, conflicts, pending, pulled, reset, errors,
  waitingUntil, uncertain }`.
- **Orígenes y enlaces.** El token y las cabeceras de `headers` solo van al origen de la API
  (§4.3); un `next` o un `Location` con otro esquema que `https` o el de la raíz no se sigue
  (§3).
- **Filtros y orden locales.** `list()` ordena los textos por puntos de código (§6.1). Un campo
  que la colección declara en `collations` con otro valor que `"codepoint"` no se ordena ni se
  filtra en local con la forma de objeto o de cadena, porque §6.1 prohíbe reproducir esa
  igualdad o ese orden: da un error; con una función, la decisión es de quien la escribe.

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

El servidor de referencia (`vendor/restep-server.mjs`, borrador 6) **no implementa
`sync`**, así que `ref-sync-server.mjs` genera una copia en un directorio temporal con unos
parches de texto (falla en voz alta si alguno ya no encaja) que añaden: `sync` en `profiles` y
`sync.retention` solo en `books` y `categories` (variable `SYNCED`); `_etag` en cada elemento de
una respuesta de colección si la referencia no lo pone ya (§7.2); `state` en cada respuesta de
una colección sincronizada, igual en todas las páginas de un recorrido y con paginación por
clave para no perder elementos (§10); `/{colección}:changes` con `has_more` (`SYNC_PAGE`, por
defecto 100), `410 state_expired` para estados de otro arranque o más antiguos que la retención,
y `404` en una colección que no declara `sync` (`products`). El original no se toca.

La prueba arranca esa copia en el puerto 8860 con `SYNC_PAGE=2`, un proxy en el 8861 por el que
pasa el cliente, que puede perder la respuesta de un `POST`, contestar un `503` con
`Retry-After` o cambiar el descubrimiento, y un servidor en el 8862 que hace de otro origen.
Cubre: sincronización inicial, cambios remotos con `has_more`, cambios por
herencia (§11.6) en otra colección, escrituras optimistas (también un `update` sobre un `create`
pendiente y dos `update` seguidos), conflicto con cada estrategia (`412` real y versión distinta
al releer), `delete` contra un elemento cambiado, reintento sin duplicar con
`Idempotent-Replayed`, `Retry-After`, escritura con el `_etag` del listado sin releer, `409
request_in_progress` reintentado tras `Retry-After`, `409 conflict` permanente, error permanente `422`, escrituras sin conexión que
sobreviven a cerrar y reabrir desde el archivo, servidor parado y vuelto a arrancar (`410` y
recarga), sincronización en segundo plano con `autoPush`, y errores al abrir. Y lo del borrador
6: una colección sin `sync` se recarga y nunca pide `:changes` (que da `404`); con
`"1.0-draft.5"` o `"1.1"` no se usa `sync`; `"2.0"`, `"1.01"` o `"1.0-beta.1"` no abren; sin
descubrimiento, solo `core`; una subcolección da error; `id_from` `"client"` sin alias y el
`409 taken` tras perder la respuesta; sin `writes`, un `create` incierto que no se repite solo,
`retryUncertain` y `discard()`; credenciales solo al origen y enlaces con otro esquema; y las
colaciones en `list()`.

Nota sobre el borrador 4: allí la misma petición aún en curso daba `409 conflict`, y este
cliente la reintentaba unas cuantas veces en un `create`. Desde el borrador 5 ese caso es `409
request_in_progress`, y un `409 conflict` se trata como permanente, así que con un servidor que
siga el borrador 4 deja de reintentar sus `409`: la operación se quita de la cola y se avisa con
`error`.

## Límites

- Sin canal de avisos (Server-Sent Events de `events`, §10): sincroniza por intervalo o bajo
  demanda. Así que tampoco nombra subcolecciones por su ruta en los eventos.
- No replica subcolecciones (`path` con plantilla y `parent`, como `/books/{book_id}/reviews` en el
  servidor de referencia), ni singletons: abrir una da un error claro. Replicarla exigiría elegir
  los elementos padre (uno, varios o todos) y un `state` por cada uno, y §10 solo define `:changes`
  para colecciones que declaran `sync`; ninguna subcolección de la referencia lo declara.
- Sincroniza colecciones enteras, como manda §10; los filtros de `list()` son locales (igualdad
  o función) y no hay índices.
- La cola es estrictamente FIFO: una operación que espera reintento bloquea las siguientes,
  también las de otras colecciones.
- Si se pierde la respuesta de un `create` y en segundo plano se traen cambios antes del
  reintento, el elemento puede verse dos veces (el provisional y el del servidor) hasta que el
  reintento confirma el id. Bajo demanda no pasa: si el envío falla por la red, no se traen
  cambios en esa ronda.
- No hay `batch` ni `actions`. `update` siempre manda Merge Patch.
- Si la API no anuncia `writes`, no manda `Idempotency-Key` (§7.1) y los `create` sin id propio
  pueden quedar inciertos (arriba). `If-Match` lo sigue mandando con la ETag que conozca, pero
  sin `writes` no puede contar con él.
- No interpreta `_inherited` ni ningún otro miembro `_`: los guarda como datos.
- El servidor de referencia guarda las claves de idempotencia en memoria: si se reinicia
  después de crear y antes de que llegue el reintento, ese reintento crea otro elemento. Un
  servidor real tiene que guardarlas de forma duradera (§7.1).

## Dudas sobre la spec

1. **ETag de los elementos sincronizados.** Resuelto en el borrador 5: en una colección que
   responde `:changes`, cada elemento que admite `PATCH` o `DELETE` lleva su `_etag` en todas
   las páginas (§7.2).
2. **Ids con comas.** `filter[id][in]=a,b` no define cómo escapar una coma dentro de un id, así
   que una colección con ids así no se puede sincronizar. En el borrador 6 un `id` solo lleva
   caracteres no reservados (§3), así que ya no puede llevar comas: resuelto.
3. **Id de `created`/`updated` que no vuelve al pedirlo.** Puede haberse borrado entre
   `:changes` y la petición, o haber dejado de ser visible. Este cliente lo quita de la copia
   y no avanza nada más; la spec no dice qué hacer, aunque el siguiente `:changes` debería
   traerlo en `destroyed`.
4. **`state` de una respuesta filtrada.** Resuelto en el borrador 6: una respuesta filtrada lleva
   el mismo `state` que la colección entera (§10). Este cliente sigue usando solo el de
   `:changes` o el de una carga completa.
5. **`409` ambiguo.** Resuelto en el borrador 5: la misma petición aún en curso da `409
   request_in_progress` (con `Retry-After`), distinto de `409 conflict`.
6. **Escrituras pendientes tras un `410`.** La spec dice que tras `410 state_expired` el cliente
   «vuelve a cargar la colección», pero no qué hace con sus escrituras pendientes basadas en
   versiones que ya no puede comprobar. Este cliente las conserva y las envía con su
   `If-Match`; si el elemento cambió, salta el conflicto.
7. **`sync` sin `writes`.** §1.2 hace que `sync` requiera `query` pero no `writes`. Con el
   borrador 6, §5.7 dice cuándo se puede repetir un `POST` sin `writes` (un id elegido por el
   cliente); fuera de eso, un cliente offline que crea necesita `writes`.
8. **Qué texto de `sync` conoce un cliente (§1.2).** Este cliente implementa el de
   `1.0-draft.6`. Cuando se publique `1.0`, ¿su `sync` será el mismo texto? Hasta saberlo, una
   API que declare `"1.0"` se recarga entera. Tampoco está claro si un `state` guardado bajo un
   borrador vale después de que el servidor declare otro: este cliente lo prueba y, si da `410`
   o `invalid_parameter` sobre `since`, recarga.
9. **`invalid_parameter` sobre `since`.** §10 da `410` cuando el servidor ya no puede calcular
   los cambios, pero un `state` que no emitió (o de otro arranque) también es un `since` malo
   (§6.4 lo dice de los cursores). Este cliente trata los dos igual: recarga.
10. **¿Cuándo «se sabe» que un create con id propio llegó?** §5.1 dice que tras un `409 taken`
    el cliente lee el elemento para saber si su primer intento llegó, pero leerlo solo dice que
    existe. Con un UUID v4 la colisión es despreciable y este cliente lo da por suyo si ya había
    intentado enviarlo antes.

## Código y avisos

Código y avisos: <https://github.com/restep-spec/restep-sync>. Ese repositorio se genera desde la web de
Restep, donde la herramienta se desarrolla junto a la [especificación](https://restep.wubytes.com/1/) y el servidor de
referencia: `vendor/` y `schema/` son copias de la web para que las pruebas funcionen solas.

Licencia MIT.
