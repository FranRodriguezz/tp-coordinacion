# TP Coordinación - Sistemas Distribuidos I (75.74) - Informe

**Alumno**: Franco Ezequiel Rodríguez

**Padrón**: 102815

**Facultad**: FIUBA — Facultad de Ingeniería, Universidad de Buenos Aires

**Materia**: Sistemas Distribuidos I

**Cuatrimestre**: 2do 2026

---

**Nota sobre el middleware**: se reutilizó la implementación propia del
TP1 (MOM), con dos modificaciones descritas en la sección
[Cambios al middleware respecto del TP1](#cambios-al-middleware-respecto-del-tp1).

## Diseño general

Cliente → Gateway → Sum (`SUM_AMOUNT` réplicas) → Aggregation (`AGGREGATION_AMOUNT` réplicas) → Join → Gateway → Cliente

| Etapa | Responsabilidad | Entrada |
|---|---|---|
| Gateway | Etiqueta los registros de cada cliente con su identificador y le devuelve su top final | TCP / cola de resultados |
| Sum | Acumula las cantidades por fruta de cada cliente | Cola compartida entre réplicas (*competing consumers*) |
| Aggregation | Consolida los totales de las frutas que le corresponden y calcula un top parcial por cliente | Cola propia, bindeada a su routing key en un exchange `direct` |
| Join | Combina los tops parciales de cada cliente y calcula su top final | Cola simple |

## Identificación de clientes

Todos los mensajes internos tienen la forma `[tipo, client_id, payload]`:

| Tipo | Valor | Payload |
|---|---|---|
| `DATA` | 1 | Un registro o un total, `[fruta, cantidad]`; entre Aggregation y Join, un top parcial `[[fruta, cantidad], ...]` |
| `EOF` | 2 | Lista `seen_by` de Sums que ya lo procesaron (vacía al salir del gateway); vacío entre Sum y Aggregation |
| `RESULT` | 3 | Top final `[[fruta, cantidad], ...]`, del Join al gateway |

- **El identificador** es un `uuid4` generado en el constructor de
  `MessageHandler`, del que el gateway crea una instancia por conexión. Se
  descartó un contador porque el gateway atiende a los clientes en procesos
  separados, donde un contador local podría repetir valores.
- **El tipo explícito** reemplaza la distinción por cantidad de campos del
  esqueleto, que deja de ser confiable al agregar el identificador.
- **El gateway filtra los resultados** con `deserialize_result_message`, que
  devuelve el top solo si el `client_id` coincide con el del handler.
- **Cada filtro guarda su estado por cliente** (diccionarios indexados por
  `client_id`) y elimina la entrada al terminar con ese cliente.

El fin de ingesta es **por cliente**: cada EOF indica que *ese* cliente
terminó, y su procesamiento avanza independientemente del resto. El sistema
no necesita saber cuántos clientes va a atender; ningún contador cuenta
clientes, solo réplicas, cuya cantidad se conoce por configuración.

## Coordinación entre instancias de Sum

**Problema**: los registros de un cliente se reparten entre todas las
réplicas de Sum, pero su EOF le llega a una sola. Si solo esa réplica
enviara sus totales, los datos de las demás nunca llegarían a Aggregation.

**Solución**: la Sum que recibe el EOF envía sus totales de ese cliente y
**republica el EOF en la misma cola de entrada**, agregando su ID a la
lista `seen_by` del payload. Como el EOF puede volver a una réplica que ya
lo procesó, cada Sum decide según `seen_by`:

| Caso | Datos del cliente | EOF |
|---|---|---|
| Su ID ya está en `seen_by` | Nada (ya los envió) | Lo republica sin cambios |
| No está, y faltan réplicas | Envía totales + EOF a Aggregation, y borra el estado | Agrega su ID y lo republica |
| No está, y con su ID se completan `SUM_AMOUNT` | Igual que el caso anterior | No lo republica |

**Por qué funciona**: RabbitMQ entrega en orden los mensajes de una misma
cola, y cada Sum los procesa de a uno. El gateway envía todos los registros
de un cliente antes que su EOF, y un EOF republicado entra al final de la
cola. Por lo tanto, cuando una Sum recibe el EOF, ya procesó todos los
registros de ese cliente que le fueron entregados.

**Alternativa descartada**: un exchange de control (`SUM_CONTROL_EXCHANGE`)
para avisar a las demás réplicas. El aviso y los datos viajarían por colas
distintas, sin orden garantizado entre ellas: una Sum podría procesar el
aviso antes que un registro pendiente, enviar totales incompletos y dejar
un estado huérfano en memoria.

## Coordinación Sum → Aggregation

**Particionado por fruta**: el broadcast del esqueleto hacía que todas las
réplicas de Aggregation calcularan el mismo top (cómputo redundante), y que
el Join recibiera copias repetidas de cada fruta. Se reemplazó por el envío
de cada total a una única réplica: `crc32(fruta) % AGGREGATION_AMOUNT`. La
regla depende solo de datos que todas las Sums conocen, por lo que la misma
fruta siempre llega a la misma réplica sin coordinación. Se usa `crc32` y
no `hash()` porque Python aleatoriza la semilla del hash de strings en cada
proceso (`PYTHONHASHSEED`), y cada Sum corre en un contenedor distinto.

**Fin de ingesta**: cada Sum envía su EOF a **todas** las réplicas de
Aggregation, incluso a las que no recibieron frutas suyas, ya que todas
necesitan saber que esa Sum terminó. Cada Aggregation calcula el top
parcial de un cliente recién al recibir `SUM_AMOUNT` EOFs de él.

**Acumulación**: el esqueleto usaba una lista ordenada y sumaba en su lugar
sin reordenar, lo que rompe el orden cuando la misma fruta llega de varias
Sums. Se reemplazó por un diccionario `{fruta: FruitItem}`, ordenando una
sola vez al armar el top. Suma y orden se delegan en `FruitItem`, cuya
implementación puede cambiar.

## Coordinación Aggregation → Join

Cada Aggregation envía **un único mensaje por cliente**, su top parcial, y
solo al terminar con ese cliente, por lo que ese mensaje funciona también
como fin de ingesta, sin un EOF adicional. El Join calcula el top final al
recibir `AGGREGATION_AMOUNT` tops parciales de un cliente.

Alcanza con enviar `TOP_SIZE` frutas por réplica: como cada fruta pertenece
a una única réplica, cualquier fruta del top final está necesariamente en
el top parcial de la réplica responsable de ella.

## Escalabilidad

- **Clientes**: el estado es independiente por cliente, pueden conectarse
  clientes nuevos en cualquier momento, y la memoria depende de los
  clientes activos, no del total atendido.
- **Volumen de datos**: los registros se reparten mensaje a mensaje entre
  las Sums, que los preagregan: hacia Aggregation viaja un mensaje por
  fruta distinta, no por registro, y hacia el Join solo `TOP_SIZE` frutas
  por réplica.
- **Cantidad de controles**: las réplicas se configuran por variables de
  entorno, sin nombres fijos en el código (verificado con el escenario de
  nombres aleatorios). Agregar Aggregations reparte frutas sin redundancia;
  agregar Sums reparte registros, con un costo de coordinación de al menos
  `SUM_AMOUNT` pasadas del EOF por cliente, independiente del volumen de
  datos.

**Limitaciones**: Gateway y Join son instancias únicas; el particionado
reparte por fruta y no por volumen, por lo que puede desbalancearse si pocas
frutas concentran los registros; y no se implementó batching entre etapas.

## Cambios al middleware respecto del TP1

- **Colas con nombre en el exchange**: en el TP1, cada instancia creaba una
  cola anónima y exclusiva. Un exchange `direct` descarta silenciosamente
  los mensajes sin cola bindeada, y `depends_on` no garantiza que Aggregation
  haya bindeado su cola antes de que Sum publique. Ahora la cola se nombra
  como la routing key, es durable y no exclusiva, y la declaran tanto el
  productor como el consumidor (la declaración es idempotente). Como
  consecuencia, dos instancias con la misma routing key comparten la cola,
  a diferencia del TP1.
- **`stop_consuming` seguro ante señales**: llamarlo desde el handler de
  SIGTERM interrumpía a pika en cualquier punto de su loop, y algunas
  réplicas quedaban bloqueadas hasta que Docker las mataba. Ahora la
  detención se agenda con `connection.add_callback_threadsafe`, y pika la
  ejecuta en un punto seguro.

## Cierre ante SIGTERM

Cada filtro registra un handler que detiene el consumo de su entrada; al
retornar `start_consuming`, cierra todas sus conexiones. El tiempo de
detención pasó de unos 5 segundos (cierre forzado por Docker) a unos 0.3.

## Limitaciones conocidas

- **Cliente sin registros**: su top vacío (`[]`) es falso en Python, por lo
  que el gateway no lo entregaría. La evaluación ocurre en archivos no
  modificables.
- **Carrera en el arranque**: en una corrida, un cliente falló al intentar
  conectarse antes de que el gateway escuchara. No se reprodujo, y ocurre en
  archivos no modificables.
- **Sin tolerancia a fallos**: el estado vive en memoria; si un filtro cae
  mientras procesa a un cliente, el resultado de ese cliente se pierde.