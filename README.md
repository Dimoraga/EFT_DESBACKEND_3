# Banco XYZ: BFF + OAuth2 + Resiliencia + Kafka en Docker

Actividad correspondiente a la Evaluación Final Transversal (EFT) del curso desarrollo Backend III. Sobre el sistema bancario que se viene construyendo en el curso (batch de la primera sumativa y BFF por canal de la semana 5), este proyecto agrega:

- **OAuth2**: un Authorization Server propio que emite tokens `client_credentials` para la comunicación servicio a servicio, y un core que los valida como Resource Server con autorización por scope.
- **Resiliencia** con Resilience4j en los 3 BFF: circuit breaker, retry, bulkhead y timeouts, con respuesta controlada **503 + `Retry-After`**.
- **Mensajería asíncrona** con Kafka: cada retiro confirmado publica un evento que consume un servicio de notificaciones.
- **Docker y docker-compose**: una imagen por servicio y un único `docker-compose.yaml` que levanta todo el sistema (9 servicios y una tarea de inicialización).

Todos los servicios usan **Java 21**, **Spring Boot 4.1.1** y Maven Wrapper.

---

## Arquitectura

```mermaid
flowchart LR
    subgraph clientes[Clientes]
        NAV[Navegador]
        APP[App móvil]
        ATM[Cajero automático]
    end

    subgraph red[Red Docker banco-net]
        AUTH[auth-server :9000<br/>Spring Authorization Server]
        WEB[bbf-web :8444]
        MOB[bbf-mobile :8445]
        CAJ[bbf-cajeros :8446]
        CORE[core-banking-api :8443<br/>Resource Server]
        DB[(MySQL 8.0<br/>springbatch_db)]
        K{{Kafka 3.9 KRaft<br/>topic retiros-realizados}}
        NOT[notificaciones<br/>consumidor]
        BATCH[bankbatchprocessor<br/>Spring Batch, one-shot]
    end

    NAV -- JWT usuario canal WEB --> WEB
    APP -- JWT usuario canal MOBILE --> MOB
    ATM -- JWT usuario canal CAJERO --> CAJ

    WEB -. 1. client_credentials .-> AUTH
    MOB -. 1. client_credentials .-> AUTH
    CAJ -. 1. client_credentials .-> AUTH
    WEB -- 2. Bearer scope core.read + Resilience4j --> CORE
    MOB -- 2. Bearer scope core.read + Resilience4j --> CORE
    CAJ -- 2. Bearer scope core.read/core.write + Resilience4j --> CORE
    CORE -. 3. valida firma con JWKS .-> AUTH

    CORE --> DB
    BATCH -- CSV a tablas de reporting --> DB
    CORE == evento RetiroRealizado ==> K
    K == consume ==> NOT
```

- **Flujo de autenticación** (líneas punteadas): cada BFF obtiene un token de servicio del `auth-server` (1), lo envía como `Bearer` al core (2), y el core verifica la firma contra el JWKS del `auth-server` y el scope del token (3). El usuario final, en cambio, se autentica con un JWT de usuario por canal que firma `core-banking-api` y que cada BFF valida localmente.
- **Flujo de eventos** (líneas dobles): cuando un retiro se confirma en la base de datos, el core publica `RetiroRealizado` en Kafka y `notificaciones` lo consume.

### Servicios

| Servicio | Puerto (host) | Rol | Tecnología |
|---|---|---|---|
| `auth-server` | 9000 | Authorization Server OAuth2: registra un cliente por BFF y emite JWT `client_credentials` | Spring Authorization Server |
| `core-banking-api` | 8443 | Único servicio con acceso de escritura a MySQL. Emite los JWT de usuario final, expone `/internal/**` como Resource Server y publica eventos de retiro | Spring Web MVC, Spring Data JPA, Spring Security OAuth2 Resource Server, Spring Kafka |
| `bbf-web` | 8444 | BFF para navegador: login usuario/clave, saldo, movimientos, dashboard | Spring Web MVC, OAuth2 Client, Resilience4j |
| `bbf-mobile` | 8445 | BFF para app móvil: login con `deviceId`, refresh token, respuestas livianas | Spring Web MVC, OAuth2 Client, Resilience4j |
| `bbf-cajeros` | 8446 | BFF para cajeros: login tarjeta/PIN, saldo y retiro | Spring Web MVC, OAuth2 Client, Resilience4j |
| `notificaciones` | — | Consume `retiros-realizados` y registra una notificación por retiro | Spring Kafka |
| `bankbatchprocessor` | — | Batch de la primera sumativa: carga CSV en las tablas de reporting y termina (repositorio git propio) | Spring Batch |
| `mysql` | 3307 → 3306 | Base `springbatch_db` (se carga desde `db-init/` la primera vez) | MySQL 8.0 |
| `kafka` | 9092 → 29092 | Broker de mensajería, un nodo en modo KRaft | Apache Kafka 3.9.0 |
| `kafka-init` | — | Tarea one-shot que crea el topic `retiros-realizados` (3 particiones) y termina | Apache Kafka 3.9.0 |

> MySQL del compose se publica en el puerto **3307** del host para no chocar con un MySQL local (por ejemplo el servicio `MySQL80` de Windows) en el 3306. Dentro de la red Docker los servicios lo usan como `mysql:3306`.

---

## Cómo levantar el sistema

### Requisitos previos

- **Docker Desktop** con el backend WSL 2 (Windows) y al menos **4 GB** de memoria asignados a Docker. El stack completo usa unos 1,9 GB.
- **Git**.
- Para reconstruir las imágenes no hace falta Java en el host (se compila dentro de Docker). Para correr los servicios fuera de Docker: **JDK 21** y un MySQL local.
- Puertos libres en el host: 3307, 9000, 8443, 8444, 8445, 8446 y 9092.

### 1. Variables de entorno (`.env`)

```powershell
Copy-Item .env.example .env
```

Completa `.env` (no se versiona) con valores reales:

| Variable | Qué es |
|---|---|
| `DB_PASSWORD` | Clave del usuario `root` del contenedor MySQL, también la usan core y bankbatchprocessor |
| `JWT_KEYSTORE_PASSWORD` | Clave del keystore `keys/core-signing-key.p12` |
| `BBF_WEB_SECRET`, `BBF_MOBILE_SECRET`, `BBF_CAJEROS_SECRET` | Secretos OAuth2 de cada BFF. Deben ser **distintos entre sí** (Spring Authorization Server rechaza secretos repetidos) |

Para generar secretos aleatorios, por ejemplo desde Git Bash: `openssl rand -base64 32`.

### 2. Llaves de firma (`keys/`)

`core-banking-api` firma los JWT de usuario con un par de llaves RS256, y los BFF validan esa firma con el certificado público. En Docker ambos archivos se montan **en solo lectura** desde `./keys` y nunca se copian dentro de las imágenes:

| Archivo | Lo usa | De dónde sale |
|---|---|---|
| `keys/core-signing-key.p12` | `core-banking-api` (llave privada) | Lo genera `generate-keystores.ps1` en `core-banking-api/src/main/resources/keys/` |
| `keys/core-signing-cert.pem` | 3 BFF (certificado público) | Mismo script, mismo directorio |

`generate-keystores.ps1` no trae contraseñas por defecto: las toma de `-StorePassword` / `-SigningKeyPassword`, de las variables de entorno `KEYSTORE_STORE_PASSWORD` / `KEYSTORE_SIGNING_PASSWORD`, o las pide por consola sin mostrarlas. Si no recibe ninguna (o tienen menos de 6 caracteres), falla sin generar nada, y nunca las imprime. La contraseña de firma es la que después va en `JWT_KEYSTORE_PASSWORD`.

```powershell
.\generate-keystores.ps1          # solo si aún no existen las llaves (pide las contraseñas)
New-Item -ItemType Directory -Force keys
Copy-Item core-banking-api\src\main\resources\keys\core-signing-key.p12, `
          core-banking-api\src\main\resources\keys\core-signing-cert.pem keys\
```

La carpeta `keys/` está en `.gitignore`.

### 3. Levantar, bajar y reiniciar

```powershell
docker compose up -d                # levanta todo (usa las imágenes banco/*:1.0)
docker compose up -d --build        # reconstruye las imágenes antes de levantar
docker compose ps -a                # estado: 8 servicios running, kafka-init y bankbatchprocessor "Exited (0)"
docker compose down                 # baja todo y conserva los datos de MySQL
docker compose down -v              # baja todo y BORRA el volumen de MySQL: el próximo "up" vuelve a cargar db-init/
```

Orden de arranque garantizado por `depends_on`: `mysql` y `kafka` healthy → `core-banking-api`; `auth-server` y core iniciados → BFF; `kafka-init` terminado OK → `notificaciones`; `mysql` healthy → `bankbatchprocessor`. Todo queda arriba en 1 a 2 minutos.

### Ejecución local sin Docker (opcional)

Cada servicio sigue funcionando con su perfil `local` (HTTPS con los keystores de `generate-keystores.ps1` y secretos en `application-local.properties`, que no se versiona):

```powershell
cd auth-server;      .\mvnw.cmd spring-boot:run
cd core-banking-api; .\mvnw.cmd spring-boot:run
cd bbf-web;          .\mvnw.cmd spring-boot:run      # igual para bbf-mobile, bbf-cajeros y notificaciones
```

---

## OAuth2

Hay **dos tipos de token**, con responsabilidades distintas.

### Token de servicio: OAuth2 `client_credentials`

`auth-server` (Spring Authorization Server, issuer `http://auth-server:9000` en Docker) registra un cliente por BFF:

| Cliente | Scopes | Para qué |
|---|---|---|
| `bbf-web` | `core.read` | Consultas: saldo, movimientos, reportes, login de usuario |
| `bbf-mobile` | `core.read` | Consultas y login/refresh de usuario |
| `bbf-cajeros` | `core.read`, `core.write` | Consultas y **retiros** |

Flujo:

1. El BFF pide un token con `POST /oauth2/token` (`grant_type=client_credentials`, autenticación `client_secret_basic`). Spring Security OAuth2 Client lo cachea y lo renueva cuando expira.
2. Lo envía como `Authorization: Bearer <token>` en cada llamada a `core-banking-api`.
3. El core, como **Resource Server**, valida la firma contra `GET /oauth2/jwks` del `auth-server`, el `issuer` y la expiración, y autoriza por scope:

| Endpoint de `core-banking-api` | Exige |
|---|---|
| `POST /internal/cuentas/{id}/retiros` | `SCOPE_core.write` (solo `bbf-cajeros`) |
| Resto de `/internal/**` (`/internal/auth/token/*`, `/internal/cuentas/{id}/saldo`, `/movimientos`, `/internal/reportes/**`) | `SCOPE_core.read` |
| Cualquier otra ruta | Denegada (`denyAll`) |
| Sin token | 401 |

### JWT de usuario final, por canal

Al hacer login en un BFF, el core emite un JWT RS256 firmado con `core-signing-key.p12` que incluye los claims `canal` (`WEB`, `MOBILE` o `CAJERO`), `cuentaId` y `roles`. Cada BFF lo valida **localmente** con `core-signing-cert.pem` y:

- exige que el claim `canal` sea el suyo: un token `WEB` usado en `bbf-cajeros` recibe **403** aunque la firma sea válida;
- exige que la cuenta del path coincida con el claim `cuentaId`.

| Canal | Login | Duración del token |
|---|---|---|
| `bbf-web` | `POST /auth/login` `{usuario, password}` | 30 min |
| `bbf-mobile` | `POST /auth/login` `{usuario, password, deviceId}` y `POST /auth/refresh` | 15 min (refresh: 7 días) |
| `bbf-cajeros` | `POST /auth/login` `{numeroTarjeta, pin}` | 5 min |

---

## Resiliencia (Resilience4j)

Los 3 BFF comparten la misma configuración (`config/ResilienceConfig.java`), aplicada como interceptor del `RestClient` hacia el core (`config/CoreBankingClientConfig.java`):

| Mecanismo | Configuración real |
|---|---|
| Timeouts | conexión 2 s, lectura 3 s |
| Bulkhead | máximo 10 llamadas concurrentes, espera máxima 200 ms |
| Circuit Breaker | ventana de 10 llamadas (por conteo), mínimo 5 llamadas, abre con ≥ 50 % de fallos, 15 s abierto, 3 llamadas de prueba en half-open |
| Retry | 3 intentos con backoff exponencial (300 ms × 2). **Solo en `GET`** |

- **Qué se reintenta**: solo consultas `GET` (saldo, movimientos, reportes). Los `POST` (login y retiros) **nunca** se reintentan, para no duplicar operaciones. Además, los retiros admiten `Idempotency-Key`.
- **Qué cuenta como fallo**: errores de red y respuestas **5xx**. Los **4xx** (errores de negocio, como saldo insuficiente) pasan intactos al cliente.
- **Fallback**: si el core no responde, el bulkhead está lleno o el circuito está abierto, el BFF responde:

```http
HTTP/1.1 503
Retry-After: 15

{"status":503,"message":"El servicio bancario no esta disponible temporalmente. Intente nuevamente en unos segundos.", ...}
```

Las transiciones del circuito quedan en el log del BFF (`CircuitBreaker core-banking-api: State transition from CLOSED to OPEN`, etc.).

---

## Mensajería asíncrona (Kafka)

| | |
|---|---|
| Topic | `retiros-realizados`: 3 particiones, replication factor 1, creado por `kafka-init` |
| Productor | `core-banking-api` (`event/RetiroEventPublisher`), key = `cuentaId` (mismo orden de eventos por cuenta) |
| Consumidor | `notificaciones` (`RetiroConsumer`), group id `notificaciones` |

Formato del evento (JSON):

```json
{"cuentaId":105,"monto":1000,"saldoDisponible":239000.00,"fecha":"2026-10-05T00:55:00"}
```

Cómo funciona el productor:

- Publica **después del commit** de la transacción del retiro (`TransactionSynchronization.afterCommit`): nunca se notifica un retiro que luego se revierte, y los reintentos idempotentes no se vuelven a publicar.
- El envío es **no bloqueante**: corre en un hilo propio con cola acotada, así que el retiro responde de inmediato aunque Kafka tarde o esté caído (`max.block.ms` = 3 s, `delivery.timeout.ms` = 5 s).
- Es **best-effort**: si Kafka no está disponible, el retiro igual responde 200 y el core solo registra `WARN No se pudo publicar evento de retiro: ...`.

El consumidor deja en el log una línea como:

```
NOTIFICACION -> Retiro de $1000 en la cuenta 105. Saldo disponible: $231000.00 (2026-10-05T17:06:32)
```

---

## Dockerización

- **Una imagen por servicio** (`<modulo>/Dockerfile`), con nombre `banco/<servicio>:1.0`:
  - **Etapa build**: `eclipse-temurin:21-jdk` + Maven Wrapper del módulo. Copia primero `pom.xml` (cachea dependencias) y después `src/`, y compila con `-DskipTests`.
  - **Etapa runtime**: `eclipse-temurin:21-jre-alpine`, usuario **no root** (`app`), `EXPOSE` del puerto y `JAVA_OPTS="-Xms64m -Xmx256m"` (384m en core y batch).
  - `.dockerignore` excluye `target/`, logs, `application-local.properties`, `*.p12` y `keys/`. **Ninguna imagen contiene secretos ni llaves.**
- **Perfil `docker`** (`SPRING_PROFILES_ACTIVE=docker`, archivo `application-docker.properties` de cada módulo): HTTP sin TLS dentro de la red, hostnames de red (`mysql`, `kafka`, `auth-server`, `core-banking-api`) y secretos solo por variables de entorno, sin valor por defecto.

### Variables de entorno por servicio

| Servicio | Obligatorias (desde `.env`) | Opcionales (valor por defecto en el perfil docker) |
|---|---|---|
| `auth-server` | `BBF_WEB_SECRET`, `BBF_MOBILE_SECRET`, `BBF_CAJEROS_SECRET` | `AUTH_ISSUER` (`http://auth-server:9000`) |
| `core-banking-api` | `DB_PASSWORD`, `JWT_KEYSTORE_PASSWORD` | `DB_URL`, `DB_USERNAME` (`root`), `JWT_KEY_PASSWORD` (igual al del keystore), `JWT_KEYSTORE_PATH` (`file:/keys/core-signing-key.p12`), `AUTH_ISSUER`, `AUTH_JWK_URI`, `KAFKA_BOOTSTRAP` (`kafka:9092`) |
| `bbf-web` / `bbf-mobile` / `bbf-cajeros` | `BBF_WEB_SECRET` / `BBF_MOBILE_SECRET` / `BBF_CAJEROS_SECRET` | `CORE_BASE_URL` (`http://core-banking-api:8443`), `AUTH_TOKEN_URI` (`http://auth-server:9000/oauth2/token`), `JWT_PUBLIC_KEY_PATH` (`file:/keys/core-signing-cert.pem`) |
| `notificaciones` | — | `KAFKA_BOOTSTRAP` (`kafka:9092`) |
| `bankbatchprocessor` | `DB_PASSWORD` | `DB_URL`, `DB_USERNAME` |
| `mysql` | `MYSQL_ROOT_PASSWORD` (= `DB_PASSWORD`) | — |

### Volúmenes

| Volumen | Servicio | Uso |
|---|---|---|
| `mysql-data` (nombrado) | `mysql` | Datos persistentes entre `down` y `up` |
| `./db-init` → `/docker-entrypoint-initdb.d` (ro) | `mysql` | Dump inicial `01-springbatch_db.sql` (solo corre con el volumen vacío) |
| `./keys` → `/keys` (ro) | core y 3 BFF | Llave privada (core) y certificado público (BFF) para los JWT de usuario |

### Límites de memoria

`mem_limit`: 768m para mysql, kafka y core; 640m para bankbatchprocessor; 512m para el resto de las JVM. Kafka corre con `KAFKA_HEAP_OPTS=-Xmx256m`.

---

## Pruebas manuales

Desde la raíz del proyecto, con el stack arriba. Los cuerpos JSON están en archivos (`login.json`, `login-mobile.json`, `login-cajero.json`, `retiro.json`). En Windows usa `curl.exe` con `--data-binary @archivo`: los pipes de PowerShell 5.1 agregan un BOM que rompe el JSON.

Credenciales **de demo** (sembradas en la base): cuenta 105 → usuario `usuario105`, tarjeta `4000-1234-5678-0105` (ver los archivos JSON).

```powershell
# 401: core sin token
curl.exe -i http://localhost:8443/internal/cuentas/105/saldo

# Login web + saldo (200)
curl.exe -X POST http://localhost:8444/auth/login -H "Content-Type: application/json" --data-binary "@login.json"
curl.exe http://localhost:8444/cuentas/105/saldo -H "Authorization: Bearer <token web>"

# Login mobile + saldo (200)
curl.exe -X POST http://localhost:8445/auth/login -H "Content-Type: application/json" --data-binary "@login-mobile.json"
curl.exe http://localhost:8445/cuentas/105/saldo -H "Authorization: Bearer <accessToken>"

# Login cajero + retiro de 1000 (200) -> genera evento Kafka
curl.exe -X POST http://localhost:8446/auth/login -H "Content-Type: application/json" --data-binary "@login-cajero.json"
curl.exe -X POST http://localhost:8446/cuentas/105/retiros -H "Authorization: Bearer <token cajero>" `
  -H "Content-Type: application/json" --data-binary "@retiro.json"

# 403: token de usuario WEB contra el BFF de cajeros (canal incorrecto)
curl.exe -i -X POST http://localhost:8446/cuentas/105/retiros -H "Authorization: Bearer <token web>" `
  -H "Content-Type: application/json" --data-binary "@retiro.json"

# Resiliencia: con core detenido, el BFF responde 503 + Retry-After: 15
docker compose stop core-banking-api
curl.exe -i http://localhost:8444/cuentas/105/saldo -H "Authorization: Bearer <token web>"
docker compose start core-banking-api
```

Logs:

```powershell
docker compose logs -f notificaciones                       # líneas "NOTIFICACION -> ..."
docker compose logs -f core-banking-api                     # "Evento de retiro publicado" / WARN si Kafka cae
docker compose logs bbf-web | Select-String CircuitBreaker  # transiciones del circuit breaker
docker compose logs bankbatchprocessor                      # jobs COMPLETED y exit 0
```

---

## Evidencia

### `evidencia/` (ejecución local sin Docker)

| Archivo | Qué demuestra |
|---|---|
| `c12-respuesta-503.txt` | Respuesta 503 + `Retry-After: 15` del BFF con core caído |
| `core-banking-api.log`, `bbf-cajeros.log`, `bbf-mobile.log` | Logs de la ejecución local de la semana (arranques, fallos de core y `ServicioNoDisponibleException` en el BFF) |
| `notificaciones.log` | Consumidor Kafka recibiendo eventos de retiro (`NOTIFICACION -> ...`) |
| `core-banking-api-kafka.log`, `core-banking-api-kafka-v2.log` | Productor Kafka del core: publicación por partición/offset y `WARN` con Kafka caído (v2: envío no bloqueante) |
| `auth-server-kafka.log` | Arranque del auth-server durante las pruebas de Kafka |
| `kafka-flujo-completo.txt` | Flujo completo core → Kafka → notificaciones y prueba de resiliencia con Kafka caído |
| `auth-server-validacion.log`, `core-banking-api-validacion.log`, `bbf-web-validacion.log` | Arranque local tras eliminar el antiguo API key interno (core arranca aunque Kafka esté apagado) |
| `docker-build.txt` | Construcción de las 7 imágenes `banco/*:1.0` y smoke test de `auth-server` |

### `evidencia/docker/` (stack completo con docker compose)

| Archivo | Qué demuestra |
|---|---|
| `docker-compose-ps.txt` | Estado de todos los servicios (running/healthy, one-shots con `Exited (0)`) |
| `docker-images.txt` | Imágenes `banco/*:1.0` y de infraestructura, con tamaños |
| `pruebas-e2e.txt` | Pruebas a–h: 401 sin token, 200 en login/saldo/retiro, 403 por canal y por scope, circuit breaker, Kafka caído y batch |
| `logs-circuit-breaker.txt` | Transiciones `CLOSED → OPEN → HALF_OPEN → CLOSED` en bbf-web |
| `logs-notificaciones.txt` | Consumidor conectado y notificaciones recibidas |
| `logs-core-kafka-caido.txt` | Core con Kafka detenido: `WARN` sin afectar el retiro, y recuperación al volver Kafka |
| `verificacion-desde-cero.txt` | `down -v` + `up -d` desde cero y prueba mínima (login, saldo, retiro y notificación) |

---

## Decisiones de diseño y limitaciones conocidas

| Tema | Decisión / limitación |
|---|---|
| HTTP sin TLS dentro de la red Docker | Simplifica certificados entre contenedores. En producción el TLS terminaría en el borde (API gateway / ingress) o se usaría mTLS en la malla. La ejecución local sigue usando HTTPS. |
| Eventos perdidos si Kafka cae | El productor es best-effort: si Kafka no está disponible el retiro se confirma igual, pero su evento se pierde. Mejora: **patrón outbox** (guardar el evento en la misma transacción y publicarlo después). |
| Horas en UTC en contenedores | Las imágenes no fijan zona horaria, así que fechas y logs salen en UTC (en local salían en -03:00). Se puede agregar `TZ: America/Santiago` por servicio. |
| `--lower-case-table-names=1` en MySQL | El dump sale de MySQL en Windows (nombres de tabla en minúsculas). Sin esta opción, MySQL en Linux distingue mayúsculas y el script `BATCH_*` de Spring Batch choca con las tablas `batch_*` del dump. Solo se aplica al crear el volumen. |
| `bankbatchprocessor` corre una vez y termina | Ejecuta sus 3 jobs (cada uno limpia su tabla antes de cargarla) y sale con código 0. Tiene `restart: "no"`; para volver a correrlo: `docker compose up bankbatchprocessor`. |
| Secretos solo por `.env` | Ningún secreto va en el yaml, en las imágenes ni en archivos versionados. `.env` y `keys/` están en `.gitignore`; `.env.example` documenta las claves. |
| Llave de firma del `auth-server` en memoria | Se regenera en cada reinicio; los tokens de servicio vigentes dejan de validar y los BFF piden uno nuevo. |
| Otras simplificaciones | Sin revocación de refresh token (solo expira), sin rate limiting (el bulkhead limita concurrencia, no tasa), sin bloqueo de tarjeta por PIN incorrecto repetido, idempotencia de retiros sin TTL. |

---

## Solución de problemas

| Síntoma | Causa / solución |
|---|---|
| `mysql` no publica el puerto o falla `up` por puerto ocupado | El compose usa **3307** en el host. Si también está ocupado, cambia el mapeo `"3307:3306"`. Desde el host: `mysql -h 127.0.0.1 -P 3307 -u root -p`. |
| `bankbatchprocessor` termina con `Exited (1)` y `Duplicate foreign key constraint name` | El volumen se creó sin `--lower-case-table-names=1`. Recrear con `docker compose down -v` y `docker compose up -d`. |
| `auth-server` no arranca: `Found duplicate client secret` | Los tres `BBF_*_SECRET` del `.env` deben ser distintos. |
| `docker compose` no responde / "Docker Desktop is starting" | Iniciar Docker Desktop y esperar el backend WSL 2. En Windows, `wsl --shutdown` y reabrir Docker Desktop suele destrabarlo. |
| Contenedores que se reinician o mueren (`OOMKilled`) | Falta memoria: asignar ≥ 4 GB a Docker (Settings → Resources o `.wslconfig`) y cerrar otros procesos Java locales. Revisar con `docker stats --no-stream`. |
| BFF responde 503 justo después de `up` | El core tarda unos 30 s en arrancar; el circuito se cierra solo tras 15 s. Reintentar después de `Retry-After`. |
| Kafka healthy pero `notificaciones` no recibe | Revisar `docker compose logs kafka-init` (debe decir `Created topic` o que ya existía) y `docker compose logs notificaciones` (`partitions assigned`). |
