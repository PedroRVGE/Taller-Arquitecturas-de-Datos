# Fuentes, reglas de calidad y diccionario de datos

## Fuentes RSS seleccionadas

El [directorio RSS de El Tiempo](https://www.eltiempo.com/rss) es una página HTML que enlaza un feed XML por sección. Se usan tres feeds, configurados en `src/feeds.yml` con medio, sección y URL:

| id | Medio | Sección | URL del feed |
| --- | --- | --- | --- |
| `eltiempo_economia` | El Tiempo | Economía | <https://www.eltiempo.com/rss/economia.xml> |
| `eltiempo_politica` | El Tiempo | Política | <https://www.eltiempo.com/rss/politica.xml> |
| `eltiempo_deportes` | El Tiempo | Deportes | <https://www.eltiempo.com/rss/deportes.xml> |

Cada feed trae unas 10 noticias con `title`, `link`, `guid`, `description`, `pubDate` y `category`. El Tiempo publica `pubDate` en ISO 8601 con zona (`2026-10-21T17:09:08-05:00`), no en el RFC 822 del estándar RSS; el parser acepta ambos. Solo se usa el contenido del RSS: no se descargan las páginas enlazadas.

## Manejo de respuestas

| Situación | `status` en `bronze_ingestions` | ¿Se guarda en Bronze? | ¿Reintento? |
| --- | --- | --- | --- |
| HTTP 200 con ítems | `ok` | Sí | — |
| HTTP 200, RSS válido sin ítems | `no_items` | Sí (fuente disponible sin noticias) | — |
| Cuerpo vacío | `empty_response` | No | No |
| XML malformado o no RSS | `malformed_xml` | No | No |
| HTTP 404 u otro 4xx | `http_error` | No | No |
| HTTP 408, 429, 5xx | `http_error` | No | Sí (3, con espera exponencial) |
| Timeout o error de conexión | `network_error` | No | Sí |

Una fuente caída queda registrada y deja la corrida en `partial`: nunca se confunde con una fuente sin noticias, y sus noticias anteriores siguen vigentes en Silver. El XML se lee con `defusedxml`, que bloquea entidades externas y expansiones maliciosas.

## Identificación de noticias

1. `news_id = sha256(GUID normalizado)[:32]`. Si el GUID es una URL (El Tiempo usa el enlace como GUID) se normaliza igual que el enlace.
2. Sin GUID: `sha256(enlace canónico)`.
3. Enlace canónico: esquema `https`, host en minúsculas, sin fragmento, sin parámetros `utm_*`, `fbclid`, `gclid` y sin barra final.

**Noticias en varios feeds.** Si el mismo artículo aparece en dos secciones, produce el mismo `news_id` y se guarda una sola vez. `source_id` y `section` son los del primer feed donde apareció (orden de `feeds.yml` dentro de una corrida) y `feeds_seen` acumula todos. Los conteos por sección usan la sección principal, así que la suma de secciones es igual al total de noticias únicas.

**Limitaciones.** Un medio que cambia la URL de un artículo (por ejemplo, al corregir el título del *slug*) genera un `news_id` nuevo; dos medios que publican la misma nota con URL distinta cuentan como dos noticias; sin GUID ni enlace válido no hay identidad y el ítem va a cuarentena.

## Reglas de calidad

| Regla | Resultado |
| --- | --- |
| `title` vacío tras limpiar HTML | Cuarentena `missing_title` |
| `link` vacío | Cuarentena `missing_link` |
| `link` sin esquema http(s) o sin host | Cuarentena `invalid_link` |
| `pubDate` ausente, ilegible, sin zona horaria o más de un día en el futuro | Se conserva con `published_date_valid = false` y partición `unknown` |
| Mismo `news_id` dos veces en la corrida | `duplicates_in_batch`, se guarda una vez |
| `news_id` conocido con igual `content_hash` | `repeated`, sin escritura |
| `news_id` conocido con otro `content_hash` y versión más reciente | `updated`, `version + 1` |

`content_hash` resume título, descripción, enlace canónico y `pubDate` original. La limpieza quita etiquetas HTML y entidades (incluso codificadas dos veces) y normaliza espacios.

**Noticias sin fecha válida.** No se pueden ubicar en un día: quedan en Silver bajo `published_date='unknown'`, se excluyen de los agregados diarios de Gold y se cuentan en la bitácora de cada procesamiento (`pipeline_runs.news_without_valid_date`).

## Diccionario de datos

Las columnas y sus comentarios viven en `terraform/glue.tf` y aparecen en la consola de Glue y en Athena (`DESCRIBE news_lake.news`).

### Silver

| Tabla | Granularidad | Partición | Columnas principales |
| --- | --- | --- | --- |
| `news` | Una fila por noticia vigente | `published_date` (UTC) | `news_id`, `guid`, `link`, `canonical_link`, `title`, `description`, `published_raw`, `published_at_utc`, `published_date_valid`, `media`, `section`, `source_id`, `feeds_seen`, `content_hash`, `analyzed_text_hash`, `language`, `enrichment_status`, `analyzed_at`, `version`, `first_seen_run_id`, `last_updated_run_id`, `bronze_key`, `ingested_at_utc` |
| `news_entities` | Una fila por entidad distinta (tipo + texto normalizado) en cada noticia vigente | `published_date` | `news_id`, `entity_type`, `entity_text`, `entity_text_normalized`, `mentions`, `max_score`, `analyzed_text_hash`, `media`, `section` |
| `news_key_phrases` | Una fila por frase clave distinta en cada noticia vigente | `published_date` | `news_id`, `key_phrase`, `key_phrase_normalized`, `mentions`, `max_score`, `analyzed_text_hash`, `media`, `section` |
| `comprehend_results` | Un análisis por texto distinto (caché) | `analyzed_date`, `run_id` | `text_hash`, `language`, `analyzed_text`, `text_bytes`, `truncated`, `status`, `analyzed_at`, `entities_json`, `key_phrases_json` |
| `news_quarantine` | Un ítem rechazado | `ingestion_date`, `run_id` | `source_id`, `bronze_key`, `item_index`, `rejection_reason`, `guid`, `link`, `title`, `published_raw`, `detected_at` |

### Gold

| Tabla | Granularidad | Métrica | Pregunta que responde |
| --- | --- | --- | --- |
| `gold_news_daily` | día × medio × sección | `news_count` = noticias distintas | ¿Cuántas noticias por día, medio y sección? |
| `gold_entity_daily` | día × tipo × entidad normalizada | `news_count` = noticias distintas que mencionan la entidad | ¿Qué personas, organizaciones y lugares aparecen en más noticias? (`03_top10_organizaciones.sql`, `12_top10_personas_y_lugares.sql`) ¿Cómo cambia su presencia entre fechas? (`04_presencia_entidad_por_dia.sql`) |
| `gold_key_phrases_daily` | día × medio × sección × frase | `news_count` = noticias distintas que contienen la frase | ¿Qué frases clave son más frecuentes por sección? |

La presencia de una entidad cuenta **noticias distintas**: si "Petro" aparece tres veces en una noticia, suma 1. Como cada noticia tiene una sola `published_date`, sumar los días no cuenta dos veces la misma noticia.

### Control

| Tabla | Granularidad | Uso |
| --- | --- | --- |
| `bronze_ingestions` | fuente × corrida | Estado HTTP, hash, bytes, ítems y archivo de Bronze |
| `pipeline_runs` | procesamiento de una corrida (una corrida repetida o recuperada suma filas, no reemplaza la anterior) | Modo (`new` = extracción de los RSS, `reprocess` = desde Bronze, `rebuild`), estado, fuentes caídas y reutilizadas, conteos recibidos, rechazados, nuevos, actualizados, repetidos, noticias sin fecha válida, llamadas, caché y errores de Comprehend, bytes escaneados |

**Recibidos frente a noticias únicas.** `received` cuenta ítems de los XML: incluye la misma noticia en dos feeds (`duplicates_in_batch`), noticias ya conocidas (`repeated`) y rechazos. Por eso `received = new + updated + repeated + duplicates_in_batch + rejected` (lo comprueba la validación `v07`) y `unique_news_current` crece solo con `new`.

## Enriquecimiento con Comprehend

- Texto analizado: `título. descripción`, en el idioma del feed (`es`).
- `DetectEntities` (se conservan PERSON, ORGANIZATION y LOCATION) y `DetectKeyPhrases`, con sus puntuaciones de confianza.
- Caché: si `sha256(idioma|texto)` ya está en `comprehend_results`, no se llama al servicio.
- Textos vacíos (`empty_text`), idiomas fuera de `supported_languages` (`unsupported_language`) y textos que superan `max_text_bytes` (se recortan en un límite UTF-8 válido y quedan `truncated`) tienen un estado explícito. Un error de la API deja la noticia en `error`, no se guarda en caché y se reintenta en la siguiente corrida.
- Limitación para la demostración: las entidades son menciones detectadas por un modelo. No se resuelven alias ("Gustavo Petro" y "Petro" son dos entidades), el modelo puede clasificar mal un nombre propio (un equipo como LOCATION) y un titular corto da poco contexto.
