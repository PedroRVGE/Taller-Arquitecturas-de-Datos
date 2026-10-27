# Glue Data Catalog con tablas explícitas (sin crawlers).
# Las columnas y sus comentarios son el diccionario de datos. El código Python lee estos
# esquemas desde Glue para escribir Parquet: Terraform es la única fuente de verdad.
#
# Particiones: el DAG las registra con la API de Glue (BatchCreatePartition/UpdatePartition)
# después de escribir los archivos. Cada partición apunta a una carpeta versionada por
# corrida; cambiar el puntero es lo que "publica" los datos para Athena.

locals {
  formats = {
    parquet = {
      input_format  = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"
      output_format = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"
      serde         = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      parameters    = { "classification" = "parquet", "parquet.compression" = "SNAPPY" }
    }
    json = {
      input_format  = "org.apache.hadoop.mapred.TextInputFormat"
      output_format = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"
      serde         = "org.openx.data.jsonserde.JsonSerDe"
      parameters    = { "classification" = "json" }
    }
  }

  by_published_date = [["published_date", "string", "Fecha de publicación en UTC (AAAA-MM-DD) o 'unknown' si pubDate no es válido"]]

  glue_tables = {
    news = {
      location    = "silver/news/"
      format      = "parquet"
      description = "Silver. Una fila por noticia vigente."
      partitions  = local.by_published_date
      columns = [
        ["news_id", "string", "sha256 del GUID o, si falta, del enlace normalizado"],
        ["guid", "string", "GUID del RSS"],
        ["link", "string", "Enlace publicado"],
        ["canonical_link", "string", "Enlace normalizado: minúsculas en el host, sin fragmento ni parámetros utm_"],
        ["title", "string", "Título sin etiquetas HTML"],
        ["description", "string", "Descripción sin etiquetas HTML"],
        ["published_raw", "string", "Valor original de pubDate"],
        ["published_at_utc", "timestamp", "pubDate normalizado a UTC; nulo si no es válido"],
        ["published_date_valid", "boolean", "pubDate interpretable y no futuro"],
        ["media", "string", "Medio de la fuente (El Tiempo)"],
        ["section", "string", "Sección del primer feed donde apareció la noticia"],
        ["source_id", "string", "Fuente principal (primer feed donde apareció)"],
        ["feeds_seen", "array<string>", "Todas las fuentes donde apareció la noticia"],
        ["content_hash", "string", "sha256 de título, descripción, enlace y pubDate: detecta cambios"],
        ["analyzed_text_hash", "string", "sha256 del idioma y el texto enviado a Comprehend"],
        ["language", "string", "Idioma usado en Comprehend"],
        ["enrichment_status", "string", "analyzed | truncated | empty_text | unsupported_language | error"],
        ["analyzed_at", "timestamp", "Fecha del análisis de Comprehend"],
        ["version", "int", "Versión de la noticia; aumenta cuando cambia content_hash"],
        ["first_seen_run_id", "string", "Corrida que vio la noticia por primera vez"],
        ["last_updated_run_id", "string", "Corrida que produjo la versión vigente"],
        ["bronze_key", "string", "Archivo de Bronze de la versión vigente"],
        ["ingested_at_utc", "timestamp", "Ingesta (UTC) de la versión vigente"],
      ]
    }

    news_entities = {
      location    = "silver/news_entities/"
      format      = "parquet"
      description = "Silver. Una fila por entidad distinta detectada en cada noticia vigente."
      partitions  = local.by_published_date
      columns = [
        ["news_id", "string", "Noticia a la que pertenece la entidad"],
        ["entity_type", "string", "PERSON | ORGANIZATION | LOCATION"],
        ["entity_text", "string", "Texto tal como lo devolvió Comprehend (primera aparición)"],
        ["entity_text_normalized", "string", "Texto en minúsculas y con espacios normalizados"],
        ["mentions", "int", "Menciones de la entidad dentro del texto analizado"],
        ["max_score", "double", "Mayor puntuación de confianza entre las menciones"],
        ["analyzed_text_hash", "string", "Texto analizado que originó la entidad"],
        ["media", "string", "Medio de la noticia"],
        ["section", "string", "Sección principal de la noticia"],
      ]
    }

    news_key_phrases = {
      location    = "silver/news_key_phrases/"
      format      = "parquet"
      description = "Silver. Una fila por frase clave distinta detectada en cada noticia vigente."
      partitions  = local.by_published_date
      columns = [
        ["news_id", "string", "Noticia a la que pertenece la frase"],
        ["key_phrase", "string", "Frase tal como la devolvió Comprehend (primera aparición)"],
        ["key_phrase_normalized", "string", "Frase en minúsculas y con espacios normalizados"],
        ["mentions", "int", "Apariciones dentro del texto analizado"],
        ["max_score", "double", "Mayor puntuación de confianza"],
        ["analyzed_text_hash", "string", "Texto analizado que originó la frase"],
        ["media", "string", "Medio de la noticia"],
        ["section", "string", "Sección principal de la noticia"],
      ]
    }

    comprehend_results = {
      location    = "silver/comprehend_results/"
      format      = "parquet"
      description = "Silver. Un análisis de Comprehend por texto distinto; funciona como caché."
      partitions  = [["analyzed_date", "string", "Fecha UTC del análisis"], ["run_id", "string", "Corrida que hizo el análisis"]]
      columns = [
        ["text_hash", "string", "sha256 del idioma y el texto analizado"],
        ["language", "string", "Idioma enviado a Comprehend"],
        ["analyzed_text", "string", "Texto analizado (título + descripción, recortado si excede el límite)"],
        ["text_bytes", "int", "Bytes UTF-8 del texto original"],
        ["truncated", "boolean", "El texto se recortó al límite configurado"],
        ["status", "string", "analyzed | truncated | empty_text | unsupported_language"],
        ["analyzed_at", "timestamp", "Fecha del análisis (UTC)"],
        ["entities_json", "string", "Respuesta de DetectEntities: tipo, texto y score"],
        ["key_phrases_json", "string", "Respuesta de DetectKeyPhrases: texto y score"],
      ]
    }

    news_quarantine = {
      location    = "silver/news_quarantine/"
      format      = "parquet"
      description = "Silver. Ítems RSS rechazados con su motivo y trazabilidad a Bronze."
      partitions  = [["ingestion_date", "string", "Fecha UTC de la ingesta"], ["run_id", "string", "Corrida"]]
      columns = [
        ["source_id", "string", "Fuente"],
        ["bronze_key", "string", "Archivo de Bronze que contenía el ítem"],
        ["item_index", "int", "Posición del ítem en el XML"],
        ["rejection_reason", "string", "missing_title | missing_link | invalid_link"],
        ["guid", "string", "GUID recibido"],
        ["link", "string", "Enlace recibido"],
        ["title", "string", "Título recibido"],
        ["published_raw", "string", "pubDate recibido"],
        ["detected_at", "timestamp", "Momento del rechazo (UTC)"],
      ]
    }

    gold_news_daily = {
      location    = "gold/news_daily/"
      format      = "parquet"
      description = "Gold. Noticias por día, medio y sección. Grano: día x medio x sección."
      partitions  = [["published_date", "string", "Fecha de publicación UTC"]]
      columns = [
        ["media", "string", "Medio"],
        ["section", "string", "Sección principal"],
        ["news_count", "bigint", "Noticias distintas publicadas"],
      ]
    }

    gold_entity_daily = {
      location    = "gold/entity_daily/"
      format      = "parquet"
      description = "Gold. Presencia de entidades por día. Grano: día x tipo x entidad."
      partitions  = [["published_date", "string", "Fecha de publicación UTC"]]
      columns = [
        ["entity_type", "string", "PERSON | ORGANIZATION | LOCATION"],
        ["entity", "string", "Entidad normalizada"],
        ["news_count", "bigint", "Noticias distintas que mencionan la entidad (no menciones)"],
      ]
    }

    gold_key_phrases_daily = {
      location    = "gold/key_phrases_daily/"
      format      = "parquet"
      description = "Gold. Frases clave por día y sección. Grano: día x medio x sección x frase."
      partitions  = [["published_date", "string", "Fecha de publicación UTC"]]
      columns = [
        ["media", "string", "Medio"],
        ["section", "string", "Sección principal"],
        ["key_phrase", "string", "Frase clave normalizada"],
        ["news_count", "bigint", "Noticias distintas que contienen la frase"],
      ]
    }

    bronze_ingestions = {
      location    = "control/bronze_log/"
      format      = "json"
      description = "Control. Una fila por fuente consultada en cada corrida."
      partitions  = []
      columns = [
        ["run_id", "string", "Corrida"],
        ["source_id", "string", "Fuente"],
        ["media", "string", "Medio"],
        ["section", "string", "Sección"],
        ["url", "string", "URL consultada"],
        ["ingested_at_utc", "string", "Momento de la ingesta (UTC, ISO 8601)"],
        ["http_status", "int", "Estado HTTP"],
        ["status", "string", "ok | no_items | empty_response | http_error | malformed_xml | network_error"],
        ["content_sha256", "string", "Hash del contenido"],
        ["bytes", "bigint", "Tamaño de la respuesta"],
        ["item_count", "int", "Ítems en el XML"],
        ["bronze_key", "string", "Archivo original en S3 (solo respuestas válidas)"],
        ["origin", "string", "http | fixture"],
        ["error", "string", "Detalle del error"],
      ]
    }

    pipeline_runs = {
      location    = "control/run_log/"
      format      = "json"
      description = "Control. Una fila por procesamiento de una corrida (nueva, repetida, recuperada o reconstruida) con sus conteos y su estado."
      partitions  = []
      columns = [
        ["run_id", "string", "Corrida"],
        ["mode", "string", "new (extracción de los RSS) | reprocess (desde Bronze) | rebuild"],
        ["status", "string", "complete | partial | failed"],
        ["started_at", "string", "Inicio (UTC)"],
        ["finished_at", "string", "Fin (UTC)"],
        ["sources_ok", "array<string>", "Fuentes con respuesta válida"],
        ["sources_failed", "array<string>", "Fuentes no disponibles o inválidas"],
        ["sources_reused", "array<string>", "Fuentes procesadas desde Bronze sin consultar el RSS"],
        ["failed_tasks", "array<string>", "Tareas fallidas"],
        ["received", "int", "Ítems recibidos en los XML"],
        ["rejected", "int", "Ítems enviados a cuarentena"],
        ["duplicates_in_batch", "int", "Repetidos dentro de la corrida"],
        ["new", "int", "Noticias nuevas"],
        ["updated", "int", "Noticias con contenido cambiado"],
        ["repeated", "int", "Noticias ya conocidas sin cambios"],
        ["unique_news_current", "bigint", "Noticias vigentes en Silver tras la corrida"],
        ["news_without_valid_date", "int", "Noticias vigentes sin fecha de publicación válida (fuera de los agregados diarios)"],
        ["comprehend_calls", "int", "Textos enviados a Comprehend"],
        ["comprehend_cached", "int", "Textos reutilizados de la caché"],
        ["comprehend_errors", "int", "Textos que Comprehend no pudo analizar"],
        ["partitions_published", "int", "Particiones creadas o actualizadas en Glue"],
        ["bytes_scanned_with_filter", "bigint", "Bytes escaneados por Athena con filtro de partición"],
        ["bytes_scanned_without_filter", "bigint", "Bytes escaneados por Athena sin filtro"],
        ["message", "string", "Resumen"],
      ]
    }
  }
}

resource "aws_glue_catalog_database" "lake" {
  name        = var.glue_database_name
  description = "Data lake de noticias RSS (silver, gold y control)"
}

resource "aws_glue_catalog_table" "lake" {
  for_each = local.glue_tables

  name          = each.key
  database_name = aws_glue_catalog_database.lake.name
  description   = each.value.description
  table_type    = "EXTERNAL_TABLE"
  parameters    = merge(local.formats[each.value.format].parameters, { EXTERNAL = "TRUE" })

  storage_descriptor {
    location      = "s3://${aws_s3_bucket.lake.id}/${each.value.location}"
    input_format  = local.formats[each.value.format].input_format
    output_format = local.formats[each.value.format].output_format

    ser_de_info {
      serialization_library = local.formats[each.value.format].serde
      parameters            = each.value.format == "json" ? { "ignore.malformed.json" = "FALSE" } : {}
    }

    dynamic "columns" {
      for_each = each.value.columns
      content {
        name    = columns.value[0]
        type    = columns.value[1]
        comment = columns.value[2]
      }
    }
  }

  dynamic "partition_keys" {
    for_each = each.value.partitions
    content {
      name    = partition_keys.value[0]
      type    = partition_keys.value[1]
      comment = partition_keys.value[2]
    }
  }
}
