"""Construcción del estado vigente de Silver a partir de corridas de Bronze.

El estado es un diccionario news_id -> fila de la tabla news. Cada corrida se fusiona
contra el estado anterior y clasifica cada ítem recibido:

    rejected            no cumple campos obligatorios o enlace válido -> cuarentena
    duplicate_in_batch  el mismo news_id ya llegó en esta corrida (otro feed o repetido)
    new                 news_id desconocido
    updated             news_id conocido con content_hash distinto y versión más reciente
    repeated            news_id conocido sin cambios (o una versión más antigua al reprocesar)

Las noticias presentes en varios feeds se guardan una sola vez: source_id y section son los
del primer feed donde aparecieron y feeds_seen acumula todos los feeds.
"""

from collections import Counter

from . import rss
from .settings import UNKNOWN_DATE


def partition_date(row):
    if row.get("published_date_valid") and row.get("published_at_utc") is not None:
        return row["published_at_utc"].date().isoformat()
    return UNKNOWN_DATE


def merge_run(state, run, parsed_sources, now=None):
    """Fusiona una corrida en el estado.

    run: {"run_id", "ingested_at"} ; parsed_sources: lista ordenada de
    {"source": {...}, "bronze_key", "ingested_at", "feed": {"language", "items"}}.
    Devuelve (filas de cuarentena, conteos). Modifica state en sitio.
    """
    counts = Counter()
    quarantine = []
    seen_in_run = {}

    for parsed in parsed_sources:
        source = parsed["source"]
        for item in parsed["feed"]["items"]:
            counts["received"] += 1
            row, reason = rss.clean_item(item, now)
            if reason:
                counts["rejected"] += 1
                quarantine.append({
                    "source_id": source["id"],
                    "bronze_key": parsed["bronze_key"],
                    "item_index": item["item_index"],
                    "rejection_reason": reason,
                    "guid": row["guid"],
                    "link": row["link"],
                    "title": row["title"],
                    "published_raw": row["published_raw"],
                    "detected_at": now,
                    "ingestion_date": parsed["ingested_at"].date().isoformat(),
                    "run_id": run["run_id"],
                })
                continue

            news_id = row["news_id"]
            existing = state.get(news_id)
            if news_id in seen_in_run:
                counts["duplicates_in_batch"] += 1
                if source["id"] not in existing["feeds_seen"]:
                    existing["feeds_seen"] = sorted(set(existing["feeds_seen"]) | {source["id"]})
                continue
            seen_in_run[news_id] = True

            language = parsed["feed"].get("language") or source["language"]
            text = rss.analysis_text(row["title"], row["description"])
            candidate = {
                **row,
                "media": source["media"],
                "section": source["section"],
                "source_id": source["id"],
                "feeds_seen": [source["id"]],
                "analyzed_text_hash": rss.analysis_hash(text, language),
                "language": language,
                "enrichment_status": None,
                "analyzed_at": None,
                "version": 1,
                "first_seen_run_id": run["run_id"],
                "last_updated_run_id": run["run_id"],
                "bronze_key": parsed["bronze_key"],
                "ingested_at_utc": parsed["ingested_at"],
                "published_date": partition_date(row),
            }

            if existing is None:
                counts["new"] += 1
                state[news_id] = candidate
                continue

            feeds = sorted(set(existing["feeds_seen"]) | {source["id"]})
            if existing["content_hash"] == candidate["content_hash"] or candidate["ingested_at_utc"] < existing["ingested_at_utc"]:
                counts["repeated"] += 1
                existing["feeds_seen"] = feeds
                continue

            counts["updated"] += 1
            state[news_id] = {
                **candidate,
                # La identidad editorial (primer feed y primera corrida) no cambia con una actualización.
                "media": existing["media"],
                "section": existing["section"],
                "source_id": existing["source_id"],
                "first_seen_run_id": existing["first_seen_run_id"],
                "feeds_seen": feeds,
                "version": existing["version"] + 1,
            }
    return quarantine, counts
