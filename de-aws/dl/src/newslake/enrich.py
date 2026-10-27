"""Enriquecimiento con Amazon Comprehend y caché por hash del texto analizado.

Un texto se envía a Comprehend solo si su hash (idioma + texto) no está en la caché
(tabla comprehend_results). Así una noticia repetida o reprocesada no genera llamadas
nuevas, y una noticia actualizada solo se reanaliza si cambió su título o descripción.

Estados (enrichment_status):
    analyzed              Comprehend respondió
    truncated             el texto superaba max_text_bytes; se analizó recortado
    empty_text            sin título ni descripción útil; no se llama al servicio
    unsupported_language  idioma no soportado; no se llama al servicio
    error                 la llamada falló; no se guarda en caché y se reintenta en otra corrida
"""

import json
import logging
import re
from collections import OrderedDict

from . import rss

LOGGER = logging.getLogger(__name__)
SPACES = re.compile(r"\s+")


def normalize_term(text):
    return SPACES.sub(" ", (text or "").strip()).casefold()


def truncate_utf8(text, max_bytes):
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text, False
    return encoded[:max_bytes].decode("utf-8", errors="ignore").rstrip(), True


def analyze_text(client, text, language, config, now):
    """Analiza un texto y devuelve un registro de caché (o estado de error)."""
    text_bytes = len(text.encode("utf-8"))
    base = {
        "language": language,
        "text_bytes": text_bytes,
        "analyzed_at": now,
        "entities_json": "[]",
        "key_phrases_json": "[]",
        "truncated": False,
    }
    if not text.strip():
        return {**base, "analyzed_text": text, "status": "empty_text"}
    if language not in config["supported_languages"]:
        return {**base, "analyzed_text": text, "status": "unsupported_language"}

    analyzed, truncated = truncate_utf8(text, config["max_text_bytes"])
    entities = client.detect_entities(Text=analyzed, LanguageCode=language)["Entities"]
    phrases = client.detect_key_phrases(Text=analyzed, LanguageCode=language)["KeyPhrases"]
    return {
        **base,
        "analyzed_text": analyzed,
        "truncated": truncated,
        "status": "truncated" if truncated else "analyzed",
        "entities_json": json.dumps(entities, ensure_ascii=False),
        "key_phrases_json": json.dumps(phrases, ensure_ascii=False),
    }


def enrich_state(state, cache, client, config, now):
    """Completa la caché con los textos nuevos. Devuelve (registros nuevos, estadísticas)."""
    pending = OrderedDict()
    for row in state.values():
        text_hash = row["analyzed_text_hash"]
        if text_hash not in cache and text_hash not in pending:
            pending[text_hash] = row

    new_records = []
    stats = {"comprehend_calls": 0, "comprehend_cached": len(state) - len(pending), "comprehend_errors": 0}
    errors = {}
    for text_hash, row in pending.items():
        text = rss.analysis_text(row["title"], row["description"])
        try:
            record = analyze_text(client, text, row["language"], config, now)
        except Exception as error:  # noqa: BLE001 - se registra y se reintenta en otra corrida
            LOGGER.warning("Comprehend falló para %s: %s", text_hash[:12], error)
            stats["comprehend_errors"] += 1
            errors[text_hash] = str(error)[:500]
            continue
        if record["status"] in ("analyzed", "truncated"):
            stats["comprehend_calls"] += 1
        record["text_hash"] = text_hash
        cache[text_hash] = record
        new_records.append(record)
    return new_records, stats, errors


def apply_enrichment(state, cache, entity_types):
    """Llena los campos de enriquecimiento de cada noticia y arma entidades y frases clave."""
    entities, phrases = [], []
    for row in state.values():
        record = cache.get(row["analyzed_text_hash"])
        if record is None:
            row["enrichment_status"] = "error"
            row["analyzed_at"] = None
            continue
        row["enrichment_status"] = record["status"]
        row["analyzed_at"] = record["analyzed_at"]

        grouped = OrderedDict()
        for entity in sorted(json.loads(record["entities_json"]), key=lambda e: e.get("BeginOffset", 0)):
            if entity["Type"] not in entity_types:
                continue
            key = (entity["Type"], normalize_term(entity["Text"]))
            current = grouped.setdefault(key, {"entity_text": entity["Text"], "mentions": 0, "max_score": 0.0})
            current["mentions"] += 1
            current["max_score"] = max(current["max_score"], float(entity["Score"]))
        for (entity_type, normalized), values in grouped.items():
            entities.append({
                "news_id": row["news_id"],
                "entity_type": entity_type,
                "entity_text": values["entity_text"],
                "entity_text_normalized": normalized,
                "mentions": values["mentions"],
                "max_score": round(values["max_score"], 6),
                "analyzed_text_hash": row["analyzed_text_hash"],
                "media": row["media"],
                "section": row["section"],
                "published_date": row["published_date"],
            })

        grouped = OrderedDict()
        for phrase in sorted(json.loads(record["key_phrases_json"]), key=lambda p: p.get("BeginOffset", 0)):
            key = normalize_term(phrase["Text"])
            current = grouped.setdefault(key, {"key_phrase": phrase["Text"], "mentions": 0, "max_score": 0.0})
            current["mentions"] += 1
            current["max_score"] = max(current["max_score"], float(phrase["Score"]))
        for normalized, values in grouped.items():
            phrases.append({
                "news_id": row["news_id"],
                "key_phrase": values["key_phrase"],
                "key_phrase_normalized": normalized,
                "mentions": values["mentions"],
                "max_score": round(values["max_score"], 6),
                "analyzed_text_hash": row["analyzed_text_hash"],
                "media": row["media"],
                "section": row["section"],
                "published_date": row["published_date"],
            })
    return entities, phrases
