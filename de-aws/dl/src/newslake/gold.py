"""Agregados de Gold. Todas las métricas cuentan noticias distintas (news_id), no menciones.

Las noticias sin fecha de publicación válida (published_date = 'unknown') no entran en los
agregados diarios: no se pueden ubicar en un día. Se cuentan en la bitácora de la corrida.
"""

from collections import defaultdict

from .settings import UNKNOWN_DATE


def _count_distinct(rows, key_fields):
    groups = defaultdict(set)
    for row in rows:
        if row["published_date"] == UNKNOWN_DATE:
            continue
        groups[tuple(row[field] for field in key_fields)].add(row["news_id"])
    return groups


def news_daily(news_rows):
    groups = _count_distinct(news_rows, ("published_date", "media", "section"))
    return [
        {"published_date": date, "media": media, "section": section, "news_count": len(ids)}
        for (date, media, section), ids in sorted(groups.items())
    ]


def entity_daily(entity_rows):
    groups = _count_distinct(entity_rows, ("published_date", "entity_type", "entity_text_normalized"))
    return [
        {"published_date": date, "entity_type": entity_type, "entity": entity, "news_count": len(ids)}
        for (date, entity_type, entity), ids in sorted(groups.items())
    ]


def key_phrases_daily(phrase_rows):
    groups = _count_distinct(phrase_rows, ("published_date", "media", "section", "key_phrase_normalized"))
    return [
        {"published_date": date, "media": media, "section": section, "key_phrase": phrase, "news_count": len(ids)}
        for (date, media, section, phrase), ids in sorted(groups.items())
    ]


def build(news_rows, entity_rows, phrase_rows):
    return {
        "gold_news_daily": news_daily(news_rows),
        "gold_entity_daily": entity_daily(entity_rows),
        "gold_key_phrases_daily": key_phrases_daily(phrase_rows),
    }
