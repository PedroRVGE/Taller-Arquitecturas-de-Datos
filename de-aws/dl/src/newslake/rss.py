"""Interpretación de RSS y reglas de limpieza, validación e identidad de una noticia.

Funciones puras (sin AWS) para poder probarlas con archivos de ejemplo.
"""

import hashlib
import html
import json
import re
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from xml.etree.ElementTree import ParseError

from defusedxml import ElementTree as SafeElementTree
from defusedxml.common import DefusedXmlException

TAG_PATTERN = re.compile(r"<[^>]+>")
SPACES = re.compile(r"\s+")
TRACKING_PARAMETERS = ("utm_", "fbclid", "gclid", "mc_cid", "mc_eid")
MAX_FUTURE = timedelta(days=1)


class FeedError(Exception):
    """Respuesta que no es un RSS válido. status: empty_response | malformed_xml."""

    def __init__(self, status, message):
        super().__init__(message)
        self.status = status


def _text(element, tag):
    child = element.find(tag)
    if child is None or child.text is None:
        return None
    value = child.text.strip()
    return value or None


def parse_feed(content):
    """Lee un RSS 2.0 con defusedxml (protege contra entidades XML maliciosas)."""
    if content is None or not content.strip():
        raise FeedError("empty_response", "La respuesta está vacía")
    try:
        root = SafeElementTree.fromstring(content)
    except (ParseError, DefusedXmlException) as error:
        raise FeedError("malformed_xml", f"XML inválido: {error}") from error
    channel = root.find("channel")
    if root.tag != "rss" or channel is None:
        raise FeedError("malformed_xml", f"No es un RSS 2.0 (raíz <{root.tag}>)")

    items = []
    for index, item in enumerate(channel.findall("item")):
        items.append({
            "item_index": index,
            "guid": _text(item, "guid"),
            "title": _text(item, "title"),
            "link": _text(item, "link"),
            "description": _text(item, "description"),
            "published_raw": _text(item, "pubDate"),
        })
    return {"language": _text(channel, "language"), "items": items}


def clean_text(value):
    """Quita etiquetas HTML y entidades (también si vienen codificadas dos veces)."""
    if value is None:
        return None
    text = value
    for _ in range(3):  # entidades codificadas dos o tres veces (&amp;lt;b&amp;gt;)
        unescaped = html.unescape(text)
        if unescaped == text:
            break
        text = unescaped
    text = TAG_PATTERN.sub(" ", text)
    text = SPACES.sub(" ", text).strip()
    return text or None


def parse_published(raw, now=None):
    """Normaliza pubDate a UTC. Acepta RFC 822 (estándar RSS) e ISO 8601 (El Tiempo).

    Devuelve (fecha UTC sin zona, válida). Una fecha sin zona horaria o más de un día en el
    futuro se considera no válida: no se puede ubicar con certeza en un día UTC.
    """
    if not raw:
        return None, False
    parsed = None
    try:
        parsed = parsedate_to_datetime(raw)
    except (TypeError, ValueError, IndexError):
        try:
            parsed = datetime.fromisoformat(raw.strip().replace("Z", "+00:00"))
        except ValueError:
            return None, False
    if parsed.tzinfo is None:
        return None, False
    utc_value = parsed.astimezone(timezone.utc).replace(tzinfo=None)
    now = now or datetime.now(timezone.utc).replace(tzinfo=None)
    if utc_value > now + MAX_FUTURE:
        return None, False
    return utc_value, True


def normalize_link(link):
    """Enlace canónico: http(s), host en minúsculas, sin fragmento ni parámetros de rastreo."""
    if not link:
        return None
    parts = urlsplit(link.strip())
    scheme = parts.scheme.lower()
    if scheme not in ("http", "https") or not parts.netloc:
        return None
    query = urlencode([
        (key, value) for key, value in parse_qsl(parts.query, keep_blank_values=True)
        if not key.lower().startswith(TRACKING_PARAMETERS)
    ])
    path = parts.path.rstrip("/") or "/"
    return urlunsplit(("https" if scheme == "http" else scheme, parts.netloc.lower(), path, query, ""))


def news_identity(guid, canonical_link):
    """Identidad de la noticia.

    1. GUID: si es una URL se normaliza igual que el enlace, así el mismo artículo en
       varios feeds (o con parámetros distintos) produce el mismo identificador.
    2. Sin GUID: el enlace canónico.
    """
    if guid:
        normalized_guid = normalize_link(guid) or guid.strip()
        basis = f"id:{normalized_guid}"
    else:
        basis = f"id:{canonical_link}"
    return hashlib.sha256(basis.encode("utf-8")).hexdigest()[:32]


def sha256_json(values):
    return hashlib.sha256(json.dumps(values, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def analysis_text(title, description):
    """Texto que se envía a Comprehend: título y descripción."""
    parts = [part for part in (title, description) if part]
    return ". ".join(parts).strip()


def analysis_hash(text, language):
    return hashlib.sha256(f"{language}|{text}".encode("utf-8")).hexdigest()


def clean_item(item, now=None):
    """Aplica limpieza y validación a un ítem crudo. Devuelve (fila limpia, motivo de rechazo)."""
    title = clean_text(item.get("title"))
    description = clean_text(item.get("description"))
    link = (item.get("link") or "").strip() or None
    guid = (item.get("guid") or "").strip() or None
    canonical_link = normalize_link(link)
    published_at, valid_date = parse_published(item.get("published_raw"), now)

    row = {
        "guid": guid,
        "link": link,
        "canonical_link": canonical_link,
        "title": title,
        "description": description,
        "published_raw": item.get("published_raw"),
        "published_at_utc": published_at,
        "published_date_valid": valid_date,
    }
    if not title:
        return row, "missing_title"
    if not link:
        return row, "missing_link"
    if canonical_link is None:
        return row, "invalid_link"
    row["news_id"] = news_identity(guid, canonical_link)
    row["content_hash"] = sha256_json([title, description, canonical_link, item.get("published_raw")])
    return row, None
