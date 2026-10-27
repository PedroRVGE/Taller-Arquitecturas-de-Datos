from datetime import datetime

import pytest
from conftest import FIXTURES
from newslake import rss

NOW = datetime(2026, 10, 21, 23, 0, 0)


def test_parse_feed_reads_items():
    feed = rss.parse_feed((FIXTURES / "economia.xml").read_bytes())
    assert len(feed["items"]) == 10
    first = feed["items"][0]
    assert first["title"] and first["link"].startswith("https://www.eltiempo.com/")
    assert first["published_raw"]


@pytest.mark.parametrize("content,status", [(b"", "empty_response"), (b"   ", "empty_response"),
                                            (b"<rss><channel>", "malformed_xml"), (b"<html></html>", "malformed_xml")])
def test_invalid_feeds(content, status):
    with pytest.raises(rss.FeedError) as error:
        rss.parse_feed(content)
    assert error.value.status == status


def test_entity_expansion_is_blocked():
    bomb = b'<?xml version="1.0"?><!DOCTYPE x [<!ENTITY a "aaaa"><!ENTITY b "&a;&a;">]><rss><channel>&b;</channel></rss>'
    with pytest.raises(rss.FeedError):
        rss.parse_feed(bomb)


def test_clean_text_removes_html_and_entities():
    assert rss.clean_text("<p>Hola &amp;lt;b&amp;gt; <b>mundo</b></p>\n ") == "Hola mundo"
    assert rss.clean_text("   ") is None


@pytest.mark.parametrize("raw,expected,valid", [
    ("2026-10-21T17:09:08-05:00", datetime(2026, 10, 21, 22, 9, 8), True),     # ISO 8601 (El Tiempo)
    ("Wed, 21 Oct 2026 17:14:59 -0500", datetime(2026, 10, 21, 22, 14, 59), True),  # RFC 822
    ("2026-10-21T17:09:08", None, False),                                      # sin zona horaria
    ("2030-01-01T00:00:00Z", None, False),                                     # futuro
    ("ayer", None, False),
    (None, None, False),
])
def test_parse_published(raw, expected, valid):
    assert rss.parse_published(raw, NOW) == (expected, valid)


def test_normalize_link_and_identity_across_feeds():
    a = rss.normalize_link("http://WWW.ElTiempo.com/economia/nota-1/?utm_source=x&id=3#top")
    b = rss.normalize_link("https://www.eltiempo.com/economia/nota-1?id=3")
    assert a == b == "https://www.eltiempo.com/economia/nota-1?id=3"
    assert rss.normalize_link("ftp://x.com/a") is None
    assert rss.news_identity(a, a) == rss.news_identity("https://www.eltiempo.com/economia/nota-1/?id=3", b)


def test_clean_item_rejections():
    base = {"item_index": 0, "guid": None, "link": "https://x.com/a", "title": "T", "description": None,
            "published_raw": "2026-10-21T10:00:00-05:00"}
    assert rss.clean_item({**base, "title": " "}, NOW)[1] == "missing_title"
    assert rss.clean_item({**base, "link": None}, NOW)[1] == "missing_link"
    assert rss.clean_item({**base, "link": "javascript:alert(1)"}, NOW)[1] == "invalid_link"
    row, reason = rss.clean_item(base, NOW)
    assert reason is None and row["news_id"] and row["published_date_valid"]
