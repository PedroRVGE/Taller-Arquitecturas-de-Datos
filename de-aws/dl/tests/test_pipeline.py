"""Recorre el DAG completo en local (sin Athena) y verifica los escenarios del taller."""

import itertools
import json
from datetime import datetime, timezone

import pytest
from conftest import AirflowFailException
from newslake import airflow_tasks as tasks
from newslake import storage
from newslake.settings import sources

SEQUENCE = ("transform_silver", "enrich_comprehend", "publish_silver", "build_gold", "failure_injection",
            "update_glue_catalog")


class FakeDate:
    def __init__(self, value):
        self.value = value

    def in_timezone(self, _):
        return self.value


class FakeTI:
    def __init__(self, task_id, state=None):
        self.task_id = task_id
        self.state = state


DAG_RUN_IDS = itertools.count(1)


class FakeDagRun:
    def __init__(self):
        self.run_id = f"manual__{next(DAG_RUN_IDS)}"
        self.start_date = datetime.now(timezone.utc)
        self.states = {}

    def get_task_instances(self):
        return [FakeTI(task_id, state) for task_id, state in self.states.items()]


def latest_run_log(run_id):
    """Último procesamiento registrado de la corrida (cada procesamiento deja su archivo)."""
    keys = sorted(storage.list_keys(f"control/run_log/{run_id}__"))
    return json.loads(storage.get(keys[-1]))


def run_dag(day, validation_ok=True, **params):
    """Ejecuta las tareas en orden, como el scheduler. Devuelve la bitácora de la corrida."""
    defaults = {"run_id": "", "reprocess_run_id": "", "rebuild_from_bronze": False, "simulate_change_from_run": "",
                "simulate_defects_from_run": "", "unavailable_source": "", "simulate_failure": False}
    dag_run = FakeDagRun()
    context = {"params": {**defaults, **params}, "logical_date": FakeDate(datetime(2026, 10, day, 12)),
               "dag_run": dag_run, "ti": FakeTI("record_run_status")}

    if validation_ok:
        tasks.validate_access(**context)
        dag_run.states["validate_access"] = "success"
    else:
        dag_run.states["validate_access"] = "failed"
    for source in sources():
        if not validation_ok:
            dag_run.states[f"ingest_bronze.{source['id']}"] = "upstream_failed"
            continue
        try:
            tasks.ingest_source(source["id"], **context)
            dag_run.states[f"ingest_bronze.{source['id']}"] = "success"
        except AirflowFailException:
            dag_run.states[f"ingest_bronze.{source['id']}"] = "failed"
    for name in SEQUENCE:
        try:
            getattr(tasks, name)(**context)
            dag_run.states[name] = "success"
        except AirflowFailException:
            dag_run.states[name] = "failed"
            break
    try:
        tasks.record_run_status(**context)
    except AirflowFailException:
        pass
    run = tasks.run_info(context)
    return latest_run_log(run["run_id"]), run


def gold_matches_silver():
    news = storage.read_published_rows("news")
    gold = storage.read_published_rows("gold_news_daily")
    expected = {}
    for row in news:
        if row["published_date"] != "unknown":
            key = (row["published_date"], row["media"], row["section"])
            expected[key] = expected.get(key, 0) + 1
    return expected == {(g["published_date"], g["media"], g["section"]): g["news_count"] for g in gold}


def test_full_scenarios(aws, comprehend, feeds):
    # 1. Carga inicial
    log, first = run_dag(1)
    assert log["status"] == "complete"
    assert log["received"] == 30
    assert log["new"] == log["unique_news_current"] > 0
    assert log["new"] + log["duplicates_in_batch"] + log["rejected"] == log["received"]
    assert log["mode"] == "new" and log["sources_reused"] == []
    assert log["news_without_valid_date"] == 0 and log["comprehend_errors"] == 0
    assert log["comprehend_calls"] == comprehend.calls == log["new"]
    news = storage.read_published_rows("news")
    assert len(news) == len({row["news_id"] for row in news}) == log["unique_news_current"]
    assert all(row["enrichment_status"] == "analyzed" for row in news)
    entity_news = {row["news_id"] for row in storage.read_published_rows("news_entities")}
    assert entity_news <= {row["news_id"] for row in news}
    assert gold_matches_silver()
    bronze_keys = list(storage.list_keys("bronze/"))
    assert len([k for k in bronze_keys if k.endswith("feed.xml")]) == 3

    # 2. Repetir la misma corrida: reutiliza Bronze, sin duplicados ni llamadas nuevas
    calls = comprehend.calls
    log, _ = run_dag(1)
    assert log["status"] == "complete" and log["new"] == 0 and log["repeated"] == log["unique_news_current"]
    assert log["mode"] == "reprocess" and sorted(log["sources_reused"]) == sorted(s["id"] for s in sources())
    history = sorted(storage.list_keys(f"control/run_log/{first['run_id']}__"))
    assert len(history) == 2  # la primera bitácora (con sus noticias nuevas) se conserva
    assert json.loads(storage.get(history[0]))["new"] == json.loads(storage.get(history[0]))["unique_news_current"]
    assert log["partitions_published"] == 0 and comprehend.calls == calls
    assert list(storage.list_keys("bronze/")) == bronze_keys
    assert len(storage.read_published_rows("news")) == log["unique_news_current"]

    # 3. Nueva corrida con el mismo contenido: nada se reescribe
    log, _ = run_dag(2)
    assert log["new"] == 0 and log["partitions_published"] == 0 and comprehend.calls == calls

    # 4. Noticia modificada (copia controlada): solo se reanaliza lo que cambió
    log, _ = run_dag(3, simulate_change_from_run=first["run_id"])
    assert log["updated"] == 3 and comprehend.calls == calls + 3
    updated = [row for row in storage.read_published_rows("news") if row["version"] == 2]
    assert len(updated) == 3 and all(row["title"].endswith("(actualizada)") for row in updated)
    assert gold_matches_silver()

    # 5. Fuente no disponible: corrida parcial, las noticias de esa fuente no desaparecen
    before = len(storage.read_published_rows("news"))
    log, _ = run_dag(4, unavailable_source="eltiempo_deportes")
    assert log["status"] == "partial" and log["sources_failed"] == ["eltiempo_deportes"]
    assert len(storage.read_published_rows("news")) == before

    # 6. Fallo controlado entre archivos y Glue, y recuperación
    log, failed_run = run_dag(5, simulate_change_from_run=first["run_id"], simulate_failure=True)
    assert log["status"] == "failed" and "failure_injection" in log["failed_tasks"]
    log, _ = run_dag(5, simulate_change_from_run=first["run_id"], simulate_failure=True)
    assert log["status"] == "complete"
    assert gold_matches_silver()

    # 7. Reconstrucción desde Bronze: mismo resultado, sin consultar RSS ni Comprehend
    calls = comprehend.calls
    news_before = sorted((r["news_id"], r["content_hash"], r["version"]) for r in storage.read_published_rows("news"))
    log, _ = run_dag(6, rebuild_from_bronze=True)
    assert log["status"] == "complete" and comprehend.calls == calls
    news_after = sorted((r["news_id"], r["content_hash"], r["version"]) for r in storage.read_published_rows("news"))
    assert news_after == news_before
    assert gold_matches_silver()


def test_reprocess_unknown_run_fails(aws, comprehend, feeds):
    log, _ = run_dag(1, reprocess_run_id="no-existe")
    assert log["status"] == "failed"
    assert sorted(log["sources_failed"]) == sorted(s["id"] for s in sources())


def test_defects_fixture_goes_to_quarantine(aws, comprehend, feeds):
    _, first = run_dag(1)
    calls = comprehend.calls
    log, run = run_dag(2, simulate_defects_from_run=first["run_id"])
    assert log["status"] == "complete"
    assert log["rejected"] == 9 and log["new"] == 0 and comprehend.calls == calls  # 3 defectos por feed
    rejected = storage.read_published_rows("news_quarantine")
    assert sorted({r["rejection_reason"] for r in rejected}) == ["invalid_link", "missing_link", "missing_title"]
    assert all(r["run_id"] == run["run_id"] and r["bronze_key"].startswith("bronze/") for r in rejected)


def test_partition_publish_is_atomic(aws, comprehend, feeds):
    """Con fallo antes de Glue, Athena (los punteros de Glue) sigue viendo la versión anterior."""
    run_dag(1)
    before = {k: v["location"] for k, v in storage.published_partitions("news").items()}
    run_dag(2, simulate_change_from_run="r20261001T120000Z", simulate_failure=True)
    after = {k: v["location"] for k, v in storage.published_partitions("news").items()}
    assert before == after


@pytest.mark.parametrize("bad_id", ["../x", "", "a" * 70])
def test_run_id_validation(bad_id):
    context = {"params": {"run_id": bad_id}, "logical_date": FakeDate(datetime(2026, 1, 1))}
    if bad_id == "":
        assert tasks.run_info(context)["run_id"].startswith("r2026")
    else:
        with pytest.raises(AirflowFailException):
            tasks.run_info(context)


def test_failed_validation_stops_processing_even_with_bronze(aws, comprehend, feeds):
    """Con Bronze ya escrito (repetición del run_id), una validación fallida no deja seguir el pipeline."""
    _, first = run_dag(1)
    published = storage.published_partitions("news")
    log, _ = run_dag(1, validation_ok=False, run_id=first["run_id"])
    assert log["status"] == "failed"
    assert "validate_access" in log["failed_tasks"] and "transform_silver" in log["failed_tasks"]
    assert storage.published_partitions("news") == published


def test_clear_after_another_run_published_is_rejected(aws, comprehend, feeds):
    """Recuperar con Clear una corrida detenida no puede pisar lo que publicó una corrida posterior."""
    _, first = run_dag(1)
    log, failed_run = run_dag(2, simulate_change_from_run=first["run_id"], simulate_failure=True)
    assert log["status"] == "failed"
    log, _ = run_dag(3, simulate_change_from_run=first["run_id"])  # corrida B publica en medio
    assert log["status"] == "complete"
    published = storage.published_partitions("news")

    context = {"params": {"run_id": failed_run["run_id"], "reprocess_run_id": "", "rebuild_from_bronze": False,
                          "simulate_change_from_run": first["run_id"], "unavailable_source": "",
                          "simulate_failure": True},
               "logical_date": FakeDate(datetime(2026, 10, 2, 12)), "dag_run": FakeDagRun(),
               "ti": FakeTI("update_glue_catalog")}
    tasks.failure_injection(**context)  # el marcador ya existe: Clear deja pasar la tarea
    with pytest.raises(AirflowFailException, match="reprocess_run_id"):
        tasks.update_glue_catalog(**context)
    assert storage.published_partitions("news") == published

    log, _ = run_dag(4, reprocess_run_id=failed_run["run_id"])  # recuperación indicada en el error
    assert log["status"] == "complete" and log["mode"] == "reprocess"
    assert gold_matches_silver()


def test_bronze_record_describes_the_stored_file(aws):
    """Si el feed.xml de la corrida ya existía, hash y tamaño son los del archivo conservado."""
    import hashlib

    from conftest import FIXTURES
    from newslake import bronze

    source = sources()[0]
    original = (FIXTURES / "economia.xml").read_bytes()
    retry = (FIXTURES / "politica.xml").read_bytes()
    ingested_at = datetime(2026, 10, 1, 12)
    first = bronze.store_valid_response("r1", source, ingested_at, original, 200, "http")
    second = bronze.store_valid_response("r1", source, ingested_at, retry, 200, "http")
    assert second["bronze_key"] == first["bronze_key"]
    assert storage.get(second["bronze_key"]) == original
    assert second["content_sha256"] == hashlib.sha256(original).hexdigest() == first["content_sha256"]
    assert second["bytes"] == len(original) and second["item_count"] == first["item_count"]
