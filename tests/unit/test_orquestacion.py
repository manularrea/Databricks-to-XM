from datetime import date

import pytest

from xm_demanda.orquestacion.chequeo import problemas_de_frescura
from xm_demanda.orquestacion.job import (
    PARAMETROS,
    TAREAS,
    definicion_job,
    orden_topologico,
    politica,
    resumen_run,
)


def _definicion():
    try:
        return definicion_job(
            "ana", "/Users/ana@x.co/Databricks-to-XM/clases/c12-jobs/notebooks/tareas", "ana@x.co"
        )
    except NotImplementedError:
        pytest.skip("Pendiente: lab 12, sección 3")


def _politica(tarea):
    try:
        return politica(tarea)
    except NotImplementedError:
        pytest.skip("Pendiente: lab 12, sección 3")


def test_politica_reintenta_solo_la_inferencia():
    inf = _politica("inferencia")
    assert inf["max_retries"] == 2 and inf["retry_on_timeout"] is True
    assert inf["min_retry_interval_millis"] == 60_000
    for t in ("bronce", "calidad", "plata", "oro"):
        assert "max_retries" not in _politica(t)
        assert _politica(t)["timeout_seconds"] > 0


def test_politica_chequeo_corre_siempre():
    ch = _politica("chequeo")
    assert ch["run_if"] == "ALL_DONE"
    assert ch["timeout_seconds"] <= 600


def test_definicion_tiene_las_tareas_en_orden_valido():
    d = _definicion()
    assert [t["task_key"] for t in d["tasks"]] == TAREAS
    assert orden_topologico(d) == TAREAS


def test_rama_condicional_y_parametros():
    d = _definicion()
    tareas = {t["task_key"]: t for t in d["tasks"]}
    assert (
        tareas["hay_datos_nuevos"]["condition_task"]["left"] == "{{tasks.bronce.values.procesar}}"
    )
    assert tareas["calidad"]["depends_on"] == [{"task_key": "hay_datos_nuevos", "outcome": "true"}]
    assert {p["name"] for p in d["parameters"]} == set(PARAMETROS)
    assert next(p for p in d["parameters"] if p["name"] == "usuario")["default"] == "ana"
    assert d["email_notifications"]["on_failure"] == ["ana@x.co"]
    assert d["max_concurrent_runs"] == 1 and d["queue"]["enabled"] is True
    assert d["schedule"]["pause_status"] == "PAUSED"


def test_orden_topologico_detecta_ciclos_y_dependencias_rotas():
    ciclo = {
        "tasks": [
            {"task_key": "a", "depends_on": [{"task_key": "b"}]},
            {"task_key": "b", "depends_on": [{"task_key": "a"}]},
        ]
    }
    with pytest.raises(ValueError, match="Ciclo"):
        orden_topologico(ciclo)
    rota = {"tasks": [{"task_key": "a", "depends_on": [{"task_key": "x"}]}]}
    with pytest.raises(ValueError, match="no existen"):
        orden_topologico(rota)


def test_resumen_run_ordena_y_calcula_duracion():
    run = {
        "tasks": [
            {
                "task_key": "chequeo",
                "run_id": 3,
                "state": {"result_state": "FAILED"},
                "start_time": 1000,
                "end_time": 31000,
            },
            {
                "task_key": "calidad",
                "run_id": 2,
                "state": {"life_cycle_state": "SKIPPED", "result_state": "EXCLUDED"},
            },
            {
                "task_key": "bronce",
                "run_id": 1,
                "state": {"result_state": "SUCCESS"},
                "start_time": 0,
                "end_time": 0,
            },
        ]
    }
    r = resumen_run(run)
    assert [f["tarea"] for f in r] == ["bronce", "calidad", "chequeo"]
    assert r[1]["estado"] == "EXCLUDED" and r[2]["duracion_s"] == 30


def test_frescura_sin_problemas():
    fechas = [date(2026, 7, 26 + i) for i in range(6)] + [date(2026, 8, 1)]
    assert problemas_de_frescura(date(2026, 7, 25), date(2026, 7, 26), fechas, 7) == []


def test_frescura_detecta_atraso_y_huecos_en_el_pronostico():
    fechas = [date(2026, 7, 26 + i) for i in range(6)] + [date(2026, 8, 1)]
    p = problemas_de_frescura(date(2026, 7, 25), date(2026, 7, 27), fechas, 7)
    assert len(p) == 2
    assert "1 día(s) de atraso" in p[0]
    assert "2026-08-02" in p[1]
