import math

import numpy as np
import pandas as pd
import pytest

from xm_demanda.monitoreo.metricas import drift_features, error_diario, nivel_psi, psi, wape_ventana
from xm_demanda.monitoreo.politica import decidir_promocion, decidir_reentrenamiento
from xm_demanda.utils.config import load_config

CFG = load_config("dev")
U, R = CFG["monitoreo"], CFG["promocion"]


def _decidir(*a):
    try:
        return decidir_reentrenamiento(*a)
    except NotImplementedError:
        pytest.skip("Pendiente: lab 15, sección 4")


def _obs():
    return pd.DataFrame({
        "fecha_pronostico": ["2026-07-20"] * 2 + ["2026-07-21"] * 2 + ["2026-07-22"],
        "alias": ["champion"] * 4 + ["champion"],
        "modelo_version": [1] * 5,
        "demanda_estimada_kwh": [110.0, 900.0, 100.0, 1000.0, 50.0],
        "demanda_observada_kwh": [100.0, 1000.0, 100.0, 1000.0, np.nan],
    })


def test_error_diario_pondera_por_energia_e_ignora_el_futuro():
    d = error_diario(_obs())
    assert len(d) == 2  # el 22 no tiene observado
    dia20 = d[d["fecha_pronostico"] == "2026-07-20"].iloc[0]
    assert dia20["wape"] == pytest.approx(110 / 1100)
    assert dia20["sesgo"] == pytest.approx(-90 / 1100)  # subestima


def test_wape_ventana_no_promedia_porcentajes():
    d = error_diario(_obs())
    w = wape_ventana(d, "champion", "2026-07-21", 7)
    assert w == pytest.approx(110 / 2200)
    assert math.isnan(wape_ventana(d, "challenger", "2026-07-21", 7))


def test_psi_cero_si_no_cambia_y_alto_si_se_corre():
    rng = np.random.default_rng(7)
    ref = rng.normal(1.0, 0.1, 5000)
    assert psi(ref, rng.normal(1.0, 0.1, 5000)) < 0.02
    assert psi(ref, rng.normal(1.15, 0.1, 5000)) > 0.25
    assert nivel_psi(0.05) == "estable" and nivel_psi(0.15) == "moderado"
    assert nivel_psi(0.4) == "alto"
    assert nivel_psi(float("nan")) == "sin datos"


def test_drift_features_ordena_de_mayor_a_menor():
    rng = np.random.default_rng(1)
    ref = pd.DataFrame({"a": rng.normal(0, 1, 2000), "b": rng.normal(0, 1, 2000)})
    act = pd.DataFrame({"a": rng.normal(0, 1, 2000), "b": rng.normal(2, 1, 2000)})
    out = drift_features(ref, act, ["a", "b"])
    assert list(out["columna"]) == ["b", "a"] and out.loc[0, "nivel"] == "alto"


def test_sin_disparadores_no_se_reentrena():
    assert _decidir(0.05, 0.05, 0.05, 10, U) == {"reentrenar": False, "motivos": []}


@pytest.mark.parametrize("args, palabra", [
    ((0.07, 0.05, 0.05, 10), "degradación"),
    ((0.05, 0.05, 0.30, 10), "drift"),
    ((0.05, 0.05, 0.05, 28), "calendario"),
])
def test_cada_disparador_basta(args, palabra):
    d = _decidir(*args, U)
    assert d["reentrenar"] and len(d["motivos"]) == 1 and d["motivos"][0].startswith(palabra)


def test_la_tolerancia_es_relativa():
    tol = U["tolerancia_degradacion"]  # la regla vive en conf/base.yml; el test no la repite
    assert not _decidir(0.05 * (1 + tol) - 0.001, 0.05, 0.0, 0, U)["reentrenar"]
    assert _decidir(0.05 * (1 + tol) + 0.001, 0.05, 0.0, 0, U)["reentrenar"]


def test_nan_no_dispara():
    assert not _decidir(float("nan"), 0.05, float("nan"), 0, U)["reentrenar"]


def test_tres_motivos_a_la_vez():
    assert len(_decidir(0.2, 0.05, 0.9, 60, U)["motivos"]) == 3


def test_promocion_exige_evidencia_y_margen():
    assert not decidir_promocion(0.05, 0.03, 7, R)["promover"]  # pocos días
    assert not decidir_promocion(0.050, 0.0495, 30, R)["promover"]  # 1 %
    d = decidir_promocion(0.050, 0.045, 30, R)
    assert d["promover"] and d["mejora"] == pytest.approx(0.10)
