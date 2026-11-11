# Databricks notebook source
# MAGIC %md
# MAGIC # Clase 15 — Monitoreo, drift y reentrenamiento: un modelo en producción se cuida, no se entrega
# MAGIC
# MAGIC El pipeline ya corre solo y se promueve solo. Falta la pregunta que más le importa al negocio: **¿el pronóstico sigue
# MAGIC siendo bueno?** Un modelo de demanda no falla con un error rojo; se degrada en silencio cuando el mundo cambia: un
# MAGIC Niño, una ola de autogeneración solar, una carga industrial nueva. Hoy el job aprende a notarlo, a decidir si reentrenar,
# MAGIC y a promover un modelo nuevo **solo si gana**, dejando escrito por qué.
# MAGIC
# MAGIC ```
# MAGIC   … ► inferencia ► chequeo ──► monitoreo ──► hay_que_reentrenar ──(true)──► reentrenar  (run_job_task)
# MAGIC                               │   error observado (WAPE, sesgo)                  │
# MAGIC                               │   drift de entradas (PSI)                        ▼
# MAGIC                               │   decidir_reentrenamiento()       entrenar ─► evaluar_promover
# MAGIC                               ▼                                   candidato     decidir_promocion()
# MAGIC              monitoreo_error · monitoreo_drift · monitoreo_decisiones · decisiones_modelo
# MAGIC ```
# MAGIC
# MAGIC Un `TODO`: `decidir_reentrenamiento()` en `src/xm_demanda/monitoreo/politica.py` (sección 4). Dos preguntas por escrito.
# MAGIC Al final, el cierre del curso. Requiere el lab 14 (la sesión abre con su paso e: el tag `v1.0.0` a qa y prod, en el
# MAGIC notebook `lab14_cicd_promocion`) y haber hecho **Pull** de `main` en tu Git folder.

# COMMAND ----------

# MAGIC %pip install -q pytest pyyaml

# COMMAND ----------

dbutils.widgets.text("usuario", "")
usuario = dbutils.widgets.get("usuario").strip().lower()
assert usuario, "Escribe tu usuario (el sufijo de tu esquema c01_<usuario>)."

# COMMAND ----------

import importlib
import os
import sys
from datetime import timedelta

import mlflow
import pandas as pd
import pytest
from databricks.sdk import WorkspaceClient


def _raiz_repo():
    d = os.getcwd()
    for _ in range(8):
        if os.path.exists(os.path.join(d, "databricks.yml")):
            return d
        d = os.path.dirname(d)
    raise FileNotFoundError("No encuentro databricks.yml: ejecuta este notebook desde tu Git folder.")


raiz = _raiz_repo()
sys.path.insert(0, os.path.join(raiz, "src"))
sys.dont_write_bytecode = True
import xm_demanda.inference.batch as batch
import xm_demanda.monitoreo.metricas as mon
import xm_demanda.monitoreo.politica as pol
from xm_demanda.model import datos
from xm_demanda.model import metricas as met
from xm_demanda.monitoreo.linaje import linaje
from xm_demanda.utils.ambiente import ubicacion
from xm_demanda.utils.config import load_config

w = WorkspaceClient()
mlflow.set_registry_uri("databricks-uc")
cliente = mlflow.MlflowClient()
cfg = load_config("dev")
M = cfg["monitoreo"]
u = ubicacion(usuario, "dev")
esquema = u["fqn"]
T_FEATURES, T_PRON, V_OBS = (f"{esquema}.{t}" for t in ("features_demanda_diaria", "pronostico_demanda", "v_observado_vs_estimado"))
print("dev:", esquema, "| umbrales:", {k: M[k] for k in ("tolerancia_degradacion", "psi_alto", "dias_max_sin_reentrenar")})

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Qué se monitorea, y con qué en Databricks
# MAGIC
# MAGIC ![Qué monitorear](./../img/que_monitorear.png)
# MAGIC
# MAGIC | Capa | Pregunta | Señal | Ya lo tenemos | En Databricks |
# MAGIC |---|---|---|---|---|
# MAGIC | **Datos** | ¿llegó, completo y a tiempo? | frescura, volumen, nulos, cuarentena | chequeo (clase 12), calidad (clase 5) | *Data quality monitoring* → **anomaly detection**: frescura y completitud por tabla, con un clic sobre el esquema |
# MAGIC | **Entradas del modelo** | ¿el mundo se parece al del entrenamiento? | drift (PSI, KS, Wasserstein) | **hoy** | *Data profiling* (antes Lakehouse Monitoring): tablas `_profile_metrics` y `_drift_metrics` |
# MAGIC | **Desempeño** | ¿el pronóstico acierta? | WAPE y sesgo observados, por segmento | la vista de la clase 11 | *Data profiling* tipo **InferenceLog** (regresión), o un monitor propio como el de hoy |
# MAGIC | **Sistema** | ¿corre, cuánto tarda, cuánto cuesta? | duración, fallas, DBUs | notificaciones (clase 12) | avisos por duración en Jobs; tablas de sistema `system.billing.usage` y `system.lakeflow.*` (no disponibles en Free Edition) |
# MAGIC
# MAGIC Dos ideas que ordenan todo lo demás. **El drift avisa antes que el error**: el error necesita el dato real, que llega
# MAGIC 7 días después del pronóstico; el drift solo necesita las entradas de hoy. Y **el promedio esconde**: un WAPE global
# MAGIC estable puede ocultar un mercado que se degradó (clase 9).
# MAGIC
# MAGIC Hoy construimos el monitor **como código**, en `src/xm_demanda/monitoreo/` con tests, porque funciona igual en Free
# MAGIC Edition y en Azure y porque la decisión de reentrenar no puede depender de que alguien abra un tablero. Al final hay una
# MAGIC extensión con *Data profiling*, la oferta administrada.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · El error observado: lo que el modelo prometió contra lo que pasó
# MAGIC
# MAGIC `error_diario()` agrega la vista de la clase 11 por día y alias: WAPE (suma de errores sobre suma de lo observado) y
# MAGIC **sesgo** (suma de errores con signo sobre lo observado: positivo = sobreestima). El WAPE dice cuánto se equivoca; el
# MAGIC sesgo dice hacia dónde, y un sesgo sostenido es la primera huella de un cambio estructural. `wape_ventana()` pondera por
# MAGIC energía: un festivo de baja demanda no pesa lo mismo que un martes.

# COMMAND ----------

spark.sql(batch.sql_vista_observado_vs_estimado(V_OBS, T_PRON, T_FEATURES))
obs = spark.table(V_OBS).toPandas()
diario = mon.error_diario(obs)
hasta = pd.to_datetime(diario["fecha_pronostico"]).max()
champ = cliente.get_model_version_by_alias(u["modelo"], "champion")
lin = linaje(cliente, champ)
print(f"Observado hasta {hasta.date()} · champion v{champ.version}: datos hasta {lin['datos_hasta']}, "
      f"WAPE de validación {lin['wape_validacion']:.2%}")
for a in ("champion", "challenger"):
    print(f"  {a:10s} WAPE 7 días {mon.wape_ventana(diario, a, hasta, 7):.2%} · 28 días {mon.wape_ventana(diario, a, hasta, 28):.2%}")

pivote = diario.pivot(index="fecha_pronostico", columns="alias", values=["wape", "sesgo"]).tail(14)
display(pivote.round(4).reset_index())

# COMMAND ----------

ultimos = obs[pd.to_datetime(obs["fecha_pronostico"]) > hasta - pd.Timedelta(days=28)].dropna(subset=["demanda_observada_kwh"])
seg = met.wape_por_segmento(ultimos[ultimos["alias"] == "champion"], "demanda_observada_kwh", "demanda_estimada_kwh", "tipo_mercado")
display(seg)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · Drift: ¿el mundo de hoy se parece al del entrenamiento?
# MAGIC
# MAGIC ![PSI y drift](./../img/psi_drift.png)
# MAGIC
# MAGIC El **PSI** (*Population Stability Index*) compara dos distribuciones de la misma variable: se cortan los valores de
# MAGIC referencia en deciles y se mide cuánto cambió la proporción de la semana actual en cada decil:
# MAGIC PSI = Σ (actual − referencia) · ln(actual / referencia). Nació en riesgo de crédito y es el estándar de facto en la
# MAGIC industria por una razón práctica: se interpreta igual para cualquier variable.
# MAGIC
# MAGIC | PSI | Lectura | Qué hacer |
# MAGIC |---|---|---|
# MAGIC | < 0,10 | estable | nada |
# MAGIC | 0,10 – 0,25 | moderado | mirar el error de esa semana y el segmento |
# MAGIC | ≥ 0,25 | alto | disparador de reentrenamiento (`psi_alto` en `conf/base.yml`) |
# MAGIC
# MAGIC Se calcula sobre las features **sin escala** (`*_rel`, la demanda dividida por la media de 28 días de su serie): si se
# MAGIC calculara en kWh, mezclar 30 kWh con 35 GWh haría que cualquier cambio de composición pareciera drift.

# COMMAND ----------

oro = spark.table(T_FEATURES).toPandas()
oro["fecha"] = pd.to_datetime(oro["fecha"])
oro = oro[oro["dias_con_datos"] >= 29].reset_index(drop=True)
X = datos.features_relativas(oro).assign(fecha=oro["fecha"].to_numpy(), tipo_mercado=oro["tipo_mercado"].to_numpy())
fin = X["fecha"].max()
act = X[X["fecha"] > fin - pd.Timedelta(days=M["ventana_reciente_dias"])]
ref = X[(X["fecha"] <= fin - pd.Timedelta(days=M["ventana_reciente_dias"]))
        & (X["fecha"] > fin - pd.Timedelta(days=M["ventana_reciente_dias"] + M["ventana_referencia_dias"]))]
drift = mon.drift_features(ref, act, M["columnas_drift"], moderado=M["psi_moderado"], alto=M["psi_alto"])
print(f"Referencia: {ref['fecha'].min().date()} → {ref['fecha'].max().date()} · actual: {act['fecha'].min().date()} → {fin.date()}")
display(drift)

# COMMAND ----------

# MAGIC %md
# MAGIC **¿Y si el régimen cambia?** No hace falta esperar a que pase para probar el monitor. En 2026 XM reportó la demanda
# MAGIC del SIN creciendo 5,7 %, 6,7 % y 8,8 % interanual en marzo, abril y mayo, con récord histórico en mayo y alertas por un
# MAGIC posible Niño intenso. La celda de abajo simula un salto de 9 % en la demanda de **todas** las series durante la
# MAGIC semana actual. No toca ninguna tabla: es una copia en memoria. (Si lo aplicaras solo al mercado regulado, que es menos
# MAGIC del 10 % de las series, el PSI agregado casi no se movería: el promedio también esconde en el drift.)

# COMMAND ----------

simulada = act.copy()
for c in ("demanda_real_kwh_rel", "lag_7_rel"):
    simulada[c] = simulada[c] * 1.09
display(mon.drift_features(ref, simulada, M["columnas_drift"], moderado=M["psi_moderado"], alto=M["psi_alto"]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · TODO: la política de reentrenamiento, como código
# MAGIC
# MAGIC ![Política de reentrenamiento](./../img/politica_reentrenamiento.png)
# MAGIC
# MAGIC "Reentrenar cuando el modelo empeore" no es una política: no dice cuánto, medido cómo, ni quién decide. Hay tres
# MAGIC familias de disparadores, y los equipos maduros combinan las tres:
# MAGIC
# MAGIC | Disparador | Regla en `conf/base.yml` | Ventaja | Debilidad |
# MAGIC |---|---|---|---|
# MAGIC | **Degradación** | WAPE 7 días > WAPE de validación × (1 + `tolerancia_degradacion`) | mide lo que importa | llega tarde: necesita el observado |
# MAGIC | **Drift** | PSI máximo ≥ `psi_alto` | llega temprano | no todo drift daña el pronóstico |
# MAGIC | **Calendario** | días de datos desde el entrenamiento ≥ `dias_max_sin_reentrenar` | simple, predecible | reentrena aunque no haga falta |
# MAGIC | **Evento** | lo decide una persona: cambio regulatorio, Niño declarado, carga industrial nueva | conocimiento del negocio | depende de que alguien avise |
# MAGIC
# MAGIC **TODO:** completa `decidir_reentrenamiento(wape_reciente, wape_referencia, psi_max, dias_desde_entrenamiento,
# MAGIC umbrales)` en `src/xm_demanda/monitoreo/politica.py`. Tres disparadores independientes (basta uno); un `nan` no dispara
# MAGIC nada; cada disparador agrega un motivo legible que empieza por `degradación:`, `drift:` o `calendario:`. Los tests de
# MAGIC `tests/unit/test_monitoreo.py` dicen exactamente qué se espera.
# MAGIC
# MAGIC Y fíjate en lo que **no** decide esta función: promover. Reentrenar produce un `@candidato`; que reemplace al champion
# MAGIC lo decide otra regla, `decidir_promocion()`, que exige evidencia (al menos 14 días que ninguno de los dos vio) y margen
# MAGIC (al menos 2 % de mejora). Entrenar no es promover.

# COMMAND ----------

importlib.reload(pol)
codigo = pytest.main(["-q", "-p", "no:cacheprovider", "-rs", os.path.join(raiz, "tests", "unit", "test_monitoreo.py")])
print("pytest test_monitoreo →", codigo, "(0 = todo pasó)")

dias = (fin - pd.Timestamp(lin["datos_hasta"])).days if lin["datos_hasta"] else 0
hoy = pol.decidir_reentrenamiento(mon.wape_ventana(diario, "champion", hasta, 7), lin["wape_validacion"], float(drift["psi"].max()), dias, M)
psi_sim = float(mon.drift_features(ref, simulada, M["columnas_drift"])["psi"].max())
nino = pol.decidir_reentrenamiento(mon.wape_ventana(diario, "champion", hasta, 7), lin["wape_validacion"], psi_sim, dias, M)
print(f"\nHoy ({dias} días de datos desde el entrenamiento): {hoy}")
print(f"Con el Niño simulado (PSI {psi_sim:.2f}): {nino}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · El monitor dentro del job, y un reentrenamiento por evento
# MAGIC
# MAGIC `resources/job_diario.yml` tiene tres tareas nuevas después del chequeo: `monitoreo` (la tarea 7, que hace lo mismo que
# MAGIC las secciones 2 a 4 y escribe tres tablas), la condición `hay_que_reentrenar`, y `reentrenar`, que no es un notebook:
# MAGIC es un **`run_job_task`** que lanza el job `reentrenamiento` y espera su resultado. Así el reentrenamiento tiene su propio
# MAGIC historial, sus propias alertas, y se puede lanzar también a mano.
# MAGIC
# MAGIC 1. **Deployments** → target **dev** → **Deploy**. En **Bundle resources** aparece un tercer job: `reentrenamiento`.
# MAGIC 2. La celda de abajo corre el job diario de dev (sin datos nuevos: la rama se excluye, pero chequeo y monitoreo corren) y
# MAGIC    después el reentrenamiento **por evento**, con `motivo = evento: crecimiento del regulado 2026`.
# MAGIC
# MAGIC El reentrenamiento tarda unos minutos (LightGBM sobre las 355 series). Mientras corre: **Jobs & Pipelines** → el job
# MAGIC `reentrenamiento` → la corrida → `entrenar` → la salida; y **Experiments** → `xm-demanda-dev`.

# COMMAND ----------

def job_del_bundle(recurso, a="dev"):
    for j in w.jobs.list():
        if ((j.settings.name if j.settings else "") or "").endswith(f"{recurso} · {usuario} · {a}"):
            return w.jobs.get(j.job_id)
    raise LookupError(f"No encuentro el job '{recurso}' de {a}: ¿desplegaste el bundle?")


diario_job = job_del_bundle("diario")
r = w.jobs.run_now_and_wait(diario_job.job_id, timeout=timedelta(minutes=30))
print("diario dev:", r.state.result_state.value, "·", r.run_page_url)
for t in r.tasks:
    print(f"  {t.task_key:19s} {t.state.result_state.value if t.state.result_state else '—'}")

# COMMAND ----------

reent = job_del_bundle("reentrenamiento")
r2 = w.jobs.run_now_and_wait(reent.job_id, job_parameters={"motivo": "evento: crecimiento del regulado 2026"},
                             timeout=timedelta(minutes=60))
print("reentrenamiento dev:", r2.state.result_state.value, "·", r2.run_page_url)
display(spark.sql(f"SELECT * FROM {esquema}.decisiones_modelo ORDER BY decidido_en DESC LIMIT 5").toPandas())
for alias in ("champion", "candidato", "anterior"):
    try:
        mv = cliente.get_model_version_by_alias(u["modelo"], alias)
        print(f"@{alias:10s} → v{mv.version} · datos hasta {mv.tags.get('datos_hasta', linaje(cliente, mv)['datos_hasta'])} · "
              f"motivo: {mv.tags.get('motivo', '—')}")
    except Exception:  # noqa: BLE001
        print(f"@{alias:10s} → (no existe)")

# COMMAND ----------

# MAGIC %md
# MAGIC Lee la decisión antes que el número. `evaluar_promover` comparó al candidato con el champion **solo en los días que
# MAGIC ninguno de los dos vio al entrenar**, y aplicó la regla. Promueva o no, la fila queda en `decisiones_modelo`: quién
# MAGIC decidió (el job), con qué evidencia (días y WAPE de cada uno) y por qué. Si promovió, el champion anterior quedó como
# MAGIC `@anterior`: volver atrás es mover un alias, sin redesplegar nada. Y si no promovió, también es un buen resultado: el
# MAGIC sistema se negó a cambiar un modelo que funciona por otro que no demostró ser mejor.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Alertas, tablero y SLOs
# MAGIC
# MAGIC **Una alerta SQL sobre el monitor** (unos 5 minutos):
# MAGIC
# MAGIC 1. **SQL Editor** → nueva consulta, guárdala como `xm-demanda · WAPE 7 días · <usuario>`:
# MAGIC    ```sql
# MAGIC    SELECT sum(wape * energia_gwh) / sum(energia_gwh) AS wape_7d
# MAGIC    FROM workspace.c01_<usuario>.monitoreo_error
# MAGIC    WHERE alias = 'champion'
# MAGIC      AND fecha_pronostico > (SELECT max(fecha_pronostico) - INTERVAL 7 DAYS FROM workspace.c01_<usuario>.monitoreo_error)
# MAGIC    ```
# MAGIC 2. **Alerts** → **Create alert** → esa consulta → condición `wape_7d > 0.058` → notificación a tu correo → calendario
# MAGIC    diario. 0,058 es el WAPE de validación (4,8 %) con la tolerancia de 20 %: el mismo umbral que la política.
# MAGIC
# MAGIC **SLOs del producto.** Un SLO es una promesa medible con su tolerancia; sin él, "el pronóstico está bien" es opinión.
# MAGIC
# MAGIC | SLO | Objetivo | Se mide en | Si se incumple |
# MAGIC |---|---|---|---|
# MAGIC | Frescura | pronóstico publicable antes de las 8:00, 95 % de los días hábiles | chequeo (clase 12) | runbook A1 |
# MAGIC | Calidad del pronóstico | WAPE 7 días del champion ≤ 5,8 % | `monitoreo_error` | runbook A3: reentrenar o escalar |
# MAGIC | Estabilidad de entradas | PSI < 0,25 en todas las features | `monitoreo_drift` | runbook A4 |
# MAGIC | Costo | el job diario por debajo de su presupuesto de DBUs | `system.billing.usage` (Azure) | revisar tamaño de cómputo y frecuencia |
# MAGIC
# MAGIC El **runbook** (`docs/runbook.md`) dice, para cada alerta, quién responde y qué hace en los primeros 30 minutos. Ábrelo:
# MAGIC es el documento que se lee a las 6:15 de un lunes festivo, no en clase.

# COMMAND ----------

# Extensión opcional: Data profiling (antes Lakehouse Monitoring) sobre una foto de observado vs. estimado.
# Puede no estar disponible en Free Edition; si falla, el monitor propio de este lab cubre lo mismo.
PROBAR_PERFILADO = False
if PROBAR_PERFILADO:
    from databricks.sdk.service.dataquality import (
        AggregationGranularity,
        DataProfilingConfig,
        InferenceLogConfig,
        InferenceProblemType,
        Monitor,
    )
    T_FOTO = f"{esquema}.observado_vs_estimado_log"
    spark.sql(f"""CREATE OR REPLACE TABLE {T_FOTO} AS
                  SELECT *, CAST(fecha_pronostico AS TIMESTAMP) AS ts, CAST(modelo_version AS STRING) AS version_modelo
                  FROM {V_OBS} WHERE demanda_observada_kwh IS NOT NULL""")
    config = DataProfilingConfig(
        output_schema_id=w.schemas.get(full_name=esquema).schema_id,
        assets_dir=f"/Workspace/Users/{w.current_user.me().user_name}/xm-demanda-monitoreo",
        inference_log=InferenceLogConfig(
            problem_type=InferenceProblemType.INFERENCE_PROBLEM_TYPE_REGRESSION,
            prediction_column="demanda_estimada_kwh", label_column="demanda_observada_kwh",
            model_id_column="version_modelo", timestamp_column="ts",
            granularities=[AggregationGranularity.AGGREGATION_GRANULARITY_1_DAY]),
        slicing_exprs=["tipo_mercado"])
    tabla = w.tables.get(full_name=T_FOTO)
    print(w.data_quality.create_monitor(monitor=Monitor(object_type="table", object_id=tabla.table_id, data_profiling_config=config)))

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta 1 (respóndela aquí):** supón que el PSI de la semana sube a 0,5, como en la simulación, pero el WAPE
# MAGIC observado todavía está en 4,9 %. La política dice "reentrenar". ¿Lo harías? ¿Qué riesgo corres si reentrenas con solo una semana del
# MAGIC régimen nuevo, y qué te protege de ese riesgo en este diseño?
# MAGIC
# MAGIC > Tu respuesta:
# MAGIC
# MAGIC **Pregunta 2 (respóndela aquí):** en tu proyecto real (analitica_trv u otro), ¿cuál de los cuatro disparadores es el más
# MAGIC importante y por qué? ¿Quién en tu organización es el "evento": quién sabe primero que algo cambió?
# MAGIC
# MAGIC > Tu respuesta:

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Cierre del curso: lo que construimos
# MAGIC
# MAGIC ![El recorrido del curso](./../img/recorrido_curso.png)
# MAGIC
# MAGIC | Hito | Qué demuestra | Dónde está |
# MAGIC |---|---|---|
# MAGIC | H1 Pregunta analítica | qué se pronostica, para quién y con qué granularidad | `docs/architecture.md`, ADR-001 |
# MAGIC | H2 Arquitectura y fuentes | bronce idempotente con republicaciones | `src/xm_demanda/ingest`, ADR-002 |
# MAGIC | H3 Gobierno y calidad | permisos, reglas, cuarentena, plata | `docs/governance.md`, `quality`, `silver` |
# MAGIC | H4 Pipeline y oro | features con contrato y pruebas en CI | `features`, `tests/` |
# MAGIC | H5 Modelo y despliegue | experimentos comparables, registro con alias, batch, endpoint y app | `model`, `inference`, `app/` |
# MAGIC | H6 Operación | job que se cuida, bundle por ambientes, CI/CD con aprobación, monitor y reentrenamiento | `orquestacion`, `databricks.yml`, `.github/`, `monitoreo`, `docs/runbook.md` |
# MAGIC
# MAGIC Lo que se llevan no es el modelo de demanda (el algoritmo importó poco, como prometimos el primer día). Es la forma:
# MAGIC código en `src/` con tests, configuración por ambiente, tareas delgadas, infraestructura declarada, promoción con
# MAGIC compuertas, y decisiones sobre el modelo que quedan escritas. Esa forma sirve para el siguiente modelo de XM, y para
# MAGIC migrar el primero que ya tienen. `docs/backlog.md` recoge lo que quedó por fuera, por clase.

# COMMAND ----------

def verificar():
    checks = []
    importlib.reload(pol)
    try:
        d = pol.decidir_reentrenamiento(0.07, 0.05, 0.05, 10, M)
        checks.append(("decidir_reentrenamiento() implementada", d["reentrenar"] and d["motivos"][0].startswith("degradación")))
    except NotImplementedError:
        checks.append(("decidir_reentrenamiento() implementada", False))
    try:
        tareas = {t.task_key for t in job_del_bundle("diario").settings.tasks}
        checks.append(("Job diario de dev con monitoreo y reentrenamiento", {"monitoreo", "hay_que_reentrenar", "reentrenar"} <= tareas))
        job_del_bundle("reentrenamiento")
        checks.append(("Job de reentrenamiento desplegado", True))
    except LookupError:
        checks.append(("Jobs de la clase 15 desplegados", False))
    for t, n in (("monitoreo_error", 1), ("monitoreo_drift", 1), ("monitoreo_decisiones", 1), ("decisiones_modelo", 1)):
        ok = spark.catalog.tableExists(f"{esquema}.{t}") and spark.table(f"{esquema}.{t}").count() >= n
        checks.append((f"Tabla {t} con filas", ok))
    try:
        cand = cliente.get_model_version_by_alias(u["modelo"], "candidato")
        checks.append(("Candidato registrado con linaje (datos_hasta, motivo)", {"datos_hasta", "motivo"} <= set(cand.tags)))
    except Exception:  # noqa: BLE001
        checks.append(("Candidato registrado con linaje (datos_hasta, motivo)", False))
    runbook = open(os.path.join(raiz, "docs", "runbook.md"), encoding="utf-8").read()
    checks.append(("docs/runbook.md con dueño en cada alerta", "<dueño>" not in runbook and "| A3 " in runbook))
    print()
    for nombre, ok in checks:
        print(("OK   " if ok else "FALTA") + "  " + nombre)
    print("\nHito 6 cerrado. Commit, push, PR, y un tag v1.1.0 para llevar el monitor a qa y prod."
          if all(ok for _, ok in checks) else "\nRevisa los puntos marcados FALTA.")


verificar()
