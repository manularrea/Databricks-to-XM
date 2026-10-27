# Databricks notebook source
# MAGIC %md
# MAGIC # Clase 11 — Del modelo registrado a quien lo usa: inferencia batch, Serving y Apps
# MAGIC
# MAGIC El modelo `@champion` de la clase 9 no le sirve a nadie mientras viva en el registro. Hoy lo ponemos frente a sus
# MAGIC consumidores por los canales que ofrece Databricks, y escogemos cuál usar para quién:
# MAGIC
# MAGIC ```
# MAGIC                                    ┌─► 1 · BATCH     pronostico_demanda (tabla Delta) ──► vista observado vs. estimado
# MAGIC   c01_<usuario>.demanda_7d         │                 champion y challenger, con versión del modelo y de los datos
# MAGIC   @champion  (clase 9)  ───────────┼─► 2 · SERVING   endpoint REST xm-demanda-7d (compartido) ──► 5 filas en milisegundos
# MAGIC   @challenger                      │
# MAGIC                                    ├─► 3 · APP       Databricks App "observado vs. estimado" (compartida) ──► lee TU vista
# MAGIC                                    │                 si y solo si le das permiso (GRANT)
# MAGIC                                    └─► 4 · AI/BI     dashboard y Genie sobre la misma vista (extensión)
# MAGIC ```
# MAGIC
# MAGIC Dos `TODO`: `preparar_pronosticos` en `src/xm_demanda/inference/batch.py` (sección 3) y dos `GRANT` para la app
# MAGIC (sección 7). Dos preguntas por escrito. Requiere el lab 9: `workspace.c01_<usuario>.demanda_7d` con `@champion` y
# MAGIC `@challenger`. El endpoint y la app los despliega la docente una vez para todo el grupo: Free Edition permite tres apps por
# MAGIC cuenta y pocos endpoints, y esa restricción también enseña algo (sección 6).

# COMMAND ----------

# MAGIC %pip install -q lightgbm

# COMMAND ----------

dbutils.widgets.text("usuario", "")
dbutils.widgets.text("endpoint", "xm-demanda-7d")
dbutils.widgets.text("app_sp", "")
usuario = dbutils.widgets.get("usuario").strip().lower()
ENDPOINT = dbutils.widgets.get("endpoint").strip()
APP_SP = dbutils.widgets.get("app_sp").strip()
assert usuario, "Escribe tu usuario (el sufijo de tu esquema c01_<usuario>)."

esquema = f"workspace.c01_{usuario}"
T_FEATURES = f"{esquema}.features_demanda_diaria"
NOMBRE_MODELO = f"{esquema}.demanda_7d"
T_PRON = f"{esquema}.pronostico_demanda"
V_OBS = f"{esquema}.v_observado_vs_estimado"
print("Oro:", T_FEATURES, "| Modelo:", NOMBRE_MODELO, "| Pronósticos:", T_PRON)

# COMMAND ----------

import importlib
import os
import sys
import time

import matplotlib.pyplot as plt
import mlflow
import numpy as np
import pandas as pd


def _raiz_repo():
    d = os.getcwd()
    for _ in range(8):
        if os.path.exists(os.path.join(d, "src", "xm_demanda")):
            return d
        d = os.path.dirname(d)
    raise FileNotFoundError("No encuentro src/xm_demanda: ejecuta este notebook desde tu Git folder.")


raiz = _raiz_repo()
sys.path.insert(0, os.path.join(raiz, "src"))
sys.dont_write_bytecode = True
import xm_demanda.inference.batch as batch
import xm_demanda.model.metricas as met
from xm_demanda.silver.transform import LLAVE_SERIE
from xm_demanda.utils.config import load_config

for m in (batch, met):
    importlib.reload(m)

cfg = load_config("dev")
H = cfg["modelo"]["horizon_days"]
OBJETIVO = f"y_h{H}"
DESDE = "2026-06-30"  # el corte de la clase 9: lo que viene después nunca lo vio el modelo
mlflow.set_registry_uri("databricks-uc")
cliente_mlflow = mlflow.MlflowClient()
print("Horizonte:", H, "días | backtest desde:", DESDE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1 · Cuatro canales para un mismo pronóstico, y cómo escoger
# MAGIC
# MAGIC ![Canales de consumo](./../img/canales_consumo.png)
# MAGIC
# MAGIC Un modelo se despliega **para alguien**. La pregunta correcta no es "¿cómo despliego?" sino "¿quién consume esto, cada
# MAGIC cuánto y con qué datos en la mano?":
# MAGIC
# MAGIC | Canal | Cuándo | Latencia | Quién lo consume en XM | En Databricks |
# MAGIC |---|---|---|---|---|
# MAGIC | **Batch** | Las features cambian una vez al día y el consumidor lee una tabla | minutos, una vez al día | planeación de la operación, informes, otros modelos | job + `pyfunc`/`spark_udf` o `ai_query` → tabla Delta |
# MAGIC | **Serving (tiempo real)** | Alguien pide un pronóstico en el momento, con datos que trae él | ms a s | otro sistema: un simulador "qué pasa si", una integración | Model Serving: endpoint REST serverless |
# MAGIC | **App** | Una persona explora, filtra y decide | interactiva | analistas de demanda, área comercial | Databricks Apps (Streamlit, Dash, Gradio, Flask…) |
# MAGIC | **AI/BI** | Indicadores fijos o preguntas en lenguaje natural | interactiva | gerencia, seguimiento | Dashboards AI/BI y Genie |
# MAGIC
# MAGIC La regla práctica: **batch por defecto**. Nuestro pronóstico es diario y a 7 días; sus features salen de la tabla de oro,
# MAGIC que se actualiza una vez al día. Un endpoint no lo haría más fresco, solo más caro. El endpoint se justifica cuando el
# MAGIC que pregunta trae información que no está en la tabla (un escenario, una carga nueva) o necesita la respuesta en el
# MAGIC momento. La app y el dashboard no reemplazan al batch: lo **leen**.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2 · Inferencia batch: la anatomía de un job que publica cifras
# MAGIC
# MAGIC ![Inferencia batch](./../img/batch_inferencia.png)
# MAGIC
# MAGIC Seis pasos, y cada uno responde una pregunta que un auditor haría:
# MAGIC
# MAGIC 1. **Resolver el alias una vez.** `@champion` se traduce a una versión concreta al inicio; si alguien mueve el alias a
# MAGIC    mitad de la corrida, esta corrida sigue usando la misma. *¿Qué modelo produjo esta cifra?*
# MAGIC 2. **Leer oro con su versión Delta.** *¿Con qué datos?*
# MAGIC 3. **Escoger las filas.** En producción, los últimos 7 días (su futuro aún no llega). Hoy además hacemos *backtest*: desde
# MAGIC    el 30 de junio, el corte de la clase 9, para tener pronósticos que ya se pueden comparar con lo observado.
# MAGIC 4. **Predecir** con el mismo `pyfunc` que se registró: la misma firma, el mismo código empacado.
# MAGIC 5. **Validar antes de publicar**: una predicción por fila, sin nulos, sin negativos.
# MAGIC 6. **Escribir idempotente** con `MERGE` sobre la llave `(fecha_pronostico, serie, modelo_version)`.

# COMMAND ----------

version_oro = spark.sql(f"DESCRIBE HISTORY {T_FEATURES}").agg({"version": "max"}).first()[0]
oro = spark.table(T_FEATURES).toPandas()
oro["fecha"] = pd.to_datetime(oro["fecha"])

modelos = {}
for alias in ("champion", "challenger"):
    mv = cliente_mlflow.get_model_version_by_alias(NOMBRE_MODELO, alias)
    run = cliente_mlflow.get_run(mv.run_id)
    modelos[alias] = {
        "version": mv.version,
        "nombre": run.data.params.get("modelo", "?"),
        "wape_run": run.data.metrics.get("wape"),
        "pyfunc": mlflow.pyfunc.load_model(f"models:/{NOMBRE_MODELO}/{mv.version}"),
    }
    print(f"@{alias:10s} → versión {mv.version} · {modelos[alias]['nombre']:16s} · wape del run {modelos[alias]['wape_run']:.4f} · run {mv.run_id[:8]}")

filas = batch.filas_a_pronosticar(oro, OBJETIVO, H, desde=DESDE)
futuro = filas["fecha"] > oro["fecha"].max() - pd.Timedelta(days=H)
print(f"\nOro versión {version_oro}: {len(filas):,} filas a pronosticar ({(~futuro).sum():,} de backtest + {futuro.sum():,} hacia el futuro)")
print(f"Fechas pronosticadas: {(filas['fecha'].min() + pd.Timedelta(days=H)).date()} → {(filas['fecha'].max() + pd.Timedelta(days=H)).date()}")

# COMMAND ----------

t0 = time.perf_counter()
yhat = {alias: m["pyfunc"].predict(filas).to_numpy() for alias, m in modelos.items()}
print(f"{len(filas):,} filas × 2 modelos en {time.perf_counter() - t0:.1f} s")
print("champion, primeras 3:", np.round(yhat["champion"][:3]), "| challenger:", np.round(yhat["challenger"][:3]))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3 · TODO 1 — `preparar_pronosticos` en `src/xm_demanda/inference/batch.py`
# MAGIC
# MAGIC Un número sin contexto no se puede publicar. Cada fila de `pronostico_demanda` lleva lo que se pronostica y **quién y con
# MAGIC qué** lo pronosticó:
# MAGIC
# MAGIC | Columna | Qué es | Por qué |
# MAGIC |---|---|---|
# MAGIC | `fecha_emision` | la fecha de la fila de oro (t) | desde qué información se pronosticó |
# MAGIC | `fecha_pronostico` | t + 7 | el día que se pronostica: con esta columna se une a lo observado |
# MAGIC | llave de la serie | agente, mercado, tipo de mercado, CIIU | igual que en oro |
# MAGIC | `demanda_estimada_kwh` | la predicción, float, nunca negativa | un consumo negativo no existe |
# MAGIC | `modelo`, `modelo_version`, `alias` | qué modelo | linaje: versión → run → código y parámetros |
# MAGIC | `version_datos` | versión Delta de oro que se leyó | linaje: con *time travel* se reconstruye la entrada |
# MAGIC | `generado_en` | cuándo corrió | para auditar y para saber si la cifra está fresca |
# MAGIC
# MAGIC La función valida antes de devolver: si `yhat` no tiene una predicción por fila, o trae nulos o infinitos, lanza
# MAGIC `ValueError`. **Un job de inferencia que publica vacíos sin fallar es peor que uno que falla.** Los tests están en
# MAGIC `tests/unit/test_inference.py`.

# COMMAND ----------

importlib.reload(batch)
generado_en = pd.Timestamp.now(tz="America/Bogota").tz_localize(None).floor("s")
pron = pd.concat(
    [
        batch.preparar_pronosticos(filas, yhat[a], H, m["nombre"], m["version"], a, version_oro, generado_en)
        for a, m in modelos.items()
    ],
    ignore_index=True,
)
assert list(pron.columns) == batch.COLUMNAS_PRONOSTICO
assert not pron.duplicated(batch.LLAVE_PRONOSTICO).any()
print(f"{len(pron):,} filas listas ({pron['modelo_version'].nunique()} versiones del modelo)")
display(pron.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4 · Escribir sin duplicar: tabla con contrato y `MERGE` idempotente
# MAGIC
# MAGIC ![MERGE idempotente](./../img/idempotencia_merge.png)
# MAGIC
# MAGIC Un job de producción se va a ejecutar dos veces el mismo día: un reintento, alguien que lo relanza, un backfill. Si
# MAGIC escribe con `append`, la segunda corrida duplica la cifra y el tablero de mañana muestra el doble de demanda. Con `MERGE`
# MAGIC sobre la llave `(fecha_pronostico, serie, modelo_version)` la segunda corrida **actualiza** las mismas filas. Lo vamos a
# MAGIC comprobar corriendo el `MERGE` dos veces y leyendo las métricas de la operación en el historial de Delta.
# MAGIC
# MAGIC La tabla se crea con DDL explícito: tipos, `NOT NULL`, comentarios y *liquid clustering* por `fecha_pronostico`, que es
# MAGIC por donde la van a filtrar todos sus consumidores. Es la misma idea del contrato de oro de la clase 8, ahora para la
# MAGIC salida.

# COMMAND ----------

ddl = batch.ddl_pronostico(T_PRON)
print(ddl)
spark.sql(ddl)

# Spark necesita fechas como date, no como timestamp, para las columnas DATE del contrato
pron_spark = spark.createDataFrame(
    pron.assign(fecha_emision=pron["fecha_emision"].dt.date, fecha_pronostico=pron["fecha_pronostico"].dt.date)
)
pron_spark.createOrReplaceTempView("pronosticos_nuevos")


def escribir():
    spark.sql(batch.sql_merge_pronosticos(T_PRON, "pronosticos_nuevos"))
    met_op = spark.sql(f"DESCRIBE HISTORY {T_PRON} LIMIT 1").first()["operationMetrics"]
    return int(met_op.get("numTargetRowsInserted", 0)), int(met_op.get("numTargetRowsUpdated", 0))


for intento in (1, 2):
    insertadas, actualizadas = escribir()
    total = spark.table(T_PRON).count()
    print(f"MERGE #{intento}: {insertadas:,} insertadas · {actualizadas:,} actualizadas · total en la tabla {total:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5 · Observado vs. estimado: la vista que leen la app, el dashboard y Genie
# MAGIC
# MAGIC Una sola definición de "error", en SQL, en un solo lugar. La vista une cada pronóstico con lo que de verdad se consumió
# MAGIC ese día (`LEFT JOIN`: el futuro queda con lo observado en nulo). Si mañana el área comercial pregunta "¿qué tan bien
# MAGIC pronosticamos el no regulado?", la app, el dashboard y Genie responden con la misma cifra porque leen la misma vista.
# MAGIC
# MAGIC Y una comprobación que me importa más que cualquier gráfica: el WAPE del champion en el backtest **tiene que dar lo mismo
# MAGIC que el run de la clase 9**, porque son las mismas filas y el mismo modelo. Si no da, la inferencia no está usando el
# MAGIC modelo que creemos.

# COMMAND ----------

spark.sql(batch.sql_vista_observado_vs_estimado(V_OBS, T_PRON, T_FEATURES))

resumen = spark.sql(f"""
SELECT alias, modelo, modelo_version,
       count(*) AS filas,
       round(sum(error_abs_kwh) / sum(abs(demanda_observada_kwh)), 4) AS wape,
       round(sum(demanda_observada_kwh) / 1e6, 1) AS energia_gwh
FROM {V_OBS}
WHERE demanda_observada_kwh IS NOT NULL
GROUP BY alias, modelo, modelo_version
ORDER BY wape
""").toPandas()
display(resumen)
for _, r in resumen.iterrows():
    w_run = modelos[r["alias"]]["wape_run"]
    print(f"@{r['alias']:10s} wape en la vista {r['wape']:.4f} vs. run de la clase 9 {w_run:.4f} → diferencia {abs(r['wape'] - w_run):.4f}")

# COMMAND ----------

obs = spark.table(V_OBS).toPandas()
obs["fecha_pronostico"] = pd.to_datetime(obs["fecha_pronostico"])
display(batch.resumen_por_segmento(obs[obs["alias"] == "champion"], "tipo_mercado"))

pais = obs.groupby(["fecha_pronostico", "alias"]).agg(
    estimado=("demanda_estimada_kwh", "sum"), observado=("demanda_observada_kwh", lambda s: s.sum(min_count=1))
).reset_index()
fig, ax = plt.subplots(figsize=(11, 4.2))
obs_pais = pais[pais["alias"] == "champion"].dropna(subset=["observado"])
ax.plot(obs_pais["fecha_pronostico"], obs_pais["observado"] / 1e6, "o-", color="#1B1B2F", lw=2.5, label="observado")
for alias, color, estilo in (("champion", "#7C3AED", "-"), ("challenger", "#B45309", "--")):
    d = pais[pais["alias"] == alias]
    ax.plot(d["fecha_pronostico"], d["estimado"] / 1e6, estilo, color=color, lw=2, label=f"{alias} (v{modelos[alias]['version']}, {modelos[alias]['nombre']})")
inicio_futuro = oro["fecha"].max() + pd.Timedelta(days=1)
ax.axvspan(inicio_futuro, pais["fecha_pronostico"].max() + pd.Timedelta(days=1), color="#E5E7EB", alpha=0.6, label="futuro: aún sin observado")
ax.set_title("Demanda total del país: observado vs. estimado (suma de las series)")
ax.set_ylabel("GWh/día")
ax.legend(loc="lower left")
fig.autofmt_xdate()
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6 · Model Serving: el mismo modelo detrás de una URL
# MAGIC
# MAGIC ![Model Serving](./../img/serving_endpoint.png)
# MAGIC
# MAGIC Un **endpoint** es un servicio REST serverless que carga una versión del modelo registrado y responde
# MAGIC `POST /serving-endpoints/<nombre>/invocations`. Lo que Databricks pone y uno no tiene que construir:
# MAGIC
# MAGIC | Pieza | Qué resuelve |
# MAGIC |---|---|
# MAGIC | *Served entity* = modelo de UC + versión | el endpoint sirve `demanda_7d` versión N; el contenedor se arma con la firma, el `code_paths` y las dependencias del run |
# MAGIC | Tamaño y *scale to zero* | Small/Medium/Large en concurrencia; en cero no cuesta, pero la primera llamada espera el arranque (*cold start*) |
# MAGIC | *Traffic split* | 90 % champion / 10 % challenger en el mismo endpoint: prueba A/B sin tocar al cliente |
# MAGIC | AI Gateway | permisos, límites de uso (*rate limits*) y **tablas de inferencia**: cada request y response queda en una tabla Delta para monitoreo |
# MAGIC | Permisos | `CAN QUERY` por usuario o grupo, como cualquier objeto gobernado |
# MAGIC
# MAGIC En Free Edition: solo CPU y pocos endpoints activos por cuenta. Por eso hay **uno compartido**, `xm-demanda-7d`, que
# MAGIC desplegó la docente con su `@champion`. Ustedes le van a mandar cinco filas de **su** tabla de oro y a comparar la respuesta
# MAGIC con su propio batch. Fíjense en la consecuencia de diseño más importante: el endpoint recibe las 23 columnas de la firma.
# MAGIC **El que llama tiene que traer las features.** En tiempo real eso se resuelve con *feature serving* / tablas en línea
# MAGIC (Lakebase), que buscan las features por llave en milisegundos. Esa es la pregunta 2.

# COMMAND ----------

from mlflow.deployments import get_deploy_client

cliente_serving = get_deploy_client("databricks")
info = cliente_serving.get_endpoint(ENDPOINT)
entidad = (info.get("config") or {}).get("served_entities", [{}])[0]
print(f"Endpoint {ENDPOINT}: {info.get('state', {}).get('ready')} · sirve {entidad.get('entity_name')} versión {entidad.get('entity_version')}"
      f" · tamaño {entidad.get('workload_size')} · scale to zero {entidad.get('scale_to_zero_enabled')}")

muestra = filas[futuro].head(5)
cuerpo = batch.payload_endpoint(muestra)
latencias = []
for _ in range(3):
    t0 = time.perf_counter()
    try:
        respuesta_endpoint = cliente_serving.predict(endpoint=ENDPOINT, inputs=cuerpo)
    except Exception as e:  # noqa: BLE001
        raise RuntimeError(f"El endpoint no respondió ({type(e).__name__}). Si está escalando desde cero, espera 2 o 3 minutos y vuelve a ejecutar.") from e
    latencias.append((time.perf_counter() - t0) * 1000)
pred_endpoint = batch.leer_predicciones(respuesta_endpoint)

comparacion = muestra[["fecha", *LLAVE_SERIE]].assign(
    endpoint_kwh=pred_endpoint, mi_batch_kwh=yhat["champion"][futuro.to_numpy()][:5]
)
comparacion["diferencia_pct"] = (comparacion["endpoint_kwh"] / comparacion["mi_batch_kwh"] - 1) * 100
display(comparacion.round(2))
print("Latencias (ms):", [round(x) for x in latencias], "← la primera puede incluir el arranque si el endpoint estaba en cero")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7 · Databricks Apps: una app que ve exactamente lo que le dejas ver
# MAGIC
# MAGIC ![Arquitectura de la app](./../img/app_arquitectura.png)
# MAGIC
# MAGIC Una Databricks App es un contenedor serverless que corre código Python (Streamlit, Dash, Gradio, Flask) o Node dentro del
# MAGIC workspace, con URL propia, inicio de sesión de Databricks y **una identidad propia: un service principal**. La app del
# MAGIC curso está en `app/` del repo: lee la vista `v_observado_vs_estimado` de cada esquema a través del SQL warehouse y dibuja
# MAGIC observado vs. estimado con filtros por segmento.
# MAGIC
# MAGIC Como la app es un principal más de Unity Catalog, **no ve nada que no se le haya concedido**. Su selector de esquemas se
# MAGIC llena con `information_schema`, que solo muestra lo que ese principal puede leer: si no le das permiso, tu esquema no
# MAGIC aparece. Es el modelo de gobierno de la clase 3 aplicado a una aplicación.
# MAGIC
# MAGIC **TODO 2:** concede a la app lo mínimo para leer tu vista, y nada más. Dos sentencias `GRANT`. Pistas: el privilegio para
# MAGIC "entrar" a un esquema no es el mismo que para leer un objeto; y la app no necesita ver ni tu tabla de oro ni tu tabla de
# MAGIC pronósticos: la vista ya se encarga de eso con los permisos de su dueño, que eres tú. El ID del service principal lo
# MAGIC comparte la docente (pestaña *Authorization* de la app); va en el widget `app_sp`.

# COMMAND ----------

APP_SP = dbutils.widgets.get("app_sp").strip()  # se relee: el widget pudo llenarse después de la celda 3
assert APP_SP, "Escribe en el widget app_sp el ID de aplicación del service principal de la app (lo comparte la docente)."
GRANTS = [
    # TODO 2 (sección 7): las dos sentencias GRANT, como f-strings. Usa {esquema}, {V_OBS} y `{APP_SP}`.
]
assert GRANTS, "TODO 2: escribe las dos sentencias GRANT"
for g in GRANTS:
    spark.sql(g)
    print("OK ", g)
# Quién puede leer la vista. information_schema lista también los permisos sobre vistas
privilegios = spark.sql(f"""
SELECT grantee, privilege_type, inherited_from
FROM workspace.information_schema.table_privileges
WHERE table_schema = 'c01_{usuario}' AND table_name = 'v_observado_vs_estimado'
""")
display(privilegios)

# COMMAND ----------

# MAGIC %md
# MAGIC ![La app](./../img/app_captura.png)
# MAGIC
# MAGIC Abre la app (el enlace lo comparte la docente), escoge tu esquema en el selector y compara con la gráfica de la sección 5:
# MAGIC tienen que coincidir, porque leen la misma vista. Prueba quitarle el permiso (`REVOKE SELECT ON TABLE … FROM …`), recarga
# MAGIC y mira tu esquema desaparecer del selector. Vuelve a darlo antes de seguir.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8 · AI/BI: dashboard para lo que se mira siempre, Genie para lo que se pregunta una vez
# MAGIC
# MAGIC ![Dashboard y Genie](./../img/genie_dashboard.png)
# MAGIC
# MAGIC No todo consumidor necesita una app hecha a mano. Para indicadores fijos (WAPE de la semana por mercado, observado vs.
# MAGIC estimado) un **dashboard AI/BI** se arma en minutos sobre la misma vista, se programa y se comparte con permisos de UC. Para
# MAGIC preguntas que nadie previó ("¿qué agentes del no regulado subestimamos más en julio?"), un **espacio de Genie** responde en
# MAGIC español generando SQL sobre las tablas que se le den, con instrucciones y consultas de ejemplo que le enseñan el negocio.
# MAGIC La docente muestra Genie en vivo. El dashboard es la extensión del lab (`lab.md`): estos son sus dos datasets.

# COMMAND ----------

print(f"""-- Dataset 1 · serie diaria (gráfico de líneas: x = fecha_pronostico, y = observado y estimado, color = alias)
SELECT fecha_pronostico, alias, tipo_mercado,
       sum(demanda_estimada_kwh) / 1e6 AS estimado_gwh,
       sum(demanda_observada_kwh) / 1e6 AS observado_gwh
FROM {V_OBS}
GROUP BY ALL;

-- Dataset 2 · WAPE por segmento (contador y barras: filtro por alias)
SELECT alias, tipo_mercado, mercado_comercializacion,
       sum(error_abs_kwh) / sum(abs(demanda_observada_kwh)) AS wape,
       sum(demanda_observada_kwh) / 1e6 AS energia_gwh
FROM {V_OBS}
WHERE demanda_observada_kwh IS NOT NULL
GROUP BY ALL;""")

# COMMAND ----------

# MAGIC %md
# MAGIC **Pregunta 1 (respóndela aquí):** XM tiene al menos cuatro consumidores para este pronóstico: (a) la planeación de la
# MAGIC operación, que programa el despacho de mañana; (b) el área comercial, que revisa por agente por qué se desvió la semana;
# MAGIC (c) la gerencia, que quiere un número por semana; (d) un simulador interno que pregunta "¿y si entra una carga industrial de
# MAGIC 50 MW en este mercado?". ¿Qué canal le das a cada uno y por qué? Justifica con latencia, frescura de los datos y quién trae
# MAGIC las features.
# MAGIC
# MAGIC > Tu respuesta:
# MAGIC
# MAGIC **Pregunta 2 (respóndela aquí):** para llamar al endpoint mandaste las 23 columnas de oro. Si el simulador del caso (d)
# MAGIC no tiene acceso a la tabla de oro, ¿qué tendría que pasar para que pueda pedir un pronóstico enviando solo la llave de la
# MAGIC serie y la fecha? ¿Qué ofrece Databricks para eso y qué riesgo de la clase 7 vuelve a aparecer?
# MAGIC
# MAGIC > Tu respuesta:

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verificación
# MAGIC
# MAGIC Cuando todo esté en OK: commit `feat(c11): inferencia batch idempotente, vista observado vs. estimado y grants para la app`
# MAGIC (incluye `src/xm_demanda/inference/`), push, PR hacia `main` de tu fork, CI en verde, merge. Al cierre se libera el tag
# MAGIC `s11`. **Hito 5 cerrado**: el modelo ya no vive solo en el registro, lo consume alguien.

# COMMAND ----------


def verificar():
    checks = []
    importlib.reload(batch)
    try:
        f = batch.filas_a_pronosticar(oro, OBJETIVO, H).head(3)
        p = batch.preparar_pronosticos(f, [1.0, -2.0, 3.0][: len(f)], H, "m", 1, "champion", 0, pd.Timestamp("2026-01-01"))
        ok = list(p.columns) == batch.COLUMNAS_PRONOSTICO and p["demanda_estimada_kwh"].min() >= 0
        try:
            batch.preparar_pronosticos(f, [np.nan] * len(f), H, "m", 1, "champion", 0, pd.Timestamp("2026-01-01"))
            ok = False
        except ValueError:
            pass
        checks.append(("preparar_pronosticos implementada (columnas, sin negativos, falla con nulos)", ok))
    except NotImplementedError:
        checks.append(("preparar_pronosticos implementada", False))

    try:
        t = spark.table(T_PRON).toPandas()
        checks.append((f"{T_PRON} con las columnas del contrato", list(t.columns) == batch.COLUMNAS_PRONOSTICO))
        checks.append(("Sin duplicados por (fecha_pronostico, serie, modelo_version)", not t.duplicated(batch.LLAVE_PRONOSTICO).any()))
        checks.append(("Champion y challenger escritos (2 versiones)", t["modelo_version"].nunique() >= 2))
        dias_futuro = pd.to_datetime(t["fecha_pronostico"]).gt(oro["fecha"].max()).groupby(t["modelo_version"]).sum()
        checks.append(("Los 7 días del futuro pronosticados por versión", bool((dias_futuro > 0).all())))
    except Exception as e:  # noqa: BLE001
        checks.append((f"Tabla {T_PRON} ({type(e).__name__})", False))

    try:
        v = spark.sql(f"""SELECT alias, sum(error_abs_kwh) / sum(abs(demanda_observada_kwh)) AS wape
                          FROM {V_OBS} WHERE demanda_observada_kwh IS NOT NULL GROUP BY alias""").toPandas()
        w = dict(zip(v["alias"], v["wape"], strict=True))
        checks.append(("Vista observado vs. estimado: WAPE champion = run de la clase 9 (±0,002)", abs(w.get("champion", 9) - modelos["champion"]["wape_run"]) < 0.002))
    except Exception as e:  # noqa: BLE001
        checks.append((f"Vista {V_OBS} ({type(e).__name__})", False))

    checks.append(("Endpoint consultado: 5 predicciones", "pred_endpoint" in globals() and len(pred_endpoint) == 5))

    try:
        g = spark.sql(f"""SELECT grantee, privilege_type FROM workspace.information_schema.table_privileges
                          WHERE table_schema = 'c01_{usuario}' AND table_name = 'v_observado_vs_estimado'""").toPandas()
        yo = spark.sql("SELECT current_user()").first()[0]
        select = g[g["privilege_type"].str.upper() == "SELECT"]
        ok = bool(APP_SP) and (select["grantee"].eq(APP_SP).any() or select["grantee"].ne(yo).any())
        checks.append(("La app puede leer tu vista (GRANT SELECT al service principal)", ok))
    except Exception as e:  # noqa: BLE001
        checks.append((f"Grants de la vista ({type(e).__name__})", False))

    print()
    for nombre, ok in checks:
        print(("OK   " if ok else "FALTA") + "  " + nombre)
    print("\nListo: commit y push (incluye src/xm_demanda/inference)." if all(ok for _, ok in checks) else "\nRevisa los puntos marcados FALTA.")


verificar()
