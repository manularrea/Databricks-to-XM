# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea 7 · monitoreo: error observado, drift y la decisión de reentrenar
# MAGIC Parámetros del job: `usuario`, `ambiente`. Corre después del chequeo (solo si lo publicado está al día). Escribe tres
# MAGIC tablas del ambiente y publica el *task value* `reentrenar` (`si`/`no`), que lee la condición `hay_que_reentrenar`:
# MAGIC
# MAGIC | Tabla | Qué guarda | Quién la lee |
# MAGIC |---|---|---|
# MAGIC | `monitoreo_error` | WAPE y sesgo por día y alias (se recalcula completa: idempotente) | tablero, Genie, alerta SQL |
# MAGIC | `monitoreo_drift` | PSI por feature, última semana vs. las 8 anteriores (se agrega una foto por corrida) | quien investiga |
# MAGIC | `monitoreo_decisiones` | cada decisión de reentrenar o no, con sus motivos (solo se agrega) | auditoría, runbook |
# MAGIC
# MAGIC Falla a propósito (y el job alerta) si el WAPE reciente supera `umbral_critico` veces la referencia: eso ya no es
# MAGIC "reentrenar mañana", es "alguien mire esto hoy".

# COMMAND ----------

dbutils.widgets.text("usuario", "")
dbutils.widgets.text("ambiente", "dev")
usuario = dbutils.widgets.get("usuario").strip().lower()
ambiente = dbutils.widgets.get("ambiente").strip().lower()
assert usuario, "Falta el parámetro usuario"

# COMMAND ----------

import os
import sys


def _raiz_repo():
    d = os.getcwd()
    for _ in range(8):
        if os.path.exists(os.path.join(d, "src", "xm_demanda")):
            return d
        d = os.path.dirname(d)
    raise FileNotFoundError("No encuentro src/xm_demanda: el job debe apuntar a los notebooks del bundle o del Git folder.")


raiz = _raiz_repo()
sys.path.insert(0, os.path.join(raiz, "src"))
sys.dont_write_bytecode = True

from xm_demanda.utils.ambiente import ubicacion

u = ubicacion(usuario, ambiente)
esquema = u["fqn"]
T_FEATURES, T_PRON = f"{esquema}.features_demanda_diaria", f"{esquema}.pronostico_demanda"
V_OBS = f"{esquema}.v_observado_vs_estimado"
T_ERROR, T_DRIFT, T_DECISIONES = (f"{esquema}.{t}" for t in ("monitoreo_error", "monitoreo_drift", "monitoreo_decisiones"))
print(f"ambiente {ambiente} · esquema {esquema}")

# COMMAND ----------

import mlflow
import pandas as pd

import xm_demanda.inference.batch as batch
from xm_demanda.model import datos
from xm_demanda.monitoreo import metricas as mon
from xm_demanda.monitoreo.linaje import linaje
from xm_demanda.monitoreo.politica import decidir_reentrenamiento
from xm_demanda.utils.config import load_config

cfg = load_config(ambiente)
M = cfg["monitoreo"]
mlflow.set_registry_uri("databricks-uc")
cliente = mlflow.MlflowClient()

# 1 · Observado vs. estimado: la vista de la clase 11, ahora mantenida por el job en cada ambiente
spark.sql(batch.sql_vista_observado_vs_estimado(V_OBS, T_PRON, T_FEATURES))
obs = spark.table(V_OBS).toPandas()
diario = mon.error_diario(obs)
mv = cliente.get_model_version_by_alias(u["modelo"], "champion")
lin = linaje(cliente, mv)
if diario.empty:
    # Ambiente recién preparado (qa, prod): solo tiene pronósticos del futuro, aún sin demanda observada.
    # No es un error: no hay nada que medir todavía. El drift sí se puede calcular.
    wape = {"champion": float("nan"), "challenger": float("nan")}
    print("Sin demanda observada para los pronósticos de este ambiente todavía: el error se medirá cuando llegue.")
else:
    spark.createDataFrame(diario.assign(fecha_pronostico=pd.to_datetime(diario["fecha_pronostico"]).dt.date)) \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_ERROR)
    hasta = pd.to_datetime(diario["fecha_pronostico"]).max()
    wape = {a: mon.wape_ventana(diario, a, hasta, M["ventana_reciente_dias"]) for a in ("champion", "challenger")}
    print(f"observado hasta {hasta.date()} · WAPE {M['ventana_reciente_dias']} días: champion {wape['champion']:.2%}, "
          f"challenger {wape['challenger']:.2%} · referencia (validación) {lin['wape_validacion']:.2%}")

# COMMAND ----------

# 2 · Drift de las entradas: última semana de oro vs. las 8 semanas anteriores, sobre features sin escala
oro = spark.table(T_FEATURES).toPandas()
oro["fecha"] = pd.to_datetime(oro["fecha"])
oro = oro[oro["dias_con_datos"] >= 29]
X = datos.features_relativas(oro).assign(fecha=oro["fecha"].to_numpy())
fin = X["fecha"].max()
act = X[X["fecha"] > fin - pd.Timedelta(days=M["ventana_reciente_dias"])]
ref = X[(X["fecha"] <= fin - pd.Timedelta(days=M["ventana_reciente_dias"]))
        & (X["fecha"] > fin - pd.Timedelta(days=M["ventana_reciente_dias"] + M["ventana_referencia_dias"]))]
drift = mon.drift_features(ref, act, M["columnas_drift"], moderado=M["psi_moderado"], alto=M["psi_alto"])
calculado = pd.Timestamp.now(tz="America/Bogota").tz_localize(None).floor("s")
spark.createDataFrame(drift.assign(fecha_datos=fin.date(), calculado_en=calculado)) \
    .write.mode("append").option("mergeSchema", "true").saveAsTable(T_DRIFT)
print(drift.to_string(index=False))

# COMMAND ----------

# 3 · La decisión, con sus motivos, y la alerta si es grave
dias = (fin - pd.Timestamp(lin["datos_hasta"])).days if lin["datos_hasta"] else 0
decision = decidir_reentrenamiento(wape["champion"], lin["wape_validacion"], float(drift["psi"].max()), dias, M)
fila = {"calculado_en": calculado, "fecha_datos": fin.date(), "ambiente": ambiente, "modelo_version": int(mv.version),
        "wape_reciente": wape["champion"], "wape_referencia": lin["wape_validacion"], "psi_max": float(drift["psi"].max()),
        "dias_desde_entrenamiento": dias, "tolerancia_degradacion": M["tolerancia_degradacion"], "reentrenar": decision["reentrenar"], "motivos": " | ".join(decision["motivos"])}
spark.createDataFrame(pd.DataFrame([fila])).write.mode("append").option("mergeSchema", "true").saveAsTable(T_DECISIONES)
dbutils.jobs.taskValues.set(key="reentrenar", value="si" if decision["reentrenar"] else "no")
dbutils.jobs.taskValues.set(key="motivos", value=fila["motivos"] or "ninguno")
print(f"modelo v{mv.version} · {dias} días de datos desde su entrenamiento · reentrenar: "
      f"{'sí — ' + fila['motivos'] if decision['reentrenar'] else 'no'}")

critico = lin["wape_validacion"] * M["umbral_critico"]
assert not wape["champion"] > critico, (
    f"WAPE de los últimos {M['ventana_reciente_dias']} días ({wape['champion']:.1%}) supera {M['umbral_critico']}× la "
    f"referencia ({lin['wape_validacion']:.1%}). Revisa docs/runbook.md, alerta A3, antes de que el pronóstico se use.")
