# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea 5 · inferencia: champion y challenger a pronostico_demanda
# MAGIC Parámetro del job: `usuario`. La lógica de la clase 11, en modo producción: los últimos 7 días de oro, los dos
# MAGIC alias, MERGE idempotente. Es la única tarea con reintentos: puede reintentarse sin miedo porque el MERGE no duplica.

# COMMAND ----------

# MAGIC %pip install -q lightgbm

# COMMAND ----------

dbutils.widgets.text("usuario", "")
usuario = dbutils.widgets.get("usuario").strip().lower()
esquema = f"workspace.c01_{usuario}"
T_FEATURES, T_PRON = f"{esquema}.features_demanda_diaria", f"{esquema}.pronostico_demanda"
NOMBRE_MODELO = f"{esquema}.demanda_7d"

# COMMAND ----------

import os
import sys


def _raiz_repo():
    d = os.getcwd()
    for _ in range(8):
        if os.path.exists(os.path.join(d, "src", "xm_demanda")):
            return d
        d = os.path.dirname(d)
    raise FileNotFoundError("No encuentro src/xm_demanda: el job debe apuntar a los notebooks del Git folder.")


raiz = _raiz_repo()
sys.path.insert(0, os.path.join(raiz, "src"))
sys.dont_write_bytecode = True

import mlflow
import pandas as pd

import xm_demanda.inference.batch as batch
from xm_demanda.utils.config import load_config

H = load_config("dev")["modelo"]["horizon_days"]
OBJETIVO = f"y_h{H}"
mlflow.set_registry_uri("databricks-uc")
cliente = mlflow.MlflowClient()

version_oro = spark.sql(f"DESCRIBE HISTORY {T_FEATURES}").agg({"version": "max"}).first()[0]
oro = spark.read.option("versionAsOf", version_oro).table(T_FEATURES).toPandas()
oro["fecha"] = pd.to_datetime(oro["fecha"])
filas = batch.filas_a_pronosticar(oro, OBJETIVO, H)
assert len(filas) > 0, "No hay filas para pronosticar"
generado_en = pd.Timestamp.now(tz="America/Bogota").tz_localize(None).floor("s")

partes = []
for alias in ("champion", "challenger"):
    mv = cliente.get_model_version_by_alias(NOMBRE_MODELO, alias)
    nombre = cliente.get_run(mv.run_id).data.params.get("modelo", "?")
    modelo = mlflow.pyfunc.load_model(f"models:/{NOMBRE_MODELO}/{mv.version}")
    yhat = modelo.predict(filas).to_numpy()
    partes.append(batch.preparar_pronosticos(filas, yhat, H, nombre, mv.version, alias, version_oro, generado_en))
pron = pd.concat(partes, ignore_index=True)

spark.sql(batch.ddl_pronostico(T_PRON))
spark.createDataFrame(
    pron.assign(fecha_emision=pron["fecha_emision"].dt.date, fecha_pronostico=pron["fecha_pronostico"].dt.date)
).createOrReplaceTempView("pronosticos_nuevos")
spark.sql(batch.sql_merge_pronosticos(T_PRON, "pronosticos_nuevos"))
desde, hasta = pron["fecha_pronostico"].min().date(), pron["fecha_pronostico"].max().date()
dbutils.jobs.taskValues.set(key="pronostico_hasta", value=str(hasta))
print(f"inferencia: {len(pron):,} filas · {desde} → {hasta} · oro v{version_oro}")
