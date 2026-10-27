# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea 2 · calidad: reglas de la clase 5 sobre la vista vigente
# MAGIC Parámetros del job: `usuario`, `umbral_cuarentena`. Escribe `demanda_validada`, `demanda_cuarentena` y agrega las
# MAGIC métricas de esta corrida a `calidad_metricas` (`origen = job`). Si la proporción en cuarentena supera el umbral, la
# MAGIC tarea **falla a propósito**: es preferible parar el job que publicar un pronóstico sobre datos que no pasaron calidad.

# COMMAND ----------

dbutils.widgets.text("usuario", "")
dbutils.widgets.text("umbral_cuarentena", "0.01")
usuario = dbutils.widgets.get("usuario").strip().lower()
umbral = float(dbutils.widgets.get("umbral_cuarentena"))
esquema = f"workspace.c01_{usuario}"
vigente = f"{esquema}.demanda_raw_vigente"
T_VALIDADA, T_CUARENTENA, T_METRICAS = (f"{esquema}.{t}" for t in ("demanda_validada", "demanda_cuarentena", "calidad_metricas"))

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

from pyspark.sql import functions as F

import xm_demanda.quality.rules as rules
import xm_demanda.quality.validador as validador

validas, cuarentena, metricas = validador.aplicar(spark.table(vigente), rules.REGLAS)
validas.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_VALIDADA)
cuarentena.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(T_CUARENTENA)
(spark.createDataFrame(metricas).withColumn("origen", F.lit("job"))
    .write.mode("append").option("mergeSchema", "true").saveAsTable(T_METRICAS))

n_validas, n_cuarentena = spark.table(T_VALIDADA).count(), spark.table(T_CUARENTENA).count()
proporcion = n_cuarentena / max(n_validas + n_cuarentena, 1)
dbutils.jobs.taskValues.set(key="cuarentena", value=n_cuarentena)
print(f"calidad: {n_validas:,} válidas · {n_cuarentena:,} en cuarentena ({proporcion:.3%}, umbral {umbral:.1%})")
assert proporcion <= umbral, f"{proporcion:.2%} de las filas en cuarentena supera el umbral de {umbral:.2%}: revisa {T_CUARENTENA}"
