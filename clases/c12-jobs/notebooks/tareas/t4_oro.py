# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea 4 · oro: features con contrato
# MAGIC Parámetro del job: `usuario`. `construir_features` + `merge_features` de la clase 7, y el contrato de la clase 8 como
# MAGIC compuerta: si oro no tiene exactamente las columnas esperadas, la tarea falla y la inferencia no corre.

# COMMAND ----------

dbutils.widgets.text("usuario", "")
usuario = dbutils.widgets.get("usuario").strip().lower()
esquema = f"workspace.c01_{usuario}"
T_DIARIA, T_FEATURES = f"{esquema}.demanda_diaria", f"{esquema}.features_demanda_diaria"

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

import pandas as pd
from pyspark.sql import functions as F

import xm_demanda.features.build as fb
from xm_demanda.utils.config import load_config

cfg = load_config("dev")
festivos = spark.createDataFrame(pd.read_csv(os.path.join(raiz, "data", "reference", "festivos_co.csv"), parse_dates=["fecha"]))
festivos = festivos.withColumn("fecha", F.to_date("fecha"))
features = fb.construir_features(spark.table(T_DIARIA), festivos, cfg)
fb.merge_features(spark, features, T_FEATURES)

oro = spark.table(T_FEATURES)
assert set(oro.columns) == set(fb.columnas_contrato(cfg)), "La tabla de oro no cumple el contrato (clase 8)"
ultima_fecha = str(oro.agg(F.max("fecha")).first()[0])
dbutils.jobs.taskValues.set(key="ultima_fecha", value=ultima_fecha)
print(f"oro: {oro.count():,} filas · última fecha {ultima_fecha}")
