# Databricks notebook source
# MAGIC %md
# MAGIC # Tarea 9 · evaluar y promover: el candidato reemplaza al champion solo si gana
# MAGIC Parámetros del job: `usuario`, `ambiente`. Compara `@candidato` con `@champion` **en los mismos días**, y solo en días
# MAGIC que ninguno de los dos vio al entrenar (después del `datos_hasta` más reciente de los dos): sin eso, la comparación tiene
# MAGIC fuga. Aplica `decidir_promocion()` (umbrales en `conf/base.yml`, sección `promocion`). Si promueve, el champion anterior
# MAGIC queda con el alias `anterior`: volver atrás es mover un alias. Cada decisión, promueva o no, queda en `decisiones_modelo`.

# COMMAND ----------

# MAGIC %pip install -q lightgbm

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
T_FEATURES, T_DECISIONES = f"{esquema}.features_demanda_diaria", f"{esquema}.decisiones_modelo"
print(f"ambiente {ambiente} · esquema {esquema}")

# COMMAND ----------

import mlflow
import pandas as pd

from xm_demanda.model import metricas
from xm_demanda.monitoreo.linaje import linaje
from xm_demanda.monitoreo.politica import decidir_promocion
from xm_demanda.utils.config import load_config

cfg = load_config(ambiente)
OBJETIVO = f"y_h{cfg['modelo']['horizon_days']}"
mlflow.set_registry_uri("databricks-uc")
cliente = mlflow.MlflowClient()
champ = cliente.get_model_version_by_alias(u["modelo"], "champion")
cand = cliente.get_model_version_by_alias(u["modelo"], "candidato")
lin_ch, lin_ca = linaje(cliente, champ), linaje(cliente, cand)
desde = max(pd.Timestamp(lin_ch["datos_hasta"] or "2100-01-01"), pd.Timestamp(lin_ca["datos_hasta"] or "2100-01-01"))

oro = spark.table(T_FEATURES).toPandas()
oro["fecha"] = pd.to_datetime(oro["fecha"])
holdout = oro[(oro["dias_con_datos"] >= 29) & oro[OBJETIVO].notna() & (oro["fecha"] > desde)]
dias = holdout["fecha"].nunique()
print(f"champion v{champ.version} (datos hasta {lin_ch['datos_hasta']}) vs. candidato v{cand.version} "
      f"(datos hasta {lin_ca['datos_hasta']}) · {dias} días que ninguno vio")

# COMMAND ----------

if dias > 0:
    w = {k: metricas.wape(holdout[OBJETIVO], mlflow.pyfunc.load_model(f"models:/{u['modelo']}/{mv.version}").predict(holdout))
         for k, mv in (("champion", champ), ("candidato", cand))}
else:
    w = {"champion": float("nan"), "candidato": float("nan")}
d = decidir_promocion(w["champion"], w["candidato"], dias, cfg["promocion"])
if d["promover"]:
    cliente.set_registered_model_alias(u["modelo"], "anterior", champ.version)
    cliente.set_registered_model_alias(u["modelo"], "champion", cand.version)
    cliente.set_model_version_tag(u["modelo"], cand.version, "promovido_por", "job reentrenamiento · decidir_promocion")

fila = {"decidido_en": pd.Timestamp.now(tz="America/Bogota").tz_localize(None).floor("s"), "ambiente": ambiente,
        "champion_version": int(champ.version), "candidato_version": int(cand.version), "dias_comparados": int(dias),
        "wape_champion": w["champion"], "wape_candidato": w["candidato"], "promovido": d["promover"], "motivo": d["motivo"]}
spark.createDataFrame(pd.DataFrame([fila])).write.mode("append").option("mergeSchema", "true").saveAsTable(T_DECISIONES)
print(f"WAPE champion {w['champion']:.2%} · candidato {w['candidato']:.2%} → "
      f"{'PROMOVIDO' if d['promover'] else 'se queda el champion'}: {d['motivo']}")
