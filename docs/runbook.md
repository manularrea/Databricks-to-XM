# Runbook — xm-demanda (pronóstico de demanda a 7 días)

Qué hacer cuando suena una alerta. Se lee a las 6:15 de un lunes festivo: frases cortas, acciones concretas.

**Producto:** job `[xm-demanda] diario · <usuario> · <ambiente>` (6:00, America/Bogota) → `pronostico_demanda` → tablero, app y Genie.
**Consumidores:** planeación (diario, antes de las 8:00), seguimiento de la operación, mercado.

## SLOs

| SLO | Objetivo | Se mide en |
|---|---|---|
| Frescura | pronóstico publicable antes de las 8:00 el 95 % de los días hábiles | tarea `chequeo` |
| Calidad | WAPE 7 días del champion ≤ 5,5 % (validación 4,8 % + 15 %) | `monitoreo_error` |
| Estabilidad | PSI < 0,25 en las features monitoreadas | `monitoreo_drift` |

## Alertas

| Alerta | Señal | Umbral | Dueño | Primeros 30 minutos | Escalar a |
|---|---|---|---|---|---|
| A1 Pronóstico no publicable | `chequeo` falla: "Pronóstico NO publicable" | oro con ≥ 1 día de atraso o días sin pronóstico | Guardia de analítica | 1) Leer el mensaje del chequeo. 2) Tarea `bronce` → task value `filas_nuevas`: ¿llegó el archivo? 3) Si llegó tarde: *Repair run* desde `bronce`. 4) Si no llegó: avisar a planeación que se usa el pronóstico de ayer | Líder técnico de analítica, si a las 7:30 no hay pronóstico |
| A2 Cuarentena alta | `calidad` falla por umbral | > 1 % de filas en cuarentena | Guardia de analítica | 1) `demanda_cuarentena`: ¿qué regla y qué agentes? 2) ¿Republicación de XM o error de la fuente? 3) Si es la fuente, no subir el umbral: reportar | Dueño de los datos de demanda (XM) |
| A3 WAPE crítico | `monitoreo` falla | WAPE 7 días > 2 × validación | Líder técnico de analítica | 1) `monitoreo_error` por día y `tipo_mercado`: ¿un segmento o todos? 2) ¿Sesgo con signo constante? → cambio estructural. 3) ¿Evento conocido (festivo, regulatorio, El Niño)? 4) Volver a `@anterior` si el champion es nuevo | Planeación: el pronóstico se publica con advertencia |
| A4 Drift o degradación | `reentrenar` se dispara | ver `monitoreo_decisiones.motivos` | Guardia de analítica | 1) Leer los motivos. 2) Revisar la decisión en `decisiones_modelo` al terminar. 3) Si no promovió y el WAPE sigue subiendo: revisar features (¿falta una variable?) | Líder técnico de analítica |
| A5 Reentrenamiento falla | job `reentrenamiento` falla | cualquier falla | Guardia de analítica | 1) Tarea `entrenar`: ¿datos o código? 2) El champion sigue sirviendo: no hay urgencia de negocio. 3) Relanzar con `motivo = reintento` tras corregir | — |
| A6 Promoción detenida | `deploy-qa` o `deploy-prod` en rojo en GitHub | cualquier falla | Autor del cambio | 1) Leer el paso que falló. 2) Si fue el job en qa: es el chequeo funcionando, no un error del pipeline. 3) Nunca desplegar prod a mano | Líder técnico de analítica |

## Volver atrás

| Qué | Cómo | Tiempo |
|---|---|---|
| El modelo | `@champion` → la versión de `@anterior` (Catalog Explorer → el modelo → Aliases, o `set_registered_model_alias`) | 1 minuto; la siguiente corrida ya usa la versión anterior |
| El código | volver a desplegar el tag anterior: GitHub → Actions → la corrida del tag `vX` → *Re-run all jobs* | lo que tarde el pipeline |
| Los datos | Delta time travel: `RESTORE TABLE … TO VERSION AS OF n` en la tabla afectada | minutos |

## Contactos

| Rol | Quién |
|---|---|
| Guardia de analítica | rotación semanal del equipo de analítica |
| Líder técnico de analítica | responsable del producto xm-demanda |
| Planeación (consumidor) | contacto de planeación de la operación |
| Plataforma Databricks | administración del workspace |
