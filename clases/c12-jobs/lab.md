# Lab 12 — El pipeline diario como código, y tres corridas

**Duración:** 20 min (más el notebook guiado) · **Modalidad:** individual, en tu propio fork · **Rama:** `feature/c12-jobs`

## Objetivo

Crear el job `[xm-demanda] diario · <usuario>` desde un notebook, como código, con siete tareas (seis notebooks delgados y una condición), política de fallas por tarea y alerta por correo; correrlo en un día normal, provocar la falla silenciosa del archivo que no llega y repararla con *Repair run*. Abre el hito 6.

## Antes de empezar

| Requisito | Si no lo tienes |
|---|---|
| Labs 4 a 11 en tu esquema: bronce y `demanda_raw_vigente`, `demanda_validada`, `dim_ciiu`, `demanda_diaria` con `ciiu_seccion`, `features_demanda_diaria`, `demanda_7d` con `@champion`/`@challenger`, `pronostico_demanda` | Corre el lab que falte; el job usa tus tablas y tu código |
| Tus TODO de las clases 4 a 11 resueltos en tu rama | El job importa tu `src/`: si una función tiene `NotImplementedError`, su tarea falla |
| Git folder en `main` actualizado | GitHub: **Sync fork** → Databricks: Git → **Pull** |
| Rama nueva | Git → Create branch → `feature/c12-jobs` |

## Pasos

Abre `clases/c12-jobs/notebooks/lab12_orquestacion_jobs`. Widget `usuario`.

| Sección | Qué haces | Qué debes ver |
|---|---|---|
| 1–2 | Leer la anatomía del job y cuándo usar job, pipeline u orquestador externo | — |
| 3 TODO 1 | `politica()` en `src/xm_demanda/orquestacion/job.py` | El DAG impreso en orden, con 2 reintentos en `inferencia` y `ALL_DONE` en `chequeo`; `tests/unit/test_orquestacion.py` sin skip |
| 4 | Crear el job (idempotente) | Un `job_id` y su enlace. Correrla dos veces no crea dos jobs |
| 5 | Corrida 1: `fecha_proceso = 2026-07-26`, `forzar = si` | Las 7 tareas en `SUCCESS` (5–10 min; más si hay cola) |
| 6 | Corrida 2: `fecha_proceso = 2026-07-27`, `forzar = no` | `bronce` y la condición en verde, la rama `EXCLUDED`, `chequeo` en `FAILED` con el mensaje de atraso; correo de alerta |
| 7 | Llega el archivo tarde + *Repair run* | Segundo intento de la misma corrida, todo en verde; oro hasta el 26-jul y pronóstico hasta el 2-ago |
| 8 | Interfaz: Runs, Tasks, Schedules & Triggers, notificaciones, permisos | — |
| Preguntas | `ALL_DONE` vs. `ALL_SUCCESS`; calendario vs. llegada de archivo | — |
| `verificar()` | 7 comprobaciones en OK | Commit `feat(c12): job diario como código con política de fallas y chequeo de frescura` → push → PR → CI → merge |

La sección 7 agrega a tu landing un archivo simulado (`demanda_2026-07-27_tardio.csv`, copia del 19 de julio con fecha 26). Si quieres volver al estado anterior, bórralo del volumen y reconstruye bronce como en el paso 6 del lab 4.

## Criterios de aceptación

- [ ] `politica()` implementada; `tests/unit/test_orquestacion.py` sin skip
- [ ] Job con 7 tareas, condición `hay_datos_nuevos` y `chequeo` con `run_if = ALL_DONE`
- [ ] Una corrida exitosa y una corrida reparada (dos intentos)
- [ ] Oro hasta el 26-jul; pronóstico del champion hasta el 2-ago
- [ ] Las dos preguntas respondidas
- [ ] `verificar()` en OK; PR en verde

**Hito 6 abierto.**

## Comparar con la solución de referencia

`https://github.com/<docente>/Databricks-to-XM/compare/s12...<tu_usuario>:Databricks-to-XM:main`

```bash
git fetch upstream --tags
git diff s12 -- src/xm_demanda/orquestacion clases/c12-jobs/notebooks
```

## Extensión opcional

1. **Disparador por llegada de archivo.** En *Schedules & Triggers*, cambia el calendario por **File arrival** sobre `/Volumes/workspace/c01_<usuario>/raw/landing`. ¿Qué valor debería tomar `fecha_proceso`?
2. **Aviso por duración.** En *Job notifications*, agrega un aviso si el job supera 20 minutos. ¿Qué te diría ese aviso que la alerta por falla no te dice?
3. **For each.** Agrega una tarea que, después de `oro`, calcule el WAPE de la última semana por `tipo_mercado` con una tarea *For each* sobre `["Regulado", "No Regulado"]`.

## Para la clase 13

Instala la CLI de Databricks (`brew install databricks/tap/databricks` o el instalador de la [documentación](https://docs.databricks.com/aws/en/dev-tools/cli/install)) y autentícate con `databricks auth login --host <url de tu workspace>`. El job de hoy se convierte en `resources/job_diario.yml`.

## Así sería en Azure Databricks

| En Free Edition (hoy) | En Azure Databricks (XM) |
|---|---|
| Job creado desde un notebook con la API | `resources/*.yml` en un bundle, desplegado por CI/CD |
| Notebooks del Git folder personal | Código de un commit (Git source o bundle) |
| Corre como el usuario | *Run as* service principal; dueño del job: un grupo |
| Calendario pausado y fecha fija | Llegada de archivo o calendario; fecha `{{job.start_time.iso_date}}` |
| 5 tareas simultáneas por cuenta | Cómputo serverless o clusters de job, límites por workspace |
