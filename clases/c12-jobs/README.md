# Clase 12 — Orquestación con Lakeflow Jobs: el pipeline diario que se cuida solo

Las clases 4 a 11 se convierten en un solo job: bronce → calidad → plata → oro → inferencia → chequeo, definido como código, con parámetros, una condición sobre un *task value*, política de fallas por tarea, alertas y *Repair run*. Abre el hito 6.

| Bloque | Duración | Material |
|---|---|---|
| Ordenar el lakehouse | 20 min | `slides/` 3–4: catálogo, esquema y objeto; cada consumidor, su capa; qué funciona en la industria. El caso del grupo se presenta en clase |
| Genie en vivo | 10 min | slide 5 (pendiente de la clase 11) |
| **Receso** | 10 min | slide 6 |
| Lakeflow Jobs | 15 min | `slides/` 7–12 |
| Notebook guiado + práctica | 60 min | `notebooks/lab12_orquestacion_jobs.py`, `notebooks/tareas/`, `src/xm_demanda/orquestacion/` |
| Cierre | 5 min | tag `s12`, "así sería en Azure", próxima clase |

**Free Edition:** máximo 5 tareas simultáneas por cuenta, compartidas por el grupo. El job tiene la cola activada: si una corrida queda en `QUEUED`, espera su turno.

El detalle de teoría, práctica, contexto del sector y entregable está en el [README del curso](../../README.md#detalle-por-clase).
