# Arquitectura de la solución

Pregunta de negocio: para cada combinación activa de operador de red, mercado de comercialización, tipo de mercado y sector CIIU, ¿cuál es la demanda de energía esperada para los próximos 7 días?

-> La demanda esperada corresponde a la serie acorde al tipo de día y al valor se la serie de cada agente por mercado de comercialización

Patrón de solución: **batch diario + machine learning**. Los datos llegan por archivo con retraso variable (5 a 205 días) y el pronóstico se necesita una vez al día. No hay caso para streaming.

## Capas y contratos

Llena una fila por tabla. Reemplaza cada `<…>`. El dueño es un rol de XM, no una persona.

| Capa | Tabla | Grano (qué es una fila) | Dueño | Frescura | Garantías |
|---|---|---|---|---|---|
| Bronce | `bronze_energia.demanda_raw` | un registro tal como llegó | Liquidacion SIC | Diariamente se tiene para el d - 5, el día 5 y 11 de cada mes se actualiza el mes anterior completo | que los datos llegan con calidad dado que provienen del datalake |
| Plata | `silver_energia.demanda_diaria` | Una serie por día | Analitica | Diariamente despues de bronce | pasó reglas de calidad |
| Plata | `silver_energia.dim_ciiu` | un registro por ciiu | Analitica | Diariamente despues de bronce | Pasó reglas de calidad |
| Oro | `gold_energia.features_demanda_diaria` | demanda totalizada por mercado y mercadocomercialziador | Analitica | diariamente despues de plata | paso reglas y totaliza la info de plata |
| Oro | `gold_energia.pronostico_demanda` | demanda y perdida pronosticada por dia, mercado y mercado de coemrcialziacion | Analitica | Semanalmente se pronostica los siguientes 7 días.  | listo para usar. |

## Llave de serie

`codigo_sic_agente`, `mercado_comercializacion`, `tipo_mercado`, `clasificacion_industrial`. 355 combinaciones activas.

## Decisiones de diseño derivadas de la exploración (lab 2)

| Hecho | Decisión | Dónde se implementa |
|---|---|---|
| Formato largo (2 filas por serie-día) | Deberia ser 0 en la variable que no llegue valor es decir en demanda_real_kwh o perdidas_kwh | Plata (clase 6) |
| Publicación con retraso variable y republicaciones | Cuando llegan los datos para una nueva fecha de publicación se sobreescriben los datos. Siempre se debe tomar la ultima fecha de publicacion para cada día. Brince debe sobreescribir los datos | Bronce (clase 4) / Plata (clase 6) |
| Series incompletas (4 de 355) | activa =false | Plata / features (clase 7) |
| Regulado vs. no regulado | Se podría realizar un modelo por tipo de mercado. Medidas de error como MAPE o MASE se deben ajustar bien. | Modelo (clase 9) — ver ADR-001 |
| Ceros (299 serie-días) | Si es comun que se presenten cero, principalmente si es en tipos de dias especificos. las reglas de calidad podrian ser que los valores sean negativos. | Reglas de calidad (clase 5) |
| Pérdidas ≤ demanda | Si las perdidas siempre deben ser menores a la demanda. Las perdidas tambien seria relevante pronosticarlas, asi sea de forma independiente. | Reglas de calidad (clase 5) |

## Diagrama

```
DemandaPerdidas.xlsx ──▶ [volumen raw] ──▶ bronze_energia.demanda_raw
                                                │
                                                ▼  (pipeline declarativo, reglas de calidad)
                                     silver_energia.demanda_diaria ◀── silver_energia.dim_ciiu
                                                │
                                                ▼  (job de features)
                                  gold_energia.features_demanda_diaria
                                                │
                                                ▼  (job de inferencia, modelo en UC)
                                     gold_energia.pronostico_demanda ──▶ tablero / app
```

En Free Edition todo vive en el catálogo `workspace`; desde la clase 3, en `dev`, `qa` y `prod`. En Azure Databricks cada capa se registra como external location sobre ADLS Gen2.

## ADRs relacionadas

- [ADR-001 — Granularidad y horizonte del pronóstico](adr/ADR-001-granularidad-horizonte.md)
