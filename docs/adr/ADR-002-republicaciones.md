# ADR-002: Republicaciones en bronce

- **Estado:** aceptada
- **Fecha:** 2026-09-29
- **Autor:** equipo del curso

## Contexto

XM publica el mismo día de dato más de una vez: la liquidación del mercado se emite en versiones sucesivas (preliminar, ajustes TX1–TX3, definitiva) a medida que llegan medidas definitivas y reclamaciones. En nuestros datos el retraso entre `Fecha` y `FechaPublicacion` va de 5 a 205 días, y el 25 de julio se publicaron 200 días de golpe. En el lab 4 llegó `raw_v2`: 7 días ya cargados, con valores corregidos (+1,5 %) y la columna `Valor` renombrada a `ValorKwh`. Bronce necesita una regla explícita para esto; sin ella, cada ingeniero resuelve distinto y auditoría no puede reconstruir qué se sabía cuándo.

## Decisión

1. Cuando llega un día que ya existe, **bronce acumula** (append-only). Nunca sobrescribe ni borra: la fila anterior sigue en la tabla con su `_source_file` y su `_ingested_at`.
2. La **llave de versión** de una fila es `FechaPublicacion` (guardada como `_publication_date`), no `_ingested_at`: la versión la define el negocio (cuándo XM publicó), no la operación (cuándo lo cargamos). `_ingested_at` solo desempata.
3. **"Vigente"** es la publicación más reciente por llave de serie + `Fecha` + `CodigoVariable`. Lo resuelve la vista `demanda_raw_vigente` (hoy) y la materializa plata con validaciones (clase 6). Bronce guarda; no decide.
4. Renombres conocidos de columnas se resuelven con el mapa `ALIAS` en `src/xm_demanda/ingest/bronze.py`, con esquema explícito y `schemaEvolutionMode = rescue`; nada se pierde y nada entra sin revisar.

## Alternativas consideradas

- **MERGE por llave en bronce (sobrescribir la versión anterior):** descartada. Destruye la evidencia de qué se publicó cuándo, que auditoría y la clase 15 (desempeño del modelo por versión) necesitan. MERGE queda para plata.
- **Usar `_ingested_at` como versión:** descartada. Si reprocesamos desde landing, todas las filas tendrían la misma `_ingested_at` y perderíamos el orden real de publicación. La versión debe venir del dato.
- **`schemaEvolutionMode = addNewColumns`:** descartada. Bronce acumularía `Valor` y `ValorKwh` con el mismo significado y plata tendría que adivinar. Con `rescue` + alias la decisión queda escrita y probada en `tests/unit/test_bronze.py`.
- **Borrar y recargar bronce cada vez que llega una republicación:** descartada. Funciona con 145 mil filas; no con medición de 15 minutos (CREG 101 001 de 2022).

## Consecuencias

- Bronce crece con cada republicación (≈ 3,4 % por cada ajuste de 7 días en el ejemplo); es el costo de tener historial.
- Toda lectura de bronce debe pasar por la vista vigente o por plata; leer bronce directo devuelve duplicados por diseño.
- Landing es la fuente de verdad: bronce se reconstruye con `DROP TABLE` + borrar checkpoint + ejecutar. El checkpoint es estado del sistema y entra en el runbook.
- Cada cambio de nombre en la fuente se registra en `ALIAS` y en un ADR corto; la ingesta no se toca.
