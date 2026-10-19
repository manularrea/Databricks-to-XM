# ADR-001: Granularidad y horizonte del pronóstico

- **Estado:** aceptada
- **Fecha:** 2026-09-1
- **Autor:** Juan Cuartas

## Contexto

Para el pronóstico de la demanda, los dias que se tienen de datos, realmente son pocos, considero que se deberian tomar una serie de datos mayor para el pronóstico. El dato llega a XM reportado por los agentes del mercado cuando se presenta alguno de los consumos, por ese motivo es normal que para algunos días esos valores sean Cero. Para el curso se podría explorar la mejor granularidad para realziar el pronostico, es decir por agente, por Codigo de clasificacion o por total de demanda regulada o no regulada.

## Decisión

La granularidad debe ser diaria, dado que es lo minimo en que tenemos los datos, aunque se podría realizar horario.
Horizonte: deberia tenerse un periodo mayor de historia.
el horizonte del pronostico deberia ser de una semana.
el alcance del modelo deberia ser por mercado de comercializacion y agente operador y para cada día.

## Alternativas consideradas

- Se descarta la opcion de hacer el pronostico por codigo de clasificacion, dado que a este nivel puede generarse ruido por los ceros.
- Se descarta la opcion de eliminar los registros que no lleguen, dado que para esos casos se debe considerar como cero.

## Consecuencias

Se gana que la serie de demanda a nivel de mercado, es una serie temporal con una estacionalidad, lo cual permite pronosticar la serie con mayor precisión.
