# Lecturas — Clase 12

## Ordenar el lakehouse

| Tema | Referencia |
|---|---|
| Unity Catalog: buenas prácticas (catálogos por ambiente o unidad de negocio, grupos dueños, service principals en producción) | https://docs.databricks.com/aws/en/data-governance/unity-catalog/best-practices |
| MLOps Stacks: estructura de proyecto y "deploy code, not models" | https://docs.databricks.com/aws/en/machine-learning/mlops/mlops-stacks |
| Lakebase vs. tablas Delta para apps que escriben | https://developers.databricks.com/perspectives/when-should-a-team-use-lakebase-instead-of-delta-tables-for-a-databricks-application |
| Z. Dehghani, *How to Move Beyond a Monolithic Data Lake to a Distributed Data Mesh* (datos como producto) | https://martinfowler.com/articles/data-monolith-to-mesh.html |

## Lakeflow Jobs

| Tema | Referencia |
|---|---|
| Lakeflow Jobs: conceptos y creación | https://docs.databricks.com/aws/en/jobs/ |
| Control de flujo: Run if, If/else, For each, reintentos, task values | https://docs.databricks.com/aws/en/jobs/control-flow |
| Parámetros del job y valores dinámicos (`{{job.start_time.iso_date}}`) | https://docs.databricks.com/aws/en/jobs/dynamic-value-references |
| Disparador por llegada de archivo | https://docs.databricks.com/aws/en/jobs/file-arrival-triggers |
| Reparar una corrida (Repair run) | https://docs.databricks.com/aws/en/jobs/repair-job-failures |
| Jobs API 2.2 (create, reset, run-now, runs/repair) | https://docs.databricks.com/api/workspace/jobs |

## Genie

| Tema | Referencia |
|---|---|
| Espacios de Genie: instrucciones y activos confiables | https://docs.databricks.com/aws/en/genie/ · https://docs.databricks.com/aws/en/genie/trusted-assets |
