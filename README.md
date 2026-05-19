# Sales Commission Pipeline

## Objetivo

Proyecto standalone de data engineering para simular un pipeline de comisiones comerciales con fuentes sintéticas, futuras capas medallion en Delta Lake, consultas federadas con Trino, validaciones de calidad y reportes finales.

## Arquitectura futura resumida

El pipeline integrará tres fuentes: representantes comerciales en SQLite, transacciones en CSV y catálogo de productos en Parquet. En fases posteriores, PySpark ingestará estas fuentes hacia Delta Lake en capas Bronze, Silver y Gold. Great Expectations validará la calidad de datos, Trino permitirá consultas federadas y el reporte final se exportará a Excel.

## Cómo generar fuentes sintéticas

```powershell
python -m venv .venv
.venv\Scripts\python -m pip install -r requirements.txt
.venv\Scripts\python src/generate_sources.py
```

## Archivos generados por `generate_sources.py`

- `data/raw/sales_reps.db`: base SQLite con la tabla `sales_reps`.
- `data/raw/transactions.csv`: dos años de transacciones sintéticas.
- `data/raw/products.parquet`: catálogo de productos con categoría y margen.

## Como generar el reporte y JSON

Despues de correr ingestion, transform y quality, genera los artefactos finales:

```powershell
$env:JAVA_HOME='C:\Program Files\Eclipse Adoptium\jdk-17.0.19.10-hotspot'
$env:HADOOP_HOME='C:\hadoop'
$env:Path="$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:Path"
.venv\Scripts\python src\report.py
```

El script lee `data/delta/gold/commissions` con Spark + Delta y genera:

- `data/commissions_report.xlsx`: reporte Excel con hojas `Commissions` y `By Region`.
- `exports/commissions.json`: JSON orientado a records, indentado y listo para frontend.

## Running Trino in GitHub Codespaces

Fase 9A valida Trino en Codespaces con un catalogo `memory` minimo. No usa todavia Delta catalog ni SQLite catalog.

1. Abre el repositorio en GitHub Codespaces.
2. Espera a que el devcontainer termine de crear el entorno. Instala Python dependencies, Docker-in-Docker y Java 17.
3. Levanta Trino:

```bash
bash scripts/setup_trino.sh
```

4. Ejecuta el healthcheck desde Python:

```bash
python src/trino_query.py
```

5. Verifica la UI/API de Trino en:

```text
http://localhost:8080
```

Resultado esperado de `SELECT 1`:

```text
|   result |
|----------|
|        1 |
```

El healthcheck guarda `exports/trino_healthcheck.json`.
