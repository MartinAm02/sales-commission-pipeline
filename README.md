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
