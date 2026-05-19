# Instructivo completo de montaje - Portfolio Web + Sales Commission Pipeline

**Desarrollador:** Martin Alvarez Martinez  
**Perfil:** Data Scientist · Data Engineer · Agentic Engineer  
**Stack principal:** Next.js 15 · Groq + LLaMA 3 · PySpark · Delta Lake · Trino · Vercel · GitHub Actions

---

## Prerrequisitos

- Node.js 18+

```bash
node -v
```

- Python 3.10+

```bash
python --version
```

- Git

```bash
git -v
```

- Docker Desktop para Trino local
- Java 11+ requerido por Spark

```bash
java -version
```

- VS Code como editor recomendado
- Cuentas necesarias:
  - GitHub
  - Vercel
  - Groq: `console.groq.com`, free tier
  - Databricks Community Edition: `community.cloud.databricks.com`, free

---

# Parte 1 - Portfolio Web

Objetivo: montar el portafolio en localhost, personalizar contenido, migrar el chatbot a Groq + LLaMA 3, subir a GitHub y hacer deploy en Vercel.

---

## Fase 1 - Levantar el proyecto localmente

### Paso 1 - Descargar y ubicar el proyecto

- Descarga la carpeta `martin-portfolio`.
- Muévela a tu directorio de trabajo, por ejemplo:

```text
C:\Users\Martin\Desktop\portfolio\martin-portfolio
```

### Paso 2 - Instalar dependencias y configurar entorno

Abre una terminal en la carpeta del proyecto y ejecuta:

```bash
cd martin-portfolio
bash setup.sh
```

El script crea `.env.local` automáticamente. Ponle un placeholder por ahora.

### Paso 3 - Configurar variables de entorno

Abre `.env.local` y agrega:

```env
GROQ_API_KEY=placeholder
# ANTHROPIC_API_KEY ya no se usa
```

Reemplazarás el placeholder en Fase 3 cuando tengas tu key de Groq.

### Paso 4 - Levantar el dev server

Ejecuta:

```bash
npm run dev
```

Abre en el navegador:

```text
http://localhost:3000
```

Deberías ver el portafolio con las 4 secciones navegables.

### Paso 5 - Verificar rutas de demos

Visita estas rutas para confirmar que funcionan:

```text
http://localhost:3000/demos/iris
http://localhost:3000/demos/abc
```

- `/demos/iris`: Iris Classifier con sliders.
- `/demos/abc`: ABC Analysis con tabla editable.

Si algo no carga, anota el error exacto de la consola para debuguearlo.

---

## Fase 2 - Personalizar el contenido

Todo el contenido vive en un solo archivo:

```text
src/lib/data.ts
```

### Paso 6 - Actualizar info personal

Abre `src/lib/data.ts` y edita el objeto `profile`:

```ts
github: 'https://github.com/TU-USUARIO-REAL',
linkedin: 'https://linkedin.com/in/TU-PERFIL-REAL',
email: 'tu@email.com',
url: 'martinramirez.dev',
```

### Paso 7 - Agregar proyectos reales

En el array `projects` de `data.ts`, reemplaza con tus proyectos reales.

Proyectos clave a incluir:

- **Valora Ops AI** - FastAPI · Supabase · Next.js · Groq · LLaMA 3 · Railway
- **Dawnly** - Rust · Tauri v2 · React · TypeScript
- **Betting Scout** - Python · Streamlit · scikit-learn · pandas
- **Demand Forecast** - Python · XGBoost · statsmodels · pandas
- **Sales Pipeline** - PySpark · Delta Lake · Trino · GitHub Actions

### Paso 8 - Actualizar la certificación

Cuando termines la AWS Cloud Practitioner, cambia en `data.ts`:

```ts
status: 'completed', // era 'in-progress'
date: 'Mayo 2025', // fecha real
// quita la línea de progress: 85
```

---

## Fase 3 - Migrar el chatbot a Groq + LLaMA 3

Este es el paso más importante: reemplaza Anthropic por tu stack propio.

### Paso 9 - Obtener tu Groq API key

- Ve a `console.groq.com` y crea una cuenta free tier.
- Genera una API key en la sección Keys.

### Paso 10 - Instalar el SDK de Groq

En la carpeta del proyecto ejecuta:

```bash
npm install groq-sdk
```

### Paso 11 - Reemplazar la API route

Abre `src/app/api/chat/route.ts` y reemplaza todo el contenido:

```ts
import Groq from 'groq-sdk'
import { NextRequest, NextResponse } from 'next/server'
import { SYSTEM_PROMPT } from '@/lib/data'

const groq = new Groq({ apiKey: process.env.GROQ_API_KEY })
const rateLimitMap = new Map()

function checkRateLimit(ip) {
  const now = Date.now()
  const entry = rateLimitMap.get(ip)

  if (!entry || now > entry.reset) {
    rateLimitMap.set(ip, { count: 1, reset: now + 3600000 })
    return true
  }

  if (entry.count >= 30) return false
  entry.count++
  return true
}

export async function POST(req) {
  const ip = req.headers.get('x-forwarded-for') ?? 'unknown'

  if (!checkRateLimit(ip)) {
    return NextResponse.json({ error: 'Rate limit' }, { status: 429 })
  }

  const { messages } = await req.json()

  const completion = await groq.chat.completions.create({
    model: 'llama-3.3-70b-versatile',
    max_tokens: 1024,
    temperature: 0.7,
    messages: [{ role: 'system', content: SYSTEM_PROMPT }, ...messages.slice(-20)]
  })

  const text = completion.choices[0]?.message?.content ?? 'No response.'
  return NextResponse.json({ text })
}
```

### Paso 12 - Actualizar `.env.local`

Reemplaza el placeholder con tu key real:

```env
GROQ_API_KEY=gsk_tu-key-real-aqui
# Borra ANTHROPIC_API_KEY si existe
```

### Paso 13 - Probar el chat

Reinicia el servidor y prueba las 4 sugerencias:

```bash
# Ctrl+C para detener, luego:
npm run dev
```

Verifica que responde sobre ti, trae clima en vivo, noticias y datos de API.

---

## Fase 4 - Subir a GitHub

### Paso 14 - Crear el repo en GitHub

Ve a `github.com/new` y configura:

- Name: `martin-portfolio`
- Visibility: Public
- No inicialices con README, ya tienes uno.

### Paso 15 - Conectar y hacer push

Ejecuta en la carpeta del proyecto:

```bash
git remote add origin https://github.com/TU-USUARIO/martin-portfolio.git
git branch -M main
git push -u origin main
```

Verifica en GitHub que `.env.local` no aparece. El `.gitignore` lo excluye.

---

## Fase 5 - Deploy en Vercel

### Paso 16 - Conectar Vercel a GitHub

Ve a `vercel.com/new` y sigue estos pasos:

1. Import Git Repository: selecciona `martin-portfolio`.
2. Framework preset: Next.js, Vercel lo detecta automáticamente.
3. Abre Environment Variables antes de hacer deploy.
4. Agrega: `GROQ_API_KEY = tu key real`.
5. Haz click en Deploy.

### Paso 17 - Configurar dominio opcional

Si tienes o compras `martinramirez.dev`:

- En Vercel: Settings → Domains → Add.
- Actualiza `profile.url` en `data.ts` con tu dominio real.

---

## Checklist - Parte 1: Portfolio Web

- [ ] `npm run dev` corre sin errores.
- [ ] `/demos/iris` funciona con sliders.
- [ ] `/demos/abc` funciona con tabla editable.
- [ ] `data.ts` tiene info real: social links, proyectos y certificación.
- [ ] `groq-sdk` instalado y `route.ts` reemplazado.
- [ ] Chat responde con LLaMA 3, no Anthropic.
- [ ] Chat trae clima, noticias y datos de API en vivo.
- [ ] Repo público en GitHub sin `.env.local`.
- [ ] Deploy exitoso en Vercel con `GROQ_API_KEY`.
- [ ] URL pública funcionando.

---

# Parte 2 - Sales Commission Data Pipeline

Proyecto standalone que demuestra PySpark + Delta Lake, Trino para federación multi-source, data quality, lineage y CI/CD con GitHub Actions.

Cubre prácticamente todo el stack de un Data Engineer senior.

**Tecnologías:** PySpark · Delta Lake · Trino · Docker · SQLite · Great Expectations · GitHub Actions · Databricks CE

---

## Arquitectura del pipeline

| Capa | Tecnología | Descripción |
|---|---|---|
| Source A | SQLite | Tabla `sales_transactions`, ventas diarias |
| Source B | PostgreSQL | Tabla `sales_reps`, cuotas, región, tier |
| Source C | CSV / Parquet | Tabla `products`, categoría, margen |
| Ingestion | PySpark | Lee las 3 fuentes y normaliza esquemas |
| Storage | Delta Lake | Bronze → Silver → Gold, medallion |
| Federation | Trino Docker | Query unificado multi-source con SQL |
| Quality | Great Expectations | Validaciones: nulls, rangos, duplicados |
| Output | Parquet + Excel | Reporte final de comisiones por rep |
| CI/CD | GitHub Actions | Lint, tests y pipeline run en cada push |

---

## Fase 6 - Estructura del repositorio y dataset

### Paso 18 - Crear el repo del pipeline

Este es un repo separado del portafolio web:

```bash
mkdir sales-commission-pipeline
cd sales-commission-pipeline
git init
```

### Paso 19 - Estructura de carpetas

Crea la siguiente estructura:

```text
sales-commission-pipeline/
├── data/
│   ├── raw/       # CSVs originales
│   └── delta/     # Delta Lake tables (gitignore)
├── src/
│   ├── generate_sources.py  # Crea SQLite + CSVs
│   ├── ingest.py            # PySpark ingestion
│   ├── transform.py         # Medallion layers
│   ├── quality.py           # Great Expectations
│   ├── trino_query.py       # Federated queries
│   └── report.py            # Excel output
├── tests/
│   └── test_pipeline.py
├── .github/workflows/pipeline.yml
├── docker-compose.yml       # Trino
├── architecture.png         # diagrama
└── README.md
```

### Paso 20 - Generar el dataset sintético

Crea `src/generate_sources.py`. Genera datos realistas de ventas:

```python
import sqlite3, pandas as pd, numpy as np
from datetime import datetime, timedelta
import random, os

random.seed(42)
np.random.seed(42)
os.makedirs("data/raw", exist_ok=True)

# Sales reps con cuotas y tiers
reps = pd.DataFrame({
    'rep_id': [f'REP{i:03d}' for i in range(1, 21)],
    'name': [f'Rep {i}' for i in range(1, 21)],
    'region': np.random.choice(['NORTE', 'SUR', 'CENTRO', 'OCCIDENTE'], 20),
    'tier': np.random.choice(['Junior', 'Mid', 'Senior'], 20, p=[0.4, 0.4, 0.2]),
    'quota': np.random.randint(50000, 200000, 20),
})

# Guardar en SQLite
conn = sqlite3.connect('data/raw/sales_reps.db')
reps.to_sql('sales_reps', conn, if_exists='replace', index=False)
conn.close()

# Transacciones: 2 años de historia
start = datetime(2023, 1, 1)
txns = []

for day in range(730):
    date = start + timedelta(days=day)
    n = random.randint(8, 25)

    for _ in range(n):
        txns.append({
            'txn_id': f'TXN{len(txns):06d}',
            'rep_id': random.choice(reps.rep_id.tolist()),
            'product_id': f'PROD{random.randint(1, 50):03d}',
            'amount': round(random.uniform(500, 15000), 2),
            'date': date.strftime('%Y-%m-%d'),
            'status': np.random.choice(['closed', 'pending'], p=[0.85, 0.15])
        })

pd.DataFrame(txns).to_csv('data/raw/transactions.csv', index=False)

# Products catalog
products = pd.DataFrame({
    'product_id': [f'PROD{i:03d}' for i in range(1, 51)],
    'name': [f'Product {i}' for i in range(1, 51)],
    'category': np.random.choice(['Software', 'Hardware', 'Services'], 50),
    'margin_pct': np.random.uniform(0.10, 0.45, 50).round(3),
})

products.to_parquet('data/raw/products.parquet', index=False)

print('Sources generated: SQLite + CSV + Parquet')
```

### Paso 21 - Ejecutar el generador

Instala dependencias y genera los datos:

```bash
pip install pandas numpy pyspark delta-spark great-expectations openpyxl
python src/generate_sources.py
```

Resultado esperado:

```text
data/raw/sales_reps.db
data/raw/transactions.csv
data/raw/products.parquet
```

---

## Fase 7 - Pipeline PySpark + Delta Lake Medallion

### Paso 22 - Crear `src/ingest.py` - Bronze layer

Lee las 3 fuentes y las aterriza en Delta Lake como Bronze:

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName('SalesCommission')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Bronze: transactions CSV
txns = (
    spark.read
    .option('header', True)
    .option('inferSchema', True)
    .csv('data/raw/transactions.csv')
)

txns.write.format('delta').mode('overwrite').save('data/delta/bronze/transactions')

# Bronze: sales_reps SQLite via JDBC
reps = (
    spark.read.format('jdbc')
    .option('url', 'jdbc:sqlite:data/raw/sales_reps.db')
    .option('dbtable', 'sales_reps')
    .option('driver', 'org.sqlite.JDBC')
    .load()
)

reps.write.format('delta').mode('overwrite').save('data/delta/bronze/sales_reps')

# Bronze: products Parquet
products = spark.read.parquet('data/raw/products.parquet')
products.write.format('delta').mode('overwrite').save('data/delta/bronze/products')

print(f'Bronze ingested: {txns.count()} txns, {reps.count()} reps')
```

### Paso 23 - Crear `src/transform.py` - Silver y Gold

Silver: limpieza y joins. Gold: comisiones calculadas por rep.

```python
from pyspark.sql import SparkSession, functions as F
from delta import configure_spark_with_delta_pip

builder = (
    SparkSession.builder
    .appName('SalesCommission')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

# Silver: join y limpieza
txns = spark.read.format('delta').load('data/delta/bronze/transactions')
reps = spark.read.format('delta').load('data/delta/bronze/sales_reps')
products = spark.read.format('delta').load('data/delta/bronze/products')

silver = (
    txns
    .filter(F.col('status') == 'closed')
    .join(reps, 'rep_id', 'left')
    .join(products, 'product_id', 'left')
    .withColumn(
        'commission_rate',
        F.when(F.col('tier') == 'Senior', 0.08)
         .when(F.col('tier') == 'Mid', 0.06)
         .otherwise(0.04)
    )
    .withColumn('commission_amt', F.col('amount') * F.col('commission_rate') * F.col('margin_pct'))
    .withColumn('ingested_at', F.current_timestamp())
)

silver.write.format('delta').mode('overwrite').save('data/delta/silver/sales_enriched')

# Gold: resumen por rep
gold = (
    silver.groupBy('rep_id', 'name', 'region', 'tier', 'quota')
    .agg(
        F.sum('amount').alias('total_sales'),
        F.sum('commission_amt').alias('total_commission'),
        F.count('txn_id').alias('num_transactions'),
        F.avg('margin_pct').alias('avg_margin')
    )
    .withColumn('quota_attainment', F.col('total_sales') / F.col('quota'))
)

gold.write.format('delta').mode('overwrite').save('data/delta/gold/commissions')

print('Silver y Gold listos')
```

---

## Fase 8 - Data Quality con Great Expectations

### Paso 24 - Crear `src/quality.py`

Valida la capa Silver antes de que llegue a Gold:

```python
import great_expectations as gx
import pandas as pd

# Leer Silver como pandas para Great Expectations
# Alternativa: usar SparkDFDataset
silver = pd.read_parquet('data/delta/silver/sales_enriched')

context = gx.get_context()
ds = context.sources.add_pandas('silver_source')
da = ds.add_dataframe_asset('sales_enriched_asset')
batch = da.build_batch_request(dataframe=silver)
suite = context.add_expectation_suite('sales_suite')

validator = context.get_validator(
    batch_request=batch,
    expectation_suite=suite
)

# Expectations
validator.expect_column_values_to_not_be_null('rep_id')
validator.expect_column_values_to_not_be_null('commission_amt')
validator.expect_column_values_to_be_between('commission_rate', min_value=0.04, max_value=0.10)
validator.expect_column_values_to_be_between('amount', min_value=0, max_value=100000)
validator.expect_column_values_to_be_in_set('tier', ['Junior', 'Mid', 'Senior'])
validator.expect_column_pair_values_a_to_be_greater_than_b('amount', 'commission_amt')

# Anomaly: comisiones outliers
validator.expect_column_stdev_to_be_between('commission_amt', min_value=0, max_value=5000)

results = validator.validate()

if not results['success']:
    print('QUALITY FAILED:', results['statistics'])
    raise SystemExit(1)

print('Quality checks passed:', results['statistics'])
```

---

## Fase 9 - Trino local con Docker

### Paso 25 - Crear `docker-compose.yml`

Levanta Trino para federar las fuentes con SQL estándar:

```yaml
version: "3.8"

services:
  trino:
    image: trinodb/trino:435
    ports:
      - "8080:8080"
    volumes:
      - ./trino/catalog:/etc/trino/catalog
      - ./data:/data
    environment:
      - JAVA_TOOL_OPTIONS=-Xmx2G
```

### Paso 26 - Crear catálogos de Trino

Crea la carpeta `trino/catalog/` con estos archivos.

#### `trino/catalog/delta.properties`

```properties
connector.name=delta
hive.metastore=file
hive.metastore.catalog.dir=file:///data/delta
```

#### `trino/catalog/sqlite.properties`

```properties
connector.name=jdbc
connection-url=jdbc:sqlite:/data/raw/sales_reps.db
connection-user=
connection-password=
```

### Paso 27 - Levantar Trino y hacer una query federada

Levanta el contenedor:

```bash
docker-compose up -d
```

Espera alrededor de 30 segundos y conecta:

```bash
docker exec -it trino
```

Query federada: Delta Lake + SQLite en un solo `SELECT`:

```sql
SELECT
  r.name,
  r.region,
  SUM(t.amount) AS total_sales,
  SUM(t.amount * 0.06) AS estimated_commission
FROM delta.silver.sales_enriched t
JOIN sqlite.default.sales_reps r ON t.rep_id = r.rep_id
GROUP BY r.name, r.region
ORDER BY total_sales DESC
LIMIT 10;
```

---

## Fase 10 - Reporte final y live demo web

### Paso 28 - Crear `src/report.py` - Excel output

Genera el reporte final de comisiones desde Gold:

```python
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
import pandas as pd

builder = (
    SparkSession.builder
    .appName('SalesCommission')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
)

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel('WARN')

gold = spark.read.format('delta').load('data/delta/gold/commissions')
df = gold.toPandas()

df['quota_attainment'] = (df['quota_attainment'] * 100).round(1)
df['total_sales'] = df['total_sales'].round(2)
df['total_commission'] = df['total_commission'].round(2)

# Guardar Excel con formato
writer = pd.ExcelWriter('data/commissions_report.xlsx', engine='openpyxl')
df.to_excel(writer, sheet_name='Commissions', index=False)

# Summary sheet
summary = df.groupby('region').agg({
    'total_sales': 'sum',
    'total_commission': 'sum',
    'num_transactions': 'sum'
}).reset_index()

summary.to_excel(writer, sheet_name='By Region', index=False)
writer.close()

print('Report saved: data/commissions_report.xlsx')
```

### Paso 29 - Integrar como live demo en el portafolio

El demo interactivo web muestra el pipeline end-to-end.

Crea:

```text
src/app/demos/pipeline/page.tsx
```

Debe incluir:

- Diagrama visual de arquitectura: Bronze → Silver → Gold.
- Tabla interactiva de comisiones por rep con filtros.
- Gráfica de quota attainment por región.
- Detección de anomalías: outliers marcados en rojo.
- Toggle de código Python para cada capa del pipeline.

Los datos del Gold layer se exportan como JSON estático y se sirven desde:

```text
/public/data/commissions.json
```

Generar JSON para el demo web:

```python
gold_df.to_json('public/data/commissions.json', orient='records', indent=2)
```

El demo web no necesita Spark en runtime. Usa los datos pre-calculados.

---

## Fase 11 - CI/CD con GitHub Actions

### Paso 30 - Crear `.github/workflows/pipeline.yml`

Automatiza lint, tests y pipeline run en cada push:

```yaml
name: Sales Commission Pipeline

on:
  push:
    branches: [main]
  pull_request:
    branches: [main]

jobs:
  pipeline:
    runs-on: ubuntu-latest

    steps:
      - uses: actions/checkout@v4

      - name: Set up Python
        uses: actions/setup-python@v4
        with:
          python-version: "3.11"

      - name: Set up Java required by Spark
        uses: actions/setup-java@v3
        with:
          java-version: "11"
          distribution: "temurin"

      - name: Install dependencies
        run: pip install -r requirements.txt

      - name: Generate sources
        run: python src/generate_sources.py

      - name: Run pipeline
        run: |
          python src/ingest.py
          python src/transform.py

      - name: Data quality checks
        run: python src/quality.py

      - name: Generate report
        run: python src/report.py

      - name: Run tests
        run: pytest tests/ -v

      - name: Upload report artifact
        uses: actions/upload-artifact@v3
        with:
          name: commissions-report
          path: data/commissions_report.xlsx
```

### Paso 31 - Crear `requirements.txt`

Lista de dependencias del proyecto:

```txt
pyspark==3.5.0
delta-spark==3.0.0
great-expectations==0.18.0
pandas==2.1.0
numpy==1.26.0
openpyxl==3.1.0
pytest==7.4.0
trino==0.327.0
```

### Paso 32 - Documentar el lineage en `README.md`

El `README.md` debe incluir:

1. Diagrama de arquitectura: `architecture.png`.
2. Data lineage: qué tabla viene de dónde.
3. Cómo correr el pipeline localmente.
4. Cómo levantar Trino con Docker.
5. Badge del GitHub Actions workflow.
6. Descripción de cada capa del medallion.

El diagrama puedes generarlo con draw.io o diagrams.net.

---

## Fase 12 - Databricks Community Edition opcional

### Paso 33 - Migrar el pipeline a Databricks CE

Databricks Community Edition es gratis y corre Spark real.

Pasos:

1. Ve a `community.cloud.databricks.com` y crea cuenta.
2. Crea un cluster con Databricks Runtime 13.3 LTS, incluye Delta Lake.
3. Importa los notebooks desde el repo.
4. Los notebooks son los mismos scripts `.py` adaptados a celdas.
5. Exporta los notebooks como HTML para mostrar en el portafolio.

Esto te da capturas reales de Spark jobs corriendo, muy visual para demos.

En Databricks, el código es idéntico pero sin `configure_spark`, porque `spark` ya está disponible como variable global:

```python
txns = spark.read.format('delta').load('/mnt/data/bronze/transactions')
txns.display()  # UI interactiva de resultados
```

---

## Checklist - Parte 2: Sales Commission Pipeline

- [ ] Repo `sales-commission-pipeline` en GitHub público.
- [ ] `generate_sources.py` genera SQLite + CSV + Parquet.
- [ ] `ingest.py` carga las 3 fuentes a Delta Lake Bronze.
- [ ] `transform.py` crea Silver, join + limpieza, y Gold, comisiones.
- [ ] `quality.py` pasa todas las validaciones de Great Expectations.
- [ ] `docker-compose.yml` levanta Trino sin errores.
- [ ] Query federada en Trino: Delta + SQLite en un `SELECT`.
- [ ] `report.py` genera Excel con hoja de comisiones y resumen.
- [ ] GitHub Actions corre el pipeline en cada push, badge verde.
- [ ] `README.md` con diagrama de arquitectura y lineage documentado.
- [ ] JSON estático generado para el demo web.
- [ ] Demo interactivo en `/demos/pipeline` del portafolio.
- [ ] Pipeline migrado a Databricks Community Edition, opcional.

---

## Lo que este proyecto demuestra

| Skill / Tecnología | Dónde aparece en el pipeline |
|---|---|
| Delta Lake / Iceberg | Bronze → Silver → Gold layers |
| PySpark | Ingestion, transforms, aggregations |
| Trino | Federated SQL multi-source |
| Multi-source integration | SQLite + CSV + Parquet unificados |
| Data quality | Great Expectations validations |
| Anomaly detection | Stdev checks, outlier flagging |
| Data lineage | README + diagrama de arquitectura |
| CI/CD | GitHub Actions workflow completo |
| Reporting | Excel output + JSON para demo web |

---

**Martín Ramírez · Data Scientist · Data Engineer · Agentic Engineer · Mexico City · Valora Consultoría de Datos**
