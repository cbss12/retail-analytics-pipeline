# 🛒 Retail Analytics — Proyecto Final Integrador

Pipeline de datos end-to-end que integra dos fuentes de retail, aplica transformaciones con modelo dimensional Kimball y expone resultados en un dashboard de Business Intelligence.

---

## 📋 Tabla de Contenidos

- [Stack Tecnológico](#stack-tecnológico)
- [Fuentes de Datos](#fuentes-de-datos)
- [Arquitectura](#arquitectura)
- [Estructura del Proyecto](#estructura-del-proyecto)
- [Modelo Dimensional](#modelo-dimensional)
- [Setup e Instalación](#setup-e-instalación)
- [Ejecución](#ejecución)
- [Tests de Calidad](#tests-de-calidad)
- [Orquestación con Prefect](#orquestación-con-prefect)
- [Dashboard](#dashboard)

---

## 🛠 Stack Tecnológico

| Herramienta | Versión | Rol |
|-------------|---------|-----|
| **Airbyte Cloud** | Latest | Extracción y carga de datos (EL) — 9 conexiones |
| **MotherDuck** | Cloud | Base de datos analítica (DuckDB Cloud) |
| **dbt Core** | 1.11.6 | Transformación, testing y documentación |
| **dbt-duckdb** | 1.10.1 | Adaptador dbt para MotherDuck |
| **dbt-expectations** | 0.10.4 | Tests de calidad de datos |
| **dbt_utils** | 1.3.3 | Utilidades para macros y tests genéricos |
| **Prefect** | 3.6.20 | Orquestación del pipeline (10 tasks) |
| **prefect-shell** | 0.3.3 | Integración de comandos shell en Prefect |
| **Metabase** | 0.52.9 (JAR) | Dashboard de Business Intelligence |
| **DuckDB Metabase Driver** | Community | Plugin para conectar Metabase a MotherDuck |
| **Java** | 25.0.2 (Temurin) | Runtime requerido para Metabase JAR |

---

## 📦 Fuentes de Datos

### Fuente 1 — Mexico Toy Sales
- **Origen:** [Maven Analytics](https://maven-datasets.s3.amazonaws.com/Maven+Toys/Maven+Toys.zip)
- **Tablas:** `sales`, `products`, `stores`, `inventory`
- **Registros:** 829,262 transacciones
- **Moneda:** USD
- **Sync mode:** Manual (CSV estático)

### Fuente 2 — Global Electronics Retailer
- **Origen:** [Maven Analytics](https://maven-datasets.s3.amazonaws.com/Global+Electronics+Retailer/Global+Electronics+Retailer.zip)
- **Tablas:** `sales`, `customers`, `products`, `stores`, `exchange_rates`
- **Registros:** 62,884 transacciones
- **Moneda:** Múltiples (normalizado a USD via exchange_rates)
- **Sync mode:** Manual (CSV estático)

---

## 🏗 Arquitectura

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────┐     ┌───────────┐
│  Airbyte Cloud  │────▶│  MotherDuck  │────▶│  dbt Core   │────▶│ Metabase  │
│  9 conexiones   │     │  (DuckDB)    │     │  (Transform)│     │(Dashboard)│
└─────────────────┘     └──────────────┘     └─────────────┘     └───────────┘
         ▲                                           ▲
         │                                           │
         └──────────────┬────────────────────────────┘
                        │
                 ┌──────▼───────┐
                 │    Prefect   │
                 │  10 tasks    │
                 └──────────────┘
```

### Capas dbt

```
airbyte_curso (MotherDuck)
├── raw_mexico_toys          ← Cargado por Airbyte (4 tablas)
├── raw_global_electronics   ← Cargado por Airbyte (5 tablas)
├── main_staging             ← Limpieza y estandarización (views)
├── main_intermediate        ← Transformaciones intermedias (views)
└── main_marts               ← Modelo dimensional final (tables)
```

---

## 📁 Estructura del Proyecto

```
proyecto_final/
├── models/
│   ├── staging/
│   │   ├── mexico_toys/
│   │   │   ├── _sources_mx.yml
│   │   │   ├── _stg_mexico_toys.yml
│   │   │   ├── stg_mexico_toys__sales.sql
│   │   │   ├── stg_mexico_toys__products.sql
│   │   │   ├── stg_mexico_toys__stores.sql
│   │   │   └── stg_mexico_toys__inventory.sql
│   │   └── global_electronics/
│   │       ├── _sources_gl.yml
│   │       ├── _stg_global_electronics.yml
│   │       ├── stg_global_electronics__sales.sql
│   │       ├── stg_global_electronics__products.sql
│   │       ├── stg_global_electronics__stores.sql
│   │       ├── stg_global_electronics__customers.sql
│   │       └── stg_global_electronics__exchange_rates.sql
│   ├── intermediate/
│   └── marts/
│       ├── dimensions/
│       │   ├── dim_date.sql
│       │   ├── dim_product.sql
│       │   ├── dim_store.sql
│       │   └── dim_customer.sql
│       └── facts/
│           ├── _fact_sales.yml
│           └── fact_sales.sql
├── pipeline.py              ← Orquestación Prefect (10 tasks)
├── pipeline.env             ← Variables de entorno (no subir a GitHub)
├── packages.yml
├── dbt_project.yml
├── .gitignore
└── README.md
```

---

## ⭐ Modelo Dimensional

Esquema en estrella (Star Schema) con una fact table central y 4 dimensiones:

```
                    ┌─────────────┐
                    │  dim_date   │
                    └──────┬──────┘
                           │
┌──────────────┐    ┌──────▼──────┐    ┌───────────────┐
│  dim_store   ├────►  fact_sales  ◄────┤  dim_product  │
└──────────────┘    └──────┬──────┘    └───────────────┘
                           │
                    ┌──────▼──────┐
                    │ dim_customer│
                    └─────────────┘
```

### Tablas

| Tabla | Tipo | Registros | Descripción |
|-------|------|-----------|-------------|
| `fact_sales` | Fact | 892,146 | Ventas unificadas en USD de ambas fuentes |
| `dim_date` | Dimensión | 3,287 | Calendario completo 2015-2023 |
| `dim_product` | Dimensión | ~2,100 | Productos unificados (clave: MX-{id} / GL-{id}) |
| `dim_store` | Dimensión | ~80 | Tiendas de México y globales |
| `dim_customer` | Dimensión | 15,266 | Clientes de Global Electronics |

---

## ⚙️ Setup e Instalación

### Prerrequisitos
- Python 3.11+
- dbt-core 1.11.6 + dbt-duckdb 1.10.1
- Cuenta en MotherDuck con base de datos `airbyte_curso`
- Cuenta en Airbyte Cloud con datos sincronizados
- Java 21+ (para Metabase JAR)

### 1. Configurar profiles.yml
En `C:\Users\HP VICTUS\.dbt\profiles.yml`:

```yaml
retail_project:
  target: dev
  outputs:
    dev:
      type: duckdb
      path: "md:airbyte_curso"
      token: "{{ env_var('MOTHERDUCK_TOKEN') }}"
      schema: main_staging
      threads: 4
```

### 2. Crear pipeline.env
En la raíz del proyecto:

```env
DBT_PROJECT_DIR=C:\Users\HP VICTUS\proyecto_final
DBT_PROFILES_DIR=C:\Users\HP VICTUS\.dbt
MOTHERDUCK_TOKEN=tu_token_aqui
AIRBYTE_API_KEY=   # Opcional: requiere plan Team/Enterprise
```

### 3. Instalar dependencias dbt
```cmd
cd proyecto_final
dbt deps
```

### 4. Instalar dependencias Python
```cmd
python -m pip install prefect prefect-dbt prefect-shell python-dotenv
```

---

## 🚀 Ejecución

### Pipeline completo con Prefect (recomendado)
```cmd
cd C:\Users\HP VICTUS\proyecto_final
python pipeline.py
```

### Ejecución manual con dbt
```cmd
# Staging — fuente 1
dbt run --select path:models/staging/mexico_toys

# Staging — fuente 2
dbt run --select path:models/staging/global_electronics

# Dimensiones
dbt run --select path:models/marts/dimensions

# Fact table
dbt run --select path:models/marts/facts

# Tests
dbt test
```

### Documentación interactiva
```cmd
dbt docs generate
dbt docs serve
# Abre http://localhost:8080
```

### Levantar Metabase
```cmd
cd C:\metabase
java -jar metabase.jar
# Abre http://localhost:3000
```

---

## ✅ Tests de Calidad

48 tests implementados con `dbt-expectations` y `dbt_utils`:

| Tipo | Cantidad | Descripción |
|------|----------|-------------|
| `not_null` | 16 | Columnas clave sin valores nulos |
| `unique` | 12 | Unicidad en PKs |
| `expect_column_values_to_be_between` | 12 | Rangos válidos en precios, unidades y fechas |
| `expect_column_values_to_be_in_set` | 4 | Valores categóricos válidos |
| `expect_table_row_count_to_be_between` | 4 | Volumen mínimo/máximo de filas |
| **Total** | **48** | **PASS=48 WARN=0 ERROR=0** |

---

## 🔄 Orquestación con Prefect

El archivo `pipeline.py` define el flow `retail_analytics_pipeline` con **10 tasks secuenciales**:

```
airbyte_sync_mx → airbyte_sync_gl → dbt_deps →
dbt_run_staging_mx → dbt_run_staging_gl →
dbt_run_dimensions → dbt_run_facts →
dbt_test_staging → dbt_test_marts → dbt_docs_generate
```

| Task | Descripción |
|------|-------------|
| `airbyte sync - mexico toys` | Triggerea sync de 4 conexiones via API (simulado en plan Trial) |
| `airbyte sync - global electronics` | Triggerea sync de 5 conexiones via API (simulado en plan Trial) |
| `dbt deps` | Instala paquetes dbt-expectations y dbt_utils |
| `dbt run - staging mexico_toys` | Limpieza y tipado de 4 modelos staging MX |
| `dbt run - staging global_electronics` | Limpieza y tipado de 5 modelos staging GL |
| `dbt run - marts dimensions` | Construye dim_date, dim_product, dim_store, dim_customer |
| `dbt run - marts facts` | Construye fact_sales unificada en USD (892K registros) |
| `dbt test - staging` | Valida calidad de datos en staging |
| `dbt test - marts` | Valida calidad de datos en marts (48 tests) |
| `dbt docs generate` | Genera documentación y lineage graph |

> **Nota sobre Airbyte API:** La integración completa requiere API Key de Airbyte Cloud (plan Team/Enterprise). En el plan Trial las tasks de sync corren en modo simulado — el código está listo para activarse cuando se disponga del API Key.

**Resultado:** Flow run completado en ~2 minutos — Finished in state `Completed()`

---

## 📊 Dashboard

Dashboard en Metabase con **4 KPIs**, **6 visualizaciones** y **filtro interactivo de fecha**:

### KPIs
| KPI | Valor |
|-----|-------|
| Revenue Total USD | $72.0M |
| Total Transacciones | 892,146 |
| Ganancia Total USD | $37.7M |
| Margen Promedio Global | 52.4% |

### Visualizaciones
1. 📈 Ventas totales por mes (línea)
2. 🏆 Top 10 productos por revenue (barras)
3. 🌍 Revenue por país (barras)
4. 💰 Margen de ganancia por categoría (barras)
5. 📦 Inventario vs ventas por producto (barras agrupadas)
6. 🔀 Revenue por fuente de datos (barras)

### Filtros Interactivos
- **Fecha** — Field Filter tipo Date Range conectado a `fact_sales.sale_date`. Controla todas las visualizaciones simultáneamente.

### Iniciar Metabase
```cmd
cd C:\metabase
java -jar metabase.jar
# Abre http://localhost:3000
```

---

## 📈 Hallazgos Principales

- **Global Electronics** genera ~$57.5M USD vs ~$14.4M de Mexico Toys, con 13x menos transacciones — tickets promedio mucho más altos en electrónica.
- El **margen de ganancia promedio es 52.4%** en ambas fuentes combinadas.
- **EE.UU. es el mercado líder** con ~$22M en revenue, seguido por Mexico con ~$14M.
- Mexico Toys mantiene **buena cobertura de inventario** para sus productos más vendidos.

---

## 👥 Integrantes

Cinthia Segovia · Jessica Pizarro · Alan Amarilla · Rodrigo Franco

**Introducción a la Ingeniería de Datos — MIA 03 — Febrero 2026**
