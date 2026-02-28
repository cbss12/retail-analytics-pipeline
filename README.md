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
| **Airbyte Cloud** | Latest | Extracción y carga de datos (EL) |
| **MotherDuck** | Cloud | Base de datos analítica (DuckDB Cloud) |
| **dbt Core** | 1.11.6 | Transformación, testing y documentación |
| **dbt-duckdb** | 1.10.1 | Adaptador dbt para MotherDuck |
| **dbt-expectations** | >=0.10.0 | Tests de calidad de datos |
| **Prefect** | 3.6.20 | Orquestación del pipeline |
| **Metabase** | 0.52.9 | Dashboard de Business Intelligence |

---

## 📦 Fuentes de Datos

### Fuente 1 — Mexico Toy Sales
- **Origen:** [Maven Analytics](https://maven-datasets.s3.amazonaws.com/Maven+Toys/Maven+Toys.zip)
- **Tablas:** `sales`, `products`, `stores`, `inventory`
- **Registros:** 829,262 transacciones
- **Moneda:** USD

### Fuente 2 — Global Electronics Retailer
- **Origen:** [Maven Analytics](https://maven-datasets.s3.amazonaws.com/Global+Electronics+Retailer/Global+Electronics+Retailer.zip)
- **Tablas:** `sales`, `customers`, `products`, `stores`, `exchange_rates`
- **Registros:** 62,884 transacciones
- **Moneda:** Múltiples (normalizado a USD via exchange_rates)

---

## 🏗 Arquitectura

```
┌─────────────────┐     ┌──────────────┐     ┌─────────────┐     ┌───────────┐
│  Airbyte Cloud  │────▶│  MotherDuck  │────▶│  dbt Core   │────▶│ Metabase  │
│  (EL - 2 fuentes)│    │  (DuckDB)    │     │  (Transform)│     │(Dashboard)│
└─────────────────┘     └──────────────┘     └─────────────┘     └───────────┘
                                                      │
                                              ┌───────▼───────┐
                                              │    Prefect    │
                                              │ (Orquestación)│
                                              └───────────────┘
```

### Capas dbt

```
airbyte_curso (MotherDuck)
├── raw_mexico_toys          ← Cargado por Airbyte
├── raw_global_electronics   ← Cargado por Airbyte
├── main_staging             ← Limpieza y estandarización (views)
├── main_intermediate        ← Transformaciones intermedias (views)
└── main_marts               ← Modelo dimensional final (tables)
```

---

## 📁 Estructura del Proyecto

```
retail_project/
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
├── pipeline_flow.py         ← Orquestación Prefect
├── packages.yml
├── dbt_project.yml
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
- Cuenta en MotherDuck
- Cuenta en Airbyte Cloud
- Java 21+ (para Metabase JAR)

### 1. Clonar el proyecto
```bash
cd C:\Users\HP VICTUS\
# El proyecto ya está en proyecto_final/
```

### 2. Configurar profiles.yml
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

### 3. Setear variable de entorno
```cmd
setx MOTHERDUCK_TOKEN "tu_token_aqui"
```

### 4. Instalar dependencias dbt
```cmd
cd proyecto_final
dbt deps
```

### 5. Instalar Prefect
```cmd
python -m pip install prefect
```

---

## 🚀 Ejecución

### Pipeline completo con Prefect
```cmd
cd C:\Users\HP VICTUS\proyecto_final
python pipeline_flow.py
```

### Ejecución manual con dbt
```cmd
# Solo staging
dbt run --select staging

# Solo marts
dbt run --select marts

# Todo
dbt run

# Tests
dbt test
```

### Documentación interactiva
```cmd
dbt docs generate
dbt docs serve
# Abre http://localhost:8080
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

El archivo `pipeline_flow.py` define el flow `retail_analytics_pipeline` con 2 tasks secuenciales:

```
dbt_run (retries=1) → dbt_test (retries=1)
```

**Resultado:** Flow run completado en ~2 minutos — Finished in state `Completed()`

---

## 📊 Dashboard

Dashboard en Metabase con **4 KPIs** y **6 visualizaciones**:

### KPIs
| KPI | Valor |
|-----|-------|
| Revenue Total USD | $71,975,910 |
| Total Transacciones | 892,146 |
| Ganancia Total USD | ~$40M |
| Margen Promedio | ~55% |

### Visualizaciones
1. 📈 Ventas totales por mes (línea)
2. 🏆 Top 10 productos por revenue (barras)
3. 🌍 Revenue por país (barras)
4. 💰 Margen de ganancia por categoría (barras)
5. 📦 Inventario vs ventas por producto (barras agrupadas)
6. 🔀 Revenue por fuente de datos (barras)

### Iniciar Metabase
```cmd
cd C:\metabase
java -jar metabase.jar
# Abre http://localhost:3000
```

---

## 📈 Hallazgos Principales

- **Global Electronics** genera ~$57.5M USD vs ~$14.4M de Mexico Toys, con 13x menos transacciones — tickets promedio mucho más altos en electrónica.
- El **margen de ganancia promedio supera el 55%** en ambas fuentes.
- Mexico Toys mantiene **buena cobertura de inventario** para sus productos más vendidos.
