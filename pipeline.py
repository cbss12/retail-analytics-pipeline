# pipeline.py
# Pipeline completo con Prefect 3
# Proyecto Final Integrador — Retail Analytics
# Fuentes: Mexico Toy Sales + Global Electronics Retailer
# Stack: Airbyte Cloud → MotherDuck → dbt → Metabase

import os
import time
import requests
import subprocess
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger
from prefect_shell import ShellOperation

# Cargar variables de entorno desde pipeline.env
load_dotenv("pipeline.env")

DBT_PROJECT_DIR  = os.getenv("DBT_PROJECT_DIR")
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")
MOTHERDUCK_TOKEN = os.getenv("MOTHERDUCK_TOKEN")
AIRBYTE_API_KEY  = os.getenv("AIRBYTE_API_KEY")   # Requiere plan Team/Enterprise

# Connection IDs de Airbyte Cloud
# Workspace: cf5345d8-9289-4d33-ab8c-663ef4c159ec
AIRBYTE_CONNECTION_IDS = [
    "afefe891-6884-4adc-b4f6-6743805e6791",  # raw_products_mexico_toys
    "92430a1b-4de7-49a0-b994-24aef642ca66",  # raw_sales_mexico_toys
    "02104334-7d0c-4806-9422-442070a7eb6b",  # raw_customers_global_electronics
    "5f356dda-96b9-4f5c-af36-baabc1ad85b2",  # raw_stores_global_electronics
    "8dad5558-9c5c-4ce3-89b5-60ec2e0c7b59",  # raw_sales_global_electronics
    "350d9088-634f-42d3-bce4-c9e02c8b261a",  # raw_products_global_electronics
    "a14c55ec-10a0-408e-8e74-4411f1dd4161",  # raw_exchange_rates_global_electronics
    "709782d7-604f-4058-b252-4ef940ed8fa8",  # raw_stores_mexico_toys
    "d43c48b8-6e5b-4889-9b54-cf88bcea04c5",  # raw_inventory_mexico_toys
]

AIRBYTE_API_URL = "https://api.airbyte.com/v1"


# ── HELPERS ────────────────────────────────────────────────────────────────

def run_dbt_command(command: list[str]) -> str:
    """Ejecuta un comando dbt con las variables de entorno del proyecto."""
    logger = get_run_logger()

    full_command = [
        "dbt", *command,
        "--project-dir", DBT_PROJECT_DIR,
        "--profiles-dir", DBT_PROFILES_DIR,
    ]

    logger.info(f"Ejecutando: {' '.join(full_command)}")

    env = os.environ.copy()
    env["MOTHERDUCK_TOKEN"] = MOTHERDUCK_TOKEN

    result = subprocess.run(
        full_command,
        capture_output=True,
        text=True,
        env=env
    )

    if result.returncode != 0:
        logger.error(result.stdout)
        logger.error(result.stderr)
        raise Exception(f"Comando dbt falló: {' '.join(command)}")

    logger.info(result.stdout)
    return result.stdout


# ── TASKS AIRBYTE ──────────────────────────────────────────────────────────

@task(name="airbyte sync - mexico toys", retries=1)
def task_airbyte_sync_mexico_toys():
    """
    Triggerea la sincronización de las 4 conexiones de Mexico Toy Sales:
    - raw_products_mexico_toys
    - raw_sales_mexico_toys
    - raw_stores_mexico_toys
    - raw_inventory_mexico_toys

    NOTA: Requiere API Key de Airbyte Cloud (plan Team/Enterprise).
    En el plan Trial, las syncs se ejecutan manualmente desde la UI.
    El código muestra la implementación completa lista para usar
    cuando se disponga del API Key.
    """
    logger = get_run_logger()

    mx_connection_ids = [
        "afefe891-6884-4adc-b4f6-6743805e6791",  # raw_products_mexico_toys
        "92430a1b-4de7-49a0-b994-24aef642ca66",  # raw_sales_mexico_toys
        "709782d7-604f-4058-b252-4ef940ed8fa8",  # raw_stores_mexico_toys
        "d43c48b8-6e5b-4889-9b54-cf88bcea04c5",  # raw_inventory_mexico_toys
    ]

    if not AIRBYTE_API_KEY:
        # SIMULACIÓN: Plan Trial no expone API Keys
        # Con API Key, el código de abajo triggerearía las syncs automáticamente
        logger.warning("AIRBYTE_API_KEY no configurado (requiere plan Team/Enterprise)")
        logger.info("Simulando sync de Mexico Toys — en producción se triggerearían via API:")
        for conn_id in mx_connection_ids:
            logger.info(f"  POST {AIRBYTE_API_URL}/connections/{conn_id}/sync")
        logger.info("Sync de Mexico Toys: OK (simulado)")
        return {"status": "simulated", "connections": mx_connection_ids}

    # Implementación real con API Key
    headers = {
        "Authorization": f"Bearer {AIRBYTE_API_KEY}",
        "Content-Type": "application/json"
    }

    job_ids = []
    for conn_id in mx_connection_ids:
        response = requests.post(
            f"{AIRBYTE_API_URL}/connections/{conn_id}/sync",
            headers=headers
        )
        response.raise_for_status()
        job_id = response.json().get("jobId")
        job_ids.append(job_id)
        logger.info(f"Sync iniciada — connection: {conn_id} | job: {job_id}")

    # Esperar a que todas las syncs terminen
    _wait_for_airbyte_jobs(job_ids, headers, logger)
    return {"status": "completed", "jobs": job_ids}


@task(name="airbyte sync - global electronics", retries=1)
def task_airbyte_sync_global_electronics():
    """
    Triggerea la sincronización de las 5 conexiones de Global Electronics:
    - raw_customers_global_electronics
    - raw_stores_global_electronics
    - raw_sales_global_electronics
    - raw_products_global_electronics
    - raw_exchange_rates_global_electronics

    NOTA: Requiere API Key de Airbyte Cloud (plan Team/Enterprise).
    """
    logger = get_run_logger()

    gl_connection_ids = [
        "02104334-7d0c-4806-9422-442070a7eb6b",  # raw_customers_global_electronics
        "5f356dda-96b9-4f5c-af36-baabc1ad85b2",  # raw_stores_global_electronics
        "8dad5558-9c5c-4ce3-89b5-60ec2e0c7b59",  # raw_sales_global_electronics
        "350d9088-634f-42d3-bce4-c9e02c8b261a",  # raw_products_global_electronics
        "a14c55ec-10a0-408e-8e74-4411f1dd4161",  # raw_exchange_rates_global_electronics
    ]

    if not AIRBYTE_API_KEY:
        logger.warning("AIRBYTE_API_KEY no configurado (requiere plan Team/Enterprise)")
        logger.info("Simulando sync de Global Electronics — en producción se triggerearían via API:")
        for conn_id in gl_connection_ids:
            logger.info(f"  POST {AIRBYTE_API_URL}/connections/{conn_id}/sync")
        logger.info("Sync de Global Electronics: OK (simulado)")
        return {"status": "simulated", "connections": gl_connection_ids}

    # Implementación real con API Key
    headers = {
        "Authorization": f"Bearer {AIRBYTE_API_KEY}",
        "Content-Type": "application/json"
    }

    job_ids = []
    for conn_id in gl_connection_ids:
        response = requests.post(
            f"{AIRBYTE_API_URL}/connections/{conn_id}/sync",
            headers=headers
        )
        response.raise_for_status()
        job_id = response.json().get("jobId")
        job_ids.append(job_id)
        logger.info(f"Sync iniciada — connection: {conn_id} | job: {job_id}")

    _wait_for_airbyte_jobs(job_ids, headers, logger)
    return {"status": "completed", "jobs": job_ids}


def _wait_for_airbyte_jobs(job_ids: list, headers: dict, logger, timeout: int = 600):
    """Espera a que todos los jobs de Airbyte terminen exitosamente."""
    start = time.time()
    pending = set(job_ids)

    while pending:
        if time.time() - start > timeout:
            raise Exception(f"Timeout esperando Airbyte jobs: {pending}")

        for job_id in list(pending):
            response = requests.get(
                f"{AIRBYTE_API_URL}/jobs/{job_id}",
                headers=headers
            )
            response.raise_for_status()
            status = response.json().get("status")

            if status == "succeeded":
                logger.info(f"Job {job_id} completado exitosamente")
                pending.remove(job_id)
            elif status in ("failed", "cancelled"):
                raise Exception(f"Airbyte job {job_id} falló con status: {status}")
            else:
                logger.info(f"Job {job_id} en progreso... status: {status}")

        if pending:
            time.sleep(15)


# ── TASKS DBT ──────────────────────────────────────────────────────────────

@task(name="dbt deps", retries=1)
def task_dbt_deps():
    """Instala los paquetes dbt: dbt-expectations y dbt_utils."""
    return run_dbt_command(["deps"])


@task(name="dbt run - staging mexico_toys", retries=1)
def task_dbt_run_staging_mx():
    """
    Limpia y estandariza los datos crudos de Mexico Toy Sales.
    Modelos: stg_mexico_toys__sales, products, stores, inventory.
    Aplica: snake_case, casteo de tipos, limpieza de $ y comas en precios.
    """
    return run_dbt_command(["run", "--select", "path:models/staging/mexico_toys"])


@task(name="dbt run - staging global_electronics", retries=1)
def task_dbt_run_staging_gl():
    """
    Limpia y estandariza los datos crudos de Global Electronics Retailer.
    Modelos: stg_global_electronics__sales, products, stores, customers, exchange_rates.
    Aplica: snake_case, casteo de tipos, limpieza de $ y comas en precios.
    """
    return run_dbt_command(["run", "--select", "path:models/staging/global_electronics"])


@task(name="dbt run - marts dimensions", retries=1)
def task_dbt_run_dimensions():
    """
    Construye las 4 dimensiones del modelo Kimball.
    - dim_date     : calendario completo 2015-2023
    - dim_product  : productos unificados MX + GL (clave MX-{id} / GL-{id})
    - dim_store    : tiendas unificadas MX + GL
    - dim_customer : clientes de Global Electronics
    """
    return run_dbt_command(["run", "--select", "path:models/marts/dimensions"])


@task(name="dbt run - marts facts", retries=1)
def task_dbt_run_facts():
    """
    Construye la fact table central del modelo Kimball.
    - fact_sales: unifica 892K transacciones de ambas fuentes.
      Normaliza revenue a USD usando exchange_rates de Global Electronics.
      Calcula total_revenue_usd, total_cost_usd y total_profit_usd.
    """
    return run_dbt_command(["run", "--select", "path:models/marts/facts"])


@task(name="dbt test - staging", retries=1)
def task_dbt_test_staging():
    """
    Ejecuta tests de calidad sobre los modelos staging.
    Incluye: not_null, unique, expect_column_values_to_be_between,
    expect_column_values_to_be_in_set.
    """
    return run_dbt_command(["test", "--select", "path:models/staging"])


@task(name="dbt test - marts", retries=1)
def task_dbt_test_marts():
    """
    Ejecuta tests de calidad sobre dimensiones y fact_sales.
    Incluye: not_null, unique, expect_table_row_count_to_be_between,
    expect_column_values_to_be_between, expect_column_values_to_be_in_set.
    Total esperado: PASS=48 WARN=0 ERROR=0.
    """
    return run_dbt_command(["test", "--select", "path:models/marts"])


@task(name="dbt docs generate")
def task_dbt_docs():
    """
    Genera la documentación interactiva y el lineage graph (DAG).
    Para visualizar: dbt docs serve → http://localhost:8080
    """
    return run_dbt_command(["docs", "generate"])


# ── FLOW ───────────────────────────────────────────────────────────────────

@flow(name="retail_analytics_pipeline", log_prints=True)
def pipeline_retail():
    """
    Pipeline completo del Proyecto Final Integrador — Retail Analytics.

    Fuentes de datos:
    - Mexico Toy Sales       : 829K registros | raw_mexico_toys
    - Global Electronics     : 62K registros  | raw_global_electronics

    Orden de ejecución:
    1. airbyte sync mx       → sincroniza 4 tablas de Mexico Toys
    2. airbyte sync gl       → sincroniza 5 tablas de Global Electronics
    3. dbt deps              → instala dbt-expectations y dbt_utils
    4. staging mexico_toys   → limpieza fuente 1 (4 modelos)
    5. staging global_elec   → limpieza fuente 2 (5 modelos)
    6. marts dimensions      → dim_date, dim_product, dim_store, dim_customer
    7. marts facts           → fact_sales unificada en USD (892K registros)
    8. test staging          → validación de calidad en staging
    9. test marts            → validación de calidad en marts (48 tests)
    10. docs generate        → documentación y DAG interactivo

    Base de datos: airbyte_curso (MotherDuck)
    Schemas de salida: main_staging | main_marts
    """
    logger = get_run_logger()
    logger.info("=" * 60)
    logger.info("Iniciando Retail Analytics Pipeline")
    logger.info("Proyecto: airbyte_curso | MotherDuck")
    logger.info("=" * 60)

    # 1. Sincronización Airbyte (ambas fuentes)
    task_airbyte_sync_mexico_toys()
    task_airbyte_sync_global_electronics()

    # 2. Instalar paquetes dbt
    task_dbt_deps()

    # 3. Staging — ambas fuentes
    task_dbt_run_staging_mx()
    task_dbt_run_staging_gl()

    # 4. Dimensiones (dependen de staging)
    task_dbt_run_dimensions()

    # 5. Fact table (depende de dimensiones y staging)
    task_dbt_run_facts()

    # 6. Tests de calidad
    task_dbt_test_staging()
    task_dbt_test_marts()

    # 7. Documentación
    task_dbt_docs()

    logger.info("=" * 60)
    logger.info("Pipeline completado exitosamente!")
    logger.info("Dashboard disponible en Metabase: http://localhost:3000")
    logger.info("Docs disponibles en: http://localhost:8080 (dbt docs serve)")
    logger.info("=" * 60)


if __name__ == "__main__":
    pipeline_retail()
