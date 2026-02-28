import subprocess
from prefect import flow, task
from prefect.logging import get_run_logger

DBT_PROJECT_PATH = r"C:\Users\HP VICTUS\proyecto_final"
DBT_EXECUTABLE = r"C:\Users\HP VICTUS\AppData\Local\Packages\PythonSoftwareFoundation.Python.3.11_qbz5n2kfra8p0\LocalCache\local-packages\Python311\Scripts\dbt.exe"

@task(name="dbt_run", retries=1)
def dbt_run():
    logger = get_run_logger()
    logger.info("Ejecutando dbt run...")
    result = subprocess.run(
        [DBT_EXECUTABLE, "run"],
        cwd=DBT_PROJECT_PATH,
        capture_output=True,
        text=True
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt run falló:\n{result.stderr}")
    logger.info("dbt run completado exitosamente.")
    return result.stdout

@task(name="dbt_test", retries=1)
def dbt_test():
    logger = get_run_logger()
    logger.info("Ejecutando dbt test...")
    result = subprocess.run(
        [DBT_EXECUTABLE, "test"],
        cwd=DBT_PROJECT_PATH,
        capture_output=True,
        text=True
    )
    logger.info(result.stdout)
    if result.returncode != 0:
        raise Exception(f"dbt test falló:\n{result.stderr}")
    logger.info("dbt test completado exitosamente.")
    return result.stdout

@flow(name="retail_analytics_pipeline", log_prints=True)
def retail_pipeline():
    logger = get_run_logger()
    logger.info("Iniciando pipeline Retail Analytics...")
    
    run_result = dbt_run()
    test_result = dbt_test()
    
    logger.info("Pipeline completado exitosamente!")
    return {"dbt_run": "OK", "dbt_test": "OK"}

if __name__ == "__main__":
    retail_pipeline()