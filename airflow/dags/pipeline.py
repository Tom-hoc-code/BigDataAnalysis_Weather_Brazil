# =====================================
# Luong Ngoc Huy - 23133028
# =====================================
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from datetime import datetime
import requests
import time

BASE_URL = "http://gr2-spark-master:8000"


# ========================
# CALL JOB API + WAIT UNTIL DONE
# ========================
def trigger_job(api_endpoint, poll_interval=10, timeout=60 * 60, **context):
    submit_url = f"{BASE_URL}{api_endpoint}"

    response = requests.get(submit_url, timeout=30)
    try:
        res = response.json()
    except Exception:
        raise Exception(f"Invalid response from {submit_url}: {response.text}")

    if response.status_code != 200:
        raise Exception(f"HTTP {response.status_code} when calling {submit_url}: {res}")

    if "job_id" not in res:
        raise Exception(f"Failed to trigger job: {res}")

    job_id = res["job_id"]
    print(f"Submitted Spark job: {job_id}")
    print(f"log_out: {res.get('log_out')}")
    print(f"log_err: {res.get('log_err')}")

    status_url = f"{BASE_URL}/job/{job_id}"
    start_ts = time.time()

    last_stdout = ""
    last_stderr = ""

    while True:
        elapsed = time.time() - start_ts
        if elapsed > timeout:
            raise Exception(f"Job {job_id} timed out after {timeout} seconds")

        status_resp = requests.get(status_url, timeout=30)
        try:
            job = status_resp.json()
        except Exception:
            raise Exception(f"Invalid status response from {status_url}: {status_resp.text}")

        if status_resp.status_code != 200:
            raise Exception(f"HTTP {status_resp.status_code} when polling {status_url}: {job}")

        status = job.get("status")
        stdout_tail = job.get("stdout_tail", "") or ""
        stderr_tail = job.get("stderr_tail", "") or ""
        return_code = job.get("return_code")

        if stdout_tail != last_stdout:
            print("\n===== SPARK STDOUT =====")
            print(stdout_tail)
            last_stdout = stdout_tail

        if stderr_tail != last_stderr:
            print("\n===== SPARK STDERR =====")
            print(stderr_tail)
            last_stderr = stderr_tail

        print(f"Job {job_id} status: {status}, return_code: {return_code}")

        if status == "success":
            print(f"Spark job {job_id} finished successfully")
            return job

        if status == "failed":
            raise Exception(
                f"Spark job {job_id} failed with return_code={return_code}\n"
                f"STDERR:\n{stderr_tail}"
            )

        if status == "unknown":
            raise Exception(f"Spark job {job_id} is unknown on FastAPI server")

        time.sleep(poll_interval)


# ========================
# DAG
# ========================
with DAG(
    dag_id="etl_bronze_to_gold_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
) as dag:

    # 1. S3 SENSOR (trigger khi có file mới)
    # wait_for_s3 = S3KeySensor(
    #     task_id="wait_for_bronze_file",
    #     bucket_name="bronze",
    #     bucket_key="*.parquet",
    #     wildcard_match=True,
    #     aws_conn_id="aws_default",
    #     poke_interval=30,
    #     timeout=60 * 60,
    # )

    # 2. bronze -> silver
    run_bronze_to_silver = PythonOperator(
        task_id="run_bronze_to_silver",
        python_callable=trigger_job,
        op_kwargs={
            "api_endpoint": "/run-job/etl/bronze-to-silver",
            "poll_interval": 10,
            "timeout": 60 * 60,
        },
    )

    run_bronze_dq = PythonOperator(
        task_id="run_bronze_data_quality",
        python_callable=trigger_job,
        op_kwargs={
            "api_endpoint": "/run-job/quality/bronze-quality",
            "poll_interval": 10,
            "timeout": 60 * 60,
        },
    )

    # 3. silver -> gold
    run_silver_to_gold = PythonOperator(
        task_id="run_silver_to_gold",
        python_callable=trigger_job,
        op_kwargs={
            "api_endpoint": "/run-job/etl/silver-to-gold",
            "poll_interval": 10,
            "timeout": 60 * 60,
        },
    )

    run_silver_dq = PythonOperator(
        task_id="run_silver_data_quality",
        python_callable=trigger_job,
        op_kwargs={
            "api_endpoint": "/run-job/quality/silver-quality",
            "poll_interval": 10,
            "timeout": 60 * 60,
        },
    )

    # FLOW
    run_bronze_dq >> run_bronze_to_silver >> run_silver_dq >> run_silver_to_gold