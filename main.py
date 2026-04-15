from fastapi import FastAPI
import subprocess
import uuid
import os
from typing import Dict, Any

app = FastAPI()

# Lưu process theo job_id để có thể poll trạng thái
JOB_REGISTRY: Dict[str, Dict[str, Any]] = {}


# =========================================================
# HELPERS
# =========================================================
def submit_spark_job(script_path: str) -> dict:
    job_id = str(uuid.uuid4())[:8]

    log_out = f"/tmp/spark_{job_id}.out"
    log_err = f"/tmp/spark_{job_id}.err"

    # đảm bảo file tồn tại
    open(log_out, "w").close()
    open(log_err, "w").close()

    stdout_f = open(log_out, "w")
    stderr_f = open(log_err, "w")

    process = subprocess.Popen(
        [
            "spark-submit",
            "--master", "spark://gr2-spark-master:7077",
            script_path,
        ],
        stdout=stdout_f,
        stderr=stderr_f,
        cwd="/app",
    )

    JOB_REGISTRY[job_id] = {
        "process": process,
        "log_out": log_out,
        "log_err": log_err,
        "script_path": script_path,
        "pid": process.pid,
    }

    return {
        "status": "submitted",
        "job_id": job_id,
        "pid": process.pid,
        "log_out": log_out,
        "log_err": log_err,
        "script_path": script_path,
    }


def read_tail(path: str, max_chars: int = 4000) -> str:
    if not os.path.exists(path):
        return ""

    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()[-max_chars:]


def get_job_status_payload(job_id: str) -> dict:
    job = JOB_REGISTRY.get(job_id)

    if job is None:
        return {
            "job_id": job_id,
            "status": "unknown",
            "return_code": None,
            "stdout_tail": "",
            "stderr_tail": "",
        }

    process = job["process"]
    return_code = process.poll()

    if return_code is None:
        status = "running"
    elif return_code == 0:
        status = "success"
    else:
        status = "failed"

    stdout_tail = read_tail(job["log_out"])
    stderr_tail = read_tail(job["log_err"])

    return {
        "job_id": job_id,
        "status": status,
        "return_code": return_code,
        "pid": job["pid"],
        "script_path": job["script_path"],
        "log_out": job["log_out"],
        "log_err": job["log_err"],
        "stdout_tail": stdout_tail,
        "stderr_tail": stderr_tail,
    }


# =========================================================
# ROUTES
# =========================================================
@app.get("/")
def root():
    return {"status": "ok"}


@app.get("/run-job")
def run_spark_job():
    try:
        return submit_spark_job("/app/jobs/test.py")
    except Exception as e:
        return {"error": str(e)}


@app.get("/run-job/etl/bronze-to-silver")
def run_bronze_to_silver_job():
    try:
        return submit_spark_job("/app/jobs/bronze_to_silver/bronze_to_silver_job.py")
    except Exception as e:
        return {"error": str(e)}


@app.get("/run-job/etl/silver-to-gold")
def run_silver_to_gold_job():
    try:
        return submit_spark_job("/app/jobs/silver_to_gold/silver_to_gold_job.py")
    except Exception as e:
        return {"error": str(e)}


@app.get("/run-job/quality/bronze-quality")
def run_bronze_quality_job():
    try:
        return submit_spark_job("/app/jobs/data_quality/bronze_quality.py")
    except Exception as e:
        return {"error": str(e)}


@app.get("/run-job/quality/silver-quality")
def run_silver_quality_job():
    try:
        return submit_spark_job("/app/jobs/data_quality/silver_quality.py")
    except Exception as e:
        return {"error": str(e)}


@app.get("/job/{job_id}")
def get_job_log(job_id: str):
    try:
        return get_job_status_payload(job_id)
    except Exception as e:
        return {"error": str(e)}