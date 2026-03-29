from fastapi import FastAPI
import subprocess
import uuid
import os

app = FastAPI()


# Health check
@app.get("/")
def root():
    return {"status": "ok"}


# Run Spark job
@app.get("/run-job")
def run_spark_job():
    try:
        job_id = str(uuid.uuid4())[:8]

        log_out = f"/tmp/spark_{job_id}.out"
        log_err = f"/tmp/spark_{job_id}.err"

        # đảm bảo file tồn tại
        open(log_out, "w").close()
        open(log_err, "w").close()

        subprocess.Popen(
            [
                "spark-submit",
                "--master", "spark://gr2-spark-master:7077",
                "/app/jobs/test.py"
            ],
            stdout=open(log_out, "w"),
            stderr=open(log_err, "w"),
            cwd="/app"
        )

        return {
            "status": "submitted",
            "job_id": job_id,
            "log_out": log_out,
            "log_err": log_err
        }

    except Exception as e:
        return {"error": str(e)}


# Check log của job
@app.get("/job/{job_id}")
def get_job_log(job_id: str):
    try:
        log_out = f"/tmp/spark_{job_id}.out"
        log_err = f"/tmp/spark_{job_id}.err"

        out = ""
        err = ""

        if os.path.exists(log_out):
            with open(log_out, "r") as f:
                out = f.read()[-2000:]

        if os.path.exists(log_err):
            with open(log_err, "r") as f:
                err = f.read()[-2000:]

        return {
            "job_id": job_id,
            "stdout_tail": out,
            "stderr_tail": err
        }

    except Exception as e:
        return {"error": str(e)}