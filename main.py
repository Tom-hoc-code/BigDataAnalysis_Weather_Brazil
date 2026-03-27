from fastapi import FastAPI
import subprocess

app = FastAPI()

@app.get("/run-job")
def run_spark_job():
    try:
        result = subprocess.run(
            [
                "spark-submit",
                "--master", "spark://gr2-spark-master:7077",
                "app/jobs/test_spark.py"
            ],
            capture_output=True,
            text=True
        )

        return {
            "stdout": result.stdout,
            "stderr": result.stderr,
            "returncode": result.returncode
        }

    except Exception as e:
        return {"error": str(e)}