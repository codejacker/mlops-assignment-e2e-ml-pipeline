import json
import os
from datetime import datetime, timezone
from pathlib import Path

import dotenv
from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = PROJECT_ROOT / "runs"

dotenv.load_dotenv(PROJECT_ROOT / ".env")

# DockerOperator talks to the HOST docker daemon (via the socket mounted into
# the airflow-scheduler container) and launches sibling containers. Those
# containers' bind mounts must reference HOST filesystem paths, which differ
# from PROJECT_ROOT when Airflow itself is running inside a container.
HOST_PROJECT_ROOT = os.environ.get("PIPELINE_HOST_PROJECT_ROOT", str(PROJECT_ROOT))
PIPELINE_IMAGE = os.environ.get("PIPELINE_IMAGE", "mlops-assignment-e2e-ml-pipeline-pipeline")


@dag(
    dag_id="evaluate_agent",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "split": Param("test", type="string"),
        "subset": Param("verified", type="string"),
        "workers": Param(5, type="integer"),
        "model": Param("nebius/moonshotai/Kimi-K2.6", type="string"),
        "task_slice": Param("0:3", type="string"),
        "run_id": Param("", type="string"),
        "cost_limit": Param(None, type=["null", "number"]),
    },
)
def evaluate_agent():
    @task
    def prepare_run(**context) -> dict:
        p = context["params"]
        run_id = p["run_id"] or datetime.now().strftime("run-%Y%m%d-%H%M%S")
        run_dir = RUNS_DIR / run_id

        (run_dir / "run-agent" / "trajectories").mkdir(parents=True, exist_ok=True)
        (run_dir / "run-eval" / "logs").mkdir(parents=True, exist_ok=True)
        (run_dir / "run-eval" / "reports").mkdir(parents=True, exist_ok=True)

        config = {
            "run_id": run_id,
            "split": p["split"],
            "subset": p["subset"],
            "workers": p["workers"],
            "model": p["model"],
            "task_slice": p["task_slice"],
            "cost_limit": p["cost_limit"],
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        (run_dir / "config.json").write_text(json.dumps(config, indent=2))
        return config

    def _pipeline_mounts() -> list[Mount]:
        return [
            Mount(source=f"{HOST_PROJECT_ROOT}/runs", target="/mlops-assignment/runs", type="bind"),
            Mount(source="/var/run/docker.sock", target="/var/run/docker.sock", type="bind"),
        ]

    run_agent = DockerOperator(
        task_id="run_agent",
        image=PIPELINE_IMAGE,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=_pipeline_mounts(),
        command=["/bin/bash", "-c", "cd /mlops-assignment && bash scripts/docker-run-agent.sh"],
        environment={
            "NEBIUS_API_KEY": os.environ.get("NEBIUS_API_KEY", ""),
            "RUN_ID": "{{ ti.xcom_pull(task_ids='prepare_run')['run_id'] }}",
            "SPLIT": "{{ params.split }}",
            "SUBSET": "{{ params.subset }}",
            "MODEL": "{{ params.model }}",
            "TASK_SLICE": "{{ params.task_slice }}",
            "WORKERS": "{{ params.workers }}",
            "COST_LIMIT": "{{ params.cost_limit if params.cost_limit is not none else '' }}",
        },
    )

    run_eval = DockerOperator(
        task_id="run_eval",
        image=PIPELINE_IMAGE,
        docker_url="unix://var/run/docker.sock",
        auto_remove="success",
        mount_tmp_dir=False,
        mounts=_pipeline_mounts(),
        command=["/bin/bash", "-c", "cd /mlops-assignment && bash scripts/docker-run-eval.sh"],
        environment={
            "RUN_ID": "{{ ti.xcom_pull(task_ids='prepare_run')['run_id'] }}",
            "MODEL": "{{ params.model }}",
            "WORKERS": "{{ params.workers }}",
        },
    )

    @task
    def summarize_and_log(**context):
        import mlflow

        config = context["ti"].xcom_pull(task_ids="prepare_run")
        run_dir = RUNS_DIR / config["run_id"]

        summary_name = f"{config['model'].replace('/', '__')}.{config['run_id']}.json"
        report_path = run_dir / "run-eval" / "reports" / summary_name
        report = json.loads(report_path.read_text())

        resolved = report.get("resolved_instances", 0)
        submitted = report.get("submitted_instances", 0)
        metrics = {
            "resolved_instances": resolved,
            "submitted_instances": submitted,
            "resolve_rate": resolved / submitted if submitted else 0.0,
        }
        (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))

        params = {
            "split": config["split"],
            "subset": config["subset"],
            "workers": config["workers"],
            "model": config["model"],
            "task_slice": config["task_slice"],
            "cost_limit": config["cost_limit"],
        }
        manifest = {
            "run_id": config["run_id"],
            "created_at": datetime.now(timezone.utc).isoformat(),
            "params": params,
            "metrics": metrics,
            "artifacts": [
                "config.json",
                "run-agent/preds.json",
                "run-agent/trajectories/",
                "run-eval/logs/",
                "run-eval/reports/",
                "metrics.json",
            ],
        }
        (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2))

        mlflow.set_tracking_uri(os.environ.get("MLFLOW_TRACKING_URI", "http://localhost:5000"))
        mlflow.set_experiment("swe-bench-eval")
        with mlflow.start_run(run_name=config["run_id"]):
            mlflow.log_params(params)
            mlflow.log_metrics(metrics)
            mlflow.log_artifacts(str(run_dir), artifact_path=config["run_id"])

    config = prepare_run()
    config >> run_agent >> run_eval >> summarize_and_log()


evaluate_agent()
