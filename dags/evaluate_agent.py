import json
import os
import shutil
import subprocess
from datetime import datetime, timezone
from pathlib import Path

import dotenv
from airflow.decorators import dag, task
from airflow.models.param import Param

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUNS_DIR = PROJECT_ROOT / "runs"

dotenv.load_dotenv(PROJECT_ROOT / ".env")


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

    @task
    def run_agent(config: dict) -> dict:
        run_dir = RUNS_DIR / config["run_id"]
        traj_dir = run_dir / "run-agent" / "trajectories"

        cmd = [
            "uv", "run", "mini-extra", "swebench",
            "--subset", config["subset"],
            "--split", config["split"],
            "--model", config["model"],
            "--slice", config["task_slice"],
            "--workers", str(config["workers"]),
            "-o", str(traj_dir),
        ]
        if config["cost_limit"] is not None:
            cmd += ["-c", f"agent.cost_limit={config['cost_limit']}"]

        subprocess.run(
            cmd,
            cwd=PROJECT_ROOT,
            check=True,
            env={**os.environ, "MSWEA_COST_TRACKING": "ignore_errors"},
        )

        preds_dst = run_dir / "run-agent" / "preds.json"
        shutil.move(traj_dir / "preds.json", preds_dst)

        return {**config, "preds_path": str(preds_dst)}

    @task
    def run_eval(result: dict) -> dict:
        run_dir = RUNS_DIR / result["run_id"]
        logs_dir = run_dir / "run-eval" / "logs"
        reports_dir = run_dir / "run-eval" / "reports"

        subprocess.run(
            [
                "uv", "run", "python", "-m", "swebench.harness.run_evaluation",
                "--dataset_name", "princeton-nlp/SWE-bench_Verified",
                "--predictions_path", result["preds_path"],
                "--max_workers", str(result["workers"]),
                "--run_id", result["run_id"],
            ],
            cwd=run_dir,
            check=True,
        )

        raw_logs = run_dir / "logs" / "run_evaluation" / result["run_id"]
        if raw_logs.exists():
            shutil.move(str(raw_logs), str(logs_dir / result["run_id"]))
            shutil.rmtree(run_dir / "logs", ignore_errors=True)

        summary_name = f"{result['model'].replace('/', '__')}.{result['split']}.json"
        summary_dst = reports_dir / summary_name
        shutil.move(run_dir / summary_name, summary_dst)

        return {**result, "report_path": str(summary_dst)}

    @task
    def summarize_and_log(result: dict):
        import mlflow

        run_dir = RUNS_DIR / result["run_id"]
        report = json.loads(Path(result["report_path"]).read_text())

        resolved = report.get("resolved_instances", 0)
        submitted = report.get("submitted_instances", 0)
        metrics = {
            "resolved_instances": resolved,
            "submitted_instances": submitted,
            "resolve_rate": resolved / submitted if submitted else 0.0,
        }
        (run_dir / "metrics.json").write_text(json.dumps(metrics, indent=2))

        params = {
            "split": result["split"],
            "subset": result["subset"],
            "workers": result["workers"],
            "model": result["model"],
            "task_slice": result["task_slice"],
            "cost_limit": result["cost_limit"],
        }
        manifest = {
            "run_id": result["run_id"],
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
        with mlflow.start_run(run_name=result["run_id"]):
            mlflow.log_params(params)
            mlflow.log_metrics(metrics)
            mlflow.log_artifacts(str(run_dir), artifact_path=result["run_id"])

    config = prepare_run()
    agent_result = run_agent(config)
    eval_result = run_eval(agent_result)
    summarize_and_log(eval_result)


evaluate_agent()
