# REPORT

## Architecture

```
Trigger DAG Run (split, subset, workers, model, task_slice, run_id, cost_limit)
        │
   [prepare_run]        make runs/<run-id>/ tree, write config.json
        │
   [run_agent]          mini-extra swebench -> preds.json + trajectories/
        │
   [run_eval]            swebench.harness.run_evaluation -> logs/ + report json
        │
   [summarize_and_log]  parse report -> metrics.json, manifest.json, log to MLflow
```

DAG: `dags/evaluate_agent.py`. Deployment (current stage): `run-airflow-standalone.sh` (Airflow standalone) + `uv run mlflow server --host 0.0.0.0 --port 5000`, both running directly on a Nebius VM (8 CPU / 32 GB, Docker installed).

## How to reproduce on a fresh VM

```bash
# prereqs: uv, Docker (see README §Prerequisites)
git clone <repo-url> && cd mlops-assignment-e2e-ml-pipeline
cp .env.example .env    # set NEBIUS_API_KEY
uv sync

bash run-airflow-standalone.sh &         # Airflow UI on :8080
uv run mlflow server --host 0.0.0.0 --port 5000 &   # MLflow UI on :5000
```

Trigger `evaluate_agent` from the Airflow UI (or `airflow dags trigger evaluate_agent --conf '{...}'`) with `split`, `subset`, `workers` at minimum.

## Artifact layout

```
runs/<run-id>/
  config.json          # exact params this run was triggered with
  run-agent/
    preds.json          # SWE-bench predictions (model_patch per instance)
    trajectories/        # per-instance agent trajectories + raw mini-swe-agent logs
  run-eval/
    logs/                # per-instance eval.sh, patch.diff, report.json, test_output.txt
    reports/             # aggregate {model}.{run_id}.json summary from the harness
  metrics.json          # resolved_instances / submitted_instances / resolve_rate
  manifest.json          # reconstruction pointer: params, metrics, artifact paths
```

## Rerun by run-id

Every artifact is namespaced under `runs/<run-id>/`; `config.json` inside it has the exact params used. To reproduce, read that file and re-trigger the DAG with the same `params` (the RNG-free steps — dataset slice, model, workers — are fully determined by them; LLM sampling itself is not guaranteed bit-identical).

## Issues encountered and fixes (chronological)

These cost real debugging time and are worth recording because they're all environment/config traps, not application bugs — exactly the kind of thing "provenance" is supposed to protect against.

1. **`docker: command not found` on the VM.** The VM had `uv` but not Docker — `run_eval` (and `run_agent`, which uses `environment_class: docker` internally) both need it. Fixed by running the README's Docker install block on the VM.

2. **`docker.errors.DockerException: ... Permission denied` after installing Docker.** `usermod -aG docker $USER` only takes effect in new login sessions; the already-running Airflow process (started before the group change) kept the old group set. Fixed by killing and relaunching Airflow standalone from a fresh shell.

3. **`mini-extra swebench` failing with `ValidationError: system_template / instance_template Field required`.** The DAG conditionally added `-c agent.cost_limit=<value>` when `cost_limit` was set — but per the CLI's own help text, setting `--config`/`-c` at all disables the tool's default config file instead of merging on top of it. This silently dropped `system_template`/`instance_template`, which come from the default `swebench.yaml`. The agent never made a single LLM call — it died in config validation. Fixed by always passing `-c swebench.yaml` (the bundled default) first, then layering `-c agent.cost_limit=...` as an additional merge on top, matching the CLI's documented multi-`-c` merge behavior.

4. **`run_eval` succeeding but `summarize_and_log` unable to find the report file.** The DAG assumed the harness names its summary `{model}.{split}.json`; verified against `swebench/harness/reporting.py` that it's actually `{model}.{run_id}.json`. Fixed the filename construction in `run_eval`.

5. **`summarize_and_log` failing with `ModuleNotFoundError: No module named 'mlflow'`.** `run-airflow-standalone.sh` launches Airflow via `uv tool run apache-airflow standalone` — an isolated tool environment separate from the project's `.venv` (where `mlflow` is a declared dependency in `pyproject.toml`). Since `summarize_and_log` is a `PythonOperator` running `import mlflow` in-process (not a subprocess like the shell-based tasks), it needed `mlflow` inside that same tool environment. Fixed by changing the launch command to `uv tool run --with mlflow apache-airflow standalone`.

6. **MLflow UI appeared to show "no data" for `swe-bench-eval`.** Verified directly against the tracking server's REST API (`/api/2.0/mlflow/experiments/search`, `/api/2.0/mlflow/runs/search`) that the experiment and run were present server-side all along, with correctly logged params and metrics. Not a pipeline bug — the browser was pointed at the wrong experiment (`Default`, id `0`) in the sidebar instead of `swe-bench-eval` (id `1`).

## First successful end-to-end run

Run `004` (`runs/004/`): `task_slice=0:1`, `workers=1`, `cost_limit=0.5`, model `nebius/moonshotai/Kimi-K2.6` — **`resolve_rate: 1.0`** (1/1 instance resolved). Logged to MLflow experiment `swe-bench-eval`, run name `004`.

## Open items / not yet done

- [ ] `DockerOperator` for `run_agent`/`run_eval` (currently `subprocess`)
- [ ] `docker-compose.yaml` deployment for Airflow + MLflow
- [ ] Object Storage (S3) upload of run artifacts
- [ ] Screenshots: `screenshots/airflow_dag.png`, `screenshots/mlflow_runs.png`
- [ ] A larger (`task_slice=0:3` or more) run for a more meaningful `resolve_rate` sample size
- [ ] Commit one example `runs/<run-id>/` folder as a deliverable
