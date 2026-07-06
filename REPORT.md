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

DAG: `dags/evaluate_agent.py`. `run_agent`/`run_eval` run as `DockerOperator` tasks, launching the project's `Dockerfile` image as a *sibling* container (via the host's Docker socket, not Docker-in-Docker) — the same pattern the swebench evaluation harness itself uses internally to spin up per-instance test containers.

## Deployment: Docker Compose

Moved off `run-airflow-standalone.sh` (a bare foreground process — killed by any SSH/session drop) onto `docker-compose.yaml`:

- `postgres` — Airflow's metadata DB
- `airflow-init` — one-shot: `airflow db migrate` + seeds the simple-auth-manager admin/admin credentials
- `airflow-apiserver`, `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer` — built from `Dockerfile.airflow` (official `apache/airflow` image + `mlflow`, `python-dotenv`, `apache-airflow-providers-docker`)
- `mlflow` — tracking server, sqlite backing store, reuses the same image
- `pipeline` — build-only (`profiles: ["build-only"]`); never started by `up`. `DockerOperator` launches it directly against the host docker daemon.

Key detail: `airflow-scheduler` mounts `/var/run/docker.sock` so its `DockerOperator` tasks can talk to the **host** docker daemon and launch sibling `pipeline` containers — not nested Docker-in-Docker. Because of that, any bind mount those sibling containers need must be a **host** path, which differs from the path Airflow sees inside its own container. `PIPELINE_HOST_PROJECT_ROOT` (set to `${PWD}` at `docker compose up` time) carries that host path into the DAG for exactly this purpose. The pipeline container only gets `runs/` bind-mounted in (plus the docker socket) — not the whole repo — so it keeps using its own image-baked `.venv` instead of accidentally picking up a host-built one with different absolute paths.

Build and run:
```bash
docker compose build
docker compose up -d
```

## How to reproduce on a fresh VM

```bash
# prereqs: Docker + Docker Compose (see README §Prerequisites)
git clone <repo-url> && cd mlops-assignment-e2e-ml-pipeline
cp .env.example .env    # set NEBIUS_API_KEY

# the scheduler needs the HOST docker group gid to reach the mounted socket:
getent group docker | cut -d: -f3        # e.g. 987 -> put in docker-compose.yaml scheduler group_add

docker compose build                     # builds airflow images + the pipeline image
docker compose build pipeline            # (explicitly, if not built by the line above)
docker compose up -d                     # postgres, mlflow, airflow-{webserver,scheduler,dag-processor,triggerer}
```

- Airflow UI: http://localhost:8080 (admin / admin — seeded by `airflow-init`)
- MLflow UI: http://localhost:5000 → open `swe-bench-eval` → toggle **Model training** for the runs table

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

### Docker Compose deployment traps (second debugging pass)

Bringing the multi-service `docker-compose.yaml` up on the VM surfaced a fresh batch — all config/environment, none application logic:

7. **`airflow-init` never seeded the admin user: `airflow users create ... invalid: -e/--email required`.** The create command omitted the mandatory `--email`, so it printed usage and exited without creating anyone — hence "no password works". Fixed by adding `--email admin@example.org` (and switching the deprecated `airflow db init` to `airflow db migrate`). For a one-off reset the user can also run `airflow users create ...` directly inside the running container.

8. **`airflow-apiserver` restart loop: `invalid choice: 'api-server'`.** `api-server` is an Airflow **3.x** subcommand; on 2.9.0 the web UI process is `webserver`. Renamed the service/command to `webserver`.

9. **`airflow-dag-processor` restart loop: `[scheduler/standalone_dag_processor] must be True`.** Running a *separate* dag-processor container requires opting into the standalone processor. Fixed by setting `AIRFLOW__SCHEDULER__STANDALONE_DAG_PROCESSOR=true` in the shared env.

10. **`run_agent` failing instantly with `jinja2.exceptions.TemplateNotFound: scripts/docker-run-agent.sh`.** The real trap: `DockerOperator` declares `template_ext = ('.sh', '.bash')`, so Airflow treats any `command` string ending in `.sh` as a Jinja **template file to load from disk**, not a literal command — and tries to open it relative to the DAGs folder. Overriding `template_fields` did *not* help (that's a different mechanism). Fixed with a small `DockerOperatorNoTemplate(DockerOperator)` subclass that sets `template_ext = ()` (keeping `template_fields = ("environment",)` so env vars are still rendered).

11. **`run_agent` then failing with `docker.errors.DockerException: ... Permission denied` on the socket.** Under `LocalExecutor` the task runs *inside* the `airflow-scheduler` container, whose UID is not in the container's docker group, so it can't open the mounted `/var/run/docker.sock`. Fixed by adding the **host** docker group gid (`getent group docker | cut -d: -f3` → `987` on this VM) to the scheduler service via `group_add: ["987"]`, then `docker compose up -d --force-recreate airflow-scheduler` (group changes need a recreate, not a restart). Cleaner than running the whole container as root.

12. **`summarize_and_log` failing with `403 'Invalid Host header - possible DNS rebinding attack detected'`.** MLflow ≥3's tracking server rejects requests whose `Host` header isn't in its allow-list; the in-cluster call to `http://mlflow:5000` was blocked. Fixed by starting the server with `--allowed-hosts "*"`. Recovered without re-running the expensive agent by clearing only the failed task (`airflow tasks clear evaluate_agent -t summarize_and_log -s <date> -y`).

13. **MLflow UI (3.14) still "No data / Traces: 0" even on the right experiment.** MLflow 3.x opens experiments in the **GenAI** view (traces/sessions), which is empty for classic runs. The run is logged as a **classic run** — visible only after toggling the top-left **`Model training`** tab. Not a logging bug; verified server-side via `mlflow.search_runs` that the run was present with `resolve_rate=0.667`.

## First successful end-to-end run (Docker Compose)

Run `06` (`runs/06/`): `task_slice=0:3`, `workers=5`, `cost_limit=0.5`, model `nebius/moonshotai/Kimi-K2.6` — **`resolve_rate: 0.667`** (2/3 instances resolved: `astropy__astropy-12907`, `-13033`, `-13236`). Full artifact tree present: `config.json`, `run-agent/preds.json` + 3 trajectories, `run-eval/logs/` per-instance (`eval.sh`, `patch.diff`, `report.json`, `test_output.txt`), `run-eval/reports/nebius__moonshotai__Kimi-K2.6.06.json`, `metrics.json`, `manifest.json`. Logged to MLflow experiment `swe-bench-eval` (run name `06`), verified server-side.

## Open items / not yet done

- [x] `DockerOperator` for `run_agent`/`run_eval` (was `subprocess`) — now run end to end on the VM; see issues 10–11 for the traps that surfaced
- [x] `docker-compose.yaml` deployment for Airflow + MLflow — running; full pipeline green
- [x] A larger (`task_slice=0:3`) run for a more meaningful `resolve_rate` sample size — run `06`, 2/3 resolved
- [x] Commit one example `runs/<run-id>/` folder as a deliverable — `runs/06/`
- [ ] Object Storage (S3) upload of run artifacts
- [ ] Screenshots: `screenshots/airflow_dag.png`, `screenshots/mlflow_runs.png`
