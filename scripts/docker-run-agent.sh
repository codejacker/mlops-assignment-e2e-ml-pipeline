#!/usr/bin/env bash
set -euo pipefail

# Expects RUN_ID, SPLIT, SUBSET, MODEL, TASK_SLICE, WORKERS, COST_LIMIT (optional)
# as environment variables, and the repo mounted at /mlops-assignment.

TRAJ_DIR="/mlops-assignment/runs/${RUN_ID}/run-agent/trajectories"
mkdir -p "$TRAJ_DIR"

CMD=(mini-extra swebench
    --subset "$SUBSET"
    --split "$SPLIT"
    --model "$MODEL"
    --slice "$TASK_SLICE"
    --workers "$WORKERS"
    -o "$TRAJ_DIR"
    -c swebench.yaml)

if [[ -n "${COST_LIMIT:-}" ]]; then
    CMD+=(-c "agent.cost_limit=${COST_LIMIT}")
fi

MSWEA_COST_TRACKING=ignore_errors "${CMD[@]}"

mv "$TRAJ_DIR/preds.json" "/mlops-assignment/runs/${RUN_ID}/run-agent/preds.json"
