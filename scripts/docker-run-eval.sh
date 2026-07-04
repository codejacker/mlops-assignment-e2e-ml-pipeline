#!/usr/bin/env bash
set -euo pipefail

# Expects RUN_ID, MODEL, WORKERS as environment variables, and the repo
# mounted at /mlops-assignment.

RUN_DIR="/mlops-assignment/runs/${RUN_ID}"
cd "$RUN_DIR"

python -m swebench.harness.run_evaluation \
    --dataset_name princeton-nlp/SWE-bench_Verified \
    --predictions_path "$RUN_DIR/run-agent/preds.json" \
    --max_workers "$WORKERS" \
    --run_id "$RUN_ID"

mkdir -p "$RUN_DIR/run-eval/logs" "$RUN_DIR/run-eval/reports"

RAW_LOGS="$RUN_DIR/logs/run_evaluation/${RUN_ID}"
if [[ -d "$RAW_LOGS" ]]; then
    mv "$RAW_LOGS" "$RUN_DIR/run-eval/logs/${RUN_ID}"
    rm -rf "$RUN_DIR/logs"
fi

SUMMARY="${MODEL//\//__}.${RUN_ID}.json"
mv "$RUN_DIR/$SUMMARY" "$RUN_DIR/run-eval/reports/$SUMMARY"
