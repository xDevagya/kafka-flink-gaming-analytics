#!/usr/bin/env bash
set -euo pipefail

JOBMANAGER="flink-jobmanager"
JOBS_DIR="/opt/flink/jobs"

jobs=(
  "raw_events.sql"
  "session_metrics.sql"
  "player_revenue.sql"
  "dead_letter.sql"
)

echo "Waiting for Flink JobManager to be ready..."
until docker exec "$JOBMANAGER" /opt/flink/bin/flink list > /dev/null 2>&1; do
  sleep 2
done
echo "JobManager ready."

for job in "${jobs[@]}"; do
  echo "Submitting $job..."
  docker exec "$JOBMANAGER" /opt/flink/bin/sql-client.sh -f "$JOBS_DIR/$job"
  echo "$job submitted."
done

echo ""
echo "All jobs submitted. Running jobs:"
docker exec "$JOBMANAGER" /opt/flink/bin/flink list
