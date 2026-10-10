#!/usr/bin/env bash
# Runs every pipeline of the playground once, in the mage container, and lists the
# tables they exported. Start the playground first: docker compose up -d
set -euo pipefail
cd "$(dirname "$0")"

pipelines=(
  pandas_sales
  polars_customer_360
  r_dplyr_cohorts
  polyglot_churn
  geo_store_coverage
  explore_web_events
)

failed=()
for pipeline in "${pipelines[@]}"; do
  echo "== ${pipeline}"
  start=$(date +%s)
  if docker compose exec -T mage mage run /home/src/shop_lab "${pipeline}" > "/tmp/playground-${pipeline}.log" 2>&1; then
    echo "   completed in $(( $(date +%s) - start )) s"
  else
    echo "   FAILED; log: /tmp/playground-${pipeline}.log"
    tail -20 "/tmp/playground-${pipeline}.log" | sed 's/^/   /'
    failed+=("${pipeline}")
  fi
done

docker compose exec -T postgres psql -U mage -d playground -c "
  SELECT table_name AS table,
         (xpath('/row/n/text()', query_to_xml(
           format('SELECT count(*) AS n FROM analytics.%I', table_name), false, true, ''
         )))[1]::text::bigint AS rows
  FROM information_schema.tables
  WHERE table_schema = 'analytics'
  ORDER BY 1"

if (( ${#failed[@]} )); then
  echo "Failed: ${failed[*]}"
  exit 1
fi
echo "Every pipeline completed."
