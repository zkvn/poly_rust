#!/usr/bin/env bash
# Sample docker stats for the indperf compose project every INTERVAL seconds
# into a CSV: unix_ts,container,cpu_pct,mem_mib. Run alongside
# docker-compose.perf.yml for the soak; ctrl-C (or kill) to stop.
#
#   bash scripts/perf/sample_docker_stats.sh out.csv [interval_secs]

set -euo pipefail

out="${1:?usage: sample_docker_stats.sh <out.csv> [interval_secs]}"
interval="${2:-30}"

if [[ ! -s "$out" ]]; then
  echo "unix_ts,container,cpu_pct,mem_mib" > "$out"
fi

echo "[sampler] writing to $out every ${interval}s (project indperf)"
while true; do
  ts=$(date +%s)
  # {{.MemUsage}} looks like "12.34MiB / 15.6GiB" — keep the used part, convert
  # KiB/MiB/GiB to MiB for a single comparable column.
  docker stats --no-stream --format '{{.Name}},{{.CPUPerc}},{{.MemUsage}}' \
    | grep '^indperf-' \
    | awk -F',' -v ts="$ts" '{
        cpu = $2; gsub(/%/, "", cpu);
        split($3, m, " / "); used = m[1];
        val = used; unit = used;
        gsub(/[0-9.]/, "", unit); gsub(/[^0-9.]/, "", val);
        if (unit == "GiB") val *= 1024;
        else if (unit == "KiB") val /= 1024;
        else if (unit == "B")  val /= 1048576;
        printf "%s,%s,%s,%.2f\n", ts, $1, cpu, val;
      }' >> "$out" || true
  sleep "$interval"
done
