#!/usr/bin/env bash
# Sourced by nixpacks build phases. Computes BUILD_JOBS from available RAM so
# each sequential compile step can use full cores without overlapping jobs.
set -euo pipefail

calc_build_jobs() {
  local nproc max_by_ram
  nproc=$(nproc)
  max_by_ram=$(awk '/MemAvailable/ { print int($2 / 524288) }' /proc/meminfo 2>/dev/null || echo 2)
  max_by_ram=${max_by_ram:-2}
  if (( max_by_ram < 1 )); then max_by_ram=1; fi
  if (( max_by_ram > nproc )); then max_by_ram=$nproc; fi
  echo "$max_by_ram"
}

export BUILD_JOBS="${BUILD_JOBS:-$(calc_build_jobs)}"
export CMAKE_BUILD_PARALLEL_LEVEL="$BUILD_JOBS"
echo "build-vps: BUILD_JOBS=$BUILD_JOBS (MemAvailable-based, single-step)"
