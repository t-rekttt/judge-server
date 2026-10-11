#!/bin/bash
# Grades submissions to real problems in Spaces (tas-problems/tests/) through remote grading, with a fake site.
# Usage: run-real-problems.sh <site dir with <problem>/init.yml + tests/<case>/test.yml> <judge config in this dir>
# The config holds real credentials, so it is git-ignored (judge-real-*.yml), and should give the judge an id of its
# own (e.g. `id: remote-real`). Runner containers for backend `docker`; for backend `microvm` the judge creates real
# DigitalOcean MicroVMs.
set -euo pipefail
cd "$(dirname "$0")"
site_dir=$1; config=$2
# Separate compose project, so this can run next to run-testsuite.sh without the two tearing each other down.
export COMPOSE_PROJECT_NAME=dmoj-remote-real
# Only this judge's runner containers (labelled with its id), so a testsuite run alongside keeps its own.
judge_id=$(sed -n 's/^id: *//p' "$config" | head -1)
runner_filter="label=dmoj.judge=${judge_id}"

cleanup() {
  docker compose -f docker-compose.yml -f docker-compose.real.yml down -v --remove-orphans >/dev/null 2>&1 || true
  docker ps -aq --filter "$runner_filter" | xargs -r docker rm -f >/dev/null || true
}
trap cleanup EXIT
export JUDGE_COUNT=$(sed -n 's/^ *judge_count: *//p' "$config" | head -1)
export JUDGE_COUNT=${JUDGE_COUNT:-1}
PROBLEMS_DIR="$site_dir" JUDGE_CONFIG="$config" docker compose -f docker-compose.yml -f docker-compose.real.yml up -d --quiet-pull site judge
status=$(docker wait "$(docker compose -f docker-compose.yml -f docker-compose.real.yml ps -aq site)")
docker compose -f docker-compose.yml -f docker-compose.real.yml logs --no-color --no-log-prefix site
docker compose -f docker-compose.yml -f docker-compose.real.yml logs --no-color --no-log-prefix judge > /tmp/real-judge.log 2>&1 || true
sleep 5
leaked=$(docker ps -q --filter "$runner_filter" | wc -l)
echo "Runner VMs left after grading: $leaked"
[ "$leaked" = 0 ] || status=1
exit "$status"
