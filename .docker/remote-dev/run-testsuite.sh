#!/bin/bash
# Grades the testsuite through remote grading with a fake site, and fails if any stored result is wrong.
# Usage: .docker/remote-dev/run-testsuite.sh [problem regex]   (builds the image first)
# Env: PROBLEMS_DIR (default: the repository testsuite), JUDGE_CONFIG (judge config file in this directory:
#      judge.yml uses Docker runners, judge-microvm.yml the fake MicroVM API), MICROVM_CHECKPOINT=1 (MicroVMs start from
#      a checkpoint), ABORT_AFTER (see fake_site.py)
set -euo pipefail
cd "$(dirname "$0")"

docker build -q -f ../microvm/Dockerfile -t vnoj/judge-microvm:dev ../.. >/dev/null
# The S3 server runs as uid 10001 and stores its data here.
mkdir -p data
docker run --rm -v "$PWD/data:/d" alpine:latest chown -R 10001:10001 /d

cleanup() {
  docker compose down -v --remove-orphans >/dev/null 2>&1 || true
  rm -f judge-microvm-checkpoint.yml
  docker ps -aq --filter label=dmoj.remote-runner | xargs -r docker rm -f >/dev/null || true
}
trap cleanup EXIT

# With the fake MicroVM API and MICROVM_CHECKPOINT=1, VMs start from a checkpoint like in production.
if [ -n "${MICROVM_CHECKPOINT:-}" ]; then
  docker compose up -d --wait microvm-api >/dev/null
  values=$(docker compose run --rm -e DIGITALOCEAN_TOKEN=fake-do-token --entrypoint /env/bin/python3 judge \
    /judge/.docker/microvm/create_checkpoint.py vnoj/judge-microvm:dev --api-url http://microvm-api:8090 \
    --endpoint-scheme http 2>/dev/null)
  python3 - "$values" <<'PY'
import sys
values = dict(line.split(': ', 1) for line in sys.argv[1].strip().splitlines())
config = open('judge-microvm.yml').read().replace(
    '    image: vnoj/judge-microvm:dev\n',
    f"    checkpoint_id: {values['checkpoint_id']}\n    checkpoint_runner_token: {values['checkpoint_runner_token']}\n",
)
open('judge-microvm-checkpoint.yml', 'w').write(config)
PY
  export JUDGE_CONFIG=judge-microvm-checkpoint.yml
fi

PROBLEMS="${1:-.*}" docker compose up -d --quiet-pull
# The site container exits when every submission is checked; its exit code is the verdict.
status=$(docker wait "$(docker compose ps -aq site)")
docker compose logs --no-color --no-log-prefix site

# Every VM must be gone once grading is over (the judge deletes them as each submission ends).
sleep 5
leaked=$(docker ps -q --filter label=dmoj.remote-runner | wc -l)
echo "Runner VMs left after grading: $leaked"
[ "$leaked" = 0 ] || status=1
exit "$status"
