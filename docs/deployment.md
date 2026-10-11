# Deploying remote grading

How to run the orchestrating judge in production, upgrade it, and operate it day to day. The design is in
[remote-grading.md](remote-grading.md); this document is the runbook.

## What you are deploying

One container, the **orchestrating judge**. It connects to the site's bridge as several judges (`judge-0` ...
`judge-7` by default), never runs submissions itself, and for each submission creates DigitalOcean MicroVMs that
pull the problem data from Spaces, compile, and grade test cases in parallel. VMs are deleted as soon as a
submission is done, so the account holds no VMs while idle.

Pieces outside the container:

| Piece | Where | Notes |
|---|---|---|
| Judge image | `registry.digitalocean.com/vnoj-judge/judge-microvm:latest` | Built from this repo; runs both the orchestrator and, inside each VM, the runner. |
| Checkpoint | DigitalOcean MicroVMs | A snapshot of a booted runner VM; VMs start from it in seconds. Tied to one image build. |
| Problem data | Spaces bucket `tas-problems`, prefix `tests/` | `<problem>/init.yml` plus the test archive, exactly as a judge's problem directory. Read-only key is enough. |
| Site bridge | the dashboard host, port 9999 | Unchanged DMOJ bridge. The judges connect out to it; nothing connects in. |

## Prerequisites

- A Linux host with Docker and Docker Compose v2. The judge needs no privileges: 1 CPU and 1 GiB of RAM are plenty,
  since it only orchestrates.
- Outbound network access to the site's bridge, to `api.digitalocean.com`, to `*.mkc1.microvm.ondigitalocean.com`
  (the VM endpoints) and to the Spaces endpoint.
- A DigitalOcean API token with the scopes `microvm:create`, `microvm:read`, `microvm:delete`, `microvm:access`,
  plus `registry:read` if the host pulls the image from the private registry.
- A Spaces access key with read access to the bucket.
- The judges registered on the site with matching names and keys (Admin, Judges). Each name the process presents
  must exist; the keys follow `judge_key_format` in the config.

## First deployment

On the judge host:

```bash
mkdir judge && cd judge
curl -fsSLO https://raw.githubusercontent.com/t-rekttt/judge-server/master/.docker/production/docker-compose.yml
curl -fsSLO https://raw.githubusercontent.com/t-rekttt/judge-server/master/.docker/production/judge.example.yml
cp judge.example.yml judge.yml && chmod 600 judge.yml
```

Edit `judge.yml`:

- `remote.judge_count`, `judge_name_format`, `judge_key_format`: how many judges this process presents and their
  names and keys on the site. `{i}` is the index.
- `remote.max_concurrent_vms`: the VM budget for the whole process (100 in production). Submissions share it
  fairly; the first VM of a submission is always granted.
- `remote.warm_idle_seconds`: how long a finished submission's VMs are kept for the next one (10). VMs beyond that
  are deleted.
- `remote.warm_reserve_per_judge`: VMs kept running at all times per judge. Set to `0` unless you want to pay for
  idle VMs in exchange for a few seconds less latency on the first submission after a quiet spell.
- `remote.microvm.api_token`, `checkpoint_id`, `checkpoint_runner_token`: from the "Image and checkpoint" section
  below.
- `remote.storage.*`: the Spaces endpoint, bucket, prefix and key.

Then:

```bash
echo DMOJ_SERVER_HOST=<site address> > .env      # add DMOJ_SERVER_PORT=... if not 9999
docker login registry.digitalocean.com           # or: doctl registry login
docker compose run --rm problems                 # ./problems/<problem>/init.yml for every problem in storage
docker compose up -d
docker compose logs -f judge                     # expect one 'Judge "judge-N" online' line per judge
```

The site's judge admin page should show every judge online within a minute. Submit something and watch
`docker compose logs -f judge`: `Accept submission`, a line per test case, `Done grading`.

**TLS.** If the bridge requires TLS, add `DMOJ_JUDGE_ARGS=-s` to `.env` (`-s -k` for a self-signed certificate).
The production bridge today uses plain TCP on port 9999.

## Image and checkpoint

VMs run the same image as the orchestrator. Every change to judge code therefore needs a new image **and** a new
checkpoint, because a checkpoint is a snapshot of a booted VM from one specific image.

```bash
# 1. Build and push the image (from the repository root).
docker build -f .docker/microvm/Dockerfile -t registry.digitalocean.com/vnoj-judge/judge-microvm:latest .
docker login registry.digitalocean.com
docker push registry.digitalocean.com/vnoj-judge/judge-microvm:latest

# 2. Create a checkpoint from it: boots one VM, waits for the runner, snapshots it, deletes the VM.
docker run --rm -e DIGITALOCEAN_TOKEN=<token> --entrypoint /env/bin/python3 \
  registry.digitalocean.com/vnoj-judge/judge-microvm:latest \
  /judge/.docker/microvm/create_checkpoint.py registry.digitalocean.com/vnoj-judge/judge-microvm:latest \
  --region mkc1 --cpus 1 --memory-mb 2048
```

It prints `checkpoint_id` and `checkpoint_runner_token`. Put both in `judge.yml`. The runner token is a secret:
every VM from this checkpoint accepts it.

Then delete the previous checkpoint; each one stores the VM's full disk (52 GiB for this size) and is billed:

```bash
curl -s -X DELETE -H "Authorization: Bearer <token>" \
  https://api.digitalocean.com/v2/microvms/checkpoints/<old checkpoint id>
```

Keep exactly one checkpoint, the one in `judge.yml`.

## Upgrading

Order matters: VMs created from the old checkpoint run the old runner, and a new orchestrator may speak to it
differently.

1. Build and push the image, create the new checkpoint (section above).
2. Update `checkpoint_id` and `checkpoint_runner_token` in `judge.yml`.
3. `docker compose pull && docker compose up -d`.
4. Watch the log for the judges coming online and for `Traceback`. Submit a test submission.
5. Delete the old checkpoint.

`docker compose up -d` recreates the container. Stopping it aborts in-flight submissions, which the site requeues,
and deletes every VM the judge holds; the grace period is 3 minutes so that finishes. A restart therefore costs
nothing but a few seconds of dashboard "judge offline".

## Adding problems

The judges announce the problems whose `init.yml` sits in `./problems/`. After uploading a problem to the bucket:

```bash
docker compose run --rm problems
docker compose restart judge
```

The runner reads the actual test data from the bucket at grading time, so the only thing to keep in sync is this
list of names.

## Day-to-day operation

**Health.** `docker compose logs --since 10m judge | grep -c '" online'` should equal `judge_count` after a start,
and the site should show them online. `docker compose ps` shows `Up` with no restarts.

**Expected log lines**, per submission: `Accept submission`, one `Test case` line per case, `Done grading`. Each
restart logs `Deleting N MicroVMs of this judge that no submission owns` if any were left from the previous run.

**Lines that mean something is wrong:**

| Line | Meaning | What to do |
|---|---|---|
| `Attempting reconnection` | The site closed this judge's connection. | Normal once at a bridge restart. Repeated every ~60 s means the site is timing judges out; check the bridge log. |
| `Reporting submission N again on the new connection` | The judge reconnected while grading and resent the submission. | Nothing; the verdict lands if the bridge has the `on_judge_free` fix (see remote-grading.md, "Dropped site connections"). Otherwise rejudge N from the dashboard. |
| `Failed to delete MicroVM` | The API refused a deletion; the sweeper retries every 3 minutes. | Check the API quota or status if it repeats. |
| `failed with 429` | The API rate limit was still hit despite pacing. | Lower `max_concurrent_vms` if it persists. |
| `Parked VM ... no longer answers` | A reused VM had died; it was replaced. | Nothing unless frequent. |
| `Unhandled exception in worker` / `Traceback` | A submission hit a judge bug; it is reported as IE. | Rejudge it; send the traceback. |

**VM count on the DigitalOcean dashboard.** While idle: 0. While grading: up to `max_concurrent_vms`. Within about
15 seconds of the last verdict: back to 0. Any VM older than a minute with nothing grading is a leak: note its
name (it carries the submission id) and delete it. The judge's sweeper runs every 3 minutes but, as a safety
against deleting a VM in use, only touches VMs older than the longest a submission can take (80 minutes); a
restart of the judge deletes every VM it finds under its tag at once. Every VM carries the tag `dmoj-judge-<id>`,
so one API call lists them:

```bash
curl -s -H "Authorization: Bearer <token>" \
  "https://api.digitalocean.com/v2/microvms?per_page=200&tag_name=dmoj-judge-judge" | jq '.meta.total'
```

**Rate limit.** The API allows 5,000 requests an hour per token. Each VM costs roughly 6 requests over its life, so
a sustained 800 VM starts an hour is the ceiling; the judge paces itself under it and slows down rather than
failing. If a contest needs more, use a second token for a second judge process.

**Cost.** Billing is per VM-second. With no reserve, cost is proportional to grading: about 6 seconds of VM start
plus compile per cold submission, then the test cases. Warm reuse removes the start cost between back-to-back
submissions. Checkpoints are billed by stored size; keep one.

## Stopping and removing

```bash
docker compose down          # aborts in-flight submissions (requeued by the site), deletes all VMs
```

To decommission entirely, also delete the checkpoint and revoke the API token and Spaces key.

## Site-side notes

- **Bridge fix for reconnects.** `judge/bridge/judge_list.py`, `on_judge_free`: `self.submission_map.pop(submission,
  None)` instead of `del`. Without it, a submission whose judge reconnected mid-grading keeps its test cases but
  stays at internal error. Merged in `t-rekttt/OJ` 31f5b11e and pinned by `t-rekttt/vnoj-docker` 12cb75b; rebuild
  the `base` and `bridged` images and restart `bridged` to activate it.
- **Languages.** The judges announce the executors in the image. A site language with no executor (today:
  `CLANGX`, Clang++) leaves its submissions queued forever. Either add the executor to the image or disable the
  language on the site.
- **Time limits** were calibrated on the previous judges. The MicroVMs are several times faster, so limits are
  more lenient than before until recalibrated.

## Local test environment

`.docker/remote-dev/run-testsuite.sh` runs the whole judge testsuite through a fake site, a fake MicroVM API and a
local S3; `run-real-problems.sh` grades real problems from the real bucket on real MicroVMs. Both are described in
[remote-grading.md](remote-grading.md). Run the testsuite before any image change; it catches most regressions in
minutes.
