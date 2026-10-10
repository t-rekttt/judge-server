# Remote grading

With `remote_grading: true`, the judge stops compiling and running submissions itself. For each submission it creates
disposable VMs, has them download the problem from S3-compatible storage, grades test cases in parallel across them, and
reports results to the site as usual. The site needs no changes.

## How a submission is graded

1. **First VM:** downloads the problem's small files, compiles the submission and grades case 1.
   - A compile error is reported right away, and no other VM is created.
2. **Other VMs** (up to `max_vms` in total) start once compilation succeeds. They download the same small files and take the
   first VM's compiled programs (submission, checkers, interactors, generators), so they don't compile again.
3. **Each VM** repeatedly takes the next case still worth grading.
   - Cases sharing an input/output pair are graded once, like the local result cache.
   - Cases already decided by short-circuiting are skipped.
4. **Verdicts are identical to local grading.** Results are replayed in case order with the same batch-dependency and
   short-circuit rules.
5. **Reporting order.** The site assigns each case to the batch opened by the last `batch-begin`, so:
   - Batches are reported one at a time, in order.
   - Cases outside batches are reported as soon as their verdict is certain, possibly out of order.
6. **Cleanup.** All VMs are deleted when grading ends, on abort and on errors.

Code: `dmoj/remote/`
- `session.py`: the orchestrator.
- `emitter.py`: verdicts and reporting order.
- `runner.py`: the service inside each VM.
- `artifact.py`: compiled-program sharing.
- `storage.py`: storage layout and presigned S3 URLs.
- `range_file.py`: reads the test archive with HTTP range requests.
- `backend.py`: VM creation.

## Problem storage

Each problem is two objects under `<prefix>/<storage namespace or "default">/<problem id>/`:

- **`files.zip`:** the problem directory (with `init.yml` at the top) minus its test archive: checkers, generators,
  interactors. Every VM downloads and unpacks it, so it should stay small (limit 256 MiB).
- **`archive.zip`:** the test archive named by `archive:` in `init.yml`, stored unchanged. No VM downloads it whole.
  A VM reads the archive's file index from its end (one request of at most 1 MiB), then only the bytes of the cases it
  grades, with HTTP range requests.
  - Opening a member requests exactly its bytes (in up to 64 MiB pieces), using the offsets and sizes in the
    archive's file index, so nothing past it is downloaded.
  - Measured locally: 40 cases in a 320 MiB archive, 4 VMs: 325 MiB fetched in total across all VMs (each case once),
    versus 1280 MiB when every VM downloaded the whole problem.
  - Cases graded from a generator need no archive reads.

VMs get presigned GET URLs valid for `url_expiry` seconds, so they never hold storage credentials. Every `/grade`
request carries fresh URLs, so grading never outlives them.

Test data that isn't in an archive (cases listed directly in `init.yml`) is part of `files.zip`. Large problems
should use `archive:`.

`.docker/remote-dev/upload_problems.py` uploads a problems directory in this layout.

The judge still needs `problem_storage_globs` (or `judge_version: 2` storages) to tell the site which problems it serves.

## Configuration

```yaml
remote_grading: true
remote:
  max_vms: 4              # VMs per submission, including the first
  backend: microvm        # or `docker` (local runner containers)
  microvm:
    api_token: dop_v1_... # scopes: microvm:create, microvm:read, microvm:delete, microvm:access
    access_token: ...     # optional: separate token with only microvm:access, for VM endpoints
    # Either start VMs from a checkpoint (fast, recommended)...
    checkpoint_id: 01a0ed98-...
    checkpoint_runner_token: ...
    # ...or from the image (slow: every VM boots and loads all languages).
    # image: registry.digitalocean.com/<team>/judge-microvm:<tag>
    # region: mkc1
    # cpus: 2             # with memory_mb, one of 1/2048, 2/4096, 8/16384, 16/32768
    # memory_mb: 4096
    vpc_uuid: ...         # optional: keep VM traffic (S3 downloads) in a VPC
    ready_timeout: 300
  docker:                 # only for backend: docker
    image: vnoj/judge-microvm:dev
    network: dmoj-remote-dev
    user: 101             # MicroVMs run the image as an arbitrary uid in group 0
    memory_mb: 2048
    cpus: 2               # see "Sizing" below
  storage:
    endpoint: https://<region>.digitaloceanspaces.com
    bucket: problems
    prefix: ''
    access_key: ...
    secret_key: ...
    region: us-east-1
    url_expiry: 900
```

The orchestrating judge doesn't need ptrace or the sandbox. Run it with `--skip-self-test`; the image's languages were
tested when it was built.

## Images

`.docker/microvm/Dockerfile` (build from the repository root) wraps `vnoj/judge-tiervnoj` so it runs as a MicroVM:
- MicroVMs can't set an entrypoint or arguments, so the image is configured through `JUDGE_MODE`:
  - `runner` (needs `RUNNER_TOKEN`)
  - `test`
  - `bench`
  - `run`
  - `idle`
- It works under an arbitrary uid with no capabilities.
- It overlays this checkout's judge code.

## DigitalOcean MicroVMs

`backend: microvm` (`dmoj/remote/microvm_backend.py`) uses the public preview API, `/v2/microvms`.

- **Creating a VM:** create, then poll until the VM is `running` and its endpoint is `ACTIVE`, then wait for the runner's
  health check. If anything fails, the VM is deleted.
- **Talking to a VM:** requests to its endpoint (`https://<id>.<region>.microvm.ondigitalocean.com`) carry the
  DigitalOcean token in `Authorization`. The runner's own token goes in `X-Runner-Token`.
- **API errors:** rate limiting (429) and server errors are retried with backoff. Other errors fail the VM.
- **Tags:** every VM is tagged `dmoj-remote-runner` and `dmoj-judge-<judge id>`. At startup the judge deletes VMs with
  its own tag, left behind if it crashed mid-grading. Leaked VMs also pause after 10 idle minutes.

**Checkpoints.** A VM created from the image boots the container and loads every language before it can grade. A VM
created from a checkpoint resumes with the runner already listening. To create one:

```bash
docker run --rm -e DIGITALOCEAN_TOKEN=... --entrypoint /env/bin/python3 <image> \
  /judge/.docker/microvm/create_checkpoint.py <image> [--region mkc1 --cpus 2 --memory-mb 4096]
```

It prints `checkpoint_id` and `checkpoint_runner_token` for the judge's configuration.
- A checkpoint fixes the region, size, image and environment, so its VMs share one runner token. Keep that token as
  secret as the API token.
- Recreate the checkpoint whenever the image changes.

## Sizing

- **CPU-time accuracy depends on VMs not sharing CPUs.** Locally, 8 runners on a busy 40-core host measured 2.2× the
  normal CPU time per case and turned AC into TLE. With one dedicated CPU per runner, times matched local grading
  within 10%.
- **Go needs about 18 CPU-seconds to build its standard library in a fresh VM.** On one vCPU that exceeds Go's 20 s
  compile limit, so use 2 vCPUs (or pre-warm the Go build cache in the image).

## Local test environment

```bash
.docker/remote-dev/run-testsuite.sh                   # whole testsuite, fails on any wrong stored result
.docker/remote-dev/run-testsuite.sh '^aplusb$'        # one problem
JUDGE_CONFIG=judge-microvm.yml MICROVM_CHECKPOINT=1 .docker/remote-dev/run-testsuite.sh   # through the MicroVM backend
ABORT_AFTER=12 PROBLEMS_DIR=/path .docker/remote-dev/run-testsuite.sh   # abort test
```

It starts the following, and checks every result the way the VNOI bridge stores it:
- an S3-compatible store (RustFS; MinIO's images are no longer published), with data in `.docker/remote-dev/data`;
- a fake site;
- a fake MicroVM API (`fake_microvm_api.py`), following DigitalOcean's published OpenAPI spec, which runs each
  "MicroVM" as a runner container;
- the judge, which starts runners directly (`judge.yml`) or through the fake MicroVM API (`judge-microvm.yml`).

## Not done yet

- Not yet run against the real DigitalOcean API: needs preview access and a token. The fake API follows the published
  spec, but the real service may differ (e.g. endpoint latency, rate limits, checkpoint restore time).
- Rust recompiles in each VM: its build output lives outside the shared cache.
