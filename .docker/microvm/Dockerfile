# Wraps the VNOJ judge image so it runs as a DigitalOcean MicroVM, which can't set entrypoint/args
# and needs a long-running process listening on its HTTP port.
ARG BASE=vnoj/judge-tiervnoj:latest
FROM ${BASE}

# The judge in the base image predates remote grading; overlay this checkout's Python sources (the compiled cptbox and
# checker modules are kept, their sources are unchanged).
COPY --chown=root:root dmoj /judge/dmoj
COPY .docker/microvm/entry /judge/.docker/microvm-entry
COPY .docker/microvm/timing_benchmark.py .docker/microvm/compare_benchmarks.py .docker/microvm/create_checkpoint.py \
     /judge/.docker/microvm/
# MicroVMs start the container as an arbitrary unprivileged uid (seen: 101, in group 0) instead of `judge` (1000).
# Give group 0 the same access as the owner (OpenShift-style) on ~judge only, so that uid can use rustup/cargo and
# Rust's shared build target, and let the entry register it in /etc/passwd (Node-based runtimes like scratch-run need
# a home dir). The judge code and its Python environment are root-owned and group 0 would otherwise inherit write
# access to them, so make them read-only for everyone but root.
RUN chmod 755 /judge/.docker/microvm-entry && \
	chgrp -R 0 /home/judge && chmod -R g=u /home/judge && \
	chmod -R go-w,a+rX /judge /env /judge-runtime-paths.yml && \
	chmod g+w /etc/passwd

ENV JUDGE_MODE=idle MICROVM_HTTP_PORT=8080
EXPOSE 8080

ENTRYPOINT ["/usr/bin/tini", "--", "/judge/.docker/microvm-entry"]
