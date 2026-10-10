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
# - ~judge is mode 700: open it up (one directory, no copied layer). Everything under it is already world-readable,
#   including the 1.5 GB Rust toolchain in ~/.rustup, which is only ever read.
# - cargo writes its caches and Rust's shared build target under ~/.cargo, so give group 0 the owner's access there.
# - The entry registers the uid in /etc/passwd: Node-based runtimes like scratch-run need a home directory.
# - Some judge code and Python environment files are group-writable; group 0 must not modify them.
# Changing only these paths keeps this layer small: a recursive chmod/chgrp copies every file it touches.
RUN chmod 755 /judge/.docker/microvm-entry /home/judge && \
	chgrp -R 0 /home/judge/.cargo && chmod -R g=u /home/judge/.cargo && \
	find /judge /env /judge-runtime-paths.yml \( -perm -g+w -o -perm -o+w \) ! -type l -exec chmod go-w {} + && \
	chmod g+w /etc/passwd

ENV JUDGE_MODE=idle MICROVM_HTTP_PORT=8080
EXPOSE 8080

ENTRYPOINT ["/usr/bin/tini", "--", "/judge/.docker/microvm-entry"]
