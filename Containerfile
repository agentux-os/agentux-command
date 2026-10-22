# The AgentUX Command image: ghcr.io/agentux-os/agentux-command:<version>,
# run by deploy/quadlet/agentux-command.container.
#
#   podman build -t agentux-command -f Containerfile .

FROM docker.io/library/rust:1.99-bookworm AS build
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY src src
COPY migrations migrations
RUN cargo build --release --locked --bins \
    && install -m 0755 target/release/agentux-command target/release/agentux-command-storage-report /usr/local/bin/

FROM docker.io/library/debian:bookworm-slim
# curl is the Quadlet's health check (curl --fail http://localhost:8080/healthz).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin agentux
COPY --from=build /usr/local/bin/agentux-command /usr/local/bin/agentux-command-storage-report /usr/local/bin/
USER agentux
ENV AGENTUX_COMMAND_ADDR=:8080
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=5s --retries=5 CMD curl --fail --silent http://localhost:8080/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/agentux-command"]
