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
# AgentUX Office, served at /office/ (agentux-apps release, checked).
ARG AGENTUX_OFFICE_VERSION=0.1.0
ARG AGENTUX_OFFICE_SHA256=cd8103c327fb0bf029398960c3f9edbf9525dfd8a3f0ff83dc3fe241b2085696
# curl is the Quadlet's health check (curl --fail http://localhost:8080/healthz).
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --no-create-home --shell /usr/sbin/nologin agentux \
    && office="agentux-office-${AGENTUX_OFFICE_VERSION}.tar.gz" \
    && curl -fsSL --retry 5 --retry-all-errors --retry-delay 5 \
        "https://github.com/agentux-os/agentux-apps/releases/download/office-v${AGENTUX_OFFICE_VERSION}/${office}" -o "/tmp/${office}" \
    && echo "${AGENTUX_OFFICE_SHA256}  /tmp/${office}" | sha256sum --check --strict \
    && mkdir -p /usr/share/agentux-command \
    && tar -xzf "/tmp/${office}" -C /usr/share/agentux-command --no-same-owner \
    && mv /usr/share/agentux-command/agentux-office /usr/share/agentux-command/office \
    && rm -f "/tmp/${office}"
COPY --from=build /usr/local/bin/agentux-command /usr/local/bin/agentux-command-storage-report /usr/local/bin/
USER agentux
ENV AGENTUX_COMMAND_ADDR=:8080 \
    AGENTUX_COMMAND_OFFICE_DIR=/usr/share/agentux-command/office
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=5s --retries=5 CMD curl --fail --silent http://localhost:8080/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/agentux-command"]
