################################################################################
# Single Dockerfile that produces one image containing BOTH the api and worker
# binaries plus the static dashboard. Each service picks which binary to run
# at start time via Docker CMD override:
#   docker compose → worker has `command: ["/app/worker"]`
#   Render        → worker has `dockerCommand: /app/worker`
#
# Why one image instead of multi-target?
#   - Render Blueprint schema doesn't support `dockerTarget`, only
#     `dockerCommand`. `dockerCommand` overrides CMD, not ENTRYPOINT — so
#     we leave ENTRYPOINT unset and use CMD only.
#   - Combined image is ~30MB (two static Go binaries + web assets). Trivial.
################################################################################

# ---------- 1. build base -----------------------------------------------------
FROM golang:1.26-alpine AS build

WORKDIR /src

# Cache go modules across source-only changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Build both binaries statically so the distroless final stage needs no libc.
# -trimpath strips local paths; -s -w drops DWARF + symbol tables.
ENV CGO_ENABLED=0 GOOS=linux

RUN go build -trimpath -ldflags="-s -w" -o /out/api    ./cmd/api && \
    go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

# ---------- 2. runtime image --------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=build /out/api    /app/api
COPY --from=build /out/worker /app/worker
# Dashboard assets — served by Gin via r.StaticFile("/", "./web/index.html").
COPY --from=build /src/web    /app/web

USER nonroot:nonroot
EXPOSE 8081

# Default to api. Override per service:
#   compose worker: `command: ["/app/worker"]`
#   render worker:  `dockerCommand: /app/worker`
CMD ["/app/api"]
