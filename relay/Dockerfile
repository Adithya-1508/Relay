################################################################################
# Single Dockerfile that builds both `api` and `worker` binaries. Pick the
# image target with --target api / --target worker. Final stages are based on
# distroless/static-debian12, so the runtime image carries no shell, no libc,
# no package manager — just the static Go binary, /etc/passwd for the nonroot
# user, and ca-certificates for outbound HTTPS.
################################################################################

# ---------- 1. shared build base ----------------------------------------------
FROM golang:1.26-alpine AS build

WORKDIR /src

# Copy go.mod/go.sum first so dependency download caches across source-only
# changes.
COPY go.mod go.sum ./
RUN go mod download

COPY . .

# Compile both binaries statically so the distroless images need no libc.
# -trimpath strips local paths from the binary; -ldflags '-s -w' drops the
# DWARF + symbol table for a smaller image.
ENV CGO_ENABLED=0 GOOS=linux

RUN go build -trimpath -ldflags="-s -w" -o /out/api    ./cmd/api && \
    go build -trimpath -ldflags="-s -w" -o /out/worker ./cmd/worker

# ---------- 2. api image -------------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot AS api
WORKDIR /app
COPY --from=build /out/api /app/api
# Dashboard assets — served by Gin via r.StaticFile("/", "./web/index.html").
COPY --from=build /src/web /app/web
USER nonroot:nonroot
EXPOSE 8081
ENTRYPOINT ["/app/api"]

# ---------- 3. worker image ----------------------------------------------------
FROM gcr.io/distroless/static-debian12:nonroot AS worker
WORKDIR /app
COPY --from=build /out/worker /app/worker
USER nonroot:nonroot
ENTRYPOINT ["/app/worker"]
