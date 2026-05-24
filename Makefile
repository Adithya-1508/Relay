.PHONY: run worker migrate-up migrate-down migrate-create sqlc-gen lint test test-integration docker-up docker-down



# ── binaries ─────────────────────────────────────────────────────────────────
run:
		go run ./cmd/api/...



worker:
		go run ./cmd/worker/...



build:
		go build -o bin/api ./cmd/api/...
		go build -o bin/worker ./cmd/worker/...



# ── database ─────────────────────────────────────────────────────────────────
migrate-up:
		migrate -path migrations -database "$(DATABASE_URL)" up



migrate-down:
		migrate -path migrations -database "$(DATABASE_URL)" down 1



migrate-create:
		migrate create -ext sql -dir migrations -seq $(name)



# ── code generation ───────────────────────────────────────────────────────────
sqlc-gen:
		sqlc generate



# ── quality ──────────────────────────────────────────────────────────────────
lint:
		golangci-lint run ./...



test:
		go test ./... -v -race -count=1



test-integration:
		go test ./tests/integration/... -v -race -count=1 -tags integration



# ── infrastructure ───────────────────────────────────────────────────────────
docker-up:
		docker compose up -d



docker-down:
		docker compose down



docker-logs:
		docker compose logs -f



# ── helpers ───────────────────────────────────────────────────────────────────
tidy:
		go mod tidy



.DEFAULT_GOAL := run