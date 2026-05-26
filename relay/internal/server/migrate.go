package server

import (
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/postgres" // registers postgres:// scheme
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/adithya/relay/migrations"
)

// RunMigrations applies any pending migrations from the embedded FS against
// the supplied DSN. Idempotent: returns nil when there are no new migrations
// to apply.
//
// On PaaS deploys (Render, Fly, etc.) call this at boot before starting the
// HTTP server. On local docker compose the standalone `migrate` service
// handles it instead, so this call is gated by config.App.AutoMigrate.
func RunMigrations(dsn string) error {
	src, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("build embedded migration source: %w", err)
	}

	// Standard "postgres" driver accepts postgres:// DSNs and uses lib/pq for
	// the migration session only. Runtime traffic still flows through pgx via
	// pgxpool — this is only used for the one-shot Up() at boot.
	m, err := migrate.NewWithSourceInstance("iofs", src, dsn)
	if err != nil {
		return fmt.Errorf("init migrator: %w", err)
	}
	defer m.Close() //nolint:errcheck

	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("apply migrations: %w", err)
	}
	return nil
}
