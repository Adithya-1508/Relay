// Package migrations exposes the SQL migration files as an embed.FS so the
// api binary can run them at boot on PaaS deploys (where there is no
// separate migrate sidecar). golang-migrate's iofs source driver consumes
// fs.FS directly, so no temp-file gymnastics needed.
package migrations

import "embed"

//go:embed *.sql
var FS embed.FS
