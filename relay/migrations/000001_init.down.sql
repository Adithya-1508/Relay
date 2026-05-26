-- 000001_init.down.sql

DROP TABLE IF EXISTS refresh_tokens;
DROP TABLE IF EXISTS users;
DROP TABLE IF EXISTS workspaces;
-- Extensions left in place; safe across re-migrations.
