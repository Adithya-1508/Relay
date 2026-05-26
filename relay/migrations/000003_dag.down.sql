-- 000003_dag.down.sql

DROP TABLE IF EXISTS job_dependencies;

ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_state_check;
ALTER TABLE jobs ADD CONSTRAINT jobs_state_check
    CHECK (state IN ('pending','running','succeeded','failed','dead','cancelled'));
