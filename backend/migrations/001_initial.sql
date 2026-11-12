-- Initial schema uses SQLAlchemy metadata; optional upgrade indexes:
CREATE INDEX window_study_split_idx ON climate_windows (study_id,split);
CREATE INDEX note_run_status_idx ON review_notes (run_id,status);
-- 中断恢复：既有库补领取/租约列并索引租约到期扫描（新库由 ensure_schema 自动补列）。
ALTER TABLE deviation_runs ADD COLUMN IF NOT EXISTS claim_token VARCHAR NOT NULL DEFAULT '';
ALTER TABLE deviation_runs ADD COLUMN IF NOT EXISTS claim_owner VARCHAR NOT NULL DEFAULT '';
ALTER TABLE deviation_runs ADD COLUMN IF NOT EXISTS attempt INTEGER NOT NULL DEFAULT 0;
ALTER TABLE deviation_runs ADD COLUMN IF NOT EXISTS lease_expires_at VARCHAR NOT NULL DEFAULT '';
ALTER TABLE deviation_runs ADD COLUMN IF NOT EXISTS started_at VARCHAR NOT NULL DEFAULT '';
ALTER TABLE deviation_runs ADD COLUMN IF NOT EXISTS finished_at VARCHAR NOT NULL DEFAULT '';
CREATE INDEX run_status_lease_idx ON deviation_runs (status,lease_expires_at);
