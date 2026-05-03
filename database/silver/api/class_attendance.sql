-- Batch-level attendance summary, one row per batch per class date
-- present_count: students marked P (Present)
-- late_count: students marked L (Late) — treated as attended for attendance_pct
-- absent_count: students marked A (Absent)
-- attendance_pct: (present + late) / (present + late + absent) * 100

CREATE TABLE IF NOT EXISTS silver.class_attendance (
    id               SERIAL PRIMARY KEY,
    batch_id         BIGINT NOT NULL,
    bundle_id        BIGINT NOT NULL,
    class_date       DATE NOT NULL,
    class_number     INTEGER,
    present_count    INTEGER DEFAULT 0,
    late_count       INTEGER DEFAULT 0,
    absent_count     INTEGER DEFAULT 0,
    total_enrolled   INTEGER,
    attendance_pct   NUMERIC(5,2),
    pull_date        DATE,
    created_at       TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (batch_id, class_date)
);

ALTER TABLE silver.class_attendance ADD COLUMN IF NOT EXISTS late_count INTEGER DEFAULT 0;

CREATE INDEX IF NOT EXISTS idx_class_att_batch  ON silver.class_attendance(batch_id);
CREATE INDEX IF NOT EXISTS idx_class_att_bundle ON silver.class_attendance(bundle_id);
CREATE INDEX IF NOT EXISTS idx_class_att_date   ON silver.class_attendance(class_date);
