CREATE TABLE IF NOT EXISTS "healthcare"."conditions"(

    id BIGSERIAL PRIMARY KEY,
    uuid UUID NOT NULL REFERENCES "healthcare"."patients"(uuid) ON DELETE CASCADE,
    condition_record_date DATE,
    date_of_abetment DATE,
    encounter_uuid UUID,
    fsn_id VARCHAR(255),
    medical_concepts TEXT,
    associated_semantics VARCHAR(100),
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_conditions_uuid ON healthcare.conditions(uuid);