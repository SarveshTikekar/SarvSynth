CREATE TABLE IF NOT EXISTS "healthcare"."encounters" (
    id BIGSERIAL PRIMARY KEY,
    encounter_id UUID UNIQUE NOT NULL,
    visit_start TIMESTAMPTZ,
    visit_end TIMESTAMPTZ,
    uuid UUID NOT NULL REFERENCES "healthcare"."patients"(uuid) ON DELETE CASCADE,
    hospital_id UUID,
    practioner_id UUID,
    encounter_type VARCHAR(100),
    encounter_reason_id VARCHAR(100),
    encounter_reason TEXT,
    visiting_base_fees FLOAT,
    visting_total_fees FLOAT,
    coverage FLOAT,
    diagnosis_id VARCHAR(100),
    diagnosis TEXT,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_encounters_patient_uuid ON healthcare.encounters(uuid);
CREATE INDEX IF NOT EXISTS idx_encounters_start_date ON healthcare.encounters(visit_start);

-- Grant permissions (matching the previous pattern)
GRANT ALL ON TABLE "healthcare"."encounters" TO "postgres";
GRANT ALL ON TABLE "healthcare"."encounters" TO "anon";
GRANT ALL ON TABLE "healthcare"."encounters" TO "authenticated";
GRANT ALL ON TABLE "healthcare"."encounters" TO "service_role";