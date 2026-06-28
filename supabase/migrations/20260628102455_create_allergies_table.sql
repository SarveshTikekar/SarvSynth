CREATE TABLE if NOT EXISTS "healthcare"."allergies"(

    allergy_detection_date DATE,
    allergy_cure_date DATE,
    uuid UUID NOT NULL REFERENCES "healthcare"."patients"(uuid) ON DELETE CASCADE,
    encounter_id UUID NOT NULL REFERENCES "healthcare"."encounters"(encounter_id),
    allergy_code BIGINT,
    coding_system TEXT CHECK (coding_system in ('SNOMED-CT', 'RxNorm')),
    allergy_description TEXT,
    allergen_nature TEXT,
    allergy_type TEXT,
    category TEXT,
    primary_symptom_code BIGINT,
    primary_symptom_description TEXT,
    primary_symptom_nature TEXT,
    primary_symptom_severity TEXT CHECK (primary_symptom_severity in ('MODERATE', 'SEVERE', 'N/A', 'MILD')),
    secondary_symptom_code BIGINT,
    secondary_symptom_description TEXT,
    secondary_symptom_nature TEXT,
    secondary_symptom_severity TEXT CHECK (secondary_symptom_severity in ('MODERATE', 'SEVERE', 'N/A', 'MILD')),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY(uuid, allergy_code)
)
