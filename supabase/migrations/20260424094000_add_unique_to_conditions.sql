-- Add unique constraint to conditions to support idempotent upserts
ALTER TABLE "healthcare"."conditions" 
ADD CONSTRAINT unique_condition_entry UNIQUE (uuid, encounter_uuid, fsn_id, condition_record_date);
