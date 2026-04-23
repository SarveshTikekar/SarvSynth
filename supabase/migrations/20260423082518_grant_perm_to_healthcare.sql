GRANT USAGE ON SCHEMA "healthcare" TO anon, authenticated, service_role;
GRANT ALL ON ALL TABLES IN SCHEMA "healthcare" TO anon, authenticated, service_role;
GRANT ALL ON ALL SEQUENCES IN SCHEMA "healthcare" TO anon, authenticated, service_role;
GRANT ALL ON ALL FUNCTIONS IN SCHEMA "healthcare" TO anon, authenticated, service_role;

-- Optional: Ensure any NEW tables created in the future also get these permissions
ALTER DEFAULT PRIVILEGES IN SCHEMA "healthcare" 
GRANT ALL ON TABLES TO anon, authenticated, service_role;