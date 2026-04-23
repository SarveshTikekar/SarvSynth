-- Grant usage on all sequences (counters) in the healthcare folder
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA healthcare TO anon, authenticated, service_role;

-- Also ensure the table itself has full permissions for the ETL
GRANT ALL ON healthcare.conditions TO service_role;