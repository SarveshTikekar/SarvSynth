-- Grant usage on all sequences in the healthcare schema to allow auto-incrementing IDs
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "healthcare" TO "postgres";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "healthcare" TO "anon";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "healthcare" TO "authenticated";
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA "healthcare" TO "service_role";
