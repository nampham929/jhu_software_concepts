-- Run as admin to capture evidence for the report/PDF.
-- Replace placeholders before execution:
--   <APP_ROLE>

-- Role flags (should show non-superuser and no create privileges).
SELECT
    rolname,
    rolsuper,
    rolcreatedb,
    rolcreaterole,
    rolreplication
FROM pg_roles
WHERE rolname = '<APP_ROLE>';

-- Table privileges (should include only SELECT, INSERT).
SELECT
    grantee,
    table_schema,
    table_name,
    privilege_type
FROM information_schema.role_table_grants
WHERE grantee = '<APP_ROLE>'
  AND table_schema = 'public'
  AND table_name = 'applicants'
ORDER BY privilege_type;

-- Sequence privileges needed for SERIAL/identity inserts.
SELECT
    grantee,
    object_schema,
    object_name,
    privilege_type
FROM information_schema.role_usage_grants
WHERE grantee = '<APP_ROLE>'
  AND object_schema = 'public'
  AND object_name = 'applicants_p_id_seq'
ORDER BY privilege_type;

