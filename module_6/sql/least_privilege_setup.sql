-- Run this script as a privileged admin role (not as the app role).
-- Replace placeholders before execution:
--   <DB_NAME>, <APP_ROLE>, <APP_PASSWORD>

-- 1) Create the runtime app role with no elevated capabilities.
CREATE ROLE <APP_ROLE>
LOGIN
PASSWORD '<APP_PASSWORD>'
NOSUPERUSER
NOCREATEDB
NOCREATEROLE
NOREPLICATION;

-- 2) Allow connection and schema usage only where needed.
GRANT CONNECT ON DATABASE <DB_NAME> TO <APP_ROLE>;
GRANT USAGE ON SCHEMA public TO <APP_ROLE>;

-- 3) Grant least-privilege table permissions required by this app.
-- The app reads and inserts rows in applicants.
GRANT SELECT, INSERT ON TABLE public.applicants TO <APP_ROLE>;

-- 4) Serial/identity inserts need sequence privileges on the PK sequence.
GRANT USAGE, SELECT ON SEQUENCE public.applicants_p_id_seq TO <APP_ROLE>;

-- Optional hardening: ensure future tables are not auto-granted.
REVOKE ALL ON ALL TABLES IN SCHEMA public FROM <APP_ROLE>;
GRANT SELECT, INSERT ON TABLE public.applicants TO <APP_ROLE>;

