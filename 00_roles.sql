-- Run ONCE as a superuser, connected to the target database:
--   psql -d electoral -v loader_pw="$LOADER_PW" -v app_pw="$APP_PW" -f sql/00_roles.sql
\set ON_ERROR_STOP on

-- Object owner: never logs in, so a leaked app/loader credential cannot alter schema.
SELECT 'CREATE ROLE electoral_owner NOLOGIN'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'electoral_owner') \gexec

-- Loader: can write data + refresh summaries. No DDL, no superuser.
SELECT 'CREATE ROLE electoral_loader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'electoral_loader') \gexec
SELECT format('ALTER ROLE electoral_loader PASSWORD %L', :'loader_pw') \gexec

-- App: read-only, only sees the "api" schema, short timeouts.
SELECT 'CREATE ROLE electoral_app LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOBYPASSRLS'
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'electoral_app') \gexec
SELECT format('ALTER ROLE electoral_app PASSWORD %L', :'app_pw') \gexec

-- Lock down the database and the public schema
REVOKE ALL ON DATABASE :"DBNAME" FROM PUBLIC;
GRANT CONNECT ON DATABASE :"DBNAME" TO electoral_loader, electoral_app;
GRANT CREATE  ON DATABASE :"DBNAME" TO electoral_owner;
REVOKE CREATE ON SCHEMA public FROM PUBLIC;
GRANT USAGE ON SCHEMA public TO electoral_owner, electoral_loader, electoral_app;  -- PostGIS lives here

-- Per-role guard rails
ALTER ROLE electoral_app
  SET search_path = api;
ALTER ROLE electoral_app SET default_transaction_read_only = on;
ALTER ROLE electoral_app SET statement_timeout = '15s';
ALTER ROLE electoral_app SET lock_timeout = '2s';
ALTER ROLE electoral_app SET idle_in_transaction_session_timeout = '10s';
ALTER ROLE electoral_app SET work_mem = '8MB';
ALTER ROLE electoral_app CONNECTION LIMIT 40;

ALTER ROLE electoral_loader SET statement_timeout = 0;
ALTER ROLE electoral_loader SET idle_in_transaction_session_timeout = '30min';
ALTER ROLE electoral_loader SET work_mem = '64MB';
ALTER ROLE electoral_loader CONNECTION LIMIT 3;
