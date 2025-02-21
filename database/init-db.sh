#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    -- Check if databases exist before creating them
    SELECT 'CREATE DATABASE airflow_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'airflow_db')\gexec;
    SELECT 'CREATE DATABASE app_db' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'app_db')\gexec;

    -- Check if users exist before creating them
    DO
    \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'airflow_user') THEN
            CREATE USER airflow_user WITH PASSWORD "$AIRFLOW_DB_PASSWORD";
        END IF;
    END
    \$\$;

    DO
    \$\$
    BEGIN
        IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'app_user') THEN
            CREATE USER app_user WITH PASSWORD "$APP_DB_PASSWORD";
        END IF;
    END
    \$\$;

    -- Grant privileges
    GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
    GRANT ALL PRIVILEGES ON DATABASE app_db TO app_user;

    -- Grant read/write access to airflow_user for app_db (for summaries)
    GRANT CONNECT ON DATABASE app_db TO airflow_user;
    GRANT USAGE ON SCHEMA public TO airflow_user;
    GRANT INSERT, SELECT, UPDATE ON ALL TABLES IN SCHEMA public TO airflow_user;
    ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT INSERT, SELECT, UPDATE ON TABLES TO airflow_user;
EOSQL
