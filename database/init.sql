-- Ensure the airflow_db database exists
DO $$  
BEGIN  
   IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'airflow_db') THEN  
      CREATE DATABASE airflow_db OWNER airflow_user;  
   END IF;  
END $$;

-- Ensure the app_db database exists
DO $$  
BEGIN  
   IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'app_db') THEN  
      CREATE DATABASE app_db OWNER app_user;  
   END IF;  
END $$;

-- Ensure the airflow_user exists
DO $$  
BEGIN  
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow_user') THEN  
      CREATE USER airflow_user WITH PASSWORD 'airflowpassword';  
   END IF;  
END $$;

-- Ensure the app_user exists
DO $$  
BEGIN  
   IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_user') THEN  
      CREATE USER app_user WITH PASSWORD 'appsecurepassword';  
   END IF;  
END $$;

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow_user;
GRANT ALL PRIVILEGES ON DATABASE app_db TO app_user;

-- Grant read/write access to airflow_user for app_db (for summaries)
GRANT CONNECT ON DATABASE app_db TO airflow_user;
GRANT USAGE ON SCHEMA public TO airflow_user;
GRANT INSERT, SELECT, UPDATE ON ALL TABLES IN SCHEMA public TO airflow_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT INSERT, SELECT, UPDATE ON TABLES TO airflow_user;
