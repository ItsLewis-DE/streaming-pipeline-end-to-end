CREATE DATABASE pipeline_admin;

-- Additional databases required by other services
CREATE DATABASE metastore;

-- Service-specific users
CREATE USER hive WITH ENCRYPTED PASSWORD 'hive';

-- Grant privileges
GRANT ALL PRIVILEGES ON DATABASE pipeline_db TO pipeline_admin;
GRANT ALL PRIVILEGES ON DATABASE metastore TO hive;

\c pipeline_db
GRANT ALL ON SCHEMA public TO pipeline_admin;

\c metastore
GRANT ALL ON SCHEMA public TO hive;

-- Superset database and user
CREATE DATABASE superset_db;
CREATE USER superset WITH ENCRYPTED PASSWORD 'superset';
GRANT ALL PRIVILEGES ON DATABASE superset_db TO superset;

\c superset_db
GRANT ALL ON SCHEMA public TO superset;
