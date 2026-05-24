-- Create separate databases for microservices
SELECT 'CREATE DATABASE orderdb' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'orderdb')\gexec
SELECT 'CREATE DATABASE trackingdb' WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'trackingdb')\gexec

GRANT ALL PRIVILEGES ON DATABASE orderdb TO smartlog;
GRANT ALL PRIVILEGES ON DATABASE trackingdb TO smartlog;
