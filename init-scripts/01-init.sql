-- Create server
CREATE SERVER IF NOT EXISTS openaq_server
    FOREIGN DATA WRAPPER postgres_fdw;

-- The database openaq_db is already created by Docker Compose
-- through the POSTGRES_DB environment variable