-- First create the extension
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Then create the server
CREATE SERVER IF NOT EXISTS openaq_server
    FOREIGN DATA WRAPPER postgres_fdw;