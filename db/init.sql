-- 1.Trigram extension
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 2. Worker Nodes
CREATE TABLE IF NOT EXISTS worker_nodes (
    worker_id SERIAL PRIMARY KEY,
    
    -- Used by the Master (Control Plane) inside the VPC/Docker Network (e.g., "worker_1")
    internal_host VARCHAR(255) NOT NULL,
    internal_port INTEGER NOT NULL,
    
    -- Used by the Client (Data Plane) outside the VPC (e.g., "127.0.0.1" or "192.168.1.40")
    advertised_host VARCHAR(255) NOT NULL,
    advertised_port INTEGER NOT NULL,
    
    free_space BIGINT NOT NULL,
    status VARCHAR(20) DEFAULT 'ACTIVE', -- Can be 'ACTIVE', 'READ_ONLY', or 'OFFLINE'
    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Ensures a unique network boundary identity
    UNIQUE(internal_host, internal_port)
);

-- 3. Users Table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Files Table (Logical Directory Layer)
CREATE TABLE IF NOT EXISTS files (
    file_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    filename VARCHAR(255) NOT NULL,
    size BIGINT NOT NULL,
    chunks TEXT[] NOT NULL,             -- PostgreSQL native array of SHA-256 hashes
    is_public BOOLEAN DEFAULT FALSE,    -- FALSE = Private, TRUE = Campus Hub
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Chunks Table (Physical Deduplication & Location Layer)
CREATE TABLE IF NOT EXISTS chunks (
    chunk_hash VARCHAR(64) PRIMARY KEY, -- SHA-256 signature
    nodes TEXT[] NOT NULL,              -- Array of internal worker coordinates: {"worker_1:9001", "worker_2:9001"}
    ref_count INT DEFAULT 0             -- Starts at 0 for Two-Phase Commit
);

-- 6. Performance & Search Optimization Indexes
CREATE INDEX idx_files_user_public ON files(user_id, is_public);
CREATE INDEX idx_files_filename_trgm ON files USING gin (filename gin_trgm_ops); 