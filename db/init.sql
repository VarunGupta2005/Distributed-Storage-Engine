-- 1. Trigram extension for fast case-insensitive partial string matches
CREATE EXTENSION IF NOT EXISTS pg_trgm;

-- 2. Worker Nodes
CREATE TABLE IF NOT EXISTS worker_nodes (
    worker_id SERIAL PRIMARY KEY,
    internal_host VARCHAR(255) NOT NULL,
    internal_port INTEGER NOT NULL,
    advertised_host VARCHAR(255) NOT NULL,
    advertised_port INTEGER NOT NULL,
    free_space BIGINT NOT NULL,
    status VARCHAR(20) DEFAULT 'ACTIVE',
    last_heartbeat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(internal_host, internal_port)
);

-- 3. Users Table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    salt VARCHAR(64) NOT NULL,           
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 4. Files Table
CREATE TABLE IF NOT EXISTS files (
    file_id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    filename VARCHAR(255) NOT NULL,
    size BIGINT NOT NULL,
    chunks TEXT[] NOT NULL,
    is_public BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 5. Chunks Table (Normalized to use worker_id)
CREATE TABLE IF NOT EXISTS chunks (
    chunk_hash VARCHAR(64) PRIMARY KEY,
    nodes INTEGER[] NOT NULL,           -- Array of worker_ids: {1, 2}
    ref_count INT DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP 
);

-- 6. Indexes
CREATE INDEX IF NOT EXISTS idx_files_user_public ON files(user_id, is_public);
CREATE INDEX idx_files_filename_trgm ON files USING gin (filename gin_trgm_ops); 
CREATE INDEX IF NOT EXISTS idx_chunks_orphans_gc ON chunks(created_at) WHERE ref_count = 0;

-- 7. Clusters Table (Normalized to use worker_id)
CREATE TABLE IF NOT EXISTS clusters (
    cluster_id SERIAL PRIMARY KEY,
    cluster_name VARCHAR(50) UNIQUE NOT NULL,
    active_nodes INTEGER[] NOT NULL,    -- Array of worker_ids: {1, 2}
    status VARCHAR(20) DEFAULT 'ACTIVE'
);

-- 8. Sharded Replica Set Directory
CREATE TABLE IF NOT EXISTS shard_map (
    bucket_id INTEGER PRIMARY KEY,
    cluster_id INTEGER NOT NULL REFERENCES clusters(cluster_id)
);
