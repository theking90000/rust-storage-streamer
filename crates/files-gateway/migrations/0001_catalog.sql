CREATE TABLE files (
    id TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL,
    completed_at INTEGER,
    name TEXT NOT NULL,
    content_type TEXT NOT NULL,
    expected_size INTEGER NOT NULL CHECK (expected_size >= 0),
    size INTEGER CHECK (size >= 0)
);

CREATE TABLE segments (
    id TEXT PRIMARY KEY,
    uri TEXT NOT NULL,
    decrypt_key BLOB NOT NULL CHECK (length(decrypt_key) = 32),
    cached_url TEXT,
    cached_url_expires_at INTEGER,
    created_at INTEGER NOT NULL,
    frame_count INTEGER NOT NULL CHECK (frame_count > 0),
    checksum BLOB NOT NULL CHECK (length(checksum) = 32),
    size INTEGER NOT NULL CHECK (size > 0)
);

CREATE TABLE file_segments (
    file_id TEXT NOT NULL REFERENCES files(id) ON DELETE CASCADE,
    segment_index INTEGER NOT NULL CHECK (segment_index >= 0),
    segment_id TEXT UNIQUE REFERENCES segments(id),
    started_at INTEGER NOT NULL,
    PRIMARY KEY (file_id, segment_index)
);

CREATE INDEX file_segments_segment_id ON file_segments(segment_id);
