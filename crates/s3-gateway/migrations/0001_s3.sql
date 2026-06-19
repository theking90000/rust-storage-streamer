CREATE TABLE credentials (
    access_key TEXT PRIMARY KEY,
    secret_key TEXT NOT NULL,
    can_create_buckets INTEGER NOT NULL DEFAULT 0,
    enabled INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL
);

CREATE TABLE buckets (
    name TEXT PRIMARY KEY,
    created_at INTEGER NOT NULL
);

CREATE TABLE bucket_grants (
    bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
    access_key TEXT NOT NULL REFERENCES credentials(access_key) ON DELETE CASCADE,
    writable INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (bucket, access_key)
);

CREATE TABLE objects (
    id TEXT PRIMARY KEY,
    bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
    object_key TEXT NOT NULL,
    size INTEGER NOT NULL CHECK (size >= 0),
    etag TEXT NOT NULL,
    content_type TEXT,
    content_encoding TEXT,
    cache_control TEXT,
    content_disposition TEXT,
    content_language TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL,
    UNIQUE(bucket, object_key)
);

CREATE TABLE segments (
    id TEXT PRIMARY KEY,
    uri TEXT NOT NULL,
    decrypt_key BLOB NOT NULL CHECK(length(decrypt_key) = 32),
    cached_url TEXT,
    cached_url_expires_at INTEGER,
    frame_count INTEGER NOT NULL CHECK(frame_count > 0),
    plaintext_size INTEGER NOT NULL CHECK(plaintext_size > 0),
    orphaned_at INTEGER
);

CREATE TABLE object_segments (
    object_id TEXT NOT NULL REFERENCES objects(id) ON DELETE CASCADE,
    segment_index INTEGER NOT NULL,
    segment_id TEXT NOT NULL REFERENCES segments(id),
    PRIMARY KEY(object_id, segment_index)
);

CREATE TABLE multipart_uploads (
    id TEXT PRIMARY KEY,
    bucket TEXT NOT NULL REFERENCES buckets(name) ON DELETE CASCADE,
    object_key TEXT NOT NULL,
    owner_access_key TEXT NOT NULL REFERENCES credentials(access_key),
    content_type TEXT,
    content_encoding TEXT,
    cache_control TEXT,
    content_disposition TEXT,
    content_language TEXT,
    metadata_json TEXT NOT NULL DEFAULT '{}',
    created_at INTEGER NOT NULL
);

CREATE TABLE multipart_parts (
    upload_id TEXT NOT NULL REFERENCES multipart_uploads(id) ON DELETE CASCADE,
    part_number INTEGER NOT NULL CHECK(part_number BETWEEN 1 AND 10000),
    size INTEGER NOT NULL CHECK(size > 0),
    etag TEXT NOT NULL,
    PRIMARY KEY(upload_id, part_number)
);

CREATE TABLE part_segments (
    upload_id TEXT NOT NULL,
    part_number INTEGER NOT NULL,
    segment_index INTEGER NOT NULL,
    segment_id TEXT NOT NULL REFERENCES segments(id),
    PRIMARY KEY(upload_id, part_number, segment_index),
    FOREIGN KEY(upload_id, part_number) REFERENCES multipart_parts(upload_id, part_number) ON DELETE CASCADE
);

CREATE INDEX objects_listing ON objects(bucket, object_key);
CREATE INDEX object_segments_segment ON object_segments(segment_id);
CREATE INDEX part_segments_segment ON part_segments(segment_id);

