use std::str::FromStr;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use frame_streamer::{DecryptKey, ObjectId, ObjectMeta, SignedUrl, StoredObject, UploadResult};
use s3s::dto::Metadata;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode};
use sqlx::{Row, SqlitePool};

#[derive(Clone)]
pub struct Catalog {
    pool: SqlitePool,
}

#[derive(Clone, Debug)]
pub struct ObjectRecord {
    pub id: String,
    pub bucket: String,
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_language: Option<String>,
    pub metadata: Metadata,
    pub created_at: i64,
    pub segments: Vec<SegmentRecord>,
}

#[derive(Clone, Debug)]
pub struct SegmentRecord {
    pub id: String,
    pub stored: StoredObject,
    pub decrypt_key: [u8; 32],
    pub frame_count: u32,
    pub plaintext_size: u64,
}

impl SegmentRecord {
    pub fn object_meta(&self) -> ObjectMeta {
        ObjectMeta {
            id: ObjectId::new(&self.id),
            uri: self.stored.uri.clone(),
            frame_count: self.frame_count,
            decrypt_key: DecryptKey::new(self.decrypt_key),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ObjectSummary {
    pub key: String,
    pub size: u64,
    pub etag: String,
    pub created_at: i64,
}

#[derive(Clone, Debug)]
pub struct MultipartRecord {
    pub bucket: String,
    pub key: String,
    pub owner_access_key: String,
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_language: Option<String>,
    pub metadata: Metadata,
}

#[derive(Clone, Debug)]
pub struct PartRecord {
    pub number: i32,
    pub size: u64,
    pub etag: String,
    pub segments: Vec<SegmentRecord>,
}

#[derive(Clone, Debug, Default)]
pub struct ObjectHeaders {
    pub content_type: Option<String>,
    pub content_encoding: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_language: Option<String>,
    pub metadata: Metadata,
}

impl Catalog {
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let mut options = SqliteConnectOptions::from_str(url)?
            .create_if_missing(true)
            .foreign_keys(true)
            .busy_timeout(Duration::from_secs(5));
        if !url.contains(":memory:") {
            options = options.journal_mode(SqliteJournalMode::Wal);
        }
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(if url.contains(":memory:") { 1 } else { 4 })
            .connect_with(options)
            .await?;
        sqlx::migrate!().run(&pool).await?;
        Ok(Self { pool })
    }

    pub async fn create_credential(
        &self,
        access_key: &str,
        secret_key: &str,
        can_create_buckets: bool,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO credentials(access_key, secret_key, can_create_buckets, created_at) VALUES (?, ?, ?, ?)")
            .bind(access_key).bind(secret_key).bind(can_create_buckets).bind(now())
            .execute(&self.pool).await?;
        Ok(())
    }

    pub async fn revoke_credential(&self, access_key: &str) -> Result<bool, sqlx::Error> {
        Ok(
            sqlx::query("UPDATE credentials SET enabled = 0 WHERE access_key = ?")
                .bind(access_key)
                .execute(&self.pool)
                .await?
                .rows_affected()
                == 1,
        )
    }

    pub async fn secret_key(&self, access_key: &str) -> Result<Option<String>, sqlx::Error> {
        sqlx::query_scalar(
            "SELECT secret_key FROM credentials WHERE access_key = ? AND enabled = 1",
        )
        .bind(access_key)
        .fetch_optional(&self.pool)
        .await
    }

    pub async fn grant(
        &self,
        access_key: &str,
        bucket: &str,
        writable: bool,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO bucket_grants(bucket, access_key, writable) VALUES (?, ?, ?) ON CONFLICT(bucket, access_key) DO UPDATE SET writable = excluded.writable")
            .bind(bucket).bind(access_key).bind(writable).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn permission(
        &self,
        access_key: &str,
        bucket: &str,
    ) -> Result<Option<bool>, sqlx::Error> {
        sqlx::query_scalar("SELECT writable FROM bucket_grants g JOIN credentials c USING(access_key) WHERE g.access_key = ? AND g.bucket = ? AND c.enabled = 1")
            .bind(access_key).bind(bucket).fetch_optional(&self.pool).await
    }

    pub async fn can_create_buckets(&self, access_key: &str) -> Result<bool, sqlx::Error> {
        Ok(sqlx::query_scalar::<_, bool>(
            "SELECT can_create_buckets FROM credentials WHERE access_key = ? AND enabled = 1",
        )
        .bind(access_key)
        .fetch_optional(&self.pool)
        .await?
        .unwrap_or(false))
    }

    pub async fn create_bucket(&self, name: &str, access_key: &str) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("INSERT INTO buckets(name, created_at) VALUES (?, ?)")
            .bind(name)
            .bind(now())
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT INTO bucket_grants(bucket, access_key, writable) VALUES (?, ?, 1)")
            .bind(name)
            .bind(access_key)
            .execute(&mut *tx)
            .await?;
        tx.commit().await
    }

    pub async fn list_buckets(&self, access_key: &str) -> Result<Vec<(String, i64)>, sqlx::Error> {
        let rows = sqlx::query("SELECT b.name, b.created_at FROM buckets b JOIN bucket_grants g ON g.bucket = b.name WHERE g.access_key = ? ORDER BY b.name")
            .bind(access_key).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|r| Ok((r.try_get(0)?, r.try_get(1)?)))
            .collect()
    }

    pub async fn bucket_exists(&self, name: &str) -> Result<bool, sqlx::Error> {
        Ok(
            sqlx::query_scalar::<_, i64>("SELECT 1 FROM buckets WHERE name = ?")
                .bind(name)
                .fetch_optional(&self.pool)
                .await?
                .is_some(),
        )
    }

    pub async fn delete_bucket(&self, name: &str) -> Result<bool, sqlx::Error> {
        let count: i64 = sqlx::query_scalar(
            "SELECT (SELECT COUNT(*) FROM objects WHERE bucket = ?) + (SELECT COUNT(*) FROM multipart_uploads WHERE bucket = ?)",
        )
            .bind(name)
            .bind(name)
            .fetch_one(&self.pool)
            .await?;
        if count != 0 {
            return Ok(false);
        }
        sqlx::query("DELETE FROM buckets WHERE name = ?")
            .bind(name)
            .execute(&self.pool)
            .await?;
        Ok(true)
    }

    pub async fn store_segment(
        &self,
        id: &str,
        key: &[u8; 32],
        result: &UploadResult,
    ) -> Result<SegmentRecord, sqlx::Error> {
        let cached = result.stored_object.cached_url.as_ref();
        sqlx::query("INSERT INTO segments(id, uri, decrypt_key, cached_url, cached_url_expires_at, frame_count, plaintext_size, orphaned_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id).bind(&result.stored_object.uri).bind(&key[..])
            .bind(cached.map(SignedUrl::as_str)).bind(cached.and_then(SignedUrl::expires_at).map(system_time))
            .bind(i64::from(result.frame_count)).bind(result.plaintext_size as i64).bind(now())
            .execute(&self.pool).await?;
        Ok(SegmentRecord {
            id: id.into(),
            stored: result.stored_object.clone(),
            decrypt_key: *key,
            frame_count: result.frame_count,
            plaintext_size: result.plaintext_size,
        })
    }

    pub async fn replace_object(
        &self,
        bucket: &str,
        key: &str,
        size: u64,
        etag: &str,
        headers: &ObjectHeaders,
        segments: &[SegmentRecord],
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let old: Option<String> =
            sqlx::query_scalar("SELECT id FROM objects WHERE bucket = ? AND object_key = ?")
                .bind(bucket)
                .bind(key)
                .fetch_optional(&mut *tx)
                .await?;
        if let Some(old) = old {
            sqlx::query("DELETE FROM objects WHERE id = ?")
                .bind(old)
                .execute(&mut *tx)
                .await?;
        }
        let id = uuid::Uuid::new_v4().to_string();
        insert_object(&mut tx, &id, bucket, key, size, etag, headers).await?;
        for (index, segment) in segments.iter().enumerate() {
            sqlx::query("INSERT INTO object_segments(object_id, segment_index, segment_id) VALUES (?, ?, ?)")
                .bind(&id).bind(index as i64).bind(&segment.id).execute(&mut *tx).await?;
            sqlx::query("UPDATE segments SET orphaned_at = NULL WHERE id = ?")
                .bind(&segment.id)
                .execute(&mut *tx)
                .await?;
        }
        mark_unreferenced(&mut tx).await?;
        tx.commit().await
    }

    pub async fn get_object(
        &self,
        bucket: &str,
        key: &str,
    ) -> Result<Option<ObjectRecord>, sqlx::Error> {
        let Some(row) = sqlx::query("SELECT id, size, etag, content_type, content_encoding, cache_control, content_disposition, content_language, metadata_json, created_at FROM objects WHERE bucket = ? AND object_key = ?")
            .bind(bucket).bind(key).fetch_optional(&self.pool).await? else { return Ok(None); };
        let id: String = row.try_get(0)?;
        let segments = self.segments("SELECT s.id, s.uri, s.decrypt_key, s.cached_url, s.cached_url_expires_at, s.frame_count, s.plaintext_size FROM object_segments os JOIN segments s ON s.id = os.segment_id WHERE os.object_id = ? ORDER BY os.segment_index", id.as_str()).await?;
        Ok(Some(ObjectRecord {
            id,
            bucket: bucket.into(),
            key: key.into(),
            size: row.try_get::<i64, _>(1)? as u64,
            etag: row.try_get(2)?,
            content_type: row.try_get(3)?,
            content_encoding: row.try_get(4)?,
            cache_control: row.try_get(5)?,
            content_disposition: row.try_get(6)?,
            content_language: row.try_get(7)?,
            metadata: serde_json::from_str(&row.try_get::<String, _>(8)?).unwrap_or_default(),
            created_at: row.try_get(9)?,
            segments,
        }))
    }

    pub async fn list_objects(
        &self,
        bucket: &str,
        prefix: &str,
        after: &str,
        limit: i64,
    ) -> Result<Vec<ObjectSummary>, sqlx::Error> {
        let rows = sqlx::query("SELECT object_key, size, etag, created_at FROM objects WHERE bucket = ? AND object_key LIKE ? ESCAPE '\\' AND object_key > ? ORDER BY object_key LIMIT ?")
            .bind(bucket).bind(format!("{}%", escape_like(prefix))).bind(after).bind(limit).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|r| {
                Ok(ObjectSummary {
                    key: r.try_get(0)?,
                    size: r.try_get::<i64, _>(1)? as u64,
                    etag: r.try_get(2)?,
                    created_at: r.try_get(3)?,
                })
            })
            .collect()
    }

    pub async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM objects WHERE bucket = ? AND object_key = ?")
            .bind(bucket)
            .bind(key)
            .execute(&mut *tx)
            .await?;
        mark_unreferenced(&mut tx).await?;
        tx.commit().await
    }

    pub async fn copy_object(
        &self,
        source: &ObjectRecord,
        bucket: &str,
        key: &str,
        headers: &ObjectHeaders,
    ) -> Result<(), sqlx::Error> {
        self.replace_object(
            bucket,
            key,
            source.size,
            &source.etag,
            headers,
            &source.segments,
        )
        .await
    }

    pub async fn create_multipart(
        &self,
        id: &str,
        bucket: &str,
        key: &str,
        access_key: &str,
        headers: &ObjectHeaders,
    ) -> Result<(), sqlx::Error> {
        sqlx::query("INSERT INTO multipart_uploads(id, bucket, object_key, owner_access_key, content_type, content_encoding, cache_control, content_disposition, content_language, metadata_json, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
            .bind(id).bind(bucket).bind(key).bind(access_key).bind(&headers.content_type).bind(&headers.content_encoding)
            .bind(&headers.cache_control).bind(&headers.content_disposition).bind(&headers.content_language)
            .bind(serde_json::to_string(&headers.metadata).unwrap()).bind(now()).execute(&self.pool).await?;
        Ok(())
    }

    pub async fn multipart(&self, id: &str) -> Result<Option<MultipartRecord>, sqlx::Error> {
        let Some(r) = sqlx::query("SELECT bucket, object_key, owner_access_key, content_type, content_encoding, cache_control, content_disposition, content_language, metadata_json FROM multipart_uploads WHERE id = ?")
            .bind(id).fetch_optional(&self.pool).await? else { return Ok(None); };
        Ok(Some(MultipartRecord {
            bucket: r.try_get(0)?,
            key: r.try_get(1)?,
            owner_access_key: r.try_get(2)?,
            content_type: r.try_get(3)?,
            content_encoding: r.try_get(4)?,
            cache_control: r.try_get(5)?,
            content_disposition: r.try_get(6)?,
            content_language: r.try_get(7)?,
            metadata: serde_json::from_str(&r.try_get::<String, _>(8)?).unwrap_or_default(),
        }))
    }

    pub async fn replace_part(
        &self,
        upload_id: &str,
        number: i32,
        size: u64,
        etag: &str,
        segments: &[SegmentRecord],
    ) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM multipart_parts WHERE upload_id = ? AND part_number = ?")
            .bind(upload_id)
            .bind(number)
            .execute(&mut *tx)
            .await?;
        sqlx::query(
            "INSERT INTO multipart_parts(upload_id, part_number, size, etag) VALUES (?, ?, ?, ?)",
        )
        .bind(upload_id)
        .bind(number)
        .bind(size as i64)
        .bind(etag)
        .execute(&mut *tx)
        .await?;
        for (index, segment) in segments.iter().enumerate() {
            sqlx::query("INSERT INTO part_segments(upload_id, part_number, segment_index, segment_id) VALUES (?, ?, ?, ?)").bind(upload_id).bind(number).bind(index as i64).bind(&segment.id).execute(&mut *tx).await?;
            sqlx::query("UPDATE segments SET orphaned_at = NULL WHERE id = ?")
                .bind(&segment.id)
                .execute(&mut *tx)
                .await?;
        }
        mark_unreferenced(&mut tx).await?;
        tx.commit().await
    }

    pub async fn parts(&self, upload_id: &str) -> Result<Vec<PartRecord>, sqlx::Error> {
        let rows = sqlx::query("SELECT part_number, size, etag FROM multipart_parts WHERE upload_id = ? ORDER BY part_number").bind(upload_id).fetch_all(&self.pool).await?;
        let mut out = Vec::with_capacity(rows.len());
        for r in rows {
            let number: i32 = r.try_get(0)?;
            let segments = self.segments("SELECT s.id, s.uri, s.decrypt_key, s.cached_url, s.cached_url_expires_at, s.frame_count, s.plaintext_size FROM part_segments ps JOIN segments s ON s.id = ps.segment_id WHERE ps.upload_id = ? AND ps.part_number = ? ORDER BY ps.segment_index", (upload_id, number)).await?;
            out.push(PartRecord {
                number,
                size: r.try_get::<i64, _>(1)? as u64,
                etag: r.try_get(2)?,
                segments,
            });
        }
        Ok(out)
    }

    pub async fn complete_multipart(
        &self,
        upload_id: &str,
        upload: &MultipartRecord,
        parts: &[PartRecord],
        etag: &str,
    ) -> Result<(), sqlx::Error> {
        let headers = ObjectHeaders {
            content_type: upload.content_type.clone(),
            content_encoding: upload.content_encoding.clone(),
            cache_control: upload.cache_control.clone(),
            content_disposition: upload.content_disposition.clone(),
            content_language: upload.content_language.clone(),
            metadata: upload.metadata.clone(),
        };
        let segments: Vec<_> = parts.iter().flat_map(|p| p.segments.clone()).collect();
        let size = parts.iter().map(|p| p.size).sum();
        let mut tx = self.pool.begin().await?;
        let old: Option<String> =
            sqlx::query_scalar("SELECT id FROM objects WHERE bucket = ? AND object_key = ?")
                .bind(&upload.bucket)
                .bind(&upload.key)
                .fetch_optional(&mut *tx)
                .await?;
        if let Some(old) = old {
            sqlx::query("DELETE FROM objects WHERE id = ?")
                .bind(old)
                .execute(&mut *tx)
                .await?;
        }
        let id = uuid::Uuid::new_v4().to_string();
        insert_object(
            &mut tx,
            &id,
            &upload.bucket,
            &upload.key,
            size,
            etag,
            &headers,
        )
        .await?;
        for (index, segment) in segments.iter().enumerate() {
            sqlx::query("INSERT INTO object_segments(object_id, segment_index, segment_id) VALUES (?, ?, ?)").bind(&id).bind(index as i64).bind(&segment.id).execute(&mut *tx).await?;
        }
        sqlx::query("DELETE FROM multipart_uploads WHERE id = ?")
            .bind(upload_id)
            .execute(&mut *tx)
            .await?;
        mark_unreferenced(&mut tx).await?;
        tx.commit().await
    }

    pub async fn abort_multipart(&self, id: &str) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("DELETE FROM multipart_uploads WHERE id = ?")
            .bind(id)
            .execute(&mut *tx)
            .await?;
        mark_unreferenced(&mut tx).await?;
        tx.commit().await
    }

    pub async fn orphaned_segments(&self, before: i64) -> Result<Vec<SegmentRecord>, sqlx::Error> {
        self.segments("SELECT id, uri, decrypt_key, cached_url, cached_url_expires_at, frame_count, plaintext_size FROM segments WHERE orphaned_at IS NOT NULL AND orphaned_at < ?", &before).await
    }

    pub async fn forget_segment(&self, id: &str) -> Result<(), sqlx::Error> {
        sqlx::query("DELETE FROM segments WHERE id = ? AND orphaned_at IS NOT NULL")
            .bind(id)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    async fn segments<'a, A>(&self, query: &str, args: A) -> Result<Vec<SegmentRecord>, sqlx::Error>
    where
        A: SegmentArgs<'a>,
    {
        let rows = args.fetch(query, &self.pool).await?;
        rows.into_iter().map(segment_from_row).collect()
    }
}

#[async_trait::async_trait]
trait SegmentArgs<'a> {
    async fn fetch(
        self,
        query: &str,
        pool: &SqlitePool,
    ) -> Result<Vec<sqlx::sqlite::SqliteRow>, sqlx::Error>;
}
#[async_trait::async_trait]
impl<'a> SegmentArgs<'a> for &'a str {
    async fn fetch(
        self,
        q: &str,
        p: &SqlitePool,
    ) -> Result<Vec<sqlx::sqlite::SqliteRow>, sqlx::Error> {
        sqlx::query(q).bind(self).fetch_all(p).await
    }
}
#[async_trait::async_trait]
impl<'a> SegmentArgs<'a> for &'a i64 {
    async fn fetch(
        self,
        q: &str,
        p: &SqlitePool,
    ) -> Result<Vec<sqlx::sqlite::SqliteRow>, sqlx::Error> {
        sqlx::query(q).bind(*self).fetch_all(p).await
    }
}
#[async_trait::async_trait]
impl<'a> SegmentArgs<'a> for (&'a str, i32) {
    async fn fetch(
        self,
        q: &str,
        p: &SqlitePool,
    ) -> Result<Vec<sqlx::sqlite::SqliteRow>, sqlx::Error> {
        sqlx::query(q).bind(self.0).bind(self.1).fetch_all(p).await
    }
}

fn segment_from_row(r: sqlx::sqlite::SqliteRow) -> Result<SegmentRecord, sqlx::Error> {
    let key: Vec<u8> = r.try_get(2)?;
    let decrypt_key: [u8; 32] = key
        .try_into()
        .map_err(|_| sqlx::Error::Decode("invalid decrypt key".into()))?;
    let cached: Option<String> = r.try_get(3)?;
    let expires: Option<i64> = r.try_get(4)?;
    Ok(SegmentRecord {
        id: r.try_get(0)?,
        stored: StoredObject {
            uri: r.try_get(1)?,
            cached_url: cached.map(|u| {
                SignedUrl::new(
                    u,
                    expires.map(|v| UNIX_EPOCH + Duration::from_secs(v as u64)),
                )
            }),
        },
        decrypt_key,
        frame_count: r.try_get::<i64, _>(5)? as u32,
        plaintext_size: r.try_get::<i64, _>(6)? as u64,
    })
}

async fn insert_object(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    id: &str,
    bucket: &str,
    key: &str,
    size: u64,
    etag: &str,
    h: &ObjectHeaders,
) -> Result<(), sqlx::Error> {
    sqlx::query("INSERT INTO objects(id,bucket,object_key,size,etag,content_type,content_encoding,cache_control,content_disposition,content_language,metadata_json,created_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)")
        .bind(id).bind(bucket).bind(key).bind(size as i64).bind(etag).bind(&h.content_type).bind(&h.content_encoding).bind(&h.cache_control).bind(&h.content_disposition).bind(&h.content_language).bind(serde_json::to_string(&h.metadata).unwrap()).bind(now()).execute(&mut **tx).await?;
    Ok(())
}

async fn mark_unreferenced(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
) -> Result<(), sqlx::Error> {
    sqlx::query("UPDATE segments SET orphaned_at = COALESCE(orphaned_at, ?) WHERE NOT EXISTS (SELECT 1 FROM object_segments o WHERE o.segment_id=segments.id) AND NOT EXISTS (SELECT 1 FROM part_segments p WHERE p.segment_id=segments.id)").bind(now()).execute(&mut **tx).await?;
    Ok(())
}

fn escape_like(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('%', "\\%")
        .replace('_', "\\_")
}
fn now() -> i64 {
    system_time(SystemTime::now())
}
fn system_time(t: SystemTime) -> i64 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}
