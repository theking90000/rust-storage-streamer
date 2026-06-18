use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

use frame_streamer::{SignedUrl, UploadResult};
use sqlx::{Row, SqlitePool};

#[derive(Clone)]
pub struct Catalog {
    pool: SqlitePool,
}

impl Catalog {
    pub async fn connect(url: &str) -> Result<Self, sqlx::Error> {
        let pool = sqlx::sqlite::SqlitePoolOptions::new()
            .max_connections(if url.contains(":memory:") { 1 } else { 4 })
            .connect(url)
            .await?;
        sqlx::query("PRAGMA journal_mode = WAL").execute(&pool).await?;
        sqlx::query("PRAGMA foreign_keys = ON").execute(&pool).await?;
        sqlx::query("PRAGMA busy_timeout = 5000").execute(&pool).await?;
        sqlx::migrate!().run(&pool).await?;
        sqlx::query("DELETE FROM file_segments WHERE segment_id IS NULL")
            .execute(&pool)
            .await?;
        Ok(Self { pool })
    }

    pub async fn create_file(&self, id: &str, expected_size: u64) -> Result<(), CatalogError> {
        sqlx::query("INSERT INTO files(id, created_at, expected_size) VALUES (?, ?, ?)")
            .bind(id)
            .bind(now())
            .bind(expected_size as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    pub async fn reserve_segment(&self, file_id: &str, index: u32) -> Result<(), CatalogError> {
        let mut tx = self.pool.begin().await?;
        let completed = sqlx::query("SELECT completed_at FROM files WHERE id = ?")
            .bind(file_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(CatalogError::NotFound)?
            .try_get::<Option<i64>, _>(0)?;
        if completed.is_some() {
            return Err(CatalogError::Completed);
        }
        let inserted = sqlx::query(
            "INSERT OR IGNORE INTO file_segments(file_id, segment_index, started_at) VALUES (?, ?, ?)",
        )
        .bind(file_id)
        .bind(i64::from(index))
        .bind(now())
        .execute(&mut *tx)
        .await?;
        if inserted.rows_affected() == 0 {
            return Err(CatalogError::IndexExists);
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn cancel_reservation(&self, file_id: &str, index: u32) {
        let _ = sqlx::query(
            "DELETE FROM file_segments WHERE file_id = ? AND segment_index = ? AND segment_id IS NULL",
        )
        .bind(file_id)
        .bind(i64::from(index))
        .execute(&self.pool)
        .await;
    }

    pub async fn attach_segment(
        &self,
        file_id: &str,
        index: u32,
        segment_id: &str,
        key: &[u8; 32],
        upload: &UploadResult,
    ) -> Result<(), CatalogError> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT expected_size, completed_at FROM files WHERE id = ?")
            .bind(file_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(CatalogError::NotFound)?;
        if row.try_get::<Option<i64>, _>(1)?.is_some() {
            return Err(CatalogError::Completed);
        }
        let expected: i64 = row.try_get(0)?;
        let used: i64 = sqlx::query(
            "SELECT COALESCE(SUM(s.size), 0) FROM file_segments fs JOIN segments s ON s.id = fs.segment_id WHERE fs.file_id = ?",
        )
        .bind(file_id)
        .fetch_one(&mut *tx)
        .await?
        .try_get(0)?;
        if used + upload.plaintext_size as i64 > expected {
            sqlx::query("DELETE FROM file_segments WHERE file_id = ? AND segment_index = ? AND segment_id IS NULL")
                .bind(file_id)
                .bind(i64::from(index))
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            return Err(CatalogError::AllocationExceeded);
        }
        let cached = upload.stored_object.cached_url.as_ref();
        sqlx::query(
            "INSERT INTO segments(id, uri, decrypt_key, cached_url, cached_url_expires_at, created_at, frame_count, checksum, size) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(segment_id)
        .bind(&upload.stored_object.uri)
        .bind(&key[..])
        .bind(cached.map(SignedUrl::as_str))
        .bind(cached.and_then(|url| url.expires_at()).map(system_time))
        .bind(now())
        .bind(i64::from(upload.frame_count))
        .bind(&upload.checksum[..])
        .bind(upload.plaintext_size as i64)
        .execute(&mut *tx)
        .await?;
        let updated = sqlx::query(
            "UPDATE file_segments SET segment_id = ? WHERE file_id = ? AND segment_index = ? AND segment_id IS NULL",
        )
        .bind(segment_id)
        .bind(file_id)
        .bind(i64::from(index))
        .execute(&mut *tx)
        .await?;
        if updated.rows_affected() != 1 {
            return Err(CatalogError::IndexExists);
        }
        tx.commit().await?;
        Ok(())
    }

    pub async fn complete_file(&self, file_id: &str, payload_size: usize) -> Result<(u64, i64), CatalogError> {
        let mut tx = self.pool.begin().await?;
        let row = sqlx::query("SELECT expected_size, completed_at FROM files WHERE id = ?")
            .bind(file_id)
            .fetch_optional(&mut *tx)
            .await?
            .ok_or(CatalogError::NotFound)?;
        if row.try_get::<Option<i64>, _>(1)?.is_some() {
            return Err(CatalogError::Completed);
        }
        let expected: i64 = row.try_get(0)?;
        let pending: i64 = sqlx::query("SELECT COUNT(*) FROM file_segments WHERE file_id = ? AND segment_id IS NULL")
            .bind(file_id).fetch_one(&mut *tx).await?.try_get(0)?;
        if pending != 0 { return Err(CatalogError::UploadInProgress); }
        let rows = sqlx::query(
            "SELECT fs.segment_index, s.size FROM file_segments fs JOIN segments s ON s.id = fs.segment_id WHERE fs.file_id = ? ORDER BY fs.segment_index",
        )
        .bind(file_id)
        .fetch_all(&mut *tx)
        .await?;
        let mut size = 0_u64;
        for (position, row) in rows.iter().enumerate() {
            let index: i64 = row.try_get(0)?;
            let segment_size: i64 = row.try_get(1)?;
            if index != position as i64 { return Err(CatalogError::SegmentGap); }
            if position + 1 < rows.len() && !(segment_size as usize).is_multiple_of(payload_size) {
                return Err(CatalogError::MisalignedSegment);
            }
            size += segment_size as u64;
        }
        if size > expected as u64 { return Err(CatalogError::AllocationExceeded); }
        let completed_at = now();
        sqlx::query("UPDATE files SET completed_at = ?, size = ? WHERE id = ?")
            .bind(completed_at).bind(size as i64).bind(file_id).execute(&mut *tx).await?;
        tx.commit().await?;
        Ok((size, completed_at))
    }
}

fn now() -> i64 { system_time(SystemTime::now()) }
fn system_time(time: SystemTime) -> i64 {
    time.duration_since(UNIX_EPOCH).unwrap_or_default().as_secs() as i64
}

#[derive(Debug)]
pub enum CatalogError {
    NotFound,
    Completed,
    IndexExists,
    AllocationExceeded,
    UploadInProgress,
    SegmentGap,
    MisalignedSegment,
    Database(sqlx::Error),
}

impl From<sqlx::Error> for CatalogError {
    fn from(value: sqlx::Error) -> Self { Self::Database(value) }
}

impl fmt::Display for CatalogError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound => f.write_str("file not found"),
            Self::Completed => f.write_str("file is already completed"),
            Self::IndexExists => f.write_str("segment index already exists"),
            Self::AllocationExceeded => f.write_str("file allocation exceeded"),
            Self::UploadInProgress => f.write_str("a segment upload is still in progress"),
            Self::SegmentGap => f.write_str("segment indices are not continuous"),
            Self::MisalignedSegment => f.write_str("a non-final segment is not frame-aligned"),
            Self::Database(error) => write!(f, "database: {error}"),
        }
    }
}

impl std::error::Error for CatalogError {}
