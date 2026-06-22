use std::collections::HashSet;
use std::io;
use std::ops::Range as ByteRange;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use base64::Engine;
use bytes::{Bytes, BytesMut};
use frame_streamer::{
    ByteRate, ByteRequest, ByteStream, ByteStreamConfig, ByteUpload, DecryptKey,
    EncryptedBytesDownloadBackend, FrameBudget, ObjectId, StorageBackend, UploadBackend,
    UploadObject,
};
use futures_util::{StreamExt, sink, stream};
use md5::{Digest, Md5};
use s3s::dto::*;
use s3s::{S3, S3Request, S3Response, S3Result, s3_error};

use crate::catalog::{Catalog, ObjectHeaders, ObjectRecord, PartRecord, SegmentRecord};

const MIN_MULTIPART_PART: u64 = 5 * 1024 * 1024;

#[derive(Clone)]
pub struct S3Storage {
    catalog: Catalog,
    upload_backend: Arc<dyn UploadBackend>,
    download_backend: Arc<dyn EncryptedBytesDownloadBackend>,
    frame_budget: FrameBudget,
    stream_config: ByteStreamConfig,
    allocated_rate: ByteRate,
    frame_size: usize,
    max_object_size: u64,
}

impl S3Storage {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        catalog: Catalog,
        storage: StorageBackend,
        frame_budget: FrameBudget,
        stream_config: ByteStreamConfig,
        allocated_rate: ByteRate,
        frame_size: usize,
        max_object_size: u64,
    ) -> Self {
        Self {
            catalog,
            upload_backend: storage.upload,
            download_backend: storage.download,
            frame_budget,
            stream_config,
            allocated_rate,
            frame_size,
            max_object_size,
        }
    }

    pub async fn collect_garbage(&self) -> Result<usize, frame_streamer::BoxError> {
        let before = now() - 3600;
        let segments = self.catalog.orphaned_segments(before).await?;
        let mut deleted = 0;
        for segment in segments {
            if self
                .upload_backend
                .delete(segment.stored.clone())
                .await
                .is_ok()
            {
                self.catalog.forget_segment(&segment.id).await?;
                deleted += 1;
            }
        }
        Ok(deleted)
    }

    async fn read_allowed(&self, access: &str, bucket: &str) -> S3Result<()> {
        match self
            .catalog
            .permission(access, bucket)
            .await
            .map_err(internal)?
        {
            Some(_) => Ok(()),
            None => Err(s3_error!(AccessDenied)),
        }
    }

    async fn write_allowed(&self, access: &str, bucket: &str) -> S3Result<()> {
        match self
            .catalog
            .permission(access, bucket)
            .await
            .map_err(internal)?
        {
            Some(true) => Ok(()),
            _ => Err(s3_error!(AccessDenied)),
        }
    }

    async fn upload_body(
        &self,
        body: Option<StreamingBlob>,
        expected_md5: Option<&str>,
    ) -> S3Result<(Vec<SegmentRecord>, u64, String)> {
        let Some(mut body) = body else {
            let digest = Md5::digest([]);
            verify_md5(expected_md5, &digest)?;
            return Ok((Vec::new(), 0, hex::encode(digest)));
        };
        let payload = self
            .frame_size
            .checked_sub(frame_streamer::TAG_SIZE)
            .ok_or_else(|| s3_error!(InternalError))?;
        let max =
            (u64::from(self.upload_backend.max_frames_per_segment()) * payload as u64) as usize;
        if max == 0 {
            return Err(s3_error!(InternalError, "upload backend has zero capacity"));
        }
        let mut pending = BytesMut::with_capacity(max);
        let mut segments = Vec::new();
        let mut size = 0_u64;
        let mut md5 = Md5::new();
        while let Some(chunk) = body.next().await {
            let chunk = chunk.map_err(internal)?;
            size = size
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| s3_error!(EntityTooLarge))?;
            if size > self.max_object_size {
                return Err(s3_error!(EntityTooLarge));
            }
            md5.update(&chunk);
            let mut offset = 0;
            while offset < chunk.len() {
                let take = (max - pending.len()).min(chunk.len() - offset);
                pending.extend_from_slice(&chunk[offset..offset + take]);
                offset += take;
                if pending.len() == max {
                    segments.push(self.upload_segment(pending.split().freeze()).await?);
                }
            }
        }
        if !pending.is_empty() {
            segments.push(self.upload_segment(pending.freeze()).await?);
        }
        let digest = md5.finalize();
        verify_md5(expected_md5, &digest)?;
        Ok((segments, size, hex::encode(digest)))
    }

    async fn upload_segment(&self, bytes: Bytes) -> S3Result<SegmentRecord> {
        let id = uuid::Uuid::new_v4().to_string();
        let key: [u8; 32] = rand::random();
        let uploader =
            ByteUpload::new(self.upload_backend.clone(), self.frame_size).map_err(internal)?;
        let result = uploader
            .upload(
                UploadObject {
                    id: ObjectId::new(&id),
                    key: DecryptKey::new(key),
                },
                stream::iter([Ok::<_, io::Error>(bytes.clone())]),
                Some(bytes.len() as u64),
            )
            .await
            .map_err(internal)?;
        match self.catalog.store_segment(&id, &key, &result).await {
            Ok(segment) => Ok(segment),
            Err(error) => {
                let _ = self.upload_backend.delete(result.stored_object).await;
                Err(internal(error))
            }
        }
    }

    fn body_for(&self, object: &ObjectRecord, range: ByteRange<u64>) -> StreamingBlob {
        if range.is_empty() {
            return StreamingBlob::wrap(stream::empty::<Result<Bytes, io::Error>>());
        }
        // The S3 body is pull-based, while StreamDriver uses a Sink to preserve
        // prefetch under backpressure. This bridge may hold one frame outside
        // FrameBudget and stays local because the adapter is intentionally tiny.
        let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Bytes, io::Error>>(1);
        let segments = object.segments.clone();
        let backend = self.download_backend.clone();
        let budget = self.frame_budget.clone();
        let config = self.stream_config;
        let rate = self.allocated_rate;
        tokio::spawn(async move {
            let mut logical = 0_u64;
            for segment in segments {
                let end = logical + segment.plaintext_size;
                let start = range.start.max(logical);
                let stop = range.end.min(end);
                if start < stop {
                    let local = (start - logical)..(stop - logical);
                    let byte_stream = match ByteStream::new(
                        stream::once(async move { Ok(segment.object_meta()) }),
                        backend.clone(),
                        match ByteRequest::new(local, rate) {
                            Ok(v) => v,
                            Err(e) => {
                                let _ = sender.send(Err(io::Error::other(e))).await;
                                return;
                            }
                        },
                        budget.clone(),
                        config,
                    ) {
                        Ok(v) => v,
                        Err(e) => {
                            let _ = sender.send(Err(io::Error::other(e))).await;
                            return;
                        }
                    };
                    let output = sink::unfold(sender.clone(), |sender, bytes| async move {
                        sender.send(Ok(bytes)).await.map_err(|_| {
                            io::Error::new(io::ErrorKind::BrokenPipe, "client disconnected")
                        })?;
                        Ok::<_, io::Error>(sender)
                    });
                    if let Err(error) = byte_stream.pipe_into(output).await {
                        let _ = sender.send(Err(io::Error::other(error.to_string()))).await;
                        return;
                    }
                }
                logical = end;
                if logical >= range.end {
                    break;
                }
            }
        });
        StreamingBlob::wrap(tokio_stream::wrappers::ReceiverStream::new(receiver))
    }
}

#[async_trait::async_trait]
impl S3 for S3Storage {
    async fn list_buckets(
        &self,
        req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let access = access_key(&req)?;
        let buckets = self
            .catalog
            .list_buckets(access)
            .await
            .map_err(internal)?
            .into_iter()
            .map(|(name, created)| Bucket {
                name: Some(name),
                creation_date: Some(timestamp(created)),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(buckets),
            ..Default::default()
        }))
    }

    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let access = access_key(&req)?.to_owned();
        if !self
            .catalog
            .can_create_buckets(&access)
            .await
            .map_err(internal)?
        {
            return Err(s3_error!(AccessDenied));
        }
        if self
            .catalog
            .bucket_exists(&req.input.bucket)
            .await
            .map_err(internal)?
        {
            return Err(s3_error!(BucketAlreadyExists));
        }
        self.catalog
            .create_bucket(&req.input.bucket, &access)
            .await
            .map_err(internal)?;
        Ok(S3Response::new(CreateBucketOutput::default()))
    }

    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        self.read_allowed(access_key(&req)?, &req.input.bucket)
            .await?;
        Ok(S3Response::new(HeadBucketOutput::default()))
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.write_allowed(access_key(&req)?, &req.input.bucket)
            .await?;
        if !self
            .catalog
            .delete_bucket(&req.input.bucket)
            .await
            .map_err(internal)?
        {
            return Err(s3_error!(BucketNotEmpty));
        }
        Ok(S3Response::new(DeleteBucketOutput::default()))
    }

    async fn get_bucket_versioning(
        &self,
        req: S3Request<GetBucketVersioningInput>,
    ) -> S3Result<S3Response<GetBucketVersioningOutput>> {
        self.read_allowed(access_key(&req)?, &req.input.bucket)
            .await?;
        Ok(S3Response::new(GetBucketVersioningOutput::default()))
    }

    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        self.write_allowed(access_key(&req)?, &req.input.bucket)
            .await?;
        let input = req.input;
        let headers = headers_from_put(&input);
        let expected_md5 = input.content_md5.clone();
        let (segments, size, etag) = self
            .upload_body(input.body, expected_md5.as_deref())
            .await?;
        self.catalog
            .replace_object(&input.bucket, &input.key, size, &etag, &headers, &segments)
            .await
            .map_err(internal)?;
        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(ETag::Strong(etag)),
            size: Some(size as i64),
            ..Default::default()
        }))
    }

    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        self.read_allowed(access_key(&req)?, &req.input.bucket)
            .await?;
        let object = self
            .catalog
            .get_object(&req.input.bucket, &req.input.key)
            .await
            .map_err(internal)?
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        Ok(S3Response::new(head_output(&object)))
    }

    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        self.read_allowed(access_key(&req)?, &req.input.bucket)
            .await?;
        let object = self
            .catalog
            .get_object(&req.input.bucket, &req.input.key)
            .await
            .map_err(internal)?
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let range = match req.input.range {
            Some(r) => r.check(object.size)?,
            None => 0..object.size,
        };
        let partial = range != (0..object.size);
        let mut output = GetObjectOutput {
            body: Some(self.body_for(&object, range.clone())),
            content_length: Some((range.end - range.start) as i64),
            content_type: object.content_type.clone(),
            content_encoding: object.content_encoding.clone(),
            cache_control: object.cache_control.clone(),
            content_disposition: object.content_disposition.clone(),
            content_language: object.content_language.clone(),
            metadata: Some(object.metadata.clone()),
            e_tag: Some(ETag::Strong(object.etag.clone())),
            last_modified: Some(timestamp(object.created_at)),
            accept_ranges: Some("bytes".into()),
            ..Default::default()
        };
        if partial {
            output.content_range = Some(format!(
                "bytes {}-{}/{}",
                range.start,
                range.end - 1,
                object.size
            ));
        }
        Ok(S3Response::new(output))
    }

    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.write_allowed(access_key(&req)?, &req.input.bucket)
            .await?;
        self.catalog
            .delete_object(&req.input.bucket, &req.input.key)
            .await
            .map_err(internal)?;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    async fn delete_objects(
        &self,
        req: S3Request<DeleteObjectsInput>,
    ) -> S3Result<S3Response<DeleteObjectsOutput>> {
        self.write_allowed(access_key(&req)?, &req.input.bucket)
            .await?;
        let quiet = req.input.delete.quiet.unwrap_or(false);
        let mut deleted = Vec::new();
        for object in req.input.delete.objects {
            self.catalog
                .delete_object(&req.input.bucket, &object.key)
                .await
                .map_err(internal)?;
            if !quiet {
                deleted.push(DeletedObject {
                    key: Some(object.key),
                    ..Default::default()
                });
            }
        }
        Ok(S3Response::new(DeleteObjectsOutput {
            deleted: Some(deleted),
            ..Default::default()
        }))
    }

    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        self.read_allowed(access_key(&req)?, &req.input.bucket)
            .await?;
        let prefix = req.input.prefix.clone().unwrap_or_default();
        let after = req
            .input
            .continuation_token
            .clone()
            .or(req.input.start_after.clone())
            .unwrap_or_default();
        let max = req.input.max_keys.unwrap_or(1000).clamp(0, 1000);
        let rows = if max == 0 {
            Vec::new()
        } else {
            self.catalog
                .list_objects(&req.input.bucket, &prefix, &after, i64::from(max) + 1)
                .await
                .map_err(internal)?
        };
        let truncated = rows.len() > max as usize;
        let next = truncated.then(|| rows[max as usize - 1].key.clone());
        let mut contents = Vec::new();
        let mut common = HashSet::new();
        for row in rows.into_iter().take(max as usize) {
            if let Some(delimiter) = req.input.delimiter.as_deref()
                && let Some(pos) = row.key[prefix.len()..].find(delimiter)
            {
                common.insert(row.key[..prefix.len() + pos + delimiter.len()].to_owned());
                continue;
            }
            contents.push(Object {
                key: Some(row.key),
                size: Some(row.size as i64),
                e_tag: Some(ETag::Strong(row.etag)),
                last_modified: Some(timestamp(row.created_at)),
                ..Default::default()
            });
        }
        let common_prefixes = common
            .into_iter()
            .map(|prefix| CommonPrefix {
                prefix: Some(prefix),
            })
            .collect::<Vec<_>>();
        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(req.input.bucket),
            prefix: Some(prefix),
            max_keys: Some(max),
            key_count: Some((contents.len() + common_prefixes.len()) as i32),
            is_truncated: Some(truncated),
            next_continuation_token: next,
            contents: Some(contents),
            common_prefixes: Some(common_prefixes),
            delimiter: req.input.delimiter,
            ..Default::default()
        }))
    }

    async fn list_objects(
        &self,
        req: S3Request<ListObjectsInput>,
    ) -> S3Result<S3Response<ListObjectsOutput>> {
        let marker = req.input.marker.clone();
        let v2 = self
            .list_objects_v2(req.map_input(Into::into))
            .await?
            .output;
        Ok(S3Response::new(ListObjectsOutput {
            name: v2.name,
            prefix: v2.prefix,
            marker,
            max_keys: v2.max_keys,
            is_truncated: v2.is_truncated,
            contents: v2.contents,
            common_prefixes: v2.common_prefixes,
            delimiter: v2.delimiter,
            next_marker: v2.next_continuation_token,
            encoding_type: v2.encoding_type,
            request_charged: v2.request_charged,
        }))
    }

    async fn create_multipart_upload(
        &self,
        req: S3Request<CreateMultipartUploadInput>,
    ) -> S3Result<S3Response<CreateMultipartUploadOutput>> {
        let access = access_key(&req)?.to_owned();
        self.write_allowed(&access, &req.input.bucket).await?;
        let id = uuid::Uuid::new_v4().to_string();
        let headers = headers_from_multipart(&req.input);
        self.catalog
            .create_multipart(&id, &req.input.bucket, &req.input.key, &access, &headers)
            .await
            .map_err(internal)?;
        Ok(S3Response::new(CreateMultipartUploadOutput {
            bucket: Some(req.input.bucket),
            key: Some(req.input.key),
            upload_id: Some(id),
            ..Default::default()
        }))
    }

    async fn upload_part(
        &self,
        req: S3Request<UploadPartInput>,
    ) -> S3Result<S3Response<UploadPartOutput>> {
        self.write_allowed(access_key(&req)?, &req.input.bucket)
            .await?;
        if !(1..=10_000).contains(&req.input.part_number) {
            return Err(s3_error!(InvalidArgument));
        }
        let upload = self
            .catalog
            .multipart(&req.input.upload_id)
            .await
            .map_err(internal)?
            .ok_or_else(|| s3_error!(NoSuchUpload))?;
        if upload.bucket != req.input.bucket || upload.key != req.input.key {
            return Err(s3_error!(NoSuchUpload));
        }
        let expected_md5 = req.input.content_md5.clone();
        let (segments, size, etag) = self
            .upload_body(req.input.body, expected_md5.as_deref())
            .await?;
        if size == 0 {
            return Err(s3_error!(EntityTooSmall));
        }
        self.catalog
            .replace_part(
                &req.input.upload_id,
                req.input.part_number,
                size,
                &etag,
                &segments,
            )
            .await
            .map_err(internal)?;
        Ok(S3Response::new(UploadPartOutput {
            e_tag: Some(ETag::Strong(etag)),
            ..Default::default()
        }))
    }

    async fn complete_multipart_upload(
        &self,
        req: S3Request<CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<CompleteMultipartUploadOutput>> {
        self.write_allowed(access_key(&req)?, &req.input.bucket)
            .await?;
        let upload = self
            .catalog
            .multipart(&req.input.upload_id)
            .await
            .map_err(internal)?
            .ok_or_else(|| s3_error!(NoSuchUpload))?;
        if upload.bucket != req.input.bucket || upload.key != req.input.key {
            return Err(s3_error!(NoSuchUpload));
        }
        let stored = self
            .catalog
            .parts(&req.input.upload_id)
            .await
            .map_err(internal)?;
        let requested = req
            .input
            .multipart_upload
            .and_then(|m| m.parts)
            .ok_or_else(|| s3_error!(InvalidPart))?;
        let mut selected = Vec::with_capacity(requested.len());
        let mut previous = 0;
        for wanted in requested {
            let number = wanted.part_number.ok_or_else(|| s3_error!(InvalidPart))?;
            if number <= previous {
                return Err(s3_error!(InvalidPartOrder));
            }
            previous = number;
            let part = stored
                .iter()
                .find(|part| part.number == number)
                .ok_or_else(|| s3_error!(InvalidPart))?;
            if wanted
                .e_tag
                .as_ref()
                .is_none_or(|tag| tag.value() != part.etag)
            {
                return Err(s3_error!(InvalidPart));
            }
            selected.push(part.clone());
        }
        for part in selected.iter().take(selected.len().saturating_sub(1)) {
            if part.size < MIN_MULTIPART_PART {
                return Err(s3_error!(EntityTooSmall));
            }
        }
        if selected.iter().map(|part| part.size).sum::<u64>() > self.max_object_size {
            return Err(s3_error!(EntityTooLarge));
        }
        let etag = multipart_etag(&selected)?;
        self.catalog
            .complete_multipart(&req.input.upload_id, &upload, &selected, &etag)
            .await
            .map_err(internal)?;
        Ok(S3Response::new(CompleteMultipartUploadOutput {
            bucket: Some(upload.bucket),
            key: Some(upload.key),
            e_tag: Some(ETag::Strong(etag)),
            ..Default::default()
        }))
    }

    async fn abort_multipart_upload(
        &self,
        req: S3Request<AbortMultipartUploadInput>,
    ) -> S3Result<S3Response<AbortMultipartUploadOutput>> {
        self.write_allowed(access_key(&req)?, &req.input.bucket)
            .await?;
        let upload = self
            .catalog
            .multipart(&req.input.upload_id)
            .await
            .map_err(internal)?
            .ok_or_else(|| s3_error!(NoSuchUpload))?;
        if upload.bucket != req.input.bucket || upload.key != req.input.key {
            return Err(s3_error!(NoSuchUpload));
        }
        self.catalog
            .abort_multipart(&req.input.upload_id)
            .await
            .map_err(internal)?;
        Ok(S3Response::new(AbortMultipartUploadOutput::default()))
    }

    async fn copy_object(
        &self,
        req: S3Request<CopyObjectInput>,
    ) -> S3Result<S3Response<CopyObjectOutput>> {
        let access = access_key(&req)?.to_owned();
        self.write_allowed(&access, &req.input.bucket).await?;
        let (source_bucket, source_key) = match &req.input.copy_source {
            CopySource::Bucket { bucket, key, .. } => (&**bucket, &**key),
            _ => return Err(s3_error!(InvalidRequest)),
        };
        self.read_allowed(&access, source_bucket).await?;
        let source = self
            .catalog
            .get_object(source_bucket, source_key)
            .await
            .map_err(internal)?
            .ok_or_else(|| s3_error!(NoSuchKey))?;
        let replace = req
            .input
            .metadata_directive
            .as_ref()
            .is_some_and(|v| v.as_str() == MetadataDirective::REPLACE);
        let headers = if replace {
            headers_from_copy(&req.input)
        } else {
            headers_from_object(&source)
        };
        self.catalog
            .copy_object(&source, &req.input.bucket, &req.input.key, &headers)
            .await
            .map_err(internal)?;
        Ok(S3Response::new(CopyObjectOutput {
            copy_object_result: Some(CopyObjectResult {
                e_tag: Some(ETag::Strong(source.etag)),
                last_modified: Some(timestamp(now())),
                ..Default::default()
            }),
            ..Default::default()
        }))
    }
}

fn access_key<T>(req: &S3Request<T>) -> S3Result<&str> {
    req.credentials
        .as_ref()
        .map(|c| c.access_key.as_str())
        .ok_or_else(|| s3_error!(AccessDenied))
}
fn internal(error: impl std::fmt::Display) -> s3s::S3Error {
    s3_error!(InternalError, "{error}")
}
fn timestamp(seconds: i64) -> Timestamp {
    Timestamp::from(UNIX_EPOCH + Duration::from_secs(seconds.max(0) as u64))
}
fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i64
}
fn verify_md5(expected: Option<&str>, digest: &[u8]) -> S3Result<()> {
    if expected
        .is_some_and(|value| value != base64::engine::general_purpose::STANDARD.encode(digest))
    {
        return Err(s3_error!(BadDigest));
    }
    Ok(())
}

fn headers_from_put(i: &PutObjectInput) -> ObjectHeaders {
    ObjectHeaders {
        content_type: i.content_type.clone(),
        content_encoding: i.content_encoding.clone(),
        cache_control: i.cache_control.clone(),
        content_disposition: i.content_disposition.clone(),
        content_language: i.content_language.clone(),
        metadata: i.metadata.clone().unwrap_or_default(),
    }
}
fn headers_from_multipart(i: &CreateMultipartUploadInput) -> ObjectHeaders {
    ObjectHeaders {
        content_type: i.content_type.clone(),
        content_encoding: i.content_encoding.clone(),
        cache_control: i.cache_control.clone(),
        content_disposition: i.content_disposition.clone(),
        content_language: i.content_language.clone(),
        metadata: i.metadata.clone().unwrap_or_default(),
    }
}
fn headers_from_copy(i: &CopyObjectInput) -> ObjectHeaders {
    ObjectHeaders {
        content_type: i.content_type.clone(),
        content_encoding: i.content_encoding.clone(),
        cache_control: i.cache_control.clone(),
        content_disposition: i.content_disposition.clone(),
        content_language: i.content_language.clone(),
        metadata: i.metadata.clone().unwrap_or_default(),
    }
}
fn headers_from_object(o: &ObjectRecord) -> ObjectHeaders {
    ObjectHeaders {
        content_type: o.content_type.clone(),
        content_encoding: o.content_encoding.clone(),
        cache_control: o.cache_control.clone(),
        content_disposition: o.content_disposition.clone(),
        content_language: o.content_language.clone(),
        metadata: o.metadata.clone(),
    }
}

fn head_output(o: &ObjectRecord) -> HeadObjectOutput {
    HeadObjectOutput {
        content_length: Some(o.size as i64),
        content_type: o.content_type.clone(),
        content_encoding: o.content_encoding.clone(),
        cache_control: o.cache_control.clone(),
        content_disposition: o.content_disposition.clone(),
        content_language: o.content_language.clone(),
        metadata: Some(o.metadata.clone()),
        e_tag: Some(ETag::Strong(o.etag.clone())),
        last_modified: Some(timestamp(o.created_at)),
        accept_ranges: Some("bytes".into()),
        ..Default::default()
    }
}

fn multipart_etag(parts: &[PartRecord]) -> S3Result<String> {
    let mut hash = Md5::new();
    for part in parts {
        hash.update(hex::decode(&part.etag).map_err(internal)?);
    }
    Ok(format!("{}-{}", hex::encode(hash.finalize()), parts.len()))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Range;
    use std::sync::Mutex;

    use frame_streamer::{
        DeleteFuture, EncryptedByteStream, EncryptedBytesUploadBackend, SignedUrl, StoredObject,
        StreamUploadBackend, UploadByteStream, UploadFuture, UrlTicket,
    };
    use s3s::auth::{Credentials, S3Auth, SecretKey};

    use super::*;
    use crate::DatabaseAuth;

    #[derive(Default)]
    struct MemoryBackend {
        objects: Arc<Mutex<HashMap<String, Vec<u8>>>>,
    }

    impl EncryptedBytesUploadBackend for MemoryBackend {
        fn max_physical_bytes_per_segment(&self) -> u64 {
            48
        }

        fn upload(
            &self,
            id: ObjectId,
            _hint: Option<u64>,
            mut bytes: UploadByteStream,
        ) -> UploadFuture {
            let objects = self.objects.clone();
            let uri = id.as_str().to_owned();
            Box::pin(async move {
                let mut data = Vec::new();
                while let Some(chunk) = bytes.next().await {
                    data.extend_from_slice(&chunk?);
                }
                objects.lock().unwrap().insert(uri.clone(), data);
                Ok(StoredObject {
                    uri,
                    cached_url: None,
                })
            })
        }

        fn delete(&self, object: StoredObject) -> DeleteFuture {
            self.objects.lock().unwrap().remove(&object.uri);
            Box::pin(async { Ok(()) })
        }
    }

    impl EncryptedBytesDownloadBackend for MemoryBackend {
        fn resolve_url(&self, object: &frame_streamer::ObjectMeta) -> UrlTicket {
            let uri = object.uri.clone();
            Box::pin(async move { Ok(SignedUrl::new(uri, None)) })
        }

        fn download(
            &self,
            _object: &frame_streamer::ObjectMeta,
            url: SignedUrl,
            range: Range<u64>,
        ) -> EncryptedByteStream {
            let data = self.objects.lock().unwrap()[url.as_str()]
                [range.start as usize..range.end as usize]
                .to_vec();
            Box::pin(stream::iter([Ok(Bytes::from(data))]))
        }
    }

    fn signed<T>(input: T, access: &str) -> S3Request<T> {
        S3Request {
            input,
            method: axum::http::Method::GET,
            uri: axum::http::Uri::from_static("/"),
            headers: Default::default(),
            extensions: Default::default(),
            credentials: Some(Credentials {
                access_key: access.into(),
                secret_key: SecretKey::from("secret"),
            }),
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    async fn app() -> (S3Storage, Catalog, Arc<MemoryBackend>) {
        let catalog = Catalog::connect("sqlite::memory:").await.unwrap();
        catalog
            .create_credential("owner", "secret", true)
            .await
            .unwrap();
        catalog
            .create_credential("stranger", "secret", true)
            .await
            .unwrap();
        catalog.create_bucket("bucket", "owner").await.unwrap();
        let raw = Arc::new(MemoryBackend::default());
        let upload = Arc::new(StreamUploadBackend::new(raw.clone(), 24).unwrap());
        let rate = ByteRate::new(1000.0).unwrap();
        let config = ByteStreamConfig::new(
            24,
            rate,
            frame_streamer::ByteTransferModel {
                object_rate: rate,
                data_ttfb: Duration::ZERO,
                url_latency: Duration::ZERO,
                frames_per_object: 2,
            },
        )
        .unwrap();
        (
            S3Storage::new(
                catalog.clone(),
                StorageBackend {
                    upload,
                    download: raw.clone(),
                },
                FrameBudget::new(20).unwrap(),
                config,
                rate,
                24,
                1024,
            ),
            catalog,
            raw,
        )
    }

    #[tokio::test]
    async fn stores_metadata_and_reads_a_range_across_unaligned_segments() {
        let (app, catalog, raw) = app().await;
        let data = Bytes::from_static(b"abcdefghijklmnopqrstuvwxyz012345678");
        let mut metadata = HashMap::new();
        metadata.insert("mtime".into(), "42.0".into());
        let put = PutObjectInput {
            bucket: "bucket".into(),
            key: "dir/file".into(),
            body: Some(StreamingBlob::wrap(stream::iter([Ok::<_, io::Error>(
                data.clone(),
            )]))),
            content_type: Some("text/plain".into()),
            metadata: Some(metadata.clone()),
            ..Default::default()
        };
        app.put_object(signed(put, "owner")).await.unwrap();
        assert_eq!(
            catalog
                .get_object("bucket", "dir/file")
                .await
                .unwrap()
                .unwrap()
                .segments
                .len(),
            3
        );

        let get = GetObjectInput {
            bucket: "bucket".into(),
            key: "dir/file".into(),
            range: Some(s3s::dto::Range::Int {
                first: 7,
                last: Some(24),
            }),
            ..Default::default()
        };
        let mut output = app
            .get_object(signed(get, "owner"))
            .await
            .unwrap()
            .output
            .body
            .unwrap();
        let mut actual = Vec::new();
        while let Some(chunk) = output.next().await {
            actual.extend_from_slice(&chunk.unwrap());
        }
        assert_eq!(actual, &data[7..25]);
        let head = app
            .head_object(signed(
                HeadObjectInput {
                    bucket: "bucket".into(),
                    key: "dir/file".into(),
                    ..Default::default()
                },
                "owner",
            ))
            .await
            .unwrap()
            .output;
        assert_eq!(head.metadata, Some(metadata));
        assert_eq!(head.content_type.as_deref(), Some("text/plain"));

        let copy = CopyObjectInput::builder()
            .bucket("bucket".into())
            .key("copy".into())
            .copy_source(CopySource::parse("bucket/dir/file").unwrap())
            .build()
            .unwrap();
        app.copy_object(signed(copy, "owner")).await.unwrap();
        assert_eq!(
            catalog
                .get_object("bucket", "copy")
                .await
                .unwrap()
                .unwrap()
                .segments
                .len(),
            3
        );
        assert_eq!(raw.objects.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn denies_unknown_keys_and_ungranted_buckets() {
        let (app, catalog, _) = app().await;
        let auth = DatabaseAuth::new(catalog);
        assert!(auth.get_secret_key("missing").await.is_err());
        let put = PutObjectInput {
            bucket: "bucket".into(),
            key: "blocked".into(),
            body: Some(StreamingBlob::wrap(
                stream::empty::<Result<Bytes, io::Error>>(),
            )),
            ..Default::default()
        };
        assert!(app.put_object(signed(put, "stranger")).await.is_err());
    }

    #[tokio::test]
    async fn completes_a_multipart_upload() {
        let (app, catalog, _) = app().await;
        let created = app
            .create_multipart_upload(signed(
                CreateMultipartUploadInput {
                    bucket: "bucket".into(),
                    key: "multipart".into(),
                    metadata: Some(HashMap::from([("mtime".into(), "7.0".into())])),
                    ..Default::default()
                },
                "owner",
            ))
            .await
            .unwrap()
            .output;
        let upload_id = created.upload_id.unwrap();
        let uploaded = app
            .upload_part(signed(
                UploadPartInput {
                    bucket: "bucket".into(),
                    key: "multipart".into(),
                    upload_id: upload_id.clone(),
                    part_number: 1,
                    body: Some(StreamingBlob::wrap(stream::iter([Ok::<_, io::Error>(
                        Bytes::from_static(b"multipart body"),
                    )]))),
                    ..Default::default()
                },
                "owner",
            ))
            .await
            .unwrap()
            .output;
        let part_etag = uploaded.e_tag.unwrap();
        let completed = app
            .complete_multipart_upload(signed(
                CompleteMultipartUploadInput {
                    bucket: "bucket".into(),
                    key: "multipart".into(),
                    upload_id,
                    multipart_upload: Some(CompletedMultipartUpload {
                        parts: Some(vec![CompletedPart {
                            e_tag: Some(part_etag),
                            part_number: Some(1),
                            ..Default::default()
                        }]),
                    }),
                    ..Default::default()
                },
                "owner",
            ))
            .await
            .unwrap()
            .output;
        assert!(completed.e_tag.unwrap().value().ends_with("-1"));
        let object = catalog
            .get_object("bucket", "multipart")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(object.size, 14);
        assert_eq!(object.metadata["mtime"], "7.0");
    }
}
