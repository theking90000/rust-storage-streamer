# s3-router

S3-compatible service layer for `frame-streamer`, implemented with `s3s`.
It owns a private SQLite catalog and does not reuse or expose `file-router`.

Implemented for rclone:

- SigV4 authentication backed by SQLite credentials;
- isolated read-only/read-write bucket grants;
- bucket create, list, head, versioning status, and delete;
- S3 v1/v2 object listing, GET/HEAD ranges, PUT, single/batch delete;
- multipart create/upload/complete/abort with standard MD5 ETags;
- server-side `CopyObject`, including metadata replacement;
- arbitrary `x-amz-meta-*` values, including rclone's `mtime` and `md5chksum`.

Large S3 requests are streamed into backend-sized encrypted segments. Each
segment keeps its exact plaintext size, so downloads can skip AES frame padding
even when S3 multipart boundaries are not frame-aligned.

Versioning, ACLs, anonymous objects, IAM policies, tagging, lifecycle rules,
presigned URLs, and multipart maintenance APIs are intentionally unsupported.

