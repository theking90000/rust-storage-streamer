# s3-host

Standalone rclone-compatible S3 server backed by Discord webhooks. It serves
the private `s3-router` API only; `file-router` is not mounted.

## Credentials

SigV4 verification needs the original secret, so credentials are stored in the
dedicated SQLite database. Protect that database as a secret.

```sh
# Prints the new secret once. This credential may create buckets.
cargo run -p s3-host -- credential create --can-create-buckets

# Grant an existing credential access to an existing bucket.
cargo run -p s3-host -- credential grant ACCESS_KEY bucket
cargo run -p s3-host -- credential grant ACCESS_KEY bucket --read-only

cargo run -p s3-host -- credential revoke ACCESS_KEY
```

Use `--database-url` or `S3_DATABASE_URL` to select another catalog.

## Run

```sh
cargo run -p s3-host -- serve --webhooks-file webhooks.txt

# Optional: route Discord API traffic through one proxy
cargo run -p s3-host -- serve \
  --webhooks-file webhooks.txt \
  --proxy-url socks5h://127.0.0.1:25344
```

The server defaults to `0.0.0.0:8080`, region `us-east-1`, and a 20 GiB object
limit. TLS must be terminated by a reverse proxy in production. Run
`s3-host serve --help` for rate and frame tuning options.

## rclone

```ini
[streamer]
type = s3
provider = Other
access_key_id = ACCESS_KEY
secret_access_key = SECRET_KEY
endpoint = https://s3.example.com
region = us-east-1
force_path_style = true
```

Normal `mkdir`, `copy`, `sync`, `check`, `ls`, `touch`, `copyto`, `moveto`,
`delete`, and `purge` operations are supported. Rclone's multipart upload is
translated into the smaller encrypted objects accepted by Discord.
