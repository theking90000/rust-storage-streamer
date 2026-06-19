use s3s::auth::{S3Auth, SecretKey};
use s3s::{S3Result, s3_error};

use crate::Catalog;

#[derive(Clone)]
pub struct DatabaseAuth(pub(crate) Catalog);

impl DatabaseAuth {
    pub fn new(catalog: Catalog) -> Self {
        Self(catalog)
    }
}

#[async_trait::async_trait]
impl S3Auth for DatabaseAuth {
    async fn get_secret_key(&self, access_key: &str) -> S3Result<SecretKey> {
        self.0
            .secret_key(access_key)
            .await
            .map_err(|error| s3_error!(error, InternalError))?
            .map(SecretKey::from)
            .ok_or_else(|| s3_error!(InvalidAccessKeyId))
    }
}
