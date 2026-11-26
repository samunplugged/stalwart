/*
 * SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
 *
 * SPDX-License-Identifier: AGPL-3.0-only OR LicenseRef-SEL
 */

use crate::{BlobStore, backend::fs::FsStore, registry::bootstrap::Bootstrap};
use registry::{
    schema::{prelude::ObjectType, structs},
    types::EnumImpl,
};

#[allow(unreachable_patterns)]
impl BlobStore {
    pub async fn build(bp: &mut Bootstrap) -> Option<Self> {
        let config = bp.setting_infallible::<structs::BlobStore>().await;
        match Self::build_from(bp, config).await {
            Ok(store) => Some(store),
            Err(err) => {
                bp.build_error(ObjectType::BlobStore.singleton(), err);
                None
            }
        }
    }

    // Sameer's fork: split out of `build` so that the mirrored blob store
    // can build its two sides (see FORK.md)
    async fn build_from(bp: &mut Bootstrap, config: structs::BlobStore) -> Result<Self, String> {
        match config {
            structs::BlobStore::Default => Ok(BlobStore::Store(bp.data_store.clone())),
            #[cfg(feature = "foundation")]
            structs::BlobStore::FoundationDb(foundation_db_store) => {
                crate::backend::foundationdb::FdbStore::open_shared(
                    foundation_db_store,
                    bp,
                    ObjectType::BlobStore.singleton(),
                )
                .await
                .map(BlobStore::Store)
            }
            #[cfg(feature = "postgres")]
            structs::BlobStore::PostgreSql(postgre_sql_store) => {
                crate::backend::postgres::PostgresStore::open(postgre_sql_store)
                    .await
                    .map(BlobStore::Store)
            }
            #[cfg(feature = "mysql")]
            structs::BlobStore::MySql(my_sql_store) => {
                crate::backend::mysql::MysqlStore::open(my_sql_store)
                    .await
                    .map(BlobStore::Store)
            }
            #[cfg(feature = "s3")]
            structs::BlobStore::S3(s3_store) => crate::backend::s3::S3Store::open(s3_store).await,
            #[cfg(feature = "azure")]
            structs::BlobStore::Azure(azure_store) => {
                crate::backend::azure::AzureStore::open(azure_store).await
            }
            structs::BlobStore::FileSystem(file_system_store) => {
                FsStore::open(file_system_store).await
            }
            // SPDX-SnippetBegin
            // SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
            // SPDX-License-Identifier: LicenseRef-SEL
            #[cfg(feature = "enterprise")]
            structs::BlobStore::Sharded(store) => {
                crate::backend::composite::sharded_blob::ShardedBlob::open(store, bp).await
            } // SPDX-SnippetEnd
            // Sameer's fork: mirrored blob store
            structs::BlobStore::Mirrored(mut store) => {
                for side in [&store.primary, &store.secondary] {
                    if !structs::MirroredBlobStore::is_valid_side(side) {
                        return Err(format!(
                            "A mirrored blob store side cannot be of type {}",
                            side.object_type().as_str()
                        ));
                    }
                }
                let primary = std::mem::take(store.primary.as_mut());
                let secondary = std::mem::take(store.secondary.as_mut());
                let primary = Box::pin(Self::build_from(bp, primary))
                    .await
                    .map_err(|err| format!("Mirrored blob store primary: {err}"))?;
                let secondary = Box::pin(Self::build_from(bp, secondary))
                    .await
                    .map_err(|err| format!("Mirrored blob store secondary: {err}"))?;
                crate::backend::mirror::MirroredBlob::open(
                    store,
                    primary,
                    secondary,
                    bp.data_store.clone(),
                )
                .await
            }
            _ => Err("Binary was not compiled with the selected blob store backend".to_string()),
        }
    }

    // SPDX-SnippetBegin
    // SPDX-FileCopyrightText: 2020 Stalwart Labs LLC <hello@stalw.art>
    // SPDX-License-Identifier: LicenseRef-SEL
    #[cfg(feature = "enterprise")]
    pub fn downgrade_store(self) -> BlobStore {
        match self {
            BlobStore::Sharded(_) => BlobStore::default(),
            other => other,
        }
    }

    #[cfg(feature = "enterprise")]
    pub fn is_enterprise(&self) -> bool {
        matches!(self, BlobStore::Sharded(_))
    }
    // SPDX-SnippetEnd
}
