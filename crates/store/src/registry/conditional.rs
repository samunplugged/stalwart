/*
 * SPDX-FileCopyrightText: 2026 Sameer Gupta
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

//! Sameer's fork: conditional updates of several registry objects in one
//! transaction, used by the organization quota transfer (see `FORK.md`).

use crate::{
    RegistryStore,
    write::{BatchBuilder, RegistryClass, ValueClass, assert::AssertValue},
};
use registry::{
    schema::prelude::{Object, ObjectType},
    types::{EnumImpl, error::ValidationError, index::IndexBuilder},
};
use trc::AddContext;
use types::id::Id;

/// One object to update: its id, the object as it was read and its new value.
pub struct ConditionalUpdate<'x> {
    pub id: Id,
    pub current: &'x Object,
    pub new: &'x Object,
}

impl RegistryStore {
    /// Writes every update in a single transaction that fails with
    /// `StoreEvent::AssertValueFailed` when any of the objects changed after
    /// it was read (its stored revision no longer matches `current`).
    ///
    /// Only for updates that leave the objects' indexes untouched, such as a
    /// change of an account's quotas: foreign keys and primary keys are not
    /// checked again, so an update that would change an index is refused.
    pub async fn write_conditional(&self, updates: &[ConditionalUpdate<'_>]) -> trc::Result<()> {
        let mut batch = BatchBuilder::new();

        for update in updates {
            let object_type = update.new.object_type();
            if object_type != update.current.object_type() || object_type == ObjectType::DataStore {
                return Err(trc::StoreEvent::NotSupported
                    .into_err()
                    .caused_by(trc::location!())
                    .details("Conditional updates cannot change an object's type"));
            }

            // The indexes must stay exactly as they are
            let mut old_index = IndexBuilder::default();
            let mut new_index = IndexBuilder::default();
            update.current.index(&mut old_index);
            update.new.index(&mut new_index);
            if old_index.keys != new_index.keys {
                return Err(trc::StoreEvent::NotSupported
                    .into_err()
                    .caused_by(trc::location!())
                    .details("Conditional updates cannot change indexed properties"));
            }

            let mut errors: Vec<ValidationError> = Vec::new();
            update.new.validate(&mut errors);
            if !errors.is_empty() {
                return Err(trc::StoreEvent::UnexpectedError
                    .into_err()
                    .caused_by(trc::location!())
                    .details("Conditional update failed validation")
                    .ctx(trc::Key::Reason, format!("{errors:?}")));
            }

            batch.assert_value(
                ValueClass::Registry(RegistryClass::Item {
                    object_id: object_type.to_id(),
                    item_id: update.id.id(),
                }),
                AssertValue::Hash(update.current.revision),
            );
        }

        // Every assertion comes before the first write, so that even a store
        // that applies operations one by one fails before changing anything
        for update in updates {
            batch.set(
                ValueClass::Registry(RegistryClass::Item {
                    object_id: update.new.object_type().to_id(),
                    item_id: update.id.id(),
                }),
                update.new.inner.to_pickled_vec(),
            );
        }

        self.store()
            .write_batch(&mut batch)
            .await
            .map(|_| ())
            .caused_by(trc::location!())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        RegistryStoreInner, Store, backend::ephemeral::EphemeralStore, write::BatchBuilder as Batch,
    };
    use registry::schema::{
        enums::StorageQuota,
        prelude::ObjectInner,
        structs::{Account, UserAccount},
    };
    use std::{path::PathBuf, sync::Arc};
    use utils::snowflake::SnowflakeIdGenerator;

    fn registry() -> RegistryStore {
        RegistryStore(Arc::new(RegistryStoreInner {
            local_path: PathBuf::new(),
            store: EphemeralStore::open(),
            node_id: 0,
            env_recovery_mode: false,
            env_update_role_permissions: false,
            env_recovery_admin: None,
            env_cluster_role: None,
            env_push_shard_id: 0,
            env_hostname: "localhost".to_string(),
            env_public_url: None,
            id_generator: SnowflakeIdGenerator::new(),
            process_nonce: [0; 16],
        }))
    }

    fn account(name: &str, quota: u64) -> Object {
        let mut user = UserAccount {
            name: name.to_string(),
            domain_id: Id::new(1),
            ..Default::default()
        };
        user.quotas.set(StorageQuota::MaxDiskQuota, quota);
        Object {
            inner: ObjectInner::Account(Box::new(Account::User(user))),
            revision: 0,
        }
    }

    fn quota(object: &Object) -> u64 {
        match &object.inner {
            ObjectInner::Account(account) => match account.as_ref() {
                Account::User(user) => *user.quotas.get(&StorageQuota::MaxDiskQuota).unwrap(),
                _ => unreachable!(),
            },
            _ => unreachable!(),
        }
    }

    async fn store_raw(store: &Store, id: u64, object: &Object) {
        let mut batch = Batch::new();
        batch.set(
            ValueClass::Registry(RegistryClass::Item {
                object_id: ObjectType::Account.to_id(),
                item_id: id,
            }),
            object.inner.to_pickled_vec(),
        );
        store.write_batch(&mut batch).await.unwrap();
    }

    async fn read(registry: &RegistryStore, id: u64) -> Object {
        registry
            .get(registry::types::id::ObjectId::new(
                ObjectType::Account,
                Id::new(id),
            ))
            .await
            .unwrap()
            .unwrap()
    }

    #[tokio::test]
    async fn conditional_updates_are_atomic() {
        let registry = registry();
        store_raw(registry.store(), 1, &account("a", 100)).await;
        store_raw(registry.store(), 2, &account("b", 500)).await;

        // Two servers read both accounts at the same time
        let (a1, b1) = (read(&registry, 1).await, read(&registry, 2).await);
        let (a2, b2) = (read(&registry, 1).await, read(&registry, 2).await);

        // The first moves 50 from b to a
        registry
            .write_conditional(&[
                ConditionalUpdate {
                    id: Id::new(1),
                    current: &a1,
                    new: &account("a", 150),
                },
                ConditionalUpdate {
                    id: Id::new(2),
                    current: &b1,
                    new: &account("b", 450),
                },
            ])
            .await
            .unwrap();

        // The second, working from the same snapshot, must fail and change nothing
        let err = registry
            .write_conditional(&[
                ConditionalUpdate {
                    id: Id::new(1),
                    current: &a2,
                    new: &account("a", 150),
                },
                ConditionalUpdate {
                    id: Id::new(2),
                    current: &b2,
                    new: &account("b", 450),
                },
            ])
            .await
            .unwrap_err();
        assert!(err.matches(trc::EventType::Store(trc::StoreEvent::AssertValueFailed)));
        assert_eq!(quota(&read(&registry, 1).await), 150);
        assert_eq!(quota(&read(&registry, 2).await), 450);

        // A stale second object alone also blocks the first one's write
        let a3 = read(&registry, 1).await;
        let err = registry
            .write_conditional(&[
                ConditionalUpdate {
                    id: Id::new(1),
                    current: &a3,
                    new: &account("a", 999),
                },
                ConditionalUpdate {
                    id: Id::new(2),
                    current: &b2,
                    new: &account("b", 1),
                },
            ])
            .await
            .unwrap_err();
        assert!(err.matches(trc::EventType::Store(trc::StoreEvent::AssertValueFailed)));
        assert_eq!(quota(&read(&registry, 1).await), 150);
        assert_eq!(quota(&read(&registry, 2).await), 450);
    }

    #[tokio::test]
    async fn conditional_updates_refuse_index_changes() {
        let registry = registry();
        store_raw(registry.store(), 1, &account("a", 100)).await;
        let current = read(&registry, 1).await;
        let err = registry
            .write_conditional(&[ConditionalUpdate {
                id: Id::new(1),
                current: &current,
                new: &account("renamed", 100),
            }])
            .await
            .unwrap_err();
        assert!(err.matches(trc::EventType::Store(trc::StoreEvent::NotSupported)));
    }
}
