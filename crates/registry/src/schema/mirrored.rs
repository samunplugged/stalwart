/*
 * SPDX-FileCopyrightText: 2026 Sameer Gupta
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

//! Settings for this fork's mirrored blob store: a primary and a
//! secondary blob store that every blob is written to. See `FORK.md`.
//!
//! Written by hand in the style of the generated schema code, so that the
//! store is configured like the other `BlobStore` variants
//! (`{"@type": "Mirrored", "primary": {...}, "secondary": {...}}`).

use crate::schema::prelude::*;

pub const MIRRORED_DEFAULT_READ_TIMEOUT_MS: u64 = 2_000;
pub const MIRRORED_DEFAULT_WRITE_TIMEOUT_MS: u64 = 5_000;
pub const MIRRORED_DEFAULT_FAILURE_THRESHOLD: u64 = 3;
pub const MIRRORED_DEFAULT_DOWN_DURATION_MS: u64 = 30_000;
pub const MIRRORED_DEFAULT_REPAIR_INTERVAL_MS: u64 = 60_000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default)]
pub struct MirroredBlobStore {
    /// Store read first. Any blob store type except `Mirrored`.
    pub primary: Box<BlobStore>,
    /// Store holding the second copy. Any blob store type except `Mirrored`.
    pub secondary: Box<BlobStore>,
    /// How long a read from the primary may take before the secondary is read.
    #[serde(rename = "readTimeout")]
    pub read_timeout: Duration,
    /// How long a write or delete on one side may take before that side's
    /// copy is recorded in the repair list instead.
    #[serde(rename = "writeTimeout")]
    pub write_timeout: Duration,
    /// Consecutive failures after which a side is marked down.
    #[serde(rename = "failureThreshold")]
    pub failure_threshold: u64,
    /// How long a side stays marked down.
    #[serde(rename = "downDuration")]
    pub down_duration: Duration,
    /// How often the repair task looks at the repair list.
    #[serde(rename = "repairInterval")]
    pub repair_interval: Duration,
}

impl Default for MirroredBlobStore {
    fn default() -> Self {
        Self {
            primary: Box::new(BlobStore::Default),
            secondary: Box::new(BlobStore::Default),
            read_timeout: Duration::from_millis(MIRRORED_DEFAULT_READ_TIMEOUT_MS),
            write_timeout: Duration::from_millis(MIRRORED_DEFAULT_WRITE_TIMEOUT_MS),
            failure_threshold: MIRRORED_DEFAULT_FAILURE_THRESHOLD,
            down_duration: Duration::from_millis(MIRRORED_DEFAULT_DOWN_DURATION_MS),
            repair_interval: Duration::from_millis(MIRRORED_DEFAULT_REPAIR_INTERVAL_MS),
        }
    }
}

impl MirroredBlobStore {
    /// Whether a blob store type may be one side of a mirror. Only plain
    /// stores qualify: never another mirror or any composite store.
    pub fn is_valid_side(store: &BlobStore) -> bool {
        matches!(
            store,
            BlobStore::Default
                | BlobStore::S3(_)
                | BlobStore::Azure(_)
                | BlobStore::FileSystem(_)
                | BlobStore::FoundationDb(_)
                | BlobStore::PostgreSql(_)
                | BlobStore::MySql(_)
        )
    }

    pub(crate) fn validate(&self, errors: &mut Vec<ValidationError>) -> bool {
        let neb = errors.len();
        for (property, side) in [
            (Property::Primary, &self.primary),
            (Property::Secondary, &self.secondary),
        ] {
            if Self::is_valid_side(side) {
                side.validate(errors);
            } else {
                errors.push(ValidationError::invalid(
                    property,
                    format!(
                        "A mirrored blob store side cannot be of type {}",
                        side.object_type().as_str()
                    ),
                ));
            }
        }
        if self.primary == self.secondary {
            errors.push(ValidationError::invalid(
                Property::Secondary,
                "The secondary store must differ from the primary store",
            ));
        }
        for (property, value) in [
            (Property::ReadTimeout, &self.read_timeout),
            (Property::WriteTimeout, &self.write_timeout),
            (Property::RepairInterval, &self.repair_interval),
        ] {
            if value.0.is_zero() {
                errors.push(ValidationError::min_value(property, 1));
            }
        }
        if self.failure_threshold < 1 {
            errors.push(ValidationError::min_value(Property::FailureThreshold, 1));
        }
        errors.len() == neb
    }
}

impl Pickle for MirroredBlobStore {
    fn pickle(&self, out: &mut Vec<u8>) {
        let block = crate::pickle::begin_block(out, 7, 0, 0);
        self.primary.as_ref().pickle(out);
        self.secondary.as_ref().pickle(out);
        self.read_timeout.pickle(out);
        self.write_timeout.pickle(out);
        self.failure_threshold.pickle(out);
        self.down_duration.pickle(out);
        self.repair_interval.pickle(out);
        crate::pickle::end_block(out, block);
    }

    fn unpickle(stream: &mut crate::pickle::PickledStream<'_>) -> Option<Self> {
        let block = stream.open_block()?;
        let default = Self::default();
        let primary: BlobStore = if block.has(0) {
            Pickle::unpickle(stream)?
        } else {
            BlobStore::Default
        };
        let secondary: BlobStore = if block.has(1) {
            Pickle::unpickle(stream)?
        } else {
            BlobStore::Default
        };
        let read_timeout: Duration = if block.has(2) {
            Pickle::unpickle(stream)?
        } else {
            default.read_timeout
        };
        let write_timeout: Duration = if block.has(3) {
            Pickle::unpickle(stream)?
        } else {
            default.write_timeout
        };
        let failure_threshold: u64 = if block.has(4) {
            Pickle::unpickle(stream)?
        } else {
            default.failure_threshold
        };
        let down_duration: Duration = if block.has(5) {
            Pickle::unpickle(stream)?
        } else {
            default.down_duration
        };
        let repair_interval: Duration = if block.has(6) {
            Pickle::unpickle(stream)?
        } else {
            default.repair_interval
        };
        stream.close_block(block);
        Some(Self {
            primary: Box::new(primary),
            secondary: Box::new(secondary),
            read_timeout,
            write_timeout,
            failure_threshold,
            down_duration,
            repair_interval,
        })
    }
}

impl IntoValue for MirroredBlobStore {
    fn into_value(self) -> JmapValue<'static> {
        self.into_value_with(ValueOptions::default())
    }

    fn into_value_with(self, opts: ValueOptions) -> JmapValue<'static> {
        let mut map = jmap_tools::Map::with_capacity(8);
        map.insert_unchecked(Property::Primary, (*self.primary).into_value_with(opts));
        map.insert_unchecked(Property::Secondary, (*self.secondary).into_value_with(opts));
        map.insert_unchecked(
            Property::ReadTimeout,
            self.read_timeout.into_value_with(opts),
        );
        map.insert_unchecked(
            Property::WriteTimeout,
            self.write_timeout.into_value_with(opts),
        );
        map.insert_unchecked(
            Property::FailureThreshold,
            self.failure_threshold.into_value_with(opts),
        );
        map.insert_unchecked(
            Property::DownDuration,
            self.down_duration.into_value_with(opts),
        );
        map.insert_unchecked(
            Property::RepairInterval,
            self.repair_interval.into_value_with(opts),
        );
        JmapValue::Object(map)
    }
}

impl RegistryJsonPropertyPatch for MirroredBlobStore {
    fn patch_property<'x>(
        &mut self,
        mut pointer: JsonPointerPatch<'_>,
        value: JmapValue<'x>,
    ) -> PatchResult<'x> {
        match pointer.next_property() {
            Some(Property::Primary) => self.primary.as_mut().patch(pointer, value),
            Some(Property::Secondary) => self.secondary.as_mut().patch(pointer, value),
            Some(Property::ReadTimeout) => self.read_timeout.patch(pointer, value),
            Some(Property::WriteTimeout) => self.write_timeout.patch(pointer, value),
            Some(Property::FailureThreshold) => self.failure_threshold.patch(pointer, value),
            Some(Property::DownDuration) => self.down_duration.patch(pointer, value),
            Some(Property::RepairInterval) => self.repair_interval.patch(pointer, value),
            Some(Property::Type) => Ok(MaybeUnpatched::Unpatched {
                property: Property::Type,
                value,
            }),
            _ => Err(PatchError::new(pointer, "Invalid property")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pickle::PickledStream;
    use jmap_tools::JsonPointer;

    fn fs(path: &str) -> BlobStore {
        BlobStore::FileSystem(FileSystemStore {
            path: path.to_string(),
            depth: 2,
        })
    }

    fn mirror() -> MirroredBlobStore {
        MirroredBlobStore {
            primary: Box::new(fs("/a")),
            secondary: Box::new(fs("/b")),
            ..Default::default()
        }
    }

    #[test]
    fn mirrored_pickle_round_trip() {
        let store = BlobStore::Mirrored(MirroredBlobStore {
            failure_threshold: 7,
            read_timeout: Duration::from_millis(1234),
            ..mirror()
        });
        // Version marker byte, then the pickled object
        let mut out = vec![0u8];
        store.pickle(&mut out);
        let mut stream = PickledStream::new(&out).unwrap();
        assert_eq!(BlobStore::unpickle(&mut stream), Some(store));
    }

    #[test]
    fn mirrored_validation() {
        let mut errors = Vec::new();
        assert!(mirror().validate(&mut errors), "{errors:?}");

        // Same store on both sides
        let mut errors = Vec::new();
        let same = MirroredBlobStore {
            secondary: Box::new(fs("/a")),
            ..mirror()
        };
        assert!(!same.validate(&mut errors));

        // A mirror inside a mirror
        let mut errors = Vec::new();
        let nested = MirroredBlobStore {
            secondary: Box::new(BlobStore::Mirrored(mirror())),
            ..mirror()
        };
        assert!(!nested.validate(&mut errors));
        assert!(matches!(
            errors.first(),
            Some(ValidationError::Invalid {
                property: Property::Secondary,
                ..
            })
        ));

        // Zero thresholds and timeouts
        let mut errors = Vec::new();
        let zero = MirroredBlobStore {
            failure_threshold: 0,
            write_timeout: Duration::from_millis(0),
            ..mirror()
        };
        assert!(!zero.validate(&mut errors));
        assert_eq!(errors.len(), 2);
    }

    #[test]
    fn mirrored_json_round_trip() {
        let store = BlobStore::Mirrored(mirror());
        let value = store.clone().into_value();
        let mut patched = BlobStore::default();
        let ptr = JsonPointer::new(vec![]);
        patched
            .patch(JsonPointerPatch::new(&ptr).with_create(true), value)
            .expect("patch failed");
        assert_eq!(patched, store);
        assert_eq!(
            BlobStoreType::parse("Mirrored"),
            Some(BlobStoreType::Mirrored)
        );
        assert_eq!(Property::parse("readTimeout"), Some(Property::ReadTimeout));
    }
}
