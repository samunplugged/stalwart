/*
 * SPDX-FileCopyrightText: 2026 Sameer Gupta
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

//! Sameer's fork: organization quota by quota transfer.
//!
//! An organization is a group on the domain named by the `Email` setting
//! `organizationQuotaDomain`. The rule is that its members' disk quotas always
//! add up to the organization's limit: Stalwart's own check per account stays
//! as it is, and when a write fails that check, unused quota is moved from the
//! member with the most free space to the account that ran out. Both quotas
//! change in one transaction that fails if either account changed, so two
//! servers cannot hand out the same space. See `FORK.md`.

use crate::{Server, auth::AccountCache, cache::invalidate::CacheInvalidationBuilder};
use registry::{
    schema::{
        enums::StorageQuota,
        prelude::{Object, ObjectInner, ObjectType, Property},
        structs::Account,
    },
    types::id::ObjectId,
};
use std::sync::Arc;
use store::{
    registry::{RegistryQuery, conditional::ConditionalUpdate},
    roaring::RoaringBitmap,
};
use trc::AddContext;
use types::id::Id;

/// Smallest amount moved in one transfer, so not every message needs one.
pub const ORG_QUOTA_MIN_TRANSFER: u64 = 50 * 1024 * 1024;
/// Free space a donor always keeps.
pub const ORG_QUOTA_DONOR_RESERVE: u64 = 10 * 1024 * 1024;

/// Quota and usage of one organization member.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuotaMember {
    pub account_id: u32,
    /// Disk quota in bytes; 0 means unlimited.
    pub quota: u64,
    pub used: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransferPlan {
    /// The item already fits within the account's quota.
    NotNeeded,
    /// Move `amount` bytes of quota from `donor_id` to the account.
    Transfer { donor_id: u32, amount: u64 },
    /// No member can give enough space.
    NoDonor,
}

/// Decides how much quota to move, and from whom.
///
/// The account needs at least its shortfall (`used + item_size - quota`).
/// It asks for the largest of the shortfall, the item's size and
/// `min_transfer`, from the member with the most free space, who keeps
/// `reserve` bytes free. A donor that cannot give the whole request gives
/// what it can, provided that covers the shortfall.
pub fn plan_transfer(
    account: &QuotaMember,
    item_size: u64,
    members: &[QuotaMember],
    min_transfer: u64,
    reserve: u64,
) -> TransferPlan {
    if account.quota == 0 {
        return TransferPlan::NotNeeded;
    }
    let shortfall = account
        .used
        .saturating_add(item_size)
        .saturating_sub(account.quota);
    if shortfall == 0 {
        return TransferPlan::NotNeeded;
    }
    let wanted = shortfall.max(item_size).max(min_transfer);

    let donor = members
        .iter()
        .filter(|member| member.account_id != account.account_id && member.quota != 0)
        .map(|member| {
            (
                member.account_id,
                member
                    .quota
                    .saturating_sub(member.used)
                    .saturating_sub(reserve),
            )
        })
        // Most free space first; the lowest id breaks ties
        .max_by(|(id_a, free_a), (id_b, free_b)| free_a.cmp(free_b).then(id_b.cmp(id_a)));

    match donor {
        Some((donor_id, available)) if available >= shortfall => TransferPlan::Transfer {
            donor_id,
            amount: wanted.min(available),
        },
        _ => TransferPlan::NoDonor,
    }
}

/// Result of one transfer attempt.
enum Attempt {
    Done,
    NotNeeded,
    NoDonor,
    NotMember,
}

impl Server {
    /// Whether the organization quota transfer is configured at all.
    #[inline]
    pub fn is_organization_quota_enabled(&self) -> bool {
        self.core.email.organization_quota_domain.is_some()
    }

    /// Returns the id of the account's organization group: the first group it
    /// belongs to on the organization quota domain. Reads only cached data.
    pub async fn organization_group_id(&self, account: &AccountCache) -> trc::Result<Option<u32>> {
        let Some(domain_name) = &self.core.email.organization_quota_domain else {
            return Ok(None);
        };
        if account.id_member_of.is_empty() {
            return Ok(None);
        }
        let Some(domain) = self.domain(domain_name).await? else {
            return Ok(None);
        };
        for group_id in account.id_member_of.iter().copied() {
            if let Some(group) = self.try_account(group_id).await?
                && group.is_group_account()
                && group
                    .addresses
                    .first()
                    .is_some_and(|address| address.domain_id == domain.id)
            {
                return Ok(Some(group_id));
            }
        }
        Ok(None)
    }

    /// Checks an account's disk quota before a write whose own quota check
    /// happens inside the write transaction (mail ingestion), and moves quota
    /// to the account when the item would not fit. Returns the account as it
    /// should be used for the write: reloaded after a transfer.
    ///
    /// Does nothing unless the organization quota is configured and the
    /// account is in an organization group.
    pub async fn prepare_organization_quota(
        &self,
        account: Arc<AccountCache>,
        item_size: u64,
    ) -> trc::Result<Arc<AccountCache>> {
        if !self.is_organization_quota_enabled()
            || account.quota_disk == 0
            || self.organization_group_id(&account).await?.is_none()
        {
            return Ok(account);
        }
        let used = self.get_used_quota_account(account.id).await?.max(0) as u64;
        if used.saturating_add(item_size) <= account.quota_disk {
            return Ok(account);
        }
        if self
            .transfer_organization_quota(account.id, item_size)
            .await?
        {
            self.account(account.id).await
        } else {
            Ok(account)
        }
    }

    /// Moves unused disk quota from the organization member with the most
    /// free space to `account_id`, which needs room for `item_size` bytes.
    /// Returns `true` when the account now has room.
    ///
    /// Call only after the account failed its own quota check. Returns `false`
    /// when the feature is off, the account is not in an organization group,
    /// or no member has room.
    pub async fn transfer_organization_quota(
        &self,
        account_id: u32,
        item_size: u64,
    ) -> trc::Result<bool> {
        if !self.is_organization_quota_enabled() {
            return Ok(false);
        }

        // Both quotas are written in one transaction that fails if either
        // account changed since it was read; on a conflict, try once more.
        for attempt in 0..2 {
            match self
                .try_organization_quota_transfer(account_id, item_size)
                .await
            {
                Ok(Attempt::Done | Attempt::NotNeeded) => return Ok(true),
                Ok(Attempt::NoDonor | Attempt::NotMember) => return Ok(false),
                Err(err)
                    if attempt == 0
                        && err
                            .matches(trc::EventType::Store(trc::StoreEvent::AssertValueFailed)) =>
                {
                    continue;
                }
                Err(err)
                    if err.matches(trc::EventType::Store(trc::StoreEvent::AssertValueFailed)) =>
                {
                    trc::event!(
                        Store(trc::StoreEvent::AssertValueFailed),
                        AccountId = account_id,
                        Details = "Organization quota transfer conflicted twice; not retrying",
                    );
                    return Ok(false);
                }
                Err(err) => return Err(err.caused_by(trc::location!())),
            }
        }

        Ok(false)
    }

    async fn try_organization_quota_transfer(
        &self,
        account_id: u32,
        item_size: u64,
    ) -> trc::Result<Attempt> {
        let Some(domain_name) = &self.core.email.organization_quota_domain else {
            return Ok(Attempt::NotMember);
        };
        let Some(domain) = self.domain(domain_name).await? else {
            return Ok(Attempt::NotMember);
        };

        // Read the account as stored, not as cached
        let Some(account) = self.fresh_user_account(account_id).await? else {
            return Ok(Attempt::NotMember);
        };
        let member_group_ids = match account.inner.as_account() {
            Some(Account::User(user)) => user
                .member_group_ids
                .iter()
                .map(|id| id.document_id())
                .collect::<Vec<_>>(),
            _ => return Ok(Attempt::NotMember),
        };

        // Find the organization group
        let mut organization_id = None;
        for group_id in member_group_ids {
            if let Some(group) = self
                .registry()
                .get(ObjectId::new(ObjectType::Account, Id::from(group_id)))
                .await
                .caused_by(trc::location!())?
                && let Some(Account::Group(group)) = group.inner.as_account()
                && group.domain_id.document_id() == domain.id
            {
                organization_id = Some(group_id);
                break;
            }
        }
        let Some(organization_id) = organization_id else {
            return Ok(Attempt::NotMember);
        };

        // Read every member's quota and usage
        let member_ids = self
            .registry()
            .query::<RoaringBitmap>(
                RegistryQuery::new(ObjectType::Account)
                    .equal(Property::MemberGroupIds, u64::from(organization_id)),
            )
            .await
            .caused_by(trc::location!())?;
        let mut members = Vec::with_capacity(member_ids.len() as usize);
        let mut objects = Vec::with_capacity(member_ids.len() as usize);
        let mut recipient = None;
        for member_id in member_ids {
            let object = if member_id == account_id {
                account.clone()
            } else if let Some(object) = self.fresh_user_account(member_id).await? {
                object
            } else {
                continue;
            };
            let member = QuotaMember {
                account_id: member_id,
                quota: disk_quota(&object),
                used: self.get_used_quota_account(member_id).await?.max(0) as u64,
            };
            if member_id == account_id {
                recipient = Some(member);
            }
            members.push(member);
            objects.push((member_id, object));
        }
        let recipient = recipient.unwrap_or(QuotaMember {
            account_id,
            quota: disk_quota(&account),
            used: self.get_used_quota_account(account_id).await?.max(0) as u64,
        });

        let (donor_id, amount) = match plan_transfer(
            &recipient,
            item_size,
            &members,
            ORG_QUOTA_MIN_TRANSFER,
            ORG_QUOTA_DONOR_RESERVE,
        ) {
            TransferPlan::Transfer { donor_id, amount } => (donor_id, amount),
            TransferPlan::NotNeeded => return Ok(Attempt::NotNeeded),
            TransferPlan::NoDonor => return Ok(Attempt::NoDonor),
        };
        let donor = objects
            .iter()
            .find(|(id, _)| *id == donor_id)
            .map(|(_, object)| object)
            .ok_or_else(|| {
                trc::StoreEvent::UnexpectedError
                    .into_err()
                    .caused_by(trc::location!())
                    .details("Organization quota donor vanished")
            })?;

        // Move the quota in one conditional transaction
        let new_account = with_disk_quota(&account, recipient.quota.saturating_add(amount));
        let new_donor = with_disk_quota(donor, disk_quota(donor).saturating_sub(amount));
        self.registry()
            .write_conditional(&[
                ConditionalUpdate {
                    id: Id::from(account_id),
                    current: &account,
                    new: &new_account,
                },
                ConditionalUpdate {
                    id: Id::from(donor_id),
                    current: donor,
                    new: &new_donor,
                },
            ])
            .await?;

        // Make every server reload both accounts
        let mut changes = CacheInvalidationBuilder::default();
        changes.process_update(Id::from(account_id), &account, &new_account);
        changes.process_update(Id::from(donor_id), donor, &new_donor);
        self.invalidate_caches(changes)
            .await
            .caused_by(trc::location!())?;

        trc::event!(
            Store(trc::StoreEvent::QuotaTransfer),
            AccountId = account_id,
            Id = donor_id,
            Size = amount,
            Limit = recipient.quota.saturating_add(amount),
            Details = "Organization quota transferred",
        );

        Ok(Attempt::Done)
    }

    async fn fresh_user_account(&self, account_id: u32) -> trc::Result<Option<Object>> {
        Ok(self
            .registry()
            .get_fresh(ObjectId::new(ObjectType::Account, Id::from(account_id)))
            .await
            .caused_by(trc::location!())?
            .filter(|object| matches!(object.inner.as_account(), Some(Account::User(_)))))
    }
}

trait AsAccount {
    fn as_account(&self) -> Option<&Account>;
}

impl AsAccount for ObjectInner {
    fn as_account(&self) -> Option<&Account> {
        match self {
            ObjectInner::Account(account) => Some(account.as_ref()),
            _ => None,
        }
    }
}

fn disk_quota(object: &Object) -> u64 {
    match object.inner.as_account() {
        Some(Account::User(user)) => user
            .quotas
            .get(&StorageQuota::MaxDiskQuota)
            .copied()
            .unwrap_or_default(),
        _ => 0,
    }
}

fn with_disk_quota(object: &Object, quota: u64) -> Object {
    let mut object = object.clone();
    if let ObjectInner::Account(account) = &mut object.inner
        && let Account::User(user) = account.as_mut()
    {
        user.quotas.set(StorageQuota::MaxDiskQuota, quota);
    }
    object
}

#[cfg(test)]
mod tests {
    use super::*;

    const MB: u64 = 1024 * 1024;

    fn member(account_id: u32, quota_mb: u64, used_mb: u64) -> QuotaMember {
        QuotaMember {
            account_id,
            quota: quota_mb * MB,
            used: used_mb * MB,
        }
    }

    fn plan(account: &QuotaMember, item: u64, members: &[QuotaMember]) -> TransferPlan {
        plan_transfer(
            account,
            item,
            members,
            ORG_QUOTA_MIN_TRANSFER,
            ORG_QUOTA_DONOR_RESERVE,
        )
    }

    #[test]
    fn transfer_happens_with_minimum_amount() {
        let full = member(1, 100, 100);
        let members = [full, member(2, 1000, 100)];
        assert_eq!(
            plan(&full, MB, &members),
            TransferPlan::Transfer {
                donor_id: 2,
                amount: ORG_QUOTA_MIN_TRANSFER
            }
        );
    }

    #[test]
    fn transfer_moves_item_size_when_larger() {
        let full = member(1, 100, 99);
        let members = [full, member(2, 1000, 0)];
        assert_eq!(
            plan(&full, 80 * MB, &members),
            TransferPlan::Transfer {
                donor_id: 2,
                amount: 80 * MB
            }
        );
    }

    #[test]
    fn transfer_covers_shortfall_when_over_quota() {
        // Already 20 MB over (quota lowered by an administrator)
        let over = member(1, 100, 120);
        let members = [over, member(2, 1000, 0)];
        assert_eq!(
            plan(&over, 60 * MB, &members),
            TransferPlan::Transfer {
                donor_id: 2,
                amount: 80 * MB
            }
        );
    }

    #[test]
    fn picks_member_with_most_free_space() {
        let full = member(1, 100, 100);
        let members = [
            full,
            member(2, 500, 400),  // 100 MB free
            member(3, 300, 0),    // 300 MB free
            member(4, 1000, 800), // 200 MB free
        ];
        assert_eq!(
            plan(&full, MB, &members),
            TransferPlan::Transfer {
                donor_id: 3,
                amount: ORG_QUOTA_MIN_TRANSFER
            }
        );
    }

    #[test]
    fn ties_go_to_lowest_account_id() {
        let full = member(9, 100, 100);
        let members = [full, member(5, 200, 0), member(3, 200, 0)];
        assert!(matches!(
            plan(&full, MB, &members),
            TransferPlan::Transfer { donor_id: 3, .. }
        ));
    }

    #[test]
    fn respects_donor_reserve() {
        let full = member(1, 100, 100);
        // 30 MB free, 20 MB above the reserve: gives 20 MB, not 50 MB
        let members = [full, member(2, 100, 70)];
        assert_eq!(
            plan(&full, 5 * MB, &members),
            TransferPlan::Transfer {
                donor_id: 2,
                amount: 20 * MB
            }
        );
        // 15 MB free, 5 MB above the reserve, but the item needs 6 MB
        let members = [full, member(2, 100, 85)];
        assert_eq!(plan(&full, 6 * MB, &members), TransferPlan::NoDonor);
        // Exactly the reserve free: nothing to give
        let members = [full, member(2, 100, 90)];
        assert_eq!(plan(&full, 1, &members), TransferPlan::NoDonor);
    }

    #[test]
    fn no_donor_when_nobody_has_room() {
        let full = member(1, 100, 100);
        assert_eq!(plan(&full, MB, &[full]), TransferPlan::NoDonor);
        assert_eq!(
            plan(&full, MB, &[full, member(2, 100, 100), member(3, 50, 49)]),
            TransferPlan::NoDonor
        );
    }

    #[test]
    fn unlimited_members_never_donate() {
        let full = member(1, 100, 100);
        assert_eq!(
            plan(&full, MB, &[full, member(2, 0, 0)]),
            TransferPlan::NoDonor
        );
    }

    #[test]
    fn nothing_to_do_when_item_fits_or_account_unlimited() {
        let roomy = member(1, 100, 10);
        assert_eq!(
            plan(&roomy, MB, &[roomy, member(2, 100, 0)]),
            TransferPlan::NotNeeded
        );
        let unlimited = member(1, 0, 10_000);
        assert_eq!(
            plan(&unlimited, MB, &[unlimited, member(2, 100, 0)]),
            TransferPlan::NotNeeded
        );
    }

    #[test]
    fn transfer_keeps_organization_total() {
        let full = member(1, 100, 100);
        let donor = member(2, 1000, 100);
        let TransferPlan::Transfer { amount, .. } = plan(&full, MB, &[full, donor]) else {
            panic!("expected a transfer");
        };
        let before = full.quota + donor.quota;
        let after = (full.quota + amount) + (donor.quota - amount);
        assert_eq!(before, after);
    }
}
