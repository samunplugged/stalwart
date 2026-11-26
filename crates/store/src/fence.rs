/*
 * SPDX-FileCopyrightText: 2026 Sameer Gupta
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

//! Sameer's fork: the account fence (see FORK.md).
//!
//! The scope a task's database reads and writes run in. A session that has authenticated (JMAP and the rest of HTTP,
//! IMAP, POP3, ManageSieve) runs in the scope of the accounts its access token opens: its own, the groups it belongs
//! to and the accounts that share with it. Everything else (delivery, the queue, indexing, reports, housekeeping and
//! administrators) runs as `System`. The PostgreSQL backend sends the scope to the database with each connection it
//! checks out, and row-level security there hides every per-account row outside it (`backend/postgres/fence.rs`).
//!
//! The scope comes from who signed in, never from the key being read, so a bug that reads another account's key is
//! stopped by the database instead of answered. A task holds a *slot* for its scope: protocol handlers open one for
//! each request or session ([`slot`]), and fill it when the session authenticates ([`enter`]). Work spawned from a
//! session takes the session's scope with it ([`inherit`]); a task without a slot runs as `System`.

use std::{cell::RefCell, fmt::Write, future::Future, sync::Arc};

/// Whose rows a task may read and write.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub enum Scope {
    /// Every row: background work and administrators.
    #[default]
    System,
    /// Only the rows of these accounts, sorted and without repeats.
    Accounts(Arc<[u32]>),
}

impl Scope {
    /// The scope of a session that may open `ids` (its own account first, then the rest).
    pub fn accounts(ids: impl IntoIterator<Item = u32>) -> Self {
        let mut ids: Vec<u32> = ids.into_iter().collect();
        ids.sort_unstable();
        ids.dedup();
        Scope::Accounts(ids.into())
    }

    pub fn is_system(&self) -> bool {
        matches!(self, Scope::System)
    }

    /// The SQL that puts this scope on a PostgreSQL connection: `stalwart.scope` (`system` or `accounts`),
    /// `stalwart.accounts` (each account id as 8 hex digits, the 4-byte big-endian prefix of its keys) and
    /// `stalwart.account_ids` (the same ids in decimal, for the search tables' `accid`). Only digits and hex go into
    /// the statement, so it can be sent as a simple query.
    pub fn postgres_statement(&self) -> String {
        match self {
            Scope::System => "SELECT set_config('stalwart.scope', 'system', false), \
                set_config('stalwart.accounts', '', false), set_config('stalwart.account_ids', '', false)"
                .to_string(),
            Scope::Accounts(ids) => {
                let mut prefixes = String::with_capacity(ids.len() * 11);
                let mut decimal = String::with_capacity(ids.len() * 11);
                for (i, id) in ids.iter().enumerate() {
                    if i > 0 {
                        prefixes.push(',');
                        decimal.push(',');
                    }
                    let _ = write!(prefixes, "{id:08x}");
                    // As the search tables store it (INTEGER).
                    let _ = write!(decimal, "{}", *id as i32);
                }
                format!(
                    "SELECT set_config('stalwart.scope', 'accounts', false), \
                     set_config('stalwart.accounts', '{prefixes}', false), \
                     set_config('stalwart.account_ids', '{decimal}', false)"
                )
            }
        }
    }
}

tokio::task_local! {
    static SLOT: RefCell<Scope>;
}

/// Runs `future` with a slot of its own, `System` until [`enter`] fills it. Protocol handlers wrap each request
/// (HTTP) or each connection (IMAP, POP3, ManageSieve) in one. The future is boxed: request and session futures are
/// large, and wrapping one in place makes it larger still.
pub fn slot<F: Future>(future: F) -> impl Future<Output = F::Output> {
    SLOT.scope(RefCell::new(Scope::System), Box::pin(future))
}

/// Puts the current task's slot in `scope`; nothing happens in a task without a slot.
pub fn enter(scope: Scope) {
    let _ = SLOT.try_with(|slot| *slot.borrow_mut() = scope);
}

/// The current task's scope: `System` outside a slot.
pub fn current() -> Scope {
    SLOT.try_with(|slot| slot.borrow().clone())
        .unwrap_or_default()
}

/// Runs `future`, which is about to be spawned, in the scope of the task spawning it.
pub fn inherit<F: Future>(future: F) -> impl Future<Output = F::Output> {
    SLOT.scope(RefCell::new(current()), Box::pin(future))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statements_carry_only_digits_and_hex() {
        assert_eq!(
            Scope::accounts([7, 1, 7, 0x0102_0304]).postgres_statement(),
            "SELECT set_config('stalwart.scope', 'accounts', false), \
             set_config('stalwart.accounts', '00000001,00000007,01020304', false), \
             set_config('stalwart.account_ids', '1,7,16909060', false)"
        );
        assert!(Scope::System.postgres_statement().contains("'system'"));
        assert_eq!(Scope::accounts([]), Scope::Accounts(Arc::from([])));
    }

    #[tokio::test]
    async fn slots_hold_a_task_scope_and_spawned_work_inherits_it() {
        assert_eq!(current(), Scope::System);
        enter(Scope::accounts([1]));
        assert_eq!(current(), Scope::System, "no slot, so nothing to fill");

        slot(async {
            assert_eq!(current(), Scope::System);
            enter(Scope::accounts([3, 2]));
            assert_eq!(current(), Scope::accounts([2, 3]));

            let inherited = tokio::spawn(inherit(async { current() }));
            let plain = tokio::spawn(async { current() });
            assert_eq!(inherited.await.unwrap(), Scope::accounts([2, 3]));
            assert_eq!(plain.await.unwrap(), Scope::System);

            // A nested slot (a second request on the same connection) starts afresh.
            slot(async { assert_eq!(current(), Scope::System) }).await;
            assert_eq!(current(), Scope::accounts([2, 3]));
        })
        .await;
        assert_eq!(current(), Scope::System);
    }
}
