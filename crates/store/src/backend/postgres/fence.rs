/*
 * SPDX-FileCopyrightText: 2026 Sameer Gupta
 *
 * SPDX-License-Identifier: AGPL-3.0-only
 */

//! Sameer's fork: the account fence on PostgreSQL (see FORK.md and `crate::fence`).
//!
//! With `PostgreSql.accountFence` on, every per-account table gets row-level security that shows a connection only
//! the rows of the accounts in its scope, or every row when its scope is `system`. A connection whose scope was never
//! set sees nothing. The backend puts the task's scope on each connection it checks out (`PostgresStore::conn`).
//!
//! The tables, by how their keys start (`write/key.rs`):
//!
//! | Table | Rows | Rule |
//! | --- | --- | --- |
//! | `p` properties and metadata, `c` immutable, `i` indexes, `v` index properties | the account id | fenced |
//! | `l` change logs | the account id | fenced, except share notifications (collection 9): written into the account being shared with |
//! | `n` counters | a kind byte, then the account id for document ids, change ids, object counts, quotas and mailbox UIDs | fenced by account, except change ids (numbers only, bumped in the account being shared with); other kinds open |
//! | `a` ACLs | the grantee, then the owner | either in scope |
//! | `s_email`, `s_cal`, `s_card`, `s_file` search | column `accid` | fenced |
//! | everything else: directory, registry, queue, blobs and their links, reports, telemetry, tasks, spam samples, archives, search queue, tracing | not by account | open |

use super::{into_error, pool::PgObject};

/// The functions the policies call. `(SELECT f())` in a policy runs once per query, not once per row.
const FUNCTIONS: &str = "
CREATE OR REPLACE FUNCTION stalwart_fence_system() RETURNS boolean LANGUAGE sql STABLE AS
  $$ SELECT coalesce(current_setting('stalwart.scope', true) = 'system', false) $$;
CREATE OR REPLACE FUNCTION stalwart_fence_accounts() RETURNS bytea[] LANGUAGE sql STABLE AS
  $$ SELECT coalesce(array(SELECT decode(x, 'hex') FROM
       unnest(string_to_array(nullif(current_setting('stalwart.accounts', true), ''), ',')) x), '{}') $$;
CREATE OR REPLACE FUNCTION stalwart_fence_account_ids() RETURNS integer[] LANGUAGE sql STABLE AS
  $$ SELECT coalesce(string_to_array(nullif(current_setting('stalwart.account_ids', true), ''), ',')::integer[],
       '{}') $$;
";

const SYSTEM: &str = "(SELECT stalwart_fence_system())";
const ACCOUNTS: &str = "(SELECT stalwart_fence_accounts())::bytea[]";
const ACCOUNT_IDS: &str = "accid = ANY ((SELECT stalwart_fence_account_ids())::integer[])";

/// Each fenced table and who may see a row of it.
fn rules() -> Vec<(&'static str, String)> {
    let first = format!("substring(k FROM 1 FOR 4) = ANY ({ACCOUNTS})");
    let after_kind = format!("substring(k FROM 2 FOR 4) = ANY ({ACCOUNTS})");
    let owner = format!("substring(k FROM 5 FOR 4) = ANY ({ACCOUNTS})");
    vec![
        ("p", first.clone()),
        ("c", first.clone()),
        ("i", first.clone()),
        ("v", first.clone()),
        ("l", format!("{first} OR get_byte(k, 4) = 9")),
        // Kinds 0 (document ids), 2 (object counts), 3 (quotas) and 4 (mailbox UIDs) are per account.
        (
            "n",
            format!("get_byte(k, 0) NOT IN (0, 2, 3, 4) OR {after_kind}"),
        ),
        ("a", format!("{first} OR {owner}")),
        ("s_email", ACCOUNT_IDS.to_string()),
        ("s_cal", ACCOUNT_IDS.to_string()),
        ("s_card", ACCOUNT_IDS.to_string()),
        ("s_file", ACCOUNT_IDS.to_string()),
    ]
}

/// The statements that put the fence on `tables` (or take it off), idempotent.
pub(crate) fn statements(tables: &[&str], on: bool) -> Vec<String> {
    let mut out = Vec::new();
    if on {
        out.push(FUNCTIONS.to_string());
    }
    for (table, rule) in rules() {
        if !tables.contains(&table) {
            continue;
        }
        out.push(format!("DROP POLICY IF EXISTS stalwart_fence ON {table}"));
        if on {
            out.push(format!(
                "CREATE POLICY stalwart_fence ON {table} USING ({SYSTEM} OR {rule}) WITH CHECK ({SYSTEM} OR {rule})"
            ));
            out.push(format!("ALTER TABLE {table} ENABLE ROW LEVEL SECURITY"));
            out.push(format!("ALTER TABLE {table} FORCE ROW LEVEL SECURITY"));
        } else {
            out.push(format!("ALTER TABLE {table} NO FORCE ROW LEVEL SECURITY"));
            out.push(format!("ALTER TABLE {table} DISABLE ROW LEVEL SECURITY"));
        }
    }
    out
}

/// The storage tables the fence covers.
pub(crate) const STORAGE_TABLES: &[&str] = &["p", "c", "i", "v", "l", "n", "a"];
/// The search tables it covers.
pub(crate) const SEARCH_TABLES: &[&str] = &["s_email", "s_cal", "s_card", "s_file"];

/// Puts the fence on `tables`, or takes it off. With it on, the role Stalwart connects as must not bypass row-level
/// security (a superuser or a role with BYPASSRLS would see everything), so that's checked first.
pub(crate) async fn apply(conn: &PgObject, tables: &[&str], on: bool) -> trc::Result<()> {
    if on {
        let row = conn
            .query_one(
                "SELECT rolsuper OR rolbypassrls FROM pg_roles WHERE rolname = current_user",
                &[],
            )
            .await
            .map_err(into_error)?;
        if row.try_get::<_, bool>(0).map_err(into_error)? {
            return Err(trc::StoreEvent::PostgresqlError.into_err().details(
                "The account fence needs a role that doesn't bypass row-level security: \
                 connect as a role that isn't a superuser and has NOBYPASSRLS",
            ));
        }
    } else {
        // Nothing to take off unless the fence was on before.
        let row = conn
            .query_one(
                "SELECT count(*) FROM pg_policies WHERE policyname = 'stalwart_fence' AND tablename = ANY($1)",
                &[&tables],
            )
            .await
            .map_err(into_error)?;
        if row.try_get::<_, i64>(0).map_err(into_error)? == 0 {
            return Ok(());
        }
    }
    for statement in statements(tables, on) {
        conn.batch_execute(&statement).await.map_err(into_error)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_fenced_table_has_a_policy_that_lets_system_through() {
        let on = statements(STORAGE_TABLES, true);
        for table in STORAGE_TABLES {
            let policy = on
                .iter()
                .find(|s| s.starts_with(&format!("CREATE POLICY stalwart_fence ON {table} ")))
                .unwrap_or_else(|| panic!("no policy for {table}"));
            assert!(
                policy.contains("USING ((SELECT stalwart_fence_system()) OR "),
                "{policy}"
            );
            assert!(on.contains(&format!("ALTER TABLE {table} FORCE ROW LEVEL SECURITY")));
        }
        let off = statements(SEARCH_TABLES, false);
        assert!(off.iter().all(|s| !s.contains("CREATE")));
        assert!(off.contains(&"ALTER TABLE s_email DISABLE ROW LEVEL SECURITY".to_string()));
    }
}
