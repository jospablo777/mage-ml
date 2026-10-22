//! The server's authorization of API requests as data (ROADMAP P3):
//! contracts/policy_matrix.json, recorded from the policies, gives the roles that may take
//! each resource's action in each DISABLE_NOTEBOOK_EDIT_ACCESS mode, the query names each role
//! may send and the payload attributes each role may write. With MAGE_GATEWAY_AUTHZ_SHADOW=1
//! the gateway decides every /api request with it and counts where it agrees with the
//! server's answer: the measure for moving authorization to Rust.
//!
//! The matrix is recorded at project level with user permissions off, so the decision is
//! the caller's project role; a request the server refuses for another reason (an attribute
//! the role may not read, a record of another user) shows as a disagreement to learn from.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::LazyLock;

use serde_json::Value;

/// A role's allowed names; `*` allows any.
type Names = BTreeMap<String, BTreeSet<String>>;

struct Rule {
    modes: Vec<u8>,
    roles: BTreeSet<String>,
}

struct NameRule {
    modes: Vec<u8>,
    names: Names,
}

type Key = (String, String);

pub struct Matrix {
    actions: BTreeMap<Key, Vec<Rule>>,
    /// The query names each role may send, by resource and action.
    queries: BTreeMap<Key, Vec<NameRule>>,
    /// The payload attributes each role may write, by resource and action.
    writes: BTreeMap<Key, Vec<NameRule>>,
}

static MATRIX: LazyLock<Matrix> = LazyLock::new(|| {
    Matrix::parse(include_str!(
        "../../../../../../contracts/policy_matrix.json"
    ))
    .expect("contracts/policy_matrix.json reads")
});

pub fn matrix() -> &'static Matrix {
    &MATRIX
}

fn modes(value: &Value) -> Vec<u8> {
    value
        .as_array()
        .map(|m| {
            m.iter()
                .filter_map(Value::as_u64)
                .map(|m| m as u8)
                .collect()
        })
        .unwrap_or_default()
}

fn strings(value: &Value) -> BTreeSet<String> {
    value
        .as_array()
        .map(|v| {
            v.iter()
                .filter_map(Value::as_str)
                .map(String::from)
                .collect()
        })
        .unwrap_or_default()
}

/// The names a role may use for the action in the mode.
fn names_for<'a>(
    rules: &'a BTreeMap<Key, Vec<NameRule>>,
    key: &Key,
    mode: u8,
    role: &str,
) -> Option<&'a BTreeSet<String>> {
    rules
        .get(key)
        .and_then(|rules| rules.iter().find(|r| r.modes.contains(&mode)))
        .and_then(|r| r.names.get(role))
}

fn all_allowed(names: &[&str], allowed: Option<&BTreeSet<String>>) -> bool {
    names
        .iter()
        .all(|name| allowed.is_some_and(|set| set.contains(*name) || set.contains("*")))
}

impl Matrix {
    pub fn parse(text: &str) -> Result<Matrix, String> {
        let value: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let mut actions: BTreeMap<Key, Vec<Rule>> = BTreeMap::new();
        for entry in value["actions"].as_array().ok_or("no actions")? {
            let (Some(resource), Some(action)) = (entry[0].as_str(), entry[1].as_str()) else {
                continue;
            };
            actions
                .entry((resource.into(), action.into()))
                .or_default()
                .push(Rule {
                    modes: modes(&entry[2]),
                    roles: strings(&entry[3]),
                });
        }
        let mut queries: BTreeMap<Key, Vec<NameRule>> = BTreeMap::new();
        let mut writes: BTreeMap<Key, Vec<NameRule>> = BTreeMap::new();
        for entry in value["attributes"].as_array().ok_or("no attributes")? {
            let rules = match entry[1].as_str() {
                Some("query") => &mut queries,
                Some("write") => &mut writes,
                _ => continue,
            };
            let (Some(resource), Some(action)) = (entry[0].as_str(), entry[2].as_str()) else {
                continue;
            };
            let mut names = Names::new();
            for (roles, list) in entry[4].as_object().into_iter().flatten() {
                let list = strings(list);
                for role in roles.split(' ') {
                    names
                        .entry(role.into())
                        .or_default()
                        .extend(list.iter().cloned());
                }
            }
            rules
                .entry((resource.into(), action.into()))
                .or_default()
                .push(NameRule {
                    modes: modes(&entry[3]),
                    names,
                });
        }
        Ok(Matrix {
            actions,
            queries,
            writes,
        })
    }

    fn has_resource(&self, resource: &str) -> bool {
        self.actions
            .range((resource.to_string(), String::new())..)
            .next()
            .is_some_and(|((r, _), _)| r == resource)
    }

    /// The resource and action of a GET path: `/api/<r>` lists, `/api/<r>/<id>` is a
    /// detail, and `/api/<parent>/<id>/<r>` and `/api/<parent>/<id>/<r>/<id>` the same for a
    /// child resource. None for paths the matrix has no resource for.
    #[cfg(test)]
    pub fn target(&self, path: &str) -> Option<(String, &'static str)> {
        self.target_for(&hyper::Method::GET, path)
    }

    /// The resource and action of a request: GET lists or details, POST to a collection
    /// creates, PUT to a member updates, DELETE to a member deletes.
    pub fn target_for(&self, method: &hyper::Method, path: &str) -> Option<(String, &'static str)> {
        let parts: Vec<&str> = path
            .trim_end_matches('/')
            .strip_prefix("/api/")?
            .split('/')
            .filter(|p| !p.is_empty())
            .collect();
        let (resource, member) = match parts.as_slice() {
            [resource] | [_, _, resource] => (*resource, false),
            [resource, _] | [_, _, resource, _] => (*resource, true),
            _ => return None,
        };
        let action = match (method.as_str(), member) {
            ("GET", false) => "list",
            ("GET", true) => "detail",
            ("POST", false) => "create",
            ("PUT", true) => "update",
            ("DELETE", true) => "delete",
            _ => return None,
        };
        self.has_resource(resource)
            .then(|| (resource.to_string(), action))
    }

    /// Whether `role` may take the action with these query names in `mode`; None when the
    /// matrix does not cover it.
    pub fn decide(
        &self,
        resource: &str,
        action: &str,
        mode: u8,
        role: &str,
        names: &[&str],
    ) -> Option<bool> {
        let key = (resource.to_string(), action.to_string());
        let rule = self
            .actions
            .get(&key)?
            .iter()
            .find(|r| r.modes.contains(&mode))?;
        if !rule.roles.contains(role) {
            return Some(false);
        }
        // BasePolicy.authorize_query: an owner sends any name unless notebook edits are
        // off; other roles only the names their policy lists.
        if names.is_empty() || (role == "owner" && mode == 0) {
            return Some(true);
        }
        Some(all_allowed(
            names,
            names_for(&self.queries, &key, mode, role),
        ))
    }

    /// A write: the action and its query names as `decide`, and every attribute of the
    /// payload one the role may write (BasePolicy.authorize_attributes; an owner writes any).
    pub fn decide_write(
        &self,
        resource: &str,
        action: &str,
        mode: u8,
        role: &str,
        names: &[&str],
        attributes: &[&str],
    ) -> Option<bool> {
        let allowed = self.decide(resource, action, mode, role, names)?;
        if !allowed || attributes.is_empty() || role == "owner" {
            return Some(allowed);
        }
        let key = (resource.to_string(), action.to_string());
        Some(all_allowed(
            attributes,
            names_for(&self.writes, &key, mode, role),
        ))
    }
}

/// The attributes of a write's payload, `{"<resource>": {...}}` (the pages send `api_key`
/// beside it); None for a payload of another shape, which is left uncounted.
pub fn payload_attributes(body: &[u8]) -> Option<Vec<String>> {
    if body.is_empty() {
        return Some(Vec::new());
    }
    let value: Value = serde_json::from_slice(body).ok()?;
    let objects: Vec<&serde_json::Map<String, Value>> = value
        .as_object()?
        .iter()
        .filter(|(key, _)| key.as_str() != "api_key")
        .filter_map(|(_, value)| value.as_object())
        .collect();
    match objects.as_slice() {
        [attributes] => Some(attributes.keys().cloned().collect()),
        [] => Some(Vec::new()),
        _ => None,
    }
}

/// The query names the policies check: Tornado's names with a value, without meta names
/// (`_limit`, `_format`) and api_key.
pub fn checked_names(query: &BTreeMap<String, String>) -> Vec<&str> {
    query
        .iter()
        .filter(|(name, value)| !value.is_empty() && !name.starts_with('_') && *name != "api_key")
        .map(|(name, _)| name.as_str())
        .collect()
}

/// DISABLE_NOTEBOOK_EDIT_ACCESS as the server reads it, which it passes as
/// MAGE_GATEWAY_DISABLE_NOTEBOOK_EDIT_ACCESS.
pub fn mode() -> u8 {
    std::env::var("MAGE_GATEWAY_DISABLE_NOTEBOOK_EDIT_ACCESS")
        .or_else(|_| std::env::var("DISABLE_NOTEBOOK_EDIT_ACCESS"))
        .ok()
        .and_then(|v| v.trim().parse::<u8>().ok())
        .filter(|m| *m <= 2)
        .unwrap_or(0)
}

/// Whether the server has user permissions on (MAGE_GATEWAY_REQUIRE_USER_PERMISSIONS): the
/// policies then read grants per entity, which the matrix does not record.
pub fn user_permissions() -> bool {
    std::env::var("MAGE_GATEWAY_REQUIRE_USER_PERMISSIONS").as_deref() == Ok("1")
}

/// What the server answered: refused (its error code 403), allowed (an answer without an
/// error), or None for another error, which says nothing about authorization.
pub fn server_decision(body: &[u8]) -> Option<bool> {
    let value: Value = serde_json::from_slice(body).ok()?;
    match value.get("error") {
        None => Some(true),
        Some(error) => match error.get("code").and_then(Value::as_i64) {
            Some(403) => Some(false),
            _ => None,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn paths_name_their_resource_and_action() {
        let m = matrix();
        assert_eq!(
            m.target("/api/pipelines"),
            Some(("pipelines".into(), "list"))
        );
        assert_eq!(
            m.target("/api/pipelines/etl"),
            Some(("pipelines".into(), "detail"))
        );
        assert_eq!(
            m.target("/api/pipelines/etl/pipeline_schedules"),
            Some(("pipeline_schedules".into(), "list"))
        );
        assert_eq!(m.target("/api/not_a_resource"), None);
        assert_eq!(m.target("/files/x"), None);
        let post = hyper::Method::POST;
        assert_eq!(
            m.target_for(&post, "/api/pipelines/etl/backfills"),
            Some(("backfills".into(), "create"))
        );
        assert_eq!(m.target_for(&post, "/api/backfills/3"), None);
        assert_eq!(
            m.target_for(&hyper::Method::PUT, "/api/backfills/3"),
            Some(("backfills".into(), "update"))
        );
    }

    #[test]
    fn roles_and_query_names_from_the_recorded_policies() {
        let m = matrix();
        // Recorded: pipelines list for owner, admin, editor and viewer, not for none.
        assert_eq!(m.decide("pipelines", "list", 0, "viewer", &[]), Some(true));
        assert_eq!(m.decide("pipelines", "list", 0, "none", &[]), Some(false));
        assert_eq!(
            m.decide("statuses", "list", 0, "anonymous", &[]),
            Some(true)
        );
        // includes_content is the owner's in mode 0; a viewer may not send it.
        assert_eq!(
            m.decide("pipelines", "list", 0, "viewer", &["search"]),
            Some(true)
        );
        assert_eq!(
            m.decide("pipelines", "list", 0, "viewer", &["includes_content"]),
            Some(false)
        );
        assert_eq!(
            m.decide("pipelines", "list", 0, "owner", &["anything"]),
            Some(true)
        );
        assert_eq!(
            m.decide("pipelines", "list", 1, "owner", &["anything"]),
            Some(false)
        );
        assert_eq!(m.decide("nothing", "list", 0, "owner", &[]), None);
    }

    #[test]
    fn writes_check_the_payloads_attributes() {
        let m = matrix();
        // Recorded: an editor creates a backfill with a name only; the owner with any.
        assert_eq!(
            m.decide_write("backfills", "create", 0, "editor", &[], &["name"]),
            Some(true)
        );
        assert_eq!(
            m.decide_write("backfills", "create", 0, "editor", &[], &["interval_units"]),
            Some(false)
        );
        assert_eq!(
            m.decide_write("backfills", "create", 0, "owner", &[], &["anything"]),
            Some(true)
        );
        assert_eq!(
            m.decide_write("backfills", "create", 0, "viewer", &[], &["name"]),
            Some(false)
        );
        assert_eq!(
            payload_attributes(br#"{"backfill": {"name": "x", "status": "initial"}}"#),
            Some(vec!["name".to_string(), "status".to_string()])
        );
        assert_eq!(payload_attributes(b""), Some(vec![]));
        assert_eq!(
            payload_attributes(br#"{"api_key": "k", "session": {"email": "e"}}"#),
            Some(vec!["email".to_string()])
        );
        assert_eq!(payload_attributes(br#"{"a": {}, "b": {}}"#), None);
    }

    #[test]
    fn meta_names_are_not_checked() {
        let query = names(&[
            ("_limit", "5"),
            ("api_key", "k"),
            ("search", "x"),
            ("blank", ""),
        ]);
        assert_eq!(checked_names(&query), ["search"]);
    }

    #[test]
    fn the_servers_answer_as_a_decision() {
        assert_eq!(server_decision(br#"{"pipelines": []}"#), Some(true));
        assert_eq!(server_decision(br#"{"error": {"code": 403}}"#), Some(false));
        assert_eq!(server_decision(br#"{"error": {"code": 404}}"#), None);
        assert_eq!(server_decision(b"not json"), None);
    }
}
