//! Who a request is and whether it may read, as MageML's server decides it, for the
//! gateway's shadow reads (gateway_shadow.rs): OAuthMiddleware's identity (the API key, then
//! a bearer token or the session cookie, a JWT signed with JWT_SECRET) and the policies'
//! has_at_least_viewer_role on a pipeline or the project. The gateway compares its decision
//! with the server's before a route may move to it.

use std::collections::BTreeMap;

use hyper::header::HeaderMap;
use serde_json::Value;

use crate::gateway_shadow::{Metadata, Raw, read_rows};

/// The server's settings the decisions depend on; MageML passes them when it starts the
/// gateway.
#[derive(Debug, Clone)]
pub struct AuthSettings {
    pub require_authentication: bool,
    pub jwt_secret: String,
    /// OAUTH2_APPLICATION_CLIENT_ID: the only API key the middleware takes.
    pub client_id: String,
    pub project_uuid: String,
}

impl AuthSettings {
    pub fn from_env() -> Option<AuthSettings> {
        Some(AuthSettings {
            require_authentication: std::env::var("MAGE_GATEWAY_REQUIRE_AUTHENTICATION")
                .ok()?
                .trim()
                == "1",
            jwt_secret: std::env::var("JWT_SECRET").unwrap_or_else(|_| "materia".to_string()),
            client_id: std::env::var("MAGE_GATEWAY_CLIENT_ID").ok()?,
            project_uuid: std::env::var("MAGE_GATEWAY_PROJECT_UUID").unwrap_or_default(),
        })
    }
}

/// The caller as the middleware leaves it on the request.
#[derive(Debug, Clone, PartialEq)]
pub enum Caller {
    /// No sign-in needed (authentication off).
    Open,
    /// A valid API key and no token: the public scope, which reads none of these routes.
    Public,
    /// The middleware's error: invalid_api_key, invalid_oauth_token or expired_oauth_token.
    Refused(&'static str),
    User(i64),
}

/// Where a read is checked: a pipeline, or the project.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Entity<'a> {
    Pipeline(&'a str),
    Project,
}

pub async fn caller(
    metadata: &Metadata,
    settings: &AuthSettings,
    headers: &HeaderMap,
    query: &BTreeMap<String, String>,
) -> Result<Caller, String> {
    if !settings.require_authentication {
        return Ok(Caller::Open);
    }
    let header = |name: &str| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    };
    let Some(api_key) = header("X-API-KEY").or_else(|| query.get("api_key").cloned()) else {
        return Ok(Caller::Refused("invalid_api_key"));
    };
    let applications = read_rows(
        metadata,
        "SELECT id FROM oauth2_application WHERE client_id = {1}",
        &[Raw::Text(api_key.clone())],
    )
    .await?;
    let Some(Raw::Int(application)) = applications.first().and_then(|r| r.first()).cloned() else {
        return Ok(Caller::Refused("invalid_api_key"));
    };
    if api_key != settings.client_id {
        return Ok(Caller::Refused("invalid_api_key"));
    }
    let bearer = header("Authorization")
        .and_then(|v| v.strip_prefix("Bearer ").map(|t| t.trim().to_string()));
    let token = match header("OAUTH-TOKEN")
        .or(bearer)
        .or_else(|| query.get("HTTP_AUTHORIZATION").cloned())
    {
        Some(token) => token,
        None => match header("Cookie").and_then(|c| cookie(&c, "oauth_token")) {
            // A cookie that does not decode is an invalid token (I-364).
            Some(cookie) => match jwt_token(&cookie, &settings.jwt_secret) {
                Some(token) => token,
                None => return Ok(Caller::Refused("invalid_oauth_token")),
            },
            None => return Ok(Caller::Public),
        },
    };
    let tokens = read_rows(
        metadata,
        "SELECT t.user_id, t.expires, u.id FROM oauth2_access_token t \
         LEFT JOIN \"user\" u ON u.id = t.user_id \
         WHERE t.oauth2_application_id = {1} AND t.token = {2}",
        &[Raw::Int(application), Raw::Text(token)],
    )
    .await?;
    let Some(row) = tokens.first() else {
        return Ok(Caller::Refused("invalid_oauth_token"));
    };
    let unexpired = match &row[1] {
        Raw::Time(at) => *at >= chrono::Utc::now(),
        Raw::Text(text) => naive_utc(text).is_some_and(|at| at >= chrono::Utc::now()),
        _ => false,
    };
    match (&row[2], unexpired) {
        (Raw::Int(user), true) => Ok(Caller::User(*user)),
        _ => Ok(Caller::Refused("expired_oauth_token")),
    }
}

/// has_at_least_viewer_role for a signed-in user: the owner flag, an owner role on the
/// project, any of the older role bits, or any access on the entity through the user's
/// roles, whose permissions on a pipeline fall back to the project's, then the global ones.
pub async fn may_view(
    metadata: &Metadata,
    settings: &AuthSettings,
    user: i64,
    entity: Entity<'_>,
) -> Result<bool, String> {
    Ok(viewer(metadata, settings, user).await?.may_view(entity))
}

/// What has_at_least_viewer_role reads about a user, read once for several entities.
pub struct Viewer {
    everything: bool,
    grants: BTreeMap<i64, Vec<Grant>>,
    project: i64,
    project_uuid: String,
    /// The user's owner flag and older role bits (1 admin, 2 editor, 4 viewer).
    owner_flag: bool,
    legacy_roles: i64,
}

impl Viewer {
    /// The owner flag, an older role bit or an owner role on the project: every entity.
    pub fn sees_everything(&self) -> bool {
        self.everything
    }

    /// The user's role on the project, named as contracts/policy_matrix.json names roles:
    /// is_owner, then has_at_least_admin_role, editor and viewer, then `none`.
    pub fn role(&self) -> &'static str {
        if self.owner_flag || self.project & OWNER != 0 {
            "owner"
        } else if self.legacy_roles & 1 != 0 || self.project & ADMIN != 0 {
            "admin"
        } else if self.legacy_roles & 2 != 0 || self.project & EDITOR != 0 {
            "editor"
        } else if self.legacy_roles & 4 != 0 || self.project & VIEWER != 0 {
            "viewer"
        } else {
            "none"
        }
    }

    pub fn may_view(&self, entity: Entity<'_>) -> bool {
        if self.everything {
            return true;
        }
        let on_entity = match entity {
            Entity::Pipeline(uuid) => {
                access(&self.grants, "PIPELINE", Some(uuid), &self.project_uuid)
            }
            Entity::Project => self.project,
        };
        on_entity & (OWNER | ADMIN | EDITOR | VIEWER) != 0
    }
}

pub async fn viewer(
    metadata: &Metadata,
    settings: &AuthSettings,
    user: i64,
) -> Result<Viewer, String> {
    let nobody = |everything, owner_flag, legacy_roles| Viewer {
        everything,
        grants: BTreeMap::new(),
        project: 0,
        project_uuid: settings.project_uuid.clone(),
        owner_flag,
        legacy_roles,
    };
    let users = read_rows(
        metadata,
        "SELECT owner, roles FROM \"user\" WHERE id = {1}",
        &[Raw::Int(user)],
    )
    .await?;
    let Some(row) = users.first() else {
        return Ok(nobody(false, false, 0));
    };
    let owner_flag = matches!(row[0], Raw::Bool(true) | Raw::Int(1));
    let legacy_roles = match row[1] {
        Raw::Int(bits) => bits,
        _ => 0,
    };
    if owner_flag || legacy_roles & 0b111 != 0 {
        return Ok(nobody(true, owner_flag, legacy_roles));
    }
    let permissions = read_rows(
        metadata,
        "SELECT ur.role_id, CAST(p.entity AS TEXT), p.entity_id, p.access FROM user_role ur \
         JOIN permission p ON p.role_id = ur.role_id WHERE ur.user_id = {1} \
         UNION ALL \
         SELECT ur.role_id, CAST(p.entity AS TEXT), p.entity_id, p.access FROM user_role ur \
         JOIN role_permission rp ON rp.role_id = ur.role_id \
         JOIN permission p ON p.id = rp.permission_id WHERE ur.user_id = {1}",
        &[Raw::Int(user)],
    )
    .await?;
    let has_roles = !read_rows(
        metadata,
        "SELECT role_id FROM user_role WHERE user_id = {1}",
        &[Raw::Int(user)],
    )
    .await?
    .is_empty();
    let grants = grants(&permissions);
    let project = access(
        &grants,
        "PROJECT",
        Some(&settings.project_uuid),
        &settings.project_uuid,
    );
    Ok(Viewer {
        everything: has_roles && project & OWNER != 0,
        grants,
        project,
        project_uuid: settings.project_uuid.clone(),
        owner_flag,
        legacy_roles,
    })
}

const OWNER: i64 = 1;
const ADMIN: i64 = 2;
const EDITOR: i64 = 4;
const VIEWER: i64 = 8;

/// One permission of one of the user's roles: (entity, entity id, access).
type Grant = (String, Option<String>, Option<i64>);

fn grants(rows: &[Vec<Raw>]) -> BTreeMap<i64, Vec<Grant>> {
    let mut by_role: BTreeMap<i64, Vec<Grant>> = BTreeMap::new();
    for row in rows {
        let Raw::Int(role) = row[0] else { continue };
        let text = |raw: &Raw| match raw {
            Raw::Text(v) => Some(v.clone()),
            _ => None,
        };
        let access = match row[3] {
            Raw::Int(v) => Some(v),
            _ => None,
        };
        by_role.entry(role).or_default().push((
            text(&row[1]).unwrap_or_default().to_uppercase(),
            text(&row[2]),
            access,
        ));
    }
    by_role
}

/// User.get_access: the roles' Role.get_access, OR-ed.
fn access(
    grants: &BTreeMap<i64, Vec<Grant>>,
    entity: &str,
    entity_id: Option<&str>,
    project_uuid: &str,
) -> i64 {
    grants
        .values()
        .map(|role| role_access(role, entity, entity_id, project_uuid))
        .fold(0, |a, b| a | b)
}

/// Role.get_access: the role's permissions on the entity (and id), or its parent's when it
/// has none there: a pipeline's parent is the project, the project's the global ones.
fn role_access(role: &[Grant], entity: &str, entity_id: Option<&str>, project_uuid: &str) -> i64 {
    let matching: Vec<&Grant> = role
        .iter()
        .filter(|(e, id, _)| e == entity && (entity_id.is_none() || id.as_deref() == entity_id))
        .collect();
    if !matching.is_empty() {
        return matching
            .iter()
            .filter_map(|(_, _, access)| *access)
            .fold(0, |a, b| a | b);
    }
    match entity {
        "PIPELINE" => role_access(role, "PROJECT", Some(project_uuid), project_uuid),
        "PROJECT" => role_access(role, "GLOBAL", None, project_uuid),
        _ => 0,
    }
}

fn cookie(header: &str, name: &str) -> Option<String> {
    header.split(';').find_map(|pair| {
        let (key, value) = pair.trim().split_once('=')?;
        (key == name).then(|| value.to_string())
    })
}

/// The `token` of a session cookie: a JWT (HS256) over {expires, token}, which PyJWT checks
/// by its signature alone.
fn jwt_token(encoded: &str, secret: &str) -> Option<String> {
    use base64::Engine;
    let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
    let mut parts = encoded.split('.');
    let (header, payload, signature) = (parts.next()?, parts.next()?, parts.next()?);
    if parts.next().is_some() {
        return None;
    }
    let header: Value = serde_json::from_slice(&engine.decode(header).ok()?).ok()?;
    if header.get("alg").and_then(Value::as_str) != Some("HS256") {
        return None;
    }
    let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
    let signed = parts_prefix(encoded)?;
    ring::hmac::verify(&key, signed.as_bytes(), &engine.decode(signature).ok()?).ok()?;
    let payload: Value = serde_json::from_slice(&engine.decode(payload).ok()?).ok()?;
    payload
        .get("token")
        .and_then(Value::as_str)
        .map(str::to_string)
}

/// `header.payload` of a JWT: what its signature covers.
fn parts_prefix(encoded: &str) -> Option<&str> {
    let end = encoded.rfind('.')?;
    Some(&encoded[..end])
}

/// A SQLite DATETIME (naive, UTC) as a UTC time.
pub(crate) fn naive_utc(text: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    let text = text.trim();
    [
        "%Y-%m-%d %H:%M:%S%.f",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S%.f",
    ]
    .iter()
    .find_map(|format| chrono::NaiveDateTime::parse_from_str(text, format).ok())
    .map(|at| at.and_utc())
    .or_else(|| {
        chrono::DateTime::parse_from_rfc3339(text)
            .ok()
            .map(|at| at.to_utc())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn jwt(payload: &str, secret: &str) -> String {
        use base64::Engine;
        let engine = base64::engine::general_purpose::URL_SAFE_NO_PAD;
        let head = engine.encode(br#"{"alg":"HS256","typ":"JWT"}"#);
        let body = engine.encode(payload.as_bytes());
        let key = ring::hmac::Key::new(ring::hmac::HMAC_SHA256, secret.as_bytes());
        let signature = ring::hmac::sign(&key, format!("{head}.{body}").as_bytes());
        format!("{head}.{body}.{}", engine.encode(signature.as_ref()))
    }

    #[test]
    fn a_session_cookie_holds_its_token_when_signed_with_the_secret() {
        let cookie = jwt(r#"{"expires": 1792631985.0, "token": "abc"}"#, "materia");
        assert_eq!(jwt_token(&cookie, "materia").as_deref(), Some("abc"));
        assert_eq!(jwt_token(&cookie, "another"), None);
        assert_eq!(jwt_token("not-a-jwt", "materia"), None);
        assert_eq!(
            cookie_value("a=1; oauth_token=xyz; b=2"),
            Some("xyz".to_string())
        );
    }

    fn cookie_value(header: &str) -> Option<String> {
        cookie(header, "oauth_token")
    }

    #[test]
    fn a_role_falls_back_from_pipeline_to_project_to_global() {
        let global_viewer = vec![("GLOBAL".to_string(), None, Some(VIEWER))];
        assert_eq!(
            role_access(&global_viewer, "PIPELINE", Some("p"), "proj"),
            VIEWER
        );
        // Nothing on this pipeline: no access, and no fallback past it.
        let denied = vec![
            (
                "PROJECT".to_string(),
                Some("proj".to_string()),
                Some(VIEWER),
            ),
            ("PIPELINE".to_string(), Some("p".to_string()), Some(0)),
        ];
        assert_eq!(role_access(&denied, "PIPELINE", Some("p"), "proj"), 0);
        assert_eq!(role_access(&denied, "PIPELINE", Some("q"), "proj"), VIEWER);
        assert_eq!(role_access(&denied, "PROJECT", Some("other"), "proj"), 0);
    }
}
