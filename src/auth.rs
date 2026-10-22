//! Who calls Command's operator API and what they may do.
//!
//! Every operator request resolves to a [`Principal`]: a subject and, per
//! organization, the capabilities it holds. Routes ask whether the
//! principal holds a capability *in the organization the request is about*,
//! taken from the record (the node, the enrollment) rather than from what the
//! client says, so one organization's operators never reach another's.
//!
//! Principals come from service tokens (`auxs1_…`, for automation and the
//! first administrator), from operator sessions (OIDC, a later slice), and,
//! on the demo server or with `AGENTUX_COMMAND_INSECURE_DEMO_TOKENS=1`,
//! from the two demo tokens. Without any of them every operator request is
//! refused: the API fails closed.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Mutex, MutexGuard};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::random_hex;

/// Reads the organization: its nodes, enrollments, projection.
pub const ORGANIZATION_READ: &str = "organization.read";
/// Decides approvals in the Office.
pub const APPROVAL_DECIDE: &str = "approval.decide";
/// Sends commands to the organization's nodes.
pub const NODE_COMMAND: &str = "node.command";
/// Issues enrollment tokens; approves and revokes nodes.
pub const NODE_ENROLL: &str = "node.enroll";
/// Issues and revokes the organization's service tokens.
pub const AUTH_ADMIN: &str = "auth.admin";
/// Drafts and edits the organization's definition (ADR 0015).
pub const ORGANIZATION_EDIT: &str = "organization.edit";
/// Publishes a definition draft (ADR 0015).
pub const ORGANIZATION_PUBLISH: &str = "organization.publish";
/// Exports the organization's audit trail.
pub const AUDIT_READ: &str = "audit.read";

/// Every capability, in a stable order.
pub const CAPABILITIES: &[&str] = &[
    ORGANIZATION_READ,
    APPROVAL_DECIDE,
    NODE_COMMAND,
    NODE_ENROLL,
    AUTH_ADMIN,
    ORGANIZATION_EDIT,
    ORGANIZATION_PUBLISH,
    AUDIT_READ,
];

/// Grants in every organization (the demo tokens only).
const ANY_ORGANIZATION: &str = "*";

/// Service tokens start with this.
pub const SERVICE_TOKEN_PREFIX: &str = "auxs1_";

/// Operator sessions (after an OIDC sign-in) start with this.
pub const SESSION_PREFIX: &str = "auxo1_";

/// How long a signed-in session lasts. Group changes at the provider take
/// effect at the next sign-in, so at most this late.
pub const SESSION_HOURS: i64 = 8;

/// Who is calling, and what it may do in each organization.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Principal {
    pub subject: String,
    grants: BTreeMap<String, BTreeSet<String>>,
}

impl Principal {
    pub fn new(subject: impl Into<String>) -> Self {
        Self {
            subject: subject.into(),
            grants: BTreeMap::new(),
        }
    }

    /// Adds `capabilities` in `organization`.
    pub fn grant<'a>(
        mut self,
        organization: &str,
        capabilities: impl IntoIterator<Item = &'a str>,
    ) -> Self {
        self.grants
            .entry(organization.to_string())
            .or_default()
            .extend(capabilities.into_iter().map(str::to_string));
        self
    }

    /// Every organization and the capabilities held there.
    pub fn grants(&self) -> &BTreeMap<String, BTreeSet<String>> {
        &self.grants
    }

    /// Whether the principal holds `capability` in `organization`.
    pub fn can(&self, organization: &str, capability: &str) -> bool {
        [organization, ANY_ORGANIZATION].iter().any(|org| {
            self.grants
                .get(*org)
                .is_some_and(|caps| caps.contains(capability))
        })
    }

    /// Whether it holds `capability` in some organization.
    pub fn can_somewhere(&self, capability: &str) -> bool {
        self.grants.values().any(|caps| caps.contains(capability))
    }

    /// The organizations it holds `capability` in; `None` means all.
    pub fn organizations_with(&self, capability: &str) -> Option<BTreeSet<String>> {
        if self.can(ANY_ORGANIZATION, capability) {
            return None;
        }
        Some(
            self.grants
                .iter()
                .filter(|(_, caps)| caps.contains(capability))
                .map(|(org, _)| org.clone())
                .collect(),
        )
    }
}

/// Resolves a bearer token to a principal.
#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, bearer: &str) -> Option<Principal>;
}

/// The demo tokens: `demo-leader` (every capability but `auth.admin`, in
/// every organization) and `demo-observer` (`organization.read`). For the
/// demo server and development only.
pub struct DemoTokens;

#[async_trait]
impl Authenticator for DemoTokens {
    async fn authenticate(&self, bearer: &str) -> Option<Principal> {
        match bearer {
            "demo-leader" => Some(Principal::new("demo-leader").grant(
                ANY_ORGANIZATION,
                [
                    ORGANIZATION_READ,
                    APPROVAL_DECIDE,
                    NODE_COMMAND,
                    NODE_ENROLL,
                    ORGANIZATION_EDIT,
                    ORGANIZATION_PUBLISH,
                    AUDIT_READ,
                ],
            )),
            "demo-observer" => {
                Some(Principal::new("demo-observer").grant(ANY_ORGANIZATION, [ORGANIZATION_READ]))
            }
            _ => None,
        }
    }
}

#[derive(Debug)]
pub enum Error {
    /// The request is not valid (shown to the caller).
    Invalid(String),
    NotFound,
    Storage(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Invalid(m) | Error::Storage(m) => f.write_str(m),
            Error::NotFound => f.write_str("unknown service token"),
        }
    }
}

impl std::error::Error for Error {}

impl From<tokio_postgres::Error> for Error {
    fn from(e: tokio_postgres::Error) -> Self {
        Error::Storage(e.to_string())
    }
}

impl From<deadpool_postgres::PoolError> for Error {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        Error::Storage(e.to_string())
    }
}

/// A service token as listed: never its secret.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ServiceToken {
    pub id: String,
    pub name: String,
    pub organization_id: String,
    pub capabilities: Vec<String>,
    #[serde(skip_serializing_if = "String::is_empty", default)]
    pub created_by: String,
    pub created_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub revoked_at: Option<DateTime<Utc>>,
}

impl ServiceToken {
    fn usable(&self, now: DateTime<Utc>) -> bool {
        self.revoked_at.is_none() && self.expires_at.is_none_or(|at| now < at)
    }

    fn principal(&self) -> Principal {
        Principal::new(format!("service-token:{}", self.id)).grant(
            &self.organization_id,
            self.capabilities.iter().map(String::as_str),
        )
    }
}

/// What an administrator asks for.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TokenRequest {
    pub name: String,
    pub organization_id: String,
    pub capabilities: Vec<String>,
    /// Days until it expires; none: until revoked.
    pub days: Option<i64>,
    #[serde(skip)]
    pub created_by: String,
}

impl TokenRequest {
    /// Checks the request; an unknown capability or an organization id of
    /// the wrong shape is refused.
    pub fn check(&mut self) -> Result<(), Error> {
        self.name = self.name.trim().to_string();
        if self.name.is_empty() || self.name.len() > 100 {
            return Err(Error::Invalid(
                "name is required (at most 100 characters)".into(),
            ));
        }
        let org_ok = !self.organization_id.is_empty()
            && self.organization_id.len() <= 63
            && self
                .organization_id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || "._-".contains(c));
        if !org_ok {
            return Err(Error::Invalid(
                "organizationId must be letters, digits, '.', '_' or '-'".into(),
            ));
        }
        if self.capabilities.is_empty() {
            return Err(Error::Invalid("capabilities are required".into()));
        }
        if let Some(unknown) = self
            .capabilities
            .iter()
            .find(|c| !CAPABILITIES.contains(&c.as_str()))
        {
            return Err(Error::Invalid(format!(
                "unknown capability {unknown:?} (one of {})",
                CAPABILITIES.join(", ")
            )));
        }
        self.capabilities.sort();
        self.capabilities.dedup();
        if let Some(days) = self.days
            && !(1..=3650).contains(&days)
        {
            return Err(Error::Invalid("days must be between 1 and 3650".into()));
        }
        Ok(())
    }

    fn token(&self, now: DateTime<Utc>) -> ServiceToken {
        ServiceToken {
            id: format!("st-{}", random_hex(6)),
            name: self.name.clone(),
            organization_id: self.organization_id.clone(),
            capabilities: self.capabilities.clone(),
            created_by: self.created_by.clone(),
            created_at: now,
            expires_at: self.days.map(|d| now + Duration::days(d)),
            revoked_at: None,
        }
    }
}

/// A token just issued: its secret, shown once.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Issued {
    #[serde(flatten)]
    pub token: ServiceToken,
    pub secret: String,
}

fn hash(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

fn new_secret() -> String {
    format!("{SERVICE_TOKEN_PREFIX}{}", random_hex(32))
}

/// Keeps service tokens (their hashes) and resolves them.
#[async_trait]
pub trait ServiceTokens: Authenticator {
    async fn issue(&self, request: TokenRequest, now: DateTime<Utc>) -> Result<Issued, Error>;
    async fn list(&self, organization_id: &str) -> Result<Vec<ServiceToken>, Error>;
    async fn get(&self, id: &str) -> Result<ServiceToken, Error>;
    async fn revoke(&self, id: &str, now: DateTime<Utc>) -> Result<(), Error>;
}

/// Service tokens in memory: the demo server and tests.
#[derive(Default)]
pub struct MemoryTokens {
    tokens: Mutex<HashMap<String, (String, ServiceToken)>>,
}

impl MemoryTokens {
    fn lock(&self) -> MutexGuard<'_, HashMap<String, (String, ServiceToken)>> {
        self.tokens.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl Authenticator for MemoryTokens {
    async fn authenticate(&self, bearer: &str) -> Option<Principal> {
        if !bearer.starts_with(SERVICE_TOKEN_PREFIX) {
            return None;
        }
        let wanted = hash(bearer);
        let tokens = self.lock();
        tokens
            .values()
            .find(|(h, t)| *h == wanted && t.usable(Utc::now()))
            .map(|(_, t)| t.principal())
    }
}

#[async_trait]
impl ServiceTokens for MemoryTokens {
    async fn issue(&self, mut request: TokenRequest, now: DateTime<Utc>) -> Result<Issued, Error> {
        request.check()?;
        let token = request.token(now);
        let secret = new_secret();
        self.lock()
            .insert(token.id.clone(), (hash(&secret), token.clone()));
        Ok(Issued { token, secret })
    }

    async fn list(&self, organization_id: &str) -> Result<Vec<ServiceToken>, Error> {
        let mut out: Vec<ServiceToken> = self
            .lock()
            .values()
            .filter(|(_, t)| t.organization_id == organization_id)
            .map(|(_, t)| t.clone())
            .collect();
        out.sort_by(|a, b| a.created_at.cmp(&b.created_at).then(a.id.cmp(&b.id)));
        Ok(out)
    }

    async fn get(&self, id: &str) -> Result<ServiceToken, Error> {
        self.lock()
            .get(id)
            .map(|(_, t)| t.clone())
            .ok_or(Error::NotFound)
    }

    async fn revoke(&self, id: &str, now: DateTime<Utc>) -> Result<(), Error> {
        let mut tokens = self.lock();
        let (_, token) = tokens.get_mut(id).ok_or(Error::NotFound)?;
        token.revoked_at.get_or_insert(now);
        Ok(())
    }
}

/// Service tokens in Command's database (migration 003_service_tokens).
pub struct PostgresTokens {
    pool: deadpool_postgres::Pool,
}

impl PostgresTokens {
    pub fn new(pool: deadpool_postgres::Pool) -> Self {
        Self { pool }
    }
}

const COLUMNS: &str =
    "token_id, name, organization_id, capabilities, created_by, created_at, expires_at, revoked_at";

fn row_token(row: &tokio_postgres::Row) -> ServiceToken {
    ServiceToken {
        id: row.get(0),
        name: row.get(1),
        organization_id: row.get(2),
        capabilities: row.get(3),
        created_by: row.get(4),
        created_at: row.get(5),
        expires_at: row.get(6),
        revoked_at: row.get(7),
    }
}

#[async_trait]
impl Authenticator for PostgresTokens {
    async fn authenticate(&self, bearer: &str) -> Option<Principal> {
        if !bearer.starts_with(SERVICE_TOKEN_PREFIX) {
            return None;
        }
        let client = self.pool.get().await.ok()?;
        let row = client
            .query_opt(
                &format!("SELECT {COLUMNS} FROM service_tokens WHERE token_hash = $1"),
                &[&hash(bearer)],
            )
            .await
            .ok()??;
        let token = row_token(&row);
        token.usable(Utc::now()).then(|| token.principal())
    }
}

#[async_trait]
impl ServiceTokens for PostgresTokens {
    async fn issue(&self, mut request: TokenRequest, now: DateTime<Utc>) -> Result<Issued, Error> {
        request.check()?;
        let token = request.token(now);
        let secret = new_secret();
        let client = self.pool.get().await?;
        client
            .execute(
                "INSERT INTO service_tokens (token_id, token_hash, name, organization_id, capabilities, created_by, created_at, expires_at)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
                &[
                    &token.id,
                    &hash(&secret),
                    &token.name,
                    &token.organization_id,
                    &token.capabilities,
                    &token.created_by,
                    &token.created_at,
                    &token.expires_at,
                ],
            )
            .await?;
        Ok(Issued { token, secret })
    }

    async fn list(&self, organization_id: &str) -> Result<Vec<ServiceToken>, Error> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                &format!("SELECT {COLUMNS} FROM service_tokens WHERE organization_id = $1 ORDER BY created_at, token_id"),
                &[&organization_id],
            )
            .await?;
        Ok(rows.iter().map(row_token).collect())
    }

    async fn get(&self, id: &str) -> Result<ServiceToken, Error> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                &format!("SELECT {COLUMNS} FROM service_tokens WHERE token_id = $1"),
                &[&id],
            )
            .await?
            .ok_or(Error::NotFound)?;
        Ok(row_token(&row))
    }

    async fn revoke(&self, id: &str, now: DateTime<Utc>) -> Result<(), Error> {
        let client = self.pool.get().await?;
        let changed = client
            .execute(
                "UPDATE service_tokens SET revoked_at = COALESCE(revoked_at, $2) WHERE token_id = $1",
                &[&id, &now],
            )
            .await?;
        if changed == 0 {
            Err(Error::NotFound)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn principals_hold_capabilities_per_organization() {
        let p = Principal::new("p").grant("acme", [ORGANIZATION_READ, NODE_COMMAND]);
        assert!(p.can("acme", NODE_COMMAND));
        assert!(!p.can("other", NODE_COMMAND), "only in its organization");
        assert!(!p.can("acme", NODE_ENROLL));
        assert_eq!(
            p.organizations_with(ORGANIZATION_READ),
            Some(BTreeSet::from(["acme".to_string()]))
        );
        let everywhere = Principal::new("demo").grant(ANY_ORGANIZATION, [ORGANIZATION_READ]);
        assert!(everywhere.can("any", ORGANIZATION_READ));
        assert_eq!(everywhere.organizations_with(ORGANIZATION_READ), None);
    }

    #[test]
    fn token_requests_are_checked() {
        let request = |caps: &[&str]| TokenRequest {
            name: "ci".into(),
            organization_id: "acme".into(),
            capabilities: caps.iter().map(|c| c.to_string()).collect(),
            ..TokenRequest::default()
        };
        assert!(request(&[NODE_COMMAND]).check().is_ok());
        assert!(request(&["root"]).check().is_err());
        assert!(request(&[]).check().is_err());
        let mut bad_org = request(&[NODE_COMMAND]);
        bad_org.organization_id = "a b".into();
        assert!(bad_org.check().is_err());
        let mut forever = request(&[NODE_COMMAND]);
        forever.days = Some(0);
        assert!(forever.check().is_err());
    }

    #[tokio::test]
    async fn service_tokens_authenticate_until_revoked_or_expired() {
        let store = MemoryTokens::default();
        let now = Utc::now();
        let issued = store
            .issue(
                TokenRequest {
                    name: "ci".into(),
                    organization_id: "acme".into(),
                    capabilities: vec![NODE_COMMAND.into()],
                    ..TokenRequest::default()
                },
                now,
            )
            .await
            .unwrap();
        assert!(issued.secret.starts_with(SERVICE_TOKEN_PREFIX));
        let p = store.authenticate(&issued.secret).await.unwrap();
        assert!(p.can("acme", NODE_COMMAND) && !p.can("acme", NODE_ENROLL));
        assert!(store.authenticate("auxs1_wrong").await.is_none());
        assert_eq!(store.list("acme").await.unwrap().len(), 1);
        assert!(store.list("other").await.unwrap().is_empty());
        store.revoke(&issued.token.id, now).await.unwrap();
        assert!(
            store.authenticate(&issued.secret).await.is_none(),
            "revoked"
        );

        let expired = store
            .issue(
                TokenRequest {
                    name: "old".into(),
                    organization_id: "acme".into(),
                    capabilities: vec![NODE_COMMAND.into()],
                    days: Some(1),
                    ..TokenRequest::default()
                },
                now - Duration::days(2),
            )
            .await
            .unwrap();
        assert!(
            store.authenticate(&expired.secret).await.is_none(),
            "expired"
        );
    }
}

/// Signed-in operators' sessions: the session secret (in a cookie) to the
/// principal its sign-in mapped to. Only hashes are stored.
#[async_trait]
pub trait Sessions: Send + Sync {
    /// Starts a session; its secret.
    async fn start(&self, principal: &Principal, now: DateTime<Utc>) -> Result<String, Error>;
    async fn resolve(&self, secret: &str, now: DateTime<Utc>) -> Option<Principal>;
    async fn end(&self, secret: &str) -> Result<(), Error>;
}

fn new_session_secret() -> String {
    format!("{SESSION_PREFIX}{}", random_hex(32))
}

/// Sessions in memory: the demo server and tests.
#[derive(Default)]
pub struct MemorySessions {
    sessions: Mutex<HashMap<String, (DateTime<Utc>, Principal)>>,
}

#[async_trait]
impl Sessions for MemorySessions {
    async fn start(&self, principal: &Principal, now: DateTime<Utc>) -> Result<String, Error> {
        let secret = new_session_secret();
        let mut sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        sessions.retain(|_, (until, _)| now < *until);
        sessions.insert(
            hash(&secret),
            (now + Duration::hours(SESSION_HOURS), principal.clone()),
        );
        Ok(secret)
    }

    async fn resolve(&self, secret: &str, now: DateTime<Utc>) -> Option<Principal> {
        let sessions = self.sessions.lock().unwrap_or_else(|e| e.into_inner());
        sessions
            .get(&hash(secret))
            .filter(|(until, _)| now < *until)
            .map(|(_, p)| p.clone())
    }

    async fn end(&self, secret: &str) -> Result<(), Error> {
        self.sessions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&hash(secret));
        Ok(())
    }
}

/// Sessions in Command's database (migration 004_operator_sessions), so
/// they survive a restart and every replica sees them.
pub struct PostgresSessions {
    pool: deadpool_postgres::Pool,
}

impl PostgresSessions {
    pub fn new(pool: deadpool_postgres::Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Sessions for PostgresSessions {
    async fn start(&self, principal: &Principal, now: DateTime<Utc>) -> Result<String, Error> {
        let secret = new_session_secret();
        let client = self.pool.get().await?;
        client
            .execute(
                "DELETE FROM operator_sessions WHERE expires_at <= $1",
                &[&now],
            )
            .await?;
        client
            .execute(
                "INSERT INTO operator_sessions (session_hash, subject, principal, created_at, expires_at) VALUES ($1, $2, $3, $4, $5)",
                &[
                    &hash(&secret),
                    &principal.subject,
                    &tokio_postgres::types::Json(principal),
                    &now,
                    &(now + Duration::hours(SESSION_HOURS)),
                ],
            )
            .await?;
        Ok(secret)
    }

    async fn resolve(&self, secret: &str, now: DateTime<Utc>) -> Option<Principal> {
        let client = self.pool.get().await.ok()?;
        let row = client
            .query_opt(
                "SELECT principal FROM operator_sessions WHERE session_hash = $1 AND expires_at > $2",
                &[&hash(secret), &now],
            )
            .await
            .ok()??;
        let tokio_postgres::types::Json(principal): tokio_postgres::types::Json<Principal> =
            row.get(0);
        Some(principal)
    }

    async fn end(&self, secret: &str) -> Result<(), Error> {
        let client = self.pool.get().await?;
        client
            .execute(
                "DELETE FROM operator_sessions WHERE session_hash = $1",
                &[&hash(secret)],
            )
            .await?;
        Ok(())
    }
}
