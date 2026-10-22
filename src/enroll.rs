//! Lets AgentUX nodes join one Command server, and only that one.
//!
//! Every Command server has an instance id. Operators issue enrollment
//! tokens that name it (`auxe1_<instanceId>_<secret>`), are single use and
//! expire. A node redeems one token at this server's /v1/enroll and gets a
//! node credential of its own (`auxn1_<secret>`), bound to this instance,
//! its organization and a node id unique here. Two Command servers in the
//! same institution never accept each other's tokens or credentials, so
//! neither can take the other's nodes; nodes pin the instance id they
//! enrolled with.
//!
//! Only hashes of tokens and credentials are stored.

use std::collections::{HashMap, HashSet};
use std::sync::{LazyLock, Mutex, MutexGuard};

use async_trait::async_trait;
use chrono::{DateTime, Duration, Utc};
use regex::Regex;
use serde::Serialize;
use sha2::{Digest, Sha256};
use subtle::ConstantTimeEq;

use crate::random_hex;

pub use auxp::enrollment::{
    CREDENTIAL_PREFIX as SECRET_PREFIX, TOKEN_PREFIX, token_instance, valid_instance_id,
};

static NODE_ID: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[a-z0-9][a-z0-9._-]{0,62}$").unwrap());
static ORGANIZATION: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,62}$").unwrap());

#[derive(Debug)]
pub enum Error {
    InvalidToken,
    OtherInstance,
    ExpiredToken,
    UsedToken,
    UnknownNode,
    /// The request is not valid (shown to the operator or node).
    Invalid(String),
    Storage(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::InvalidToken => f.write_str("the enrollment token is not valid here"),
            Error::OtherInstance => {
                f.write_str("the enrollment token belongs to another Command server")
            }
            Error::ExpiredToken => f.write_str("the enrollment token has expired"),
            Error::UsedToken => f.write_str("the enrollment token was already used"),
            Error::UnknownNode => f.write_str("unknown node"),
            Error::Invalid(message) | Error::Storage(message) => f.write_str(message),
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

/// What an operator asks for.
#[derive(Debug, Clone, Default)]
pub struct TokenRequest {
    pub organization_id: String,
    pub node_name: String,
    pub count: i64,
    pub ttl: Duration,
    pub require_approval: bool,
    pub created_by: String,
}

/// Shown once, when it is created.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IssuedToken {
    pub token: String,
    pub expires_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub node_name: String,
}

/// A node's membership of this Command server.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Credential {
    pub node_id: String,
    pub organization_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    pub hostname: String,
    pub approved: bool,
    pub enrolled_at: DateTime<Utc>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub revoked_at: Option<DateTime<Utc>>,
}

impl Credential {
    /// The node may send and receive.
    pub fn active(&self) -> bool {
        self.approved && self.revoked_at.is_none()
    }
}

/// What a node gets for a valid token.
#[derive(Debug, Clone)]
pub struct Redemption {
    pub credential: Credential,
    /// The node's secret, shown once.
    pub secret: String,
}

/// Keeps the instance id, tokens and credentials.
#[async_trait]
pub trait Store: Send + Sync {
    async fn instance_id(&self) -> Result<String, Error>;
    async fn issue_tokens(
        &self,
        request: TokenRequest,
        now: DateTime<Utc>,
    ) -> Result<Vec<IssuedToken>, Error>;
    /// Checks the token, takes a node id and stores the credential, all at
    /// once: a token is never redeemed twice.
    async fn redeem(
        &self,
        token: &str,
        node_name: &str,
        hostname: &str,
        now: DateTime<Utc>,
    ) -> Result<Redemption, Error>;
    async fn authenticate(&self, node_id: &str, secret: &str) -> Result<Credential, Error>;
    async fn credentials(&self) -> Result<Vec<Credential>, Error>;
    async fn approve(&self, node_id: &str, now: DateTime<Utc>) -> Result<(), Error>;
    async fn revoke(&self, node_id: &str, now: DateTime<Utc>) -> Result<(), Error>;
}

/// `cmd-` and 16 hex digits.
pub fn new_instance_id() -> String {
    format!("cmd-{}", random_hex(8))
}

fn hash(secret: &str) -> String {
    hex::encode(Sha256::digest(secret.as_bytes()))
}

fn same(a: &str, b: &str) -> bool {
    a.as_bytes().ct_eq(b.as_bytes()).into()
}

fn new_token(instance: &str) -> String {
    auxp::enrollment::token(instance, &random_hex(32))
}

fn new_secret() -> String {
    auxp::enrollment::credential(&random_hex(32))
}

/// Checks and fills a token request.
fn validate_request(request: &mut TokenRequest) -> Result<(), Error> {
    if !ORGANIZATION.is_match(&request.organization_id) {
        return Err(Error::Invalid(
            "organizationId must be letters, digits, '.', '_' or '-'".into(),
        ));
    }
    request.node_name = request.node_name.trim().to_lowercase();
    if !request.node_name.is_empty() && !NODE_ID.is_match(&request.node_name) {
        return Err(Error::Invalid(
            "nodeName must be lowercase letters, digits, '.', '_' or '-'".into(),
        ));
    }
    if request.count < 1 {
        request.count = 1;
    }
    if request.count > 500 {
        return Err(Error::Invalid("at most 500 tokens at a time".into()));
    }
    if !request.node_name.is_empty() && request.count > 1 {
        return Err(Error::Invalid(
            "a token for a named node is issued alone".into(),
        ));
    }
    if request.ttl <= Duration::zero() {
        request.ttl = Duration::hours(24);
    }
    if request.ttl > Duration::days(30) {
        return Err(Error::Invalid("tokens live at most 30 days".into()));
    }
    Ok(())
}

/// The id this node would like: the token's name, else the node's own name
/// or host name.
fn base_id(token_name: &str, node_name: &str, hostname: &str) -> String {
    let mut base = token_name.to_string();
    if base.is_empty() {
        base = clean(node_name);
    }
    if base.is_empty() {
        base = clean(&format!("node-{hostname}"));
    }
    if base.is_empty() || base == "node-" {
        base = format!("node-{}", random_hex(3));
    }
    base
}

/// Picks this node's id from `base`, made unique with a suffix when taken.
/// A named token is for that node only.
fn node_id(base: &str, named: bool, taken: impl Fn(&str) -> bool) -> Result<String, Error> {
    if named {
        if taken(base) {
            return Err(Error::Invalid(format!(
                "node {base:?} is already enrolled; revoke it first to enroll it again"
            )));
        }
        return Ok(base.to_string());
    }
    if !taken(base) {
        return Ok(base.to_string());
    }
    (2..1000)
        .map(|i| format!("{base}-{i}"))
        .find(|candidate| !taken(candidate))
        .ok_or_else(|| Error::Invalid(format!("no free node id for {base:?}")))
}

fn clean(name: &str) -> String {
    let cleaned: String = name
        .trim()
        .to_lowercase()
        .chars()
        .map(|c| match c {
            'a'..='z' | '0'..='9' | '-' | '_' | '.' => c,
            _ => '-',
        })
        .collect();
    let mut out = cleaned.trim_matches(|c| c == '-' || c == '.').to_string();
    out.truncate(63);
    if NODE_ID.is_match(&out) {
        out
    } else {
        String::new()
    }
}

/// What a store keeps for a token.
#[derive(Debug, Clone)]
struct TokenRecord {
    organization_id: String,
    node_name: String,
    require_approval: bool,
    expires_at: DateTime<Utc>,
    used: bool,
}

/// A store in memory: the demo server and tests. Its nodes have to enroll
/// again after a restart.
pub struct Memory {
    instance: String,
    state: Mutex<MemoryState>,
}

#[derive(Default)]
struct MemoryState {
    tokens: HashMap<String, TokenRecord>,
    nodes: HashMap<String, Credential>,
    /// Node id to its secret's hash.
    secrets: HashMap<String, String>,
}

impl Memory {
    /// A store for `instance`, or a new instance id when it is not one.
    pub fn new(instance: &str) -> Self {
        let instance = if valid_instance_id(instance) {
            instance.to_string()
        } else {
            new_instance_id()
        };
        Self {
            instance,
            state: Mutex::default(),
        }
    }

    /// This store's instance id.
    pub fn instance(&self) -> &str {
        &self.instance
    }

    fn lock(&self) -> MutexGuard<'_, MemoryState> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
}

#[async_trait]
impl Store for Memory {
    async fn instance_id(&self) -> Result<String, Error> {
        Ok(self.instance.clone())
    }

    async fn issue_tokens(
        &self,
        mut request: TokenRequest,
        now: DateTime<Utc>,
    ) -> Result<Vec<IssuedToken>, Error> {
        validate_request(&mut request)?;
        let mut state = self.lock();
        let issued = (0..request.count)
            .map(|_| {
                let token = new_token(&self.instance);
                let expires_at = now + request.ttl;
                state.tokens.insert(
                    hash(&token),
                    TokenRecord {
                        organization_id: request.organization_id.clone(),
                        node_name: request.node_name.clone(),
                        require_approval: request.require_approval,
                        expires_at,
                        used: false,
                    },
                );
                IssuedToken {
                    token,
                    expires_at,
                    node_name: request.node_name.clone(),
                }
            })
            .collect();
        Ok(issued)
    }

    async fn redeem(
        &self,
        token: &str,
        node_name: &str,
        hostname: &str,
        now: DateTime<Utc>,
    ) -> Result<Redemption, Error> {
        let instance = token_instance(token).ok_or(Error::InvalidToken)?;
        if instance != self.instance {
            return Err(Error::OtherInstance);
        }
        let mut state = self.lock();
        let key = hash(token.trim());
        let record = state.tokens.get(&key).cloned().ok_or(Error::InvalidToken)?;
        if record.used {
            return Err(Error::UsedToken);
        }
        if now >= record.expires_at {
            return Err(Error::ExpiredToken);
        }
        let base = base_id(&record.node_name, node_name, hostname);
        let id = node_id(&base, !record.node_name.is_empty(), |id| {
            state.nodes.get(id).is_some_and(|c| c.revoked_at.is_none())
        })?;
        let secret = new_secret();
        let credential = Credential {
            node_id: id.clone(),
            organization_id: record.organization_id.clone(),
            hostname: hostname.to_string(),
            approved: !record.require_approval,
            enrolled_at: now,
            revoked_at: None,
        };
        if let Some(record) = state.tokens.get_mut(&key) {
            record.used = true;
        }
        state.nodes.insert(id.clone(), credential.clone());
        state.secrets.insert(id, hash(&secret));
        Ok(Redemption { credential, secret })
    }

    async fn authenticate(&self, node_id: &str, secret: &str) -> Result<Credential, Error> {
        let state = self.lock();
        let credential = state.nodes.get(node_id).ok_or(Error::UnknownNode)?;
        let stored = state.secrets.get(node_id).map(String::as_str).unwrap_or("");
        if !same(stored, &hash(secret)) || credential.revoked_at.is_some() {
            return Err(Error::UnknownNode);
        }
        Ok(credential.clone())
    }

    async fn credentials(&self) -> Result<Vec<Credential>, Error> {
        let mut out: Vec<Credential> = self.lock().nodes.values().cloned().collect();
        out.sort_by(|a, b| a.node_id.cmp(&b.node_id));
        Ok(out)
    }

    async fn approve(&self, node_id: &str, _now: DateTime<Utc>) -> Result<(), Error> {
        let mut state = self.lock();
        match state.nodes.get_mut(node_id) {
            Some(c) if c.revoked_at.is_none() => {
                c.approved = true;
                Ok(())
            }
            _ => Err(Error::UnknownNode),
        }
    }

    async fn revoke(&self, node_id: &str, now: DateTime<Utc>) -> Result<(), Error> {
        let mut state = self.lock();
        let credential = state.nodes.get_mut(node_id).ok_or(Error::UnknownNode)?;
        credential.revoked_at = Some(now);
        Ok(())
    }
}

/// A store in Command's database (migration 002_node_enrollment).
pub struct Postgres {
    pool: deadpool_postgres::Pool,
}

impl Postgres {
    pub fn new(pool: deadpool_postgres::Pool) -> Self {
        Self { pool }
    }
}

#[async_trait]
impl Store for Postgres {
    /// Created on first use and kept for the database's life.
    async fn instance_id(&self) -> Result<String, Error> {
        let client = self.pool.get().await?;
        client
            .execute(
                "INSERT INTO command_instance (singleton, instance_id) VALUES (true, $1) ON CONFLICT (singleton) DO NOTHING",
                &[&new_instance_id()],
            )
            .await
            .map_err(|e| Error::Storage(format!("create the instance id: {e}")))?;
        let row = client
            .query_one(
                "SELECT instance_id FROM command_instance WHERE singleton",
                &[],
            )
            .await
            .map_err(|e| Error::Storage(format!("read the instance id: {e}")))?;
        Ok(row.get(0))
    }

    async fn issue_tokens(
        &self,
        mut request: TokenRequest,
        now: DateTime<Utc>,
    ) -> Result<Vec<IssuedToken>, Error> {
        validate_request(&mut request)?;
        let instance = self.instance_id().await?;
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let mut issued = Vec::new();
        for _ in 0..request.count {
            let token = new_token(&instance);
            let expires_at = now + request.ttl;
            tx.execute(
                "INSERT INTO enrollment_tokens (token_hash, organization_id, node_name, require_approval, expires_at, created_by) VALUES ($1, $2, $3, $4, $5, $6)",
                &[&hash(&token), &request.organization_id, &request.node_name, &request.require_approval, &expires_at, &request.created_by],
            )
            .await
            .map_err(|e| Error::Storage(format!("store a token: {e}")))?;
            issued.push(IssuedToken {
                token,
                expires_at,
                node_name: request.node_name.clone(),
            });
        }
        tx.commit().await?;
        Ok(issued)
    }

    async fn redeem(
        &self,
        token: &str,
        node_name: &str,
        hostname: &str,
        now: DateTime<Utc>,
    ) -> Result<Redemption, Error> {
        let token_instance = token_instance(token).ok_or(Error::InvalidToken)?;
        let instance = self.instance_id().await?;
        if token_instance != instance {
            return Err(Error::OtherInstance);
        }
        let token_hash = hash(token.trim());
        let mut client = self.pool.get().await?;
        let tx = client.transaction().await?;
        let row = tx
            .query_opt(
                "SELECT organization_id, node_name, require_approval, expires_at, used_at FROM enrollment_tokens WHERE token_hash = $1 FOR UPDATE",
                &[&token_hash],
            )
            .await?
            .ok_or(Error::InvalidToken)?;
        let organization_id: String = row.get(0);
        let token_name: String = row.get(1);
        let require_approval: bool = row.get(2);
        let expires_at: DateTime<Utc> = row.get(3);
        let used_at: Option<DateTime<Utc>> = row.get(4);
        if used_at.is_some() {
            return Err(Error::UsedToken);
        }
        if now >= expires_at {
            return Err(Error::ExpiredToken);
        }
        // Node ids are taken one enrollment at a time.
        tx.execute("SELECT pg_advisory_xact_lock(842120)", &[])
            .await?;
        let base = base_id(&token_name, node_name, hostname);
        // Every active id this base or its suffixes could clash with ('_'
        // in LIKE matches more, never less).
        let taken: HashSet<String> = tx
            .query(
                "SELECT node_id FROM node_credentials WHERE revoked_at IS NULL AND (node_id = $1 OR node_id LIKE $1 || '-%')",
                &[&base],
            )
            .await?
            .iter()
            .map(|row| row.get(0))
            .collect();
        let id = node_id(&base, !token_name.is_empty(), |id| taken.contains(id))?;
        let secret = new_secret();
        let approved = !require_approval;
        // A revoked node's id may be enrolled again.
        tx.execute(
            "INSERT INTO node_credentials (node_id, organization_id, secret_hash, hostname, approved, enrolled_at, approved_at)
             VALUES ($1, $2, $3, $4, $5, $6, CASE WHEN $5 THEN $6::timestamptz END)
             ON CONFLICT (node_id) DO UPDATE SET organization_id = EXCLUDED.organization_id, secret_hash = EXCLUDED.secret_hash,
                 hostname = EXCLUDED.hostname, approved = EXCLUDED.approved, enrolled_at = EXCLUDED.enrolled_at,
                 approved_at = EXCLUDED.approved_at, revoked_at = NULL
             WHERE node_credentials.revoked_at IS NOT NULL",
            &[&id, &organization_id, &hash(&secret), &hostname, &approved, &now],
        )
        .await
        .map_err(|e| Error::Storage(format!("store the node: {e}")))?;
        tx.execute(
            "UPDATE enrollment_tokens SET used_at = $2, used_by_node = $3 WHERE token_hash = $1",
            &[&token_hash, &now, &id],
        )
        .await?;
        tx.commit().await?;
        Ok(Redemption {
            credential: Credential {
                node_id: id,
                organization_id,
                hostname: hostname.to_string(),
                approved,
                enrolled_at: now,
                revoked_at: None,
            },
            secret,
        })
    }

    async fn authenticate(&self, node_id: &str, secret: &str) -> Result<Credential, Error> {
        let client = self.pool.get().await?;
        let row = client
            .query_opt(
                "SELECT node_id, organization_id, hostname, approved, enrolled_at, revoked_at, secret_hash FROM node_credentials WHERE node_id = $1",
                &[&node_id],
            )
            .await?
            .ok_or(Error::UnknownNode)?;
        let revoked_at: Option<DateTime<Utc>> = row.get(5);
        let stored: String = row.get(6);
        if revoked_at.is_some() || !same(&stored, &hash(secret)) {
            return Err(Error::UnknownNode);
        }
        Ok(Credential {
            node_id: row.get(0),
            organization_id: row.get(1),
            hostname: row.get(2),
            approved: row.get(3),
            enrolled_at: row.get(4),
            revoked_at,
        })
    }

    async fn credentials(&self) -> Result<Vec<Credential>, Error> {
        let client = self.pool.get().await?;
        let rows = client
            .query(
                "SELECT node_id, organization_id, hostname, approved, enrolled_at, revoked_at FROM node_credentials ORDER BY node_id",
                &[],
            )
            .await?;
        Ok(rows
            .iter()
            .map(|row| Credential {
                node_id: row.get(0),
                organization_id: row.get(1),
                hostname: row.get(2),
                approved: row.get(3),
                enrolled_at: row.get(4),
                revoked_at: row.get(5),
            })
            .collect())
    }

    async fn approve(&self, node_id: &str, now: DateTime<Utc>) -> Result<(), Error> {
        let client = self.pool.get().await?;
        let changed = client
            .execute(
                "UPDATE node_credentials SET approved = true, approved_at = $2 WHERE node_id = $1 AND revoked_at IS NULL",
                &[&node_id, &now],
            )
            .await?;
        if changed == 0 {
            Err(Error::UnknownNode)
        } else {
            Ok(())
        }
    }

    async fn revoke(&self, node_id: &str, now: DateTime<Utc>) -> Result<(), Error> {
        let client = self.pool.get().await?;
        let changed = client
            .execute(
                "UPDATE node_credentials SET revoked_at = $2 WHERE node_id = $1 AND revoked_at IS NULL",
                &[&node_id, &now],
            )
            .await?;
        if changed == 0 {
            Err(Error::UnknownNode)
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_instances_are_read_from_well_formed_tokens_only() {
        let instance = new_instance_id();
        let token = new_token(&instance);
        assert_eq!(token_instance(&token), Some(instance.as_str()));
        assert_eq!(
            token_instance(&format!(" {token}\n")),
            Some(instance.as_str())
        );
        assert_eq!(token_instance("auxe1_cmd-xyz_abc"), None);
        assert_eq!(token_instance(&new_secret()), None);
    }

    #[test]
    fn node_ids_come_from_names_and_get_suffixes() {
        assert_eq!(base_id("", "", "Build Box 1"), "node-build-box-1");
        assert_eq!(base_id("", "My Laptop", "x"), "my-laptop");
        assert_eq!(base_id("gpu-1", "other", "x"), "gpu-1");
        assert_eq!(base_id("", "", ""), "node");
        let taken = ["box", "box-2"];
        assert_eq!(
            node_id("box", false, |id| taken.contains(&id)).unwrap(),
            "box-3"
        );
        assert!(node_id("box", true, |id| taken.contains(&id)).is_err());
    }
}
