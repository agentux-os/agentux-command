//! Signing people in with an OpenID Connect provider: Ferriskey, Keycloak,
//! Entra ID, Google, or GitHub through a provider that brokers it.
//!
//! The authorization code flow with PKCE (S256) and a nonce. Command reads
//! the provider's discovery document, sends the browser to its authorization
//! endpoint, exchanges the code at its token endpoint, and accepts the ID
//! token only when its signature checks against the provider's published
//! keys (JWKS) and its issuer, audience, expiry and nonce are right. The
//! token's groups map to capabilities per organization ([`RoleMap`]); a
//! person whose groups map to nothing is not signed in.

use std::collections::{BTreeMap, HashMap};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation};
use serde::Deserialize;
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::auth::{CAPABILITIES, Principal};
use crate::random_hex;

/// How long a sign-in may take between leaving and coming back.
const PENDING_FOR: Duration = Duration::from_secs(600);
/// How long the provider's keys are reused before they are fetched again.
const JWKS_FOR: Duration = Duration::from_secs(3600);

#[derive(Debug)]
pub enum Error {
    /// Sign-in is not configured, or the configuration is wrong.
    Config(String),
    /// The provider could not be reached or answered something unusable.
    Provider(String),
    /// The callback is not one this server started, or came back too late.
    UnknownState,
    /// The ID token did not check out.
    InvalidToken(String),
    /// The person signed in, but none of their groups gives any access.
    NoAccess(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::Config(m) => write!(f, "sign-in configuration: {m}"),
            Error::Provider(m) => write!(f, "identity provider: {m}"),
            Error::UnknownState => {
                f.write_str("this sign-in was not started here, or took too long; start again")
            }
            Error::InvalidToken(m) => write!(f, "the identity provider's token was refused: {m}"),
            Error::NoAccess(who) => write!(
                f,
                "{who} signed in, but none of their groups gives access to AgentUX Command"
            ),
        }
    }
}

impl std::error::Error for Error {}

/// Groups to capabilities per organization:
/// `{ "groups": { "agentux-leads": { "acme": ["organization.read", …] } } }`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RoleMap {
    groups: BTreeMap<String, BTreeMap<String, Vec<String>>>,
}

impl RoleMap {
    /// Parses and checks a role map: every capability must be one Command
    /// knows.
    pub fn parse(text: &str) -> Result<Self, Error> {
        let map: RoleMap =
            serde_json::from_str(text).map_err(|e| Error::Config(format!("roles: {e}")))?;
        for (group, orgs) in &map.groups {
            for (org, caps) in orgs {
                if let Some(unknown) = caps.iter().find(|c| !CAPABILITIES.contains(&c.as_str())) {
                    return Err(Error::Config(format!(
                        "roles: group {group:?} in {org:?} has unknown capability {unknown:?} (one of {})",
                        CAPABILITIES.join(", ")
                    )));
                }
            }
        }
        Ok(map)
    }

    /// The principal for `subject` with `groups`; `None` when the groups
    /// give nothing.
    pub fn principal(&self, subject: &str, groups: &[String]) -> Option<Principal> {
        let mut principal = Principal::new(subject);
        let mut any = false;
        for group in groups {
            for (org, caps) in self.groups.get(group).into_iter().flatten() {
                principal = principal.grant(org, caps.iter().map(String::as_str));
                any |= !caps.is_empty();
            }
        }
        any.then_some(principal)
    }
}

/// The provider and how to talk to it.
#[derive(Debug, Clone)]
pub struct Config {
    /// The issuer URL; discovery is `<issuer>/.well-known/openid-configuration`.
    pub issuer: String,
    pub client_id: String,
    pub client_secret: String,
    /// `<public URL>/v1/auth/callback`, registered with the provider.
    pub redirect_url: String,
    pub scopes: Vec<String>,
    /// The ID token claim holding the person's groups (`groups`).
    pub groups_claim: String,
    pub roles: RoleMap,
}

#[derive(Debug, Clone, Deserialize)]
struct Discovery {
    issuer: String,
    authorization_endpoint: String,
    token_endpoint: String,
    jwks_uri: String,
}

struct Pending {
    nonce: String,
    verifier: String,
    return_to: String,
    started: Instant,
}

/// Who signed in.
#[derive(Debug, Clone)]
pub struct SignedIn {
    pub principal: Principal,
    /// Where to send the browser back to (a path on this server).
    pub return_to: String,
    pub email: Option<String>,
    pub name: Option<String>,
}

/// A configured provider, with the sign-ins in flight.
pub struct Oidc {
    config: Config,
    http: reqwest::Client,
    discovery: tokio::sync::OnceCell<Discovery>,
    jwks: Mutex<Option<(Instant, JwkSet)>>,
    pending: Mutex<HashMap<String, Pending>>,
}

/// A path on this server, or `/`: never another site (no open redirect).
fn local_path(return_to: &str) -> String {
    let ok =
        return_to.starts_with('/') && !return_to.starts_with("//") && !return_to.contains('\\');
    if ok {
        return_to.to_string()
    } else {
        "/".to_string()
    }
}

fn challenge(verifier: &str) -> String {
    URL_SAFE_NO_PAD.encode(Sha256::digest(verifier.as_bytes()))
}

#[derive(Deserialize)]
struct TokenAnswer {
    id_token: String,
}

impl Oidc {
    pub fn new(config: Config) -> Result<Self, Error> {
        for (name, value) in [
            ("issuer", &config.issuer),
            ("client id", &config.client_id),
            ("redirect URL", &config.redirect_url),
        ] {
            if value.trim().is_empty() {
                return Err(Error::Config(format!("the {name} is required")));
            }
        }
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(15))
            .redirect(reqwest::redirect::Policy::none())
            .user_agent(concat!("agentux-command/", env!("CARGO_PKG_VERSION")))
            .build()
            .map_err(|e| Error::Config(e.to_string()))?;
        Ok(Self {
            config,
            http,
            discovery: tokio::sync::OnceCell::new(),
            jwks: Mutex::new(None),
            pending: Mutex::new(HashMap::new()),
        })
    }

    async fn discovery(&self) -> Result<&Discovery, Error> {
        self.discovery
            .get_or_try_init(|| async {
                let url = format!(
                    "{}/.well-known/openid-configuration",
                    self.config.issuer.trim_end_matches('/')
                );
                let found: Discovery = self
                    .http
                    .get(&url)
                    .send()
                    .await
                    .and_then(reqwest::Response::error_for_status)
                    .map_err(|e| Error::Provider(format!("discovery at {url}: {e}")))?
                    .json()
                    .await
                    .map_err(|e| Error::Provider(format!("discovery at {url}: {e}")))?;
                if found.issuer.trim_end_matches('/') != self.config.issuer.trim_end_matches('/') {
                    return Err(Error::Provider(format!(
                        "discovery names issuer {}, configured {}",
                        found.issuer, self.config.issuer
                    )));
                }
                Ok(found)
            })
            .await
    }

    async fn jwks(&self, refresh: bool) -> Result<JwkSet, Error> {
        if !refresh
            && let Some((at, set)) = self.jwks.lock().unwrap_or_else(|e| e.into_inner()).as_ref()
            && at.elapsed() < JWKS_FOR
        {
            return Ok(set.clone());
        }
        let uri = &self.discovery().await?.jwks_uri;
        let set: JwkSet = self
            .http
            .get(uri)
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| Error::Provider(format!("keys at {uri}: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Provider(format!("keys at {uri}: {e}")))?;
        *self.jwks.lock().unwrap_or_else(|e| e.into_inner()) = Some((Instant::now(), set.clone()));
        Ok(set)
    }

    /// Where to send the browser to sign in; it comes back to `return_to`.
    pub async fn login_url(&self, return_to: &str) -> Result<String, Error> {
        let discovery = self.discovery().await?;
        let state = random_hex(16);
        let nonce = random_hex(16);
        let verifier = format!("{}{}", random_hex(24), random_hex(24));
        let mut url = url::Url::parse(&discovery.authorization_endpoint)
            .map_err(|e| Error::Provider(format!("authorization endpoint: {e}")))?;
        url.query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("client_id", &self.config.client_id)
            .append_pair("redirect_uri", &self.config.redirect_url)
            .append_pair("scope", &self.config.scopes.join(" "))
            .append_pair("state", &state)
            .append_pair("nonce", &nonce)
            .append_pair("code_challenge", &challenge(&verifier))
            .append_pair("code_challenge_method", "S256");
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        pending.retain(|_, p| p.started.elapsed() < PENDING_FOR);
        pending.insert(
            state,
            Pending {
                nonce,
                verifier,
                return_to: local_path(return_to),
                started: Instant::now(),
            },
        );
        Ok(url.to_string())
    }

    /// Finishes a sign-in the provider sent back with `code` and `state`.
    pub async fn callback(&self, code: &str, state: &str) -> Result<SignedIn, Error> {
        let pending = self
            .pending
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(state)
            .filter(|p| p.started.elapsed() < PENDING_FOR)
            .ok_or(Error::UnknownState)?;
        let discovery = self.discovery().await?;
        let answer: TokenAnswer = self
            .http
            .post(&discovery.token_endpoint)
            .basic_auth(&self.config.client_id, Some(&self.config.client_secret))
            .form(&[
                ("grant_type", "authorization_code"),
                ("code", code),
                ("redirect_uri", &self.config.redirect_url),
                ("client_id", &self.config.client_id),
                ("code_verifier", &pending.verifier),
            ])
            .send()
            .await
            .and_then(reqwest::Response::error_for_status)
            .map_err(|e| Error::Provider(format!("token exchange: {e}")))?
            .json()
            .await
            .map_err(|e| Error::Provider(format!("token exchange: {e}")))?;
        let claims = self.verify(&answer.id_token, &discovery.issuer).await?;
        if claims.get("nonce").and_then(Value::as_str) != Some(pending.nonce.as_str()) {
            return Err(Error::InvalidToken(
                "the nonce does not match this sign-in".into(),
            ));
        }
        let subject = claims
            .get("sub")
            .and_then(Value::as_str)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| Error::InvalidToken("no subject".into()))?;
        let email = claims
            .get("email")
            .and_then(Value::as_str)
            .map(str::to_string);
        let name = claims
            .get("name")
            .or_else(|| claims.get("preferred_username"))
            .and_then(Value::as_str)
            .map(str::to_string);
        let groups: Vec<String> = match claims.get(&self.config.groups_claim) {
            Some(Value::Array(items)) => items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect(),
            Some(Value::String(one)) => vec![one.clone()],
            _ => Vec::new(),
        };
        let who = email.clone().unwrap_or_else(|| subject.to_string());
        let principal = self
            .config
            .roles
            .principal(&format!("oidc:{subject}"), &groups)
            .ok_or(Error::NoAccess(who))?;
        Ok(SignedIn {
            principal,
            return_to: pending.return_to,
            email,
            name,
        })
    }

    /// Checks an ID token's signature and standard claims; its claims.
    async fn verify(
        &self,
        token: &str,
        issuer: &str,
    ) -> Result<serde_json::Map<String, Value>, Error> {
        let header =
            jsonwebtoken::decode_header(token).map_err(|e| Error::InvalidToken(e.to_string()))?;
        let allowed = [
            Algorithm::RS256,
            Algorithm::RS384,
            Algorithm::RS512,
            Algorithm::PS256,
            Algorithm::ES256,
            Algorithm::ES384,
            Algorithm::EdDSA,
        ];
        if !allowed.contains(&header.alg) {
            return Err(Error::InvalidToken(format!(
                "{:?} is not accepted",
                header.alg
            )));
        }
        // A key the provider just rotated in: fetch its keys once more.
        let mut key = None;
        for refresh in [false, true] {
            let set = self.jwks(refresh).await?;
            key = match &header.kid {
                Some(kid) => set.find(kid).cloned(),
                None => set.keys.first().cloned(),
            };
            if key.is_some() {
                break;
            }
        }
        let key = key.ok_or_else(|| {
            Error::InvalidToken("signed with a key the provider does not publish".into())
        })?;
        let key = DecodingKey::from_jwk(&key).map_err(|e| Error::InvalidToken(e.to_string()))?;
        let mut validation = Validation::new(header.alg);
        validation.set_issuer(&[issuer]);
        validation.set_audience(&[&self.config.client_id]);
        validation.set_required_spec_claims(&["exp", "iss", "aud", "sub"]);
        validation.leeway = 60;
        let data = jsonwebtoken::decode::<serde_json::Map<String, Value>>(token, &key, &validation)
            .map_err(|e| Error::InvalidToken(e.to_string()))?;
        Ok(data.claims)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{NODE_COMMAND, ORGANIZATION_READ};

    #[test]
    fn groups_map_to_capabilities_per_organization() {
        let roles = RoleMap::parse(
            r#"{"groups": {"leads": {"acme": ["organization.read", "node.command"]}, "auditors": {"acme": ["organization.read"], "beta": ["organization.read"]}}}"#,
        )
        .unwrap();
        let lead = roles
            .principal("oidc:1", &["leads".into(), "other".into()])
            .unwrap();
        assert!(lead.can("acme", NODE_COMMAND) && !lead.can("beta", ORGANIZATION_READ));
        let auditor = roles.principal("oidc:2", &["auditors".into()]).unwrap();
        assert!(auditor.can("beta", ORGANIZATION_READ) && !auditor.can("acme", NODE_COMMAND));
        assert!(
            roles.principal("oidc:3", &["nobody".into()]).is_none(),
            "no mapped group, no access"
        );
        assert!(RoleMap::parse(r#"{"groups": {"x": {"acme": ["root"]}}}"#).is_err());
    }

    #[test]
    fn return_paths_stay_on_this_server() {
        assert_eq!(local_path("/console"), "/console");
        for other in [
            "https://evil.example",
            "//evil.example",
            "/\\evil",
            "console",
        ] {
            assert_eq!(local_path(other), "/", "{other}");
        }
    }

    #[test]
    fn pkce_uses_s256() {
        // RFC 7636 appendix B.
        assert_eq!(
            challenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
            "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"
        );
    }
}
