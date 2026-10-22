//! Signing in with an OIDC provider, against a provider run by the test:
//! discovery, PKCE, a real RS256 signature, and every way a sign-in must be
//! refused.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use agentux_command::oidc::{Config, Oidc, RoleMap};
use agentux_command::server::Handler;
use axum::Router;
use axum::body::Body;
use axum::extract::{Form, Query, State};
use axum::http::{HeaderMap, Request, StatusCode, header};
use axum::response::{IntoResponse, Redirect, Response};
use axum::routing::{get, post};
use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use http_body_util::BodyExt;
use rsa::pkcs1::EncodeRsaPrivateKey;
use rsa::traits::PublicKeyParts;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tower::ServiceExt;

/// How the test provider spoils the ID token it issues.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Spoil {
    Nothing,
    Audience,
    Expired,
    ForeignKey,
    Nonce,
    NoGroups,
}

struct Provider {
    issuer: String,
    key: String,
    foreign_key: String,
    jwk: Value,
    spoil: Spoil,
    /// code → (code_challenge, nonce)
    codes: HashMap<String, (String, String)>,
}

type Shared = Arc<Mutex<Provider>>;

fn rsa_key() -> (String, Value) {
    let key = rsa::RsaPrivateKey::new(&mut rand::thread_rng(), 2048).unwrap();
    let pem = key
        .to_pkcs1_pem(rsa::pkcs8::LineEnding::LF)
        .unwrap()
        .to_string();
    let jwk = json!({
        "kty": "RSA", "kid": "k1", "use": "sig", "alg": "RS256",
        "n": URL_SAFE_NO_PAD.encode(key.n().to_bytes_be()),
        "e": URL_SAFE_NO_PAD.encode(key.e().to_bytes_be()),
    });
    (pem, jwk)
}

async fn discovery(State(p): State<Shared>) -> Response {
    let issuer = p.lock().unwrap().issuer.clone();
    axum::Json(json!({
        "issuer": issuer,
        "authorization_endpoint": format!("{issuer}/authorize"),
        "token_endpoint": format!("{issuer}/token"),
        "jwks_uri": format!("{issuer}/jwks"),
    }))
    .into_response()
}

async fn jwks(State(p): State<Shared>) -> Response {
    axum::Json(json!({"keys": [p.lock().unwrap().jwk.clone()]})).into_response()
}

async fn authorize(State(p): State<Shared>, Query(q): Query<HashMap<String, String>>) -> Response {
    assert_eq!(q["response_type"], "code");
    assert_eq!(q["code_challenge_method"], "S256", "PKCE with S256");
    let code = format!("code-{}", p.lock().unwrap().codes.len());
    p.lock().unwrap().codes.insert(
        code.clone(),
        (q["code_challenge"].clone(), q["nonce"].clone()),
    );
    Redirect::to(&format!(
        "{}?code={code}&state={}",
        q["redirect_uri"], q["state"]
    ))
    .into_response()
}

async fn token(
    State(p): State<Shared>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response {
    let auth = headers[header::AUTHORIZATION].to_str().unwrap();
    assert_eq!(
        auth,
        format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode("command:s3cret")
        )
    );
    let p = p.lock().unwrap();
    let Some((challenge, nonce)) = p.codes.get(&form["code"]).cloned() else {
        return (StatusCode::BAD_REQUEST, "unknown code").into_response();
    };
    let verified =
        URL_SAFE_NO_PAD.encode(Sha256::digest(form["code_verifier"].as_bytes())) == challenge;
    if !verified {
        return (StatusCode::BAD_REQUEST, "PKCE failed").into_response();
    }
    let now = chrono::Utc::now().timestamp();
    let mut claims = json!({
        "iss": p.issuer, "aud": "command", "sub": "user-1", "email": "ana@acme.org",
        "groups": ["agentux-leads"], "nonce": nonce, "iat": now, "exp": now + 300,
    });
    let mut key = &p.key;
    match p.spoil {
        Spoil::Nothing => {}
        Spoil::Audience => claims["aud"] = json!("someone-else"),
        Spoil::Expired => claims["exp"] = json!(now - 3600),
        Spoil::ForeignKey => key = &p.foreign_key,
        Spoil::Nonce => claims["nonce"] = json!("not-the-one"),
        Spoil::NoGroups => claims["groups"] = json!(["marketing"]),
    }
    let mut header = jsonwebtoken::Header::new(jsonwebtoken::Algorithm::RS256);
    header.kid = Some("k1".into());
    let id_token = jsonwebtoken::encode(
        &header,
        &claims,
        &jsonwebtoken::EncodingKey::from_rsa_pem(key.as_bytes()).unwrap(),
    )
    .unwrap();
    axum::Json(json!({"id_token": id_token, "access_token": "x", "token_type": "Bearer"}))
        .into_response()
}

struct Setup {
    app: Router,
    provider: Shared,
}

async fn setup() -> Setup {
    let (key, jwk) = rsa_key();
    let (foreign_key, _) = rsa_key();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let issuer = format!("http://{}", listener.local_addr().unwrap());
    let provider: Shared = Arc::new(Mutex::new(Provider {
        issuer: issuer.clone(),
        key,
        foreign_key,
        jwk,
        spoil: Spoil::Nothing,
        codes: HashMap::new(),
    }));
    let idp = Router::new()
        .route("/.well-known/openid-configuration", get(discovery))
        .route("/jwks", get(jwks))
        .route("/authorize", get(authorize))
        .route("/token", post(token))
        .with_state(provider.clone());
    tokio::spawn(axum::serve(listener, idp).into_future());

    let roles = RoleMap::parse(
        r#"{"groups": {"agentux-leads": {"acme": ["organization.read", "node.enroll"]}}}"#,
    )
    .unwrap();
    let oidc = Oidc::new(Config {
        issuer,
        client_id: "command".into(),
        client_secret: "s3cret".into(),
        redirect_url: "http://command.test/v1/auth/callback".into(),
        scopes: vec!["openid".into(), "email".into(), "groups".into()],
        groups_claim: "groups".into(),
        roles,
    })
    .unwrap();
    let app = Handler::new()
        .with_demo_tokens(false)
        .with_public_url("http://command.test")
        .with_oidc(oidc)
        .router();
    Setup { app, provider }
}

async fn send(
    app: &Router,
    method: &str,
    uri: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, HeaderMap, Value) {
    let mut request = Request::builder().method(method).uri(uri);
    for (name, value) in headers {
        request = request.header(*name, *value);
    }
    let response = app
        .clone()
        .oneshot(
            request
                .header("content-type", "application/json")
                .body(Body::from("{\"organizationId\":\"acme\"}"))
                .unwrap(),
        )
        .await
        .unwrap();
    let (parts, body) = response.into_parts();
    let bytes = body.collect().await.unwrap().to_bytes();
    (
        parts.status,
        parts.headers,
        serde_json::from_slice(&bytes).unwrap_or(Value::Null),
    )
}

/// Login → provider → the callback URL the browser would be sent to.
async fn through_provider(app: &Router) -> String {
    let (status, headers, _) = send(app, "GET", "/v1/auth/login?returnTo=/console", &[]).await;
    assert_eq!(status, StatusCode::SEE_OTHER);
    let authorize = headers[header::LOCATION].to_str().unwrap();
    assert!(authorize.contains("code_challenge=") && authorize.contains("nonce="));
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();
    let answer = client.get(authorize).send().await.unwrap();
    let back = answer.headers()["location"].to_str().unwrap().to_string();
    back.strip_prefix("http://command.test")
        .unwrap()
        .to_string()
}

fn session_cookie(headers: &HeaderMap) -> String {
    let set = headers[header::SET_COOKIE].to_str().unwrap();
    assert!(
        set.contains("HttpOnly") && set.contains("SameSite=Lax"),
        "{set}"
    );
    set.split(';').next().unwrap().to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_signed_in_operator_gets_a_session_with_their_groups_capabilities() {
    let s = setup().await;
    let callback = through_provider(&s.app).await;
    let (status, headers, body) = send(&s.app, "GET", &callback, &[]).await;
    assert_eq!(status, StatusCode::SEE_OTHER, "{body}");
    assert_eq!(headers[header::LOCATION], "/console");
    let cookie = session_cookie(&headers);
    assert!(cookie.starts_with("agentux_session=auxo1_"));

    let (status, _, me) = send(&s.app, "GET", "/v1/auth/me", &[("cookie", &cookie)]).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(me["subject"], "oidc:user-1");
    assert_eq!(
        me["organizations"],
        json!({"acme": ["node.enroll", "organization.read"]})
    );
    let (status, _, _) = send(&s.app, "GET", "/v1/nodes", &[("cookie", &cookie)]).await;
    assert_eq!(status, StatusCode::OK);

    // The same callback again: its state was used.
    let (status, _, _) = send(&s.app, "GET", &callback, &[]).await;
    assert_eq!(status, StatusCode::BAD_REQUEST);

    // A state-changing request on the cookie alone must come from this site.
    let (status, _, _) = send(
        &s.app,
        "POST",
        "/v1/enrollment-tokens",
        &[("cookie", &cookie)],
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "no Origin: refused");
    let (status, _, _) = send(
        &s.app,
        "POST",
        "/v1/enrollment-tokens",
        &[("cookie", &cookie), ("origin", "https://evil.example")],
    )
    .await;
    assert_eq!(status, StatusCode::FORBIDDEN, "another origin: refused");
    let (status, _, body) = send(
        &s.app,
        "POST",
        "/v1/enrollment-tokens",
        &[("cookie", &cookie), ("origin", "http://command.test")],
    )
    .await;
    assert_eq!(status, StatusCode::CREATED, "{body}");

    let (status, _, _) = send(
        &s.app,
        "POST",
        "/v1/auth/logout",
        &[("cookie", &cookie), ("origin", "http://command.test")],
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let (status, _, _) = send(&s.app, "GET", "/v1/auth/me", &[("cookie", &cookie)]).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED, "signed out");
}

#[tokio::test(flavor = "multi_thread")]
async fn spoiled_tokens_and_unmapped_people_are_refused() {
    let s = setup().await;
    for (spoil, expected) in [
        (Spoil::Audience, StatusCode::UNAUTHORIZED),
        (Spoil::Expired, StatusCode::UNAUTHORIZED),
        (Spoil::ForeignKey, StatusCode::UNAUTHORIZED),
        (Spoil::Nonce, StatusCode::UNAUTHORIZED),
        (Spoil::NoGroups, StatusCode::FORBIDDEN),
    ] {
        s.provider.lock().unwrap().spoil = spoil;
        let callback = through_provider(&s.app).await;
        let (status, headers, body) = send(&s.app, "GET", &callback, &[]).await;
        assert_eq!(status, expected, "{spoil:?}: {body}");
        assert!(
            headers.get(header::SET_COOKIE).is_none(),
            "{spoil:?}: no session"
        );
    }
    let (status, _, _) = send(&s.app, "GET", "/v1/auth/callback?code=x&state=forged", &[]).await;
    assert_eq!(
        status,
        StatusCode::BAD_REQUEST,
        "a state this server never issued"
    );
}

#[tokio::test]
async fn sign_in_is_off_until_configured() {
    let app = Handler::new().router();
    let (status, _, _) = send(&app, "GET", "/v1/auth/login", &[]).await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}
