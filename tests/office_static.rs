//! AgentUX Office served by Command at /office/, on the origin where
//! operators' sessions are.

use agentux_command::auth::{ORGANIZATION_READ, TokenRequest};
use agentux_command::server::Handler;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use chrono::Utc;
use http_body_util::BodyExt;
use tower::ServiceExt;

/// Status, headers and body text of GET `path`.
async fn get(app: &Router, path: &str, token: &str) -> (StatusCode, axum::http::HeaderMap, String) {
    let mut request = Request::get(path);
    if !token.is_empty() {
        request = request.header("authorization", format!("Bearer {token}"));
    }
    let response = app
        .clone()
        .oneshot(request.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let (status, headers) = (response.status(), response.headers().clone());
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, headers, String::from_utf8_lossy(&body).into_owned())
}

#[tokio::test]
async fn office_is_served_at_office_with_a_config_for_the_operator() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("index.html"),
        "<title>AgentUX Office</title>",
    )
    .unwrap();
    std::fs::write(dir.path().join("app.js"), "console.log('office')").unwrap();
    std::fs::write(dir.path().join("styles.css"), "body{}").unwrap();
    std::fs::write(dir.path().join(".env"), "SECRET=1").unwrap();
    std::fs::write(dir.path().join("notes.txt"), "not a web file").unwrap();
    std::fs::write(dir.path().parent().unwrap().join("outside.html"), "outside").ok();

    let handler = Handler::new().with_office_dir(dir.path().to_path_buf(), "");
    let acme = handler
        .service_tokens()
        .issue(
            TokenRequest {
                name: "reader".into(),
                organization_id: "acme".into(),
                capabilities: vec![ORGANIZATION_READ.into()],
                ..TokenRequest::default()
            },
            Utc::now(),
        )
        .await
        .unwrap()
        .secret;
    let app = handler.router();

    let (status, headers, _) = get(&app, "/office", "").await;
    assert_eq!(status, StatusCode::PERMANENT_REDIRECT);
    assert_eq!(headers["location"], "/office/");

    let (status, headers, body) = get(&app, "/office/", "").await;
    assert_eq!(status, StatusCode::OK);
    assert!(body.contains("AgentUX Office"));
    assert_eq!(headers["content-type"], "text/html; charset=utf-8");
    let csp = headers["content-security-policy"].to_str().unwrap();
    assert!(
        csp.contains("default-src 'self'") && csp.contains("frame-ancestors 'none'"),
        "{csp}"
    );
    assert_eq!(headers["x-content-type-options"], "nosniff");

    let (status, headers, _) = get(&app, "/office/app.js", "").await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-type"], "text/javascript; charset=utf-8");
    assert_eq!(get(&app, "/office/styles.css", "").await.0, StatusCode::OK);

    // Only the bundle's web files: nothing hidden, nothing else, nothing above.
    for path in [
        "/office/.env",
        "/office/notes.txt",
        "/office/..%2Foutside.html",
        "/office/missing.js",
    ] {
        assert_eq!(get(&app, path, "").await.0, StatusCode::NOT_FOUND, "{path}");
    }

    // config.js is made for whoever asks.
    let (status, headers, config) = get(&app, "/office/config.js", &acme).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(headers["content-type"], "text/javascript; charset=utf-8");
    assert!(config.contains(r#""organizationId":"acme""#), "{config}");
    assert!(config.contains(r#""command":true"#), "{config}");
    assert!(
        config.contains(r#""signIn":false"#),
        "no OIDC here: {config}"
    );
    let (_, _, anonymous) = get(&app, "/office/config.js", "").await;
    assert!(
        anonymous.contains(r#""organizationId":"org-demo""#),
        "{anonymous}"
    );

    // Without a bundle, nothing is served there.
    let plain = Handler::new().router();
    assert_eq!(get(&plain, "/office/", "").await.0, StatusCode::NOT_FOUND);
}
