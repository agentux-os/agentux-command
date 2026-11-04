//! Low-cardinality process metrics in Prometheus text exposition format.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};
use std::time::Duration;

const BUCKETS: [f64; 10] = [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0];

#[derive(Default)]
struct HttpStats {
    requests: u64,
    errors: u64,
}

#[derive(Default)]
struct LatencyStats {
    observations: u64,
    seconds: f64,
    buckets: [u64; BUCKETS.len()],
}

/// Process-local HTTP request counters and latency distribution.
#[derive(Default)]
pub struct Metrics {
    http: Mutex<BTreeMap<(&'static str, &'static str, &'static str), HttpStats>>,
    latency: Mutex<BTreeMap<(&'static str, &'static str), LatencyStats>>,
}

impl Metrics {
    fn lock(
        &self,
    ) -> MutexGuard<'_, BTreeMap<(&'static str, &'static str, &'static str), HttpStats>> {
        self.http
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Record one request. Inputs are mapped to fixed label vocabularies.
    pub fn record_request(&self, method: &str, path: &str, status: u16, elapsed: Duration) {
        let method = method_label(method);
        let route = route_label(path);
        let status_class = status_label(status);
        let mut all = self.lock();
        let entry = all.entry((method, route, status_class)).or_default();
        entry.requests += 1;
        if status >= 400 {
            entry.errors += 1;
        }
        drop(all);
        let seconds = elapsed.as_secs_f64();
        let mut latency = self
            .latency
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let entry = latency.entry((method, route)).or_default();
        entry.observations += 1;
        entry.seconds += seconds;
        for (index, bucket) in BUCKETS.iter().enumerate() {
            if seconds <= *bucket {
                entry.buckets[index] += 1;
            }
        }
    }

    /// Render the request metrics without any request-controlled label values.
    pub fn render_http(&self) -> String {
        let all = self.lock();
        let mut out = String::from(
            "# HELP command_http_requests_total HTTP requests handled by Command.\n# TYPE command_http_requests_total counter\n",
        );
        for ((method, route, status), stats) in &*all {
            out.push_str(&format!("command_http_requests_total{{method=\"{method}\",route=\"{route}\",status_class=\"{status}\"}} {}\n", stats.requests));
        }
        out.push_str("# HELP command_http_errors_total HTTP responses with 4xx or 5xx status.\n# TYPE command_http_errors_total counter\n");
        for ((method, route, status), stats) in &*all {
            if stats.errors > 0 {
                out.push_str(&format!("command_http_errors_total{{method=\"{method}\",route=\"{route}\",status_class=\"{status}\"}} {}\n", stats.errors));
            }
        }
        out.push_str("# HELP command_http_request_duration_seconds HTTP request latency in seconds.\n# TYPE command_http_request_duration_seconds histogram\n");
        let latency = self
            .latency
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        for ((method, route), stats) in &*latency {
            for (index, bucket) in BUCKETS.iter().enumerate() {
                out.push_str(&format!("command_http_request_duration_seconds_bucket{{method=\"{method}\",route=\"{route}\",le=\"{bucket}\"}} {}\n", stats.buckets[index]));
            }
            out.push_str(&format!("command_http_request_duration_seconds_bucket{{method=\"{method}\",route=\"{route}\",le=\"+Inf\"}} {}\n", stats.observations));
            out.push_str(&format!("command_http_request_duration_seconds_sum{{method=\"{method}\",route=\"{route}\"}} {}\n", stats.seconds));
            out.push_str(&format!("command_http_request_duration_seconds_count{{method=\"{method}\",route=\"{route}\"}} {}\n", stats.observations));
        }
        out
    }
}

fn method_label(method: &str) -> &'static str {
    match method {
        "GET" => "GET",
        "POST" => "POST",
        "PUT" => "PUT",
        "PATCH" => "PATCH",
        "DELETE" => "DELETE",
        "HEAD" => "HEAD",
        "OPTIONS" => "OPTIONS",
        _ => "OTHER",
    }
}

fn route_label(path: &str) -> &'static str {
    if path == "/healthz" {
        "health"
    } else if path == "/readyz" {
        "ready"
    } else if path == "/livez" {
        "live"
    } else if path == "/metrics" {
        "metrics"
    } else if path == "/v1/protocol/messages" || path == "/v1/protocol/connect" {
        "protocol"
    } else if path.starts_with("/v1/organizations/") && path.contains("/runs") {
        "runs_api"
    } else if path.starts_with("/v1/organizations/") {
        "office_api"
    } else if path == "/v1/nodes" || path.starts_with("/v1/nodes/") {
        "nodes_api"
    } else if path.starts_with("/v1/") {
        "api"
    } else if path == "/office" || path.starts_with("/office/") {
        "office_static"
    } else if path == "/console" || path.starts_with("/console/") {
        "console_static"
    } else {
        "other"
    }
}

fn status_label(status: u16) -> &'static str {
    match status / 100 {
        1 => "1xx",
        2 => "2xx",
        3 => "3xx",
        4 => "4xx",
        5 => "5xx",
        _ => "other",
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::Metrics;

    #[test]
    fn request_labels_are_bounded_and_latency_is_aggregated() {
        let metrics = Metrics::default();
        metrics.record_request(
            "GET",
            "/v1/organizations/secret-org/runs/secret-run",
            503,
            Duration::from_millis(20),
        );
        metrics.record_request(
            "CUSTOM-VERB",
            "/v1/organizations/other-org/audit",
            403,
            Duration::from_millis(150),
        );
        metrics.record_request(
            "GET",
            "/v1/organizations/secret-org/runs/secret-run",
            200,
            Duration::from_millis(10),
        );
        let output = metrics.render_http();
        assert!(output.contains("route=\"runs_api\",status_class=\"5xx\""));
        assert!(output.contains("method=\"OTHER\",route=\"office_api\",status_class=\"4xx\""));
        assert!(output.contains(
            "command_http_request_duration_seconds_count{method=\"GET\",route=\"runs_api\"} 2"
        ));
        assert!(!output.contains("secret-org"));
        assert!(!output.contains("secret-run"));
        assert!(!output.contains("CUSTOM-VERB"));
    }
}
