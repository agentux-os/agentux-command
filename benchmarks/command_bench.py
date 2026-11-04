#!/usr/bin/env python3
"""Bounded HTTP load runner for a single AgentUX Command instance."""

import argparse
import concurrent.futures
import datetime as dt
import json
import os
import platform
import shutil
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path


class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, request, response, code, message, headers, new_url):
        return None


HTTP_OPENER = urllib.request.build_opener(NoRedirectHandler)


def validate_config(config):
    if not isinstance(config, dict):
        raise ValueError("configuration root must be a JSON object")
    base_url = config.get("base_url", "")
    parsed_url = urllib.parse.urlsplit(base_url) if isinstance(base_url, str) else None
    requests = config.get("requests")
    concurrency = config.get("concurrency")
    timeout = config.get("timeout_seconds", 10)
    max_duration = config.get("max_duration_seconds", 3600)
    scenarios = config.get("scenarios")
    if (parsed_url is None or parsed_url.scheme not in {"http", "https"}
            or not parsed_url.hostname or parsed_url.username or parsed_url.password):
        raise ValueError("base_url must be an http(s) origin without embedded credentials")
    if not isinstance(requests, int) or isinstance(requests, bool) or not 1 <= requests <= 100_000:
        raise ValueError("requests must be an integer between 1 and 100000")
    if not isinstance(concurrency, int) or isinstance(concurrency, bool) or concurrency < 1 or concurrency > requests or concurrency > 512:
        raise ValueError("concurrency must be between 1 and min(requests, 512)")
    if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0 or timeout > 300:
        raise ValueError("timeout_seconds must be in (0, 300]")
    if not isinstance(max_duration, (int, float)) or isinstance(max_duration, bool) or max_duration <= 0 or max_duration > 3600:
        raise ValueError("max_duration_seconds must be in (0, 3600]")
    if not isinstance(scenarios, list) or not scenarios:
        raise ValueError("scenarios must be a non-empty list")
    names = set()
    for scenario in scenarios:
        if not isinstance(scenario, dict) or not isinstance(scenario.get("name"), str) or not scenario["name"].strip():
            raise ValueError("each scenario requires a name")
        if scenario["name"] in names:
            raise ValueError("scenario names must be unique")
        names.add(scenario["name"])
        method = scenario.get("method", "GET")
        if not isinstance(method, str) or method.upper() not in {"GET", "POST", "PUT", "PATCH", "DELETE"}:
            raise ValueError(f"unsupported method in scenario {scenario['name']}")
        if not isinstance(scenario.get("path"), str) or not scenario["path"].startswith("/"):
            raise ValueError(f"scenario {scenario['name']} path must start with /")
        if scenario.get("json") is not None and not isinstance(scenario["json"], (dict, list)):
            raise ValueError(f"scenario {scenario['name']} json body must be an object or array")
        interval = scenario.get("interval_seconds", 0)
        if not isinstance(interval, (int, float)) or isinstance(interval, bool) or interval < 0 or interval > 300:
            raise ValueError(f"scenario {scenario['name']} interval_seconds must be in [0, 300]")
        token_env = scenario.get("token_env")
        if token_env is not None and (not isinstance(token_env, str) or not token_env):
            raise ValueError(f"scenario {scenario['name']} token_env must be a non-empty environment variable name")
        if token_env:
            token_names = ([token_env.replace("{lane}", str(lane)) for lane in range(1, concurrency + 1)]
                           if "{lane}" in token_env else [token_env])
            missing = [name for name in token_names if not os.environ.get(name)]
            if missing:
                raise ValueError(f"environment variable(s) required for scenario {scenario['name']}: {', '.join(missing)}")
    return config


def percentiles(values):
    if not values:
        return {"p50": None, "p95": None, "p99": None}
    ordered = sorted(values)

    def percentile(p):
        position = (len(ordered) - 1) * p
        low = int(position)
        high = min(low + 1, len(ordered) - 1)
        return round(ordered[low] + (ordered[high] - ordered[low]) * (position - low), 3)

    return {"p50": percentile(0.50), "p95": percentile(0.95), "p99": percentile(0.99)}


def _expand(value, index, lane, lane_sequence):
    if isinstance(value, str):
        numeric = {"{index_int}": index + 1, "{lane_sequence}": lane_sequence}
        if value in numeric:
            return numeric[value]
        replacements = {"{index}": index + 1, "{lane}": lane + 1}
        for marker, number in replacements.items():
            value = value.replace(marker, str(number))
        return value
    if isinstance(value, list):
        return [_expand(item, index, lane, lane_sequence) for item in value]
    if isinstance(value, dict):
        return {key: _expand(item, index, lane, lane_sequence) for key, item in value.items()}
    return value


def _request(base_url, scenario, index, lane, lane_sequence, timeout):
    method = scenario.get("method", "GET").upper()
    path = _expand(scenario["path"], index, lane, lane_sequence)
    body = _expand(scenario.get("json"), index, lane, lane_sequence)
    headers = {"Accept": "application/json"}
    token_env = scenario.get("token_env")
    if token_env:
        if "{lane}" in token_env:
            token_env = token_env.replace("{lane}", str(lane + 1))
        headers["Authorization"] = "Bearer " + os.environ[token_env]
    data = None
    if body is not None:
        data = json.dumps(body, separators=(",", ":")).encode()
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(base_url.rstrip("/") + path, data=data, headers=headers, method=method)
    started = time.perf_counter()
    try:
        with HTTP_OPENER.open(request, timeout=timeout) as response:
            # Ordinary API latency ends when response headers arrive. Reading
            # arbitrary bodies can defeat the request deadline and allocate
            # unbounded memory; stream scenarios opt into exactly one byte.
            if scenario.get("read_first_byte"):
                response.read(1)
            code = response.status
        ok = 200 <= code < 400
        detail = None if ok else f"HTTP {code}"
    except urllib.error.HTTPError as error:
        code, ok, detail = error.code, False, f"HTTP {error.code}"
        error.close()
    except Exception as error:  # timeout, DNS, refused connection, or malformed response
        code, ok, detail = None, False, type(error).__name__
    elapsed_ms = (time.perf_counter() - started) * 1000
    return {"latency_ms": round(elapsed_ms, 3), "status": code, "ok": ok, "error": detail}


def run_scenario(base_url, scenario, requests, concurrency, timeout, deadline=None):
    samples = []
    lock = threading.Lock()
    start = time.perf_counter()
    interval = scenario.get("interval_seconds", 0)

    def worker(lane):
        next_start = time.monotonic()
        for index in range(lane, requests, concurrency):
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                break
            wait = max(0, next_start - now)
            if deadline is not None:
                wait = min(wait, max(0, deadline - now))
            if wait:
                time.sleep(wait)
            now = time.monotonic()
            if deadline is not None and now >= deadline:
                break
            lane_sequence = index // concurrency + 1
            request_timeout = min(timeout, deadline - now) if deadline is not None else timeout
            sample = _request(base_url, scenario, index, lane, lane_sequence, request_timeout)
            with lock:
                samples.append(sample)
            next_start += interval

    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as pool:
        futures = [pool.submit(worker, lane) for lane in range(concurrency)]
        for future in futures:
            future.result()
    elapsed = time.perf_counter() - start
    latencies = [item["latency_ms"] for item in samples]
    successes = sum(item["ok"] for item in samples)
    errors = len(samples) - successes
    counts = {}
    for sample in samples:
        key = str(sample["status"]) if sample["status"] is not None else "transport_error"
        counts[key] = counts.get(key, 0) + 1
    return {
        "name": scenario["name"], "requested": requests, "requests": len(samples),
        "successes": successes, "errors": errors,
        "input": scenario_metadata(scenario),
        "elapsed_seconds": round(elapsed, 6), "throughput_requests_per_second": round(len(samples) / elapsed, 3) if elapsed else None,
        "latency_ms": latencies, "latency_percentiles_ms": percentiles(latencies),
        "http_status_counts": counts, "error_samples": [s["error"] for s in samples if s["error"]][:20],
        "stopped_due_to_duration": deadline is not None and len(samples) < requests and time.monotonic() >= deadline,
    }


def scenario_metadata(scenario):
    body = scenario.get("json")
    body_bytes = len(json.dumps(body, separators=(",", ":")).encode()) if body is not None else 0
    return {
        "name": scenario["name"],
        "method": scenario.get("method", "GET").upper(),
        "path": urllib.parse.urlsplit(scenario["path"]).path,
        "request_body_bytes": body_bytes,
        "interval_seconds": scenario.get("interval_seconds", 0),
        "read_first_byte": bool(scenario.get("read_first_byte", False)),
        "latency_measurement": "time_to_first_byte" if scenario.get("read_first_byte") else "time_to_response_headers",
        "authenticated": bool(scenario.get("token_env")),
    }


def hardware_metadata():
    info = {"captured_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(), "hostname": platform.node(),
            "os": platform.platform(), "architecture": platform.machine(), "python": platform.python_version(),
            "cpu_logical_count": os.cpu_count(), "cpu_model": None, "total_memory_bytes": None}
    try:
        disk = shutil.disk_usage(Path.cwd())
        info["output_filesystem_total_bytes"] = disk.total
        info["output_filesystem_free_bytes"] = disk.free
    except OSError:
        info["output_filesystem_free_bytes"] = None
    try:
        import psutil
        info["total_memory_bytes"] = psutil.virtual_memory().total
        info["cpu_model"] = platform.processor() or None
    except ImportError:
        info["resource_instrumentation"] = "psutil unavailable; cpu_model and total_memory_bytes omitted"
    return info


def process_monitor(pid, stop):
    try:
        import psutil
        process = psutil.Process(pid)
        observations = [{"cpu_percent": process.cpu_percent(interval=0.1),
                         "rss_bytes": process.memory_info().rss}]
        while not stop.wait(0.5):
            try:
                observations.append({"cpu_percent": process.cpu_percent(),
                                     "rss_bytes": process.memory_info().rss})
            except Exception:
                break
        if not observations:
            return {"pid": pid, "status": "unavailable", "reason": "no process samples collected"}
        return {"pid": pid, "status": "available", "sample_count": len(observations),
                "cpu_percent_average": round(sum(item["cpu_percent"] for item in observations) / len(observations), 2),
                "cpu_percent_max": round(max(item["cpu_percent"] for item in observations), 2),
                "rss_bytes_max": max(item["rss_bytes"] for item in observations)}
    except ImportError:
        return {"pid": pid, "status": "unavailable", "reason": "install optional psutil for target-process CPU/RSS"}
    except Exception as error:
        return {"pid": pid, "status": "unavailable", "reason": f"{type(error).__name__}: {error}"}


def database_size(database_url):
    try:
        parsed = urllib.parse.urlsplit(database_url)
        if parsed.scheme not in {"postgres", "postgresql"} or not parsed.hostname or not parsed.path:
            return {"status": "unavailable", "reason": "database URL must be a PostgreSQL URL"}
        connection_env = os.environ.copy()
        connection_env.update({"PGHOST": parsed.hostname, "PGPORT": str(parsed.port or 5432),
                               "PGDATABASE": urllib.parse.unquote(parsed.path.lstrip("/"))})
        if parsed.username:
            connection_env["PGUSER"] = urllib.parse.unquote(parsed.username)
        if parsed.password:
            connection_env["PGPASSWORD"] = urllib.parse.unquote(parsed.password)
        query = urllib.parse.parse_qs(parsed.query)
        if query.get("sslmode"):
            connection_env["PGSSLMODE"] = query["sslmode"][0]
        query_text = "SELECT pg_database_size(current_database())::text || '|' || current_setting('server_version') || '|' || current_setting('shared_buffers')"
        result = subprocess.run(["psql", "-XAt", "-c", query_text],
                                env=connection_env, check=True, capture_output=True, text=True, timeout=15)
        size, version, shared_buffers = result.stdout.strip().split("|", 2)
        return {"status": "available", "bytes": int(size), "postgres_version": version,
                "shared_buffers": shared_buffers}
    except Exception as error:
        reason = "psql is unavailable or the database size query failed"
        if isinstance(error, FileNotFoundError):
            reason = "psql executable is unavailable"
        return {"status": "unavailable", "reason": f"{type(error).__name__}: {reason}"}


def safe_origin(url):
    parsed = urllib.parse.urlsplit(url)
    host = parsed.hostname or "unknown"
    if parsed.port:
        host += f":{parsed.port}"
    return f"{parsed.scheme}://{host}"


def database_url_from_env(variable):
    if not variable:
        return None
    value = os.environ.get(variable)
    if not value:
        raise ValueError(f"environment variable {variable} is required")
    return value


def source_commit():
    try:
        result = subprocess.run(["git", "rev-parse", "HEAD"], check=True, capture_output=True,
                                text=True, timeout=3)
        return result.stdout.strip()
    except Exception:
        return None


def execute(config, pid=None, database_url=None):
    validate_config(config)
    result = {"schema_version": 1, "hardware": hardware_metadata(),
              "target": {"origin": safe_origin(config["base_url"]), "command_image_digest": config.get("command_image_digest"),
                         "source_commit": source_commit(),
                         "topology": config.get("topology", "one host, one Command process, one PostgreSQL instance"),
                         "requests_per_scenario": config["requests"], "concurrency": config["concurrency"],
                         "timeout_seconds": config.get("timeout_seconds", 10),
                         "max_duration_seconds": config.get("max_duration_seconds", 3600),
                         "deadline_semantics": "cooperative: stops starting requests; in-flight socket operations may finish later"},
              "instrumentation": {
                  "target_process": {"status": "unavailable", "reason": "no Command PID was configured"},
                  "database": {"status": "unavailable", "reason": "no database URL environment variable was configured"},
              }, "scenarios": []}
    stop_monitor = threading.Event()
    monitor = None
    if pid is not None:
        samples = []
        monitor = threading.Thread(target=lambda: samples.append(process_monitor(pid, stop_monitor)), daemon=True)
        monitor.start()
    if database_url:
        result["instrumentation"]["database_before"] = database_size(database_url)
        before = result["instrumentation"]["database_before"]
        result["instrumentation"]["database"] = (
            {"status": "available"} if before.get("status") == "available" else
            {"status": "unavailable", "reason": before.get("reason", "database sampling failed")}
        )
    deadline = time.monotonic() + config.get("max_duration_seconds", 3600)
    for scenario in config["scenarios"]:
        result["scenarios"].append(run_scenario(config["base_url"], scenario, config["requests"],
                                                config["concurrency"], config.get("timeout_seconds", 10), deadline))
    if monitor is not None:
        stop_monitor.set()
        monitor.join()
        result["instrumentation"]["target_process"] = samples[0]
    if database_url:
        result["instrumentation"]["database_after"] = database_size(database_url)
        before = result["instrumentation"]["database_before"]
        after = result["instrumentation"]["database_after"]
        result["instrumentation"]["database_growth_bytes"] = (
            after["bytes"] - before["bytes"] if before.get("status") == after.get("status") == "available" else None)
        if after.get("status") != "available":
            result["instrumentation"]["database"] = {
                "status": "unavailable", "reason": after.get("reason", "database sampling failed")
            }
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--output", type=Path, help="write full JSON report here (default: stdout)")
    parser.add_argument("--pid", type=int, help="sample this Command process (requires optional psutil)")
    parser.add_argument("--database-url-env", help="environment variable holding the database URL (keeps credentials out of process arguments)")
    args = parser.parse_args(argv)
    try:
        config = json.loads(args.config.read_text(encoding="utf-8"))
        report = execute(config, args.pid, database_url_from_env(args.database_url_env))
        rendered = json.dumps(report, indent=2, sort_keys=True) + "\n"
        if args.output:
            args.output.write_text(rendered, encoding="utf-8")
        else:
            sys.stdout.write(rendered)
        return 0 if all(item["errors"] == 0 and not item["stopped_due_to_duration"]
                        for item in report["scenarios"]) else 2
    except (OSError, ValueError, json.JSONDecodeError) as error:
        print(f"command-bench: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
