import importlib.util
import json
import os
import sys
import threading
import time
import unittest
from unittest import mock
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


MODULE_PATH = Path(__file__).with_name("command_bench.py")
spec = importlib.util.spec_from_file_location("command_bench", MODULE_PATH)
bench = importlib.util.module_from_spec(spec)
spec.loader.exec_module(bench)


class FixtureHandler(BaseHTTPRequestHandler):
    active = 0
    peak = 0
    authorizations = {}
    lock = threading.Lock()

    def do_GET(self):
        with self.lock:
            type(self).active += 1
            type(self).peak = max(type(self).peak, type(self).active)
            type(self).authorizations[self.path] = self.headers.get("Authorization")
        time.sleep(0.015)
        try:
            status = 503 if self.path == "/fail" else 302 if self.path == "/redirect" else 200
            self.send_response(status)
            if status == 302:
                self.send_header("Location", "/ok")
            self.end_headers()
            self.wfile.write(b"ok")
        except OSError:
            pass  # the timeout test intentionally closes its client socket
        finally:
            with self.lock:
                type(self).active -= 1

    def log_message(self, *_args):
        pass


class BenchmarkTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.server = ThreadingHTTPServer(("127.0.0.1", 0), FixtureHandler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.base = f"http://127.0.0.1:{cls.server.server_port}"

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()

    def test_rejects_invalid_bounds(self):
        with self.assertRaises(ValueError):
            bench.validate_config({"base_url": self.base, "requests": 0, "concurrency": 1})
        with self.assertRaises(ValueError):
            bench.validate_config({"base_url": self.base, "requests": 2, "concurrency": 3})
        with self.assertRaises(ValueError):
            bench.validate_config({"base_url": "https://u:p@example.test", "requests": 1,
                                   "concurrency": 1, "scenarios": [{"name": "x", "path": "/"}]})
        with self.assertRaises(ValueError):
            bench.validate_config({"base_url": self.base, "requests": 1, "concurrency": 1,
                                   "max_duration_seconds": 0, "scenarios": [{"name": "x", "path": "/"}]})
        with self.assertRaises(ValueError):
            bench.validate_config({"base_url": self.base, "requests": 1, "concurrency": 1,
                                   "scenarios": [{"name": "x", "path": "/", "interval_seconds": -1}]})
        with self.assertRaises(ValueError):
            bench.validate_config([])

    def test_worker_pool_is_bounded_and_http_errors_are_counted(self):
        # Earlier tests may have intentionally timed out their clients while
        # the fixture server is still finishing those handlers. Drain them so
        # this assertion measures only the request batch below.
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            with FixtureHandler.lock:
                if FixtureHandler.active == 0:
                    FixtureHandler.peak = 0
                    break
            time.sleep(0.005)
        else:
            self.fail("fixture server did not drain before bounded-pool assertion")
        scenario = {"name": "bounded", "method": "GET", "path": "/ok"}
        result = bench.run_scenario(self.base, scenario, requests=12, concurrency=3, timeout=2)
        self.assertEqual(result["successes"], 12)
        self.assertLessEqual(FixtureHandler.peak, 3)
        self.assertEqual(len(result["latency_ms"]), 12)
        failed = bench.run_scenario(
            self.base, {**scenario, "path": "/fail"}, requests=3, concurrency=2, timeout=2
        )
        self.assertEqual(failed["errors"], 3)
        self.assertEqual(failed["successes"], 0)
        redirected = bench.run_scenario(
            self.base, {**scenario, "path": "/redirect"}, requests=1, concurrency=1, timeout=2
        )
        self.assertEqual(redirected["errors"], 1)
        self.assertEqual(redirected["http_status_counts"], {"302": 1})
        timed_out = bench.run_scenario(
            self.base, scenario, requests=1, concurrency=1, timeout=0.001
        )
        self.assertEqual(timed_out["errors"], 1)

    def test_scenario_interval_paces_requests_per_lane(self):
        result = bench.run_scenario(
            self.base, {"name": "paced", "path": "/ok", "interval_seconds": 0.03},
            requests=2, concurrency=1, timeout=2,
        )
        self.assertEqual(result["requests"], 2)
        self.assertGreaterEqual(result["elapsed_seconds"], 0.04)

    def test_scenario_stops_at_the_total_run_deadline(self):
        deadline = time.monotonic() + 0.06
        result = bench.run_scenario(
            self.base, {"name": "bounded-time", "path": "/ok"},
            requests=100, concurrency=2, timeout=2, deadline=deadline,
        )
        self.assertLess(result["requests"], 100)
        self.assertEqual(result["requested"], 100)
        self.assertTrue(result["stopped_due_to_duration"])

    def test_summary_percentiles_and_empty_sample(self):
        self.assertEqual(bench.percentiles([]), {"p50": None, "p95": None, "p99": None})
        self.assertEqual(bench.percentiles([1, 2, 3, 4])["p50"], 2.5)

    def test_lane_templates_preserve_global_ids_and_per_lane_sequences(self):
        value = bench._expand({"event": "evt-{index}", "node": "node-{lane}",
                               "sequence": "{lane_sequence}"}, 7, 1, 3)
        self.assertEqual(value, {"event": "evt-8", "node": "node-2", "sequence": 3})

    def test_config_loads_checked_in_scenarios(self):
        data = json.loads((Path(__file__).with_name("scenarios.example.json")).read_text())
        checked = {"base_url": self.base, "requests": 10, "concurrency": 2, "scenarios": data["scenarios"]}
        heartbeat = next(item for item in checked["scenarios"] if item["name"] == "heartbeats")
        self.assertIsInstance(heartbeat["json"]["params"].get("capacity"), dict)
        self.assertTrue(heartbeat["json"]["params"]["capacity"])
        original = dict(os.environ)
        try:
            os.environ.update({"BENCH_NODE_TOKEN_1": "node-one", "BENCH_NODE_TOKEN_2": "node-two",
                               "BENCH_OPERATOR_TOKEN": "fixture"})
            bench.validate_config(checked)
        finally:
            os.environ.clear()
            os.environ.update(original)

    def test_node_token_template_uses_each_lane_bound_credential(self):
        FixtureHandler.authorizations.clear()
        with mock.patch.dict(os.environ, {"NODE_TOKEN_1": "node-one", "NODE_TOKEN_2": "node-two"}):
            result = bench.run_scenario(
                self.base,
                {"name": "lane-token", "path": "/lane-{lane}", "token_env": "NODE_TOKEN_{lane}"},
                requests=2, concurrency=2, timeout=2,
            )
        self.assertEqual(result["errors"], 0)
        self.assertEqual(FixtureHandler.authorizations["/lane-1"], "Bearer node-one")
        self.assertEqual(FixtureHandler.authorizations["/lane-2"], "Bearer node-two")

    def test_example_scenarios_use_declared_server_routes(self):
        routes = (Path(__file__).parents[1] / "src" / "server.rs").read_text(encoding="utf-8")
        for route in (
            '.route("/v1/protocol/messages", post(message))',
            '.route("/v1/office/projection", get(projection))',
            '.route("/v1/organizations/{org}/events", get(organization_events))',
            '.route("/v1/organizations/{org}/events/stream", get(stream_events))',
            '"/v1/organizations/{org}/runs/{run}/{action}"',
            "post(control_run)",
        ):
            self.assertIn(route, routes, route)

    def test_missing_optional_collectors_are_reported_unavailable(self):
        stop = threading.Event()
        with mock.patch.dict(sys.modules, {"psutil": None}):
            process = bench.process_monitor(123, stop)
        self.assertEqual(process["status"], "unavailable")
        with mock.patch.object(bench.subprocess, "run", side_effect=FileNotFoundError("psql")):
            database = bench.database_size("postgresql://redacted")
        self.assertEqual(database["status"], "unavailable")

    def test_unconfigured_process_and_database_collectors_are_reported(self):
        config = {"base_url": self.base, "requests": 1, "concurrency": 1,
                  "scenarios": [{"name": "stub", "path": "/ok"}]}
        with mock.patch.object(bench, "hardware_metadata", return_value={}), \
             mock.patch.object(bench, "source_commit", return_value="fixture"), \
             mock.patch.object(bench, "run_scenario", return_value={"name": "stub", "errors": 0,
                                                                    "stopped_due_to_duration": False}):
            report = bench.execute(config)
        self.assertEqual(report["instrumentation"]["target_process"]["status"], "unavailable")
        self.assertEqual(report["instrumentation"]["database"]["status"], "unavailable")

    def test_report_origin_does_not_include_credentials_or_paths(self):
        self.assertEqual(bench.safe_origin("https://operator:secret@example.test:8443/private"),
                         "https://example.test:8443")

    def test_scenario_report_records_safe_workload_inputs_not_payloads(self):
        scenario = {"name": "safe-report", "method": "POST", "path": "/v1/events?cursor=secret",
                    "json": {"token": "must-not-be-reported"}, "token_env": "SECRET_TOKEN",
                    "interval_seconds": 1, "read_first_byte": False}
        metadata = bench.scenario_metadata(scenario)
        self.assertEqual(metadata["path"], "/v1/events")
        self.assertGreater(metadata["request_body_bytes"], 0)
        self.assertTrue(metadata["authenticated"])
        self.assertNotIn("must-not-be-reported", json.dumps(metadata))
        self.assertNotIn("SECRET_TOKEN", json.dumps(metadata))

    def test_database_url_is_read_by_environment_variable_name(self):
        with mock.patch.dict(os.environ, {"BENCH_DATABASE_URL": "postgresql://operator:secret@db/test"}):
            self.assertEqual(bench.database_url_from_env("BENCH_DATABASE_URL"),
                             "postgresql://operator:secret@db/test")
        with mock.patch.dict(os.environ, {}, clear=True):
            self.assertIsNone(bench.database_url_from_env(None))
            with self.assertRaises(ValueError):
                bench.database_url_from_env("BENCH_DATABASE_URL")


if __name__ == "__main__":
    unittest.main()
