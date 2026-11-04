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
    lock = threading.Lock()

    def do_GET(self):
        with self.lock:
            type(self).active += 1
            type(self).peak = max(type(self).peak, type(self).active)
        time.sleep(0.015)
        try:
            self.send_response(503 if self.path == "/fail" else 200)
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
            bench.validate_config([])

    def test_example_heartbeat_matches_the_protocol_contract(self):
        example_path = Path(__file__).with_name("scenarios.example.json")
        example = json.loads(example_path.read_text(encoding="utf-8"))
        heartbeat = next(item for item in example["scenarios"] if item["name"] == "heartbeats")
        self.assertEqual(heartbeat["json"]["method"], "node.heartbeat")
        self.assertEqual(heartbeat["json"]["params"]["status"], "ready")
        self.assertIsInstance(heartbeat["json"]["params"]["capacity"], dict)
        self.assertTrue(heartbeat["json"]["params"]["capacity"])

    def test_worker_pool_is_bounded_and_http_errors_are_counted(self):
        FixtureHandler.peak = 0
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
        timed_out = bench.run_scenario(
            self.base, scenario, requests=1, concurrency=1, timeout=0.001
        )
        self.assertEqual(timed_out["errors"], 1)

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
            os.environ.update({"BENCH_NODE_TOKEN": "fixture", "BENCH_OPERATOR_TOKEN": "fixture"})
            bench.validate_config(checked)
        finally:
            os.environ.clear()
            os.environ.update(original)

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

    def test_report_origin_does_not_include_credentials_or_paths(self):
        self.assertEqual(bench.safe_origin("https://operator:secret@example.test:8443/private"),
                         "https://example.test:8443")


if __name__ == "__main__":
    unittest.main()
