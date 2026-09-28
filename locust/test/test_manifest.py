from locust.env import Environment
from locust.manifest import build_manifest, compare_manifests
from locust.util.load_locustfile import load_locustfile

import json
import os
import tempfile
import unittest
from unittest.mock import patch

import msgpack

LOCUSTFILE_TEMPLATE = """
from locust import User, task

class WebUser(User):
    weight = {weight}
    fixed_count = {fixed_count}

    @task
    def index(self):
        {body}
"""

SHAPE_LOCUSTFILE_TEMPLATE = """
from locust import User, task, LoadTestShape

class WebUser(User):
    @task
    def index(self):
        pass

class {shape_name}(LoadTestShape):
    def tick(self):
        return None
"""


def _write_locustfile(directory: str, content: str, filename: str = "locustfile.py") -> str:
    path = os.path.join(directory, filename)
    with open(path, "w") as f:
        f.write(content)
    return path


def _env_from_locustfile(path: str) -> Environment:
    user_classes, shape_instances = load_locustfile(path)
    shape_classes = {type(shape).__name__: shape for shape in shape_instances}
    return Environment(
        user_classes=list(user_classes.values()),
        available_user_classes=user_classes,
        available_shape_classes=shape_classes,
    )


class TestManifest(unittest.TestCase):
    def test_identical_locustfiles_produce_same_manifest(self):
        content = LOCUSTFILE_TEMPLATE.format(weight=1, fixed_count=0, body="pass")
        with tempfile.TemporaryDirectory() as dir1, tempfile.TemporaryDirectory() as dir2:
            manifest1 = build_manifest(_env_from_locustfile(_write_locustfile(dir1, content)))
            manifest2 = build_manifest(_env_from_locustfile(_write_locustfile(dir2, content)))

        self.assertEqual(manifest1["fingerprint"], manifest2["fingerprint"])
        self.assertEqual(manifest1["files"], manifest2["files"])
        self.assertEqual(manifest1["user_classes"], manifest2["user_classes"])
        self.assertEqual(compare_manifests(manifest1, manifest2), [])

    def test_changed_task_body_changes_fingerprint_and_reports_file_reason(self):
        old_body = LOCUSTFILE_TEMPLATE.format(weight=1, fixed_count=0, body="pass")
        new_body = LOCUSTFILE_TEMPLATE.format(weight=1, fixed_count=0, body="x = 42")
        with tempfile.TemporaryDirectory() as dir1, tempfile.TemporaryDirectory() as dir2:
            reference = build_manifest(_env_from_locustfile(_write_locustfile(dir1, old_body)))
            candidate = build_manifest(_env_from_locustfile(_write_locustfile(dir2, new_body)))

        self.assertNotEqual(reference["fingerprint"], candidate["fingerprint"])
        reasons = compare_manifests(reference, candidate)
        self.assertTrue(any("locustfile content differs" in reason for reason in reasons))

    def test_missing_user_class_is_reported(self):
        content = LOCUSTFILE_TEMPLATE.format(weight=1, fixed_count=0, body="pass")
        with tempfile.TemporaryDirectory() as directory:
            reference = build_manifest(_env_from_locustfile(_write_locustfile(directory, content)))

        worker_env = Environment(available_user_classes={})
        candidate = build_manifest(worker_env)

        self.assertNotEqual(reference["fingerprint"], candidate["fingerprint"])
        reasons = compare_manifests(reference, candidate)
        self.assertIn("user class missing on worker: WebUser", reasons)

    def test_extra_user_class_is_reported(self):
        content = LOCUSTFILE_TEMPLATE.format(weight=1, fixed_count=0, body="pass")
        with tempfile.TemporaryDirectory() as directory:
            candidate = build_manifest(_env_from_locustfile(_write_locustfile(directory, content)))
        reference = build_manifest(Environment(available_user_classes={}))

        reasons = compare_manifests(reference, candidate)
        self.assertIn("unexpected user class on worker: WebUser", reasons)

    def test_weight_and_fixed_count_differences_are_reported(self):
        reference = {
            "version_prefix": "2.99",
            "files": [],
            "user_classes": [{"name": "WebUser", "weight": 10, "fixed_count": 0}],
            "shape_class": None,
        }
        candidate = {
            "version_prefix": "2.99",
            "files": [],
            "user_classes": [{"name": "WebUser", "weight": 5, "fixed_count": 3}],
            "shape_class": None,
        }
        reasons = compare_manifests(reference, candidate)
        self.assertTrue(any("WebUser weight differs" in reason for reason in reasons))
        self.assertTrue(any("WebUser fixed_count differs" in reason for reason in reasons))

    def test_shape_class_difference_is_reported(self):
        shape_a = SHAPE_LOCUSTFILE_TEMPLATE.format(shape_name="ShapeA")
        shape_b = SHAPE_LOCUSTFILE_TEMPLATE.format(shape_name="ShapeB")
        with tempfile.TemporaryDirectory() as dir1, tempfile.TemporaryDirectory() as dir2:
            reference = build_manifest(_env_from_locustfile(_write_locustfile(dir1, shape_a)))
            candidate = build_manifest(_env_from_locustfile(_write_locustfile(dir2, shape_b)))

        reasons = compare_manifests(reference, candidate)
        self.assertTrue(any("shape class differs" in reason for reason in reasons))
        self.assertNotEqual(reference["fingerprint"], candidate["fingerprint"])

    def test_version_prefix_difference_is_reported(self):
        content = LOCUSTFILE_TEMPLATE.format(weight=1, fixed_count=0, body="pass")
        with tempfile.TemporaryDirectory() as directory:
            reference = build_manifest(_env_from_locustfile(_write_locustfile(directory, content)))
            with patch("locust.manifest.__version__", "9.99.0"):
                candidate = build_manifest(_env_from_locustfile(_write_locustfile(directory, content)))

        reasons = compare_manifests(reference, candidate)
        self.assertTrue(any("locust version mismatch" in reason for reason in reasons))

    def test_dynamically_defined_class_uses_code_fallback(self):
        def make_dynamic_user(value: str):
            namespace: dict = {"__name__": "dynamic_manifest_test"}
            exec(
                f"""
from locust import User, task
class DynamicUser(User):
    @task
    def do_it(self):
        x = {value}
""",
                namespace,
            )
            return namespace["DynamicUser"]

        user_a = make_dynamic_user("1")
        user_b = make_dynamic_user("2")

        with self.assertRaises((TypeError, OSError)):
            # classes created through exec have no inspectable source file
            import inspect

            inspect.getfile(user_a)

        env_a = Environment(available_user_classes={"DynamicUser": user_a})
        env_b = Environment(available_user_classes={"DynamicUser": user_b})
        manifest_a = build_manifest(env_a)
        manifest_b = build_manifest(env_b)

        self.assertTrue(manifest_a["files"][0]["filename"].startswith("<dynamic>:"))
        self.assertNotEqual(manifest_a["files"][0]["sha256"], manifest_b["files"][0]["sha256"])

        same_user_b = make_dynamic_user("1")
        env_a_again = Environment(available_user_classes={"DynamicUser": same_user_b})
        self.assertEqual(manifest_a["files"][0]["sha256"], build_manifest(env_a_again)["files"][0]["sha256"])

    def test_manifest_is_serializable_and_contains_no_source_code(self):
        content = LOCUSTFILE_TEMPLATE.format(weight=1, fixed_count=0, body="this_is_a_unique_source_token_8421")
        with tempfile.TemporaryDirectory() as directory:
            manifest = build_manifest(_env_from_locustfile(_write_locustfile(directory, content)))
        json_payload = json.dumps(manifest)
        msgpack_payload = msgpack.packb(manifest)

        self.assertIn("fingerprint", manifest)
        self.assertNotIn("this_is_a_unique_source_token_8421", json_payload)
        self.assertNotIn(b"this_is_a_unique_source_token_8421", msgpack_payload)
        for entry in manifest["files"]:
            self.assertEqual(len(entry["sha256"]), 64)
