from __future__ import annotations

from locust import __version__

import hashlib
import inspect
import json
import marshal
import os
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .env import Environment

# Versions only differ in their patch number are considered compatible, mirroring
# the historical version check in MasterRunner.handle_message ("client_ready").
VERSION_PREFIX_LENGTH = 4

MANIFEST_KIND = "locust-execution-manifest"
MANIFEST_VERSION = 1


def _sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _hash_object_code(cls: type) -> str:
    """Fallback fingerprint for classes that cannot be located in a source file."""
    digest = hashlib.sha256()
    for member in sorted(vars(cls), key=str):
        value = getattr(cls, member)
        code = getattr(value, "__code__", None)
        if code is not None:
            digest.update(member.encode())
            digest.update(marshal.dumps(code))
    return digest.hexdigest()


def _hash_definition(definition: type | Any) -> tuple[str, str]:
    """
    Return (filename, sha256) for the source backing a User/LoadTestShape definition.

    User classes are types, shape classes are instances, so accept both.
    """
    cls = definition if isinstance(definition, type) else type(definition)
    try:
        path = inspect.getfile(cls)
    except (TypeError, OSError):
        # Dynamically defined classes (exec/inline) have no source file.
        try:
            return f"<dynamic>:{cls.__name__}", _sha256_hex(inspect.getsource(cls).encode())
        except (TypeError, OSError):
            return f"<dynamic>:{cls.__name__}", _hash_object_code(cls)

    try:
        with open(path, "rb") as f:
            return os.path.basename(path), _sha256_hex(f.read())
    except OSError:
        # The file existed when the class was imported but has since been removed.
        try:
            return f"<dynamic>:{cls.__name__}", _sha256_hex(inspect.getsource(cls).encode())
        except (TypeError, OSError):
            return f"<dynamic>:{cls.__name__}", _hash_object_code(cls)


def _collect_files(environment: Environment) -> list[dict[str, str]]:
    definitions: list[Any] = list((environment.available_user_classes or {}).values())
    definitions.extend((environment.available_shape_classes or {}).values())

    # Deduplicate by resolved absolute path on this node while the transported
    # identifier only contains the basename, so paths across hosts never cause
    # false mismatches.
    files_by_name: dict[str, set[str]] = {}
    seen_paths: set[str] = set()
    for definition in definitions:
        cls = definition if isinstance(definition, type) else type(definition)
        try:
            path = inspect.getfile(cls)
        except (TypeError, OSError):
            path = None

        if path is not None:
            if path in seen_paths:
                continue
            seen_paths.add(path)

        filename, file_hash = _hash_definition(definition)
        files_by_name.setdefault(filename, set()).add(file_hash)

    return [
        {"filename": filename, "sha256": file_hash}
        for filename in sorted(files_by_name)
        for file_hash in sorted(files_by_name[filename])
    ]


def _collect_user_classes(environment: Environment) -> list[dict[str, Any]]:
    user_classes = environment.available_user_classes or {}
    return [
        {"name": name, "weight": user_class.weight, "fixed_count": user_class.fixed_count}
        for name, user_class in sorted(user_classes.items())
    ]


def _collect_shape_class(environment: Environment) -> str | None:
    if not environment.available_shape_classes:
        return None
    return sorted(environment.available_shape_classes)[0]


def build_manifest(environment: Environment) -> dict[str, Any]:
    """
    Build the execution manifest of the locally loaded locustfiles.

    Contains only hashes and structural metadata (no source code), so it is cheap
    to serialize and safe to send over the worker/master RPC channel.
    """
    manifest = {
        "kind": MANIFEST_KIND,
        "manifest_version": MANIFEST_VERSION,
        "version_prefix": __version__[0:VERSION_PREFIX_LENGTH],
        "files": _collect_files(environment),
        "user_classes": _collect_user_classes(environment),
        "shape_class": _collect_shape_class(environment),
    }
    fingerprint_payload = {k: v for k, v in manifest.items() if k != "fingerprint"}
    manifest["fingerprint"] = _sha256_hex(
        json.dumps(fingerprint_payload, sort_keys=True, separators=(",", ":")).encode()
    )
    return manifest


def compare_manifests(reference: dict[str, Any], candidate: dict[str, Any]) -> list[str]:
    """
    Compare two manifests and return human-readable mismatch reasons.

    An empty list means the manifests describe the same execution definition.
    """
    reasons: list[str] = []

    reference_version = reference.get("version_prefix")
    candidate_version = candidate.get("version_prefix")
    if reference_version != candidate_version:
        reasons.append(f"locust version mismatch: worker runs {candidate_version!r}, master runs {reference_version!r}")

    reference_files = {entry["filename"]: entry["sha256"] for entry in reference.get("files", [])}
    candidate_files = {entry["filename"]: entry["sha256"] for entry in candidate.get("files", [])}
    for filename in sorted(set(reference_files) | set(candidate_files)):
        if filename not in candidate_files:
            reasons.append(f"locustfile missing on worker: {filename}")
        elif filename not in reference_files:
            reasons.append(f"unexpected locustfile on worker: {filename}")
        elif reference_files[filename] != candidate_files[filename]:
            reasons.append(f"locustfile content differs: {filename}")

    reference_classes = {entry["name"]: entry for entry in reference.get("user_classes", [])}
    candidate_classes = {entry["name"]: entry for entry in candidate.get("user_classes", [])}
    for name in sorted(set(reference_classes) | set(candidate_classes)):
        if name not in candidate_classes:
            reasons.append(f"user class missing on worker: {name}")
        elif name not in reference_classes:
            reasons.append(f"unexpected user class on worker: {name}")
        else:
            ref_entry = reference_classes[name]
            cand_entry = candidate_classes[name]
            if ref_entry.get("weight") != cand_entry.get("weight"):
                reasons.append(
                    f"user class {name} weight differs: worker {cand_entry.get('weight')}, "
                    f"master {ref_entry.get('weight')}"
                )
            if ref_entry.get("fixed_count") != cand_entry.get("fixed_count"):
                reasons.append(
                    f"user class {name} fixed_count differs: worker {cand_entry.get('fixed_count')}, "
                    f"master {ref_entry.get('fixed_count')}"
                )

    reference_shape = reference.get("shape_class")
    candidate_shape = candidate.get("shape_class")
    if reference_shape != candidate_shape:
        reasons.append(f"shape class differs: worker {candidate_shape!r}, master {reference_shape!r}")

    return reasons
