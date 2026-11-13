"""Committed, checksummed workflow artifacts on S3 or a local test filesystem."""

from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from pathlib import Path

import boto3
from botocore.exceptions import ClientError


def digest(path: Path) -> str:
    hasher = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            hasher.update(block)
    return hasher.hexdigest()


class Store:
    def __init__(self, uri: str):
        if uri.startswith("s3://"):
            parts = uri[5:].split("/", 1)
            if len(parts) != 2 or not parts[0] or not parts[1].strip("/"):
                raise ValueError("An S3 artifact URI requires a bucket and prefix")
            self.bucket, self.prefix = parts[0], parts[1].rstrip("/")
            self.root = None
            self.client = boto3.client("s3")
        else:
            self.root = Path(uri.removeprefix("file://")).resolve()
            self.bucket = self.prefix = None
            self.client = None
        self.uri = uri.rstrip("/")

    @staticmethod
    def _key(key: str) -> str:
        if not key or key.startswith("/") or ".." in key.split("/"):
            raise ValueError(f"Invalid artifact key: {key!r}")
        return key

    def read(self, key: str) -> bytes | None:
        key = self._key(key)
        if self.root is not None:
            path = self.root / key
            return path.read_bytes() if path.exists() else None
        try:
            return self.client.get_object(
                Bucket=self.bucket, Key=f"{self.prefix}/{key}"
            )["Body"].read()
        except ClientError as exc:
            if exc.response["Error"]["Code"] in ("NoSuchKey", "404"):
                return None
            raise

    def write(self, key: str, data: bytes, *, exclusive: bool = False) -> None:
        key = self._key(key)
        if self.root is not None:
            path = self.root / key
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_name(f".{path.name}.{uuid.uuid4()}.tmp")
            temporary.write_bytes(data)
            try:
                if exclusive:
                    os.link(temporary, path)
                else:
                    os.replace(temporary, path)
            finally:
                temporary.unlink(missing_ok=True)
        else:
            kwargs = {"IfNoneMatch": "*"} if exclusive else {}
            self.client.put_object(
                Bucket=self.bucket, Key=f"{self.prefix}/{key}", Body=data, **kwargs
            )

    def json(self, key: str) -> dict | list | None:
        data = self.read(key)
        return json.loads(data) if data is not None else None

    def write_json(self, key: str, value, *, exclusive: bool = False) -> None:
        self.write(key, json.dumps(value, sort_keys=True).encode(), exclusive=exclusive)

    def keys(self, prefix: str = "") -> list[str]:
        if self.root is not None:
            return sorted(
                str(path.relative_to(self.root))
                for path in self.root.rglob("*")
                if path.is_file()
                and str(path.relative_to(self.root)).startswith(prefix)
            )
        result = []
        for page in self.client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=f"{self.prefix}/{prefix}"
        ):
            result.extend(
                obj["Key"][len(self.prefix) + 1 :] for obj in page.get("Contents", [])
            )
        return sorted(result)

    def commit(self, path: Path, key: str) -> None:
        """Publish immutable data first, then its small conditional receipt."""
        key = self._key(key)
        receipt_key = key + ".receipt.json"
        checksum = digest(path)
        existing = self.json(receipt_key)
        if existing is not None:
            if existing["sha256"] != checksum:
                raise ValueError(f"Conflicting artifact commit: {key}")
            return
        object_key = f"objects/{uuid.uuid4()}/{path.name}"
        if self.root is not None:
            import shutil

            target = self.root / object_key
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(path, target)
        else:
            self.client.upload_file(
                str(path), self.bucket, f"{self.prefix}/{object_key}"
            )
        receipt = {
            "object": object_key,
            "sha256": checksum,
            "size": path.stat().st_size,
        }
        try:
            self.write_json(receipt_key, receipt, exclusive=True)
        except (FileExistsError, ClientError) as exc:
            if isinstance(exc, ClientError) and exc.response["Error"]["Code"] not in (
                "PreconditionFailed",
                "412",
                "ConditionalRequestConflict",
            ):
                raise
            if self.json(receipt_key) != receipt:
                previous = self.json(receipt_key)
                if previous is None or previous["sha256"] != checksum:
                    raise ValueError(f"Concurrent conflicting commit: {key}") from exc

    def restore(self, key: str, path: Path) -> None:
        receipt = self.json(key + ".receipt.json")
        if receipt is None:
            raise FileNotFoundError(f"Missing committed artifact: {key}")
        object_key = self._key(receipt["object"])
        path.parent.mkdir(parents=True, exist_ok=True)
        if self.root is not None:
            import shutil

            shutil.copyfile(self.root / object_key, path)
        else:
            self.client.download_file(
                self.bucket, f"{self.prefix}/{object_key}", str(path)
            )
        if digest(path) != receipt["sha256"]:
            path.unlink()
            raise ValueError(f"Corrupt artifact: {key}")


def checkpoint_key(name: str) -> str:
    match = re.fullmatch(
        r"(?:mb_)?f([0-9]+)_r[0-9]+(?:_g[0-9]+|_sample)?\.(?:rds|json|parquet)", name
    )
    if not match:
        raise ValueError(f"Invalid checkpoint name: {name}")
    return f"f{match[1]}/{name}"
