#!/usr/bin/env python3
"""Bulk-import OpenTelemetry trace spans from JSON into LangSmith.

Reads spans from newline-delimited JSON (the "BigQuery JSON" shape: one span
object per line) or a JSON array, rebuilds them as OTLP protobuf, and POSTs
them to LangSmith's OTLP ingestion endpoint in batches.

Usage:
    export LANGSMITH_API_KEY=lsv2_pt_...
    python import_otel_to_langsmith.py \
        --input spans.jsonl \
        --langsmith-project imported-otel

    # or pipe it in
    cat spans.jsonl | python import_otel_to_langsmith.py --langsmith-project imported-otel

Each JSON record is one span. If your field names differ from the defaults,
override them with the --col-* flags (see `FIELD MAPPING` below) — no code
changes needed.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any, Iterable, TextIO

from opentelemetry.proto.collector.trace.v1 import trace_service_pb2
from opentelemetry.proto.common.v1 import common_pb2
from opentelemetry.proto.resource.v1 import resource_pb2
from opentelemetry.proto.trace.v1 import trace_pb2
import requests

logger = logging.getLogger("otel_import")

# LangSmith OTLP endpoints. US is the default; use the EU host for EU workspaces.
DEFAULT_ENDPOINT = "https://api.smith.langchain.com/otel/v1/traces"


# --------------------------------------------------------------------------- #
# FIELD MAPPING
# Defaults match a conventional OTel span record. Override via flags if your
# JSON names things differently.
# --------------------------------------------------------------------------- #
DEFAULT_COLUMNS = {
    "trace_id": "trace_id",            # hex string (32 chars) or bytes
    "span_id": "span_id",              # hex string (16 chars) or bytes
    "parent_span_id": "parent_span_id",  # hex string / bytes / null
    "name": "name",
    "kind": "kind",                    # int (0-5) or SPAN_KIND_* string
    "start_time": "start_time",        # ISO-8601 string or int unix nanos
    "end_time": "end_time",            # ISO-8601 string or int unix nanos
    "status_code": "status_code",      # int (0-2) or STATUS_CODE_* string, nullable
    "status_message": "status_message",  # nullable
    "attributes": "attributes",        # map/object or array of key-value
    "resource_attributes": "resource_attributes",  # map/object or array of key-value
}


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", "-i", default="-",
                   help="Path to a .jsonl / .json file of spans, or '-' for stdin (default).")
    p.add_argument("--langsmith-project", default=os.environ.get("LANGSMITH_PROJECT", "default"),
                   help="Destination LangSmith project (default: $LANGSMITH_PROJECT or 'default').")
    p.add_argument("--endpoint", default=os.environ.get("LANGSMITH_OTEL_ENDPOINT", DEFAULT_ENDPOINT),
                   help=f"LangSmith OTLP traces endpoint (default: {DEFAULT_ENDPOINT}).")
    p.add_argument("--since", default=None,
                   help="Only import spans with start_time >= this ISO-8601 timestamp.")
    p.add_argument("--until", default=None,
                   help="Only import spans with start_time < this ISO-8601 timestamp.")
    p.add_argument("--limit", type=int, default=None, help="Max spans to read (debugging).")
    p.add_argument("--batch-size", type=int, default=500, help="Spans per OTLP POST (default: 500).")
    p.add_argument("--max-retries", type=int, default=5, help="Retries per batch on 429/5xx (default: 5).")
    p.add_argument("--timeout", type=float, default=60.0, help="Per-request timeout in seconds.")
    p.add_argument("--dry-run", action="store_true", help="Build batches but do not POST.")
    p.add_argument("--verbose", "-v", action="store_true", help="Debug logging.")
    # Field-name overrides.
    for key, default in DEFAULT_COLUMNS.items():
        p.add_argument(f"--col-{key.replace('_', '-')}", dest=f"col_{key}", default=default,
                       help=f"Field for {key} (default: {default}).")
    return p.parse_args(argv)


# --------------------------------------------------------------------------- #
# JSON read
# --------------------------------------------------------------------------- #
def _iter_records(stream: TextIO) -> Iterable[dict[str, Any]]:
    """Yield records from NDJSON (one object per line) or a single JSON array."""
    first = stream.read(1)
    while first and first.isspace():
        first = stream.read(1)
    if not first:
        return
    rest = stream.read()
    text = first + rest
    if first == "[":
        # Whole document is a JSON array.
        for obj in json.loads(text):
            yield obj
    else:
        # NDJSON: one JSON object per non-blank line.
        for line in text.splitlines():
            line = line.strip()
            if line:
                yield json.loads(line)


def read_spans(args: argparse.Namespace) -> Iterable[dict[str, Any]]:
    cols = {k: getattr(args, f"col_{k}") for k in DEFAULT_COLUMNS}
    since = _parse_ts(args.since) if args.since else None
    until = _parse_ts(args.until) if args.until else None

    if args.input == "-":
        source: TextIO = sys.stdin
        close = False
    else:
        source = open(args.input, "r", encoding="utf-8")
        close = True

    count = 0
    try:
        for record in _iter_records(source):
            # Remap source field names -> canonical keys used downstream.
            row = {key: record.get(col) for key, col in cols.items()}
            if since or until:
                ts = _to_datetime(row.get("start_time"))
                if since and (ts is None or ts < since):
                    continue
                if until and (ts is None or ts >= until):
                    continue
            yield row
            count += 1
            if args.limit and count >= args.limit:
                break
    finally:
        if close:
            source.close()


def _parse_ts(value: str) -> datetime:
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _to_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, int):
        return datetime.fromtimestamp(value / 1e9, tz=timezone.utc)  # unix nanos
    if isinstance(value, float):
        return datetime.fromtimestamp(value, tz=timezone.utc)        # unix seconds
    try:
        return _parse_ts(str(value))
    except ValueError:
        return None


# --------------------------------------------------------------------------- #
# Row -> OTLP protobuf mapping
# --------------------------------------------------------------------------- #
_KIND_NAMES = {
    "SPAN_KIND_UNSPECIFIED": 0, "SPAN_KIND_INTERNAL": 1, "SPAN_KIND_SERVER": 2,
    "SPAN_KIND_CLIENT": 3, "SPAN_KIND_PRODUCER": 4, "SPAN_KIND_CONSUMER": 5,
    "INTERNAL": 1, "SERVER": 2, "CLIENT": 3, "PRODUCER": 4, "CONSUMER": 5,
}
_STATUS_NAMES = {
    "STATUS_CODE_UNSET": 0, "STATUS_CODE_OK": 1, "STATUS_CODE_ERROR": 2,
    "UNSET": 0, "OK": 1, "ERROR": 2,
}


def _hex_to_bytes(value: Any, expected_len: int) -> bytes:
    """Accept hex strings or raw bytes; return fixed-length id bytes (b'' if empty)."""
    if value in (None, ""):
        return b""
    if isinstance(value, (bytes, bytearray)):
        return bytes(value)
    s = str(value).strip().replace("-", "")
    if not s or set(s) == {"0"}:
        return b""
    return bytes.fromhex(s).rjust(expected_len, b"\x00")[-expected_len:]


def _to_unix_nano(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, int):
        # OTLP's native unit is nanoseconds; treat plain ints as already nanos.
        return value
    if isinstance(value, float):
        return int(value * 1e9)  # assume seconds
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp() * 1e9)
    return int(_parse_ts(str(value)).timestamp() * 1e9)  # ISO-8601 string


def _any_value(value: Any) -> common_pb2.AnyValue:
    av = common_pb2.AnyValue()
    if value is None:
        return av
    if isinstance(value, bool):
        av.bool_value = value
    elif isinstance(value, int):
        av.int_value = value
    elif isinstance(value, float):
        av.double_value = value
    elif isinstance(value, (list, tuple)):
        av.array_value.values.extend(_any_value(v) for v in value)
    elif isinstance(value, dict):
        for k, v in value.items():
            av.kvlist_value.values.append(common_pb2.KeyValue(key=str(k), value=_any_value(v)))
    else:
        av.string_value = str(value)
    return av


def _attributes(raw: Any) -> list[common_pb2.KeyValue]:
    """Normalize attributes from the common JSON shapes into a KeyValue list."""
    if raw is None:
        return []
    out: list[common_pb2.KeyValue] = []
    # Shape A: array of {key, value} records (OTLP repeated-field export).
    if isinstance(raw, (list, tuple)):
        for item in raw:
            if isinstance(item, dict) and "key" in item:
                out.append(common_pb2.KeyValue(key=str(item["key"]),
                                               value=_unwrap_otlp_value(item.get("value"))))
            elif isinstance(item, dict):
                for k, v in item.items():
                    out.append(common_pb2.KeyValue(key=str(k), value=_any_value(v)))
        return out
    # Shape B: plain object/map.
    if isinstance(raw, dict):
        for k, v in raw.items():
            out.append(common_pb2.KeyValue(key=str(k), value=_any_value(v)))
        return out
    # Shape C: JSON string.
    if isinstance(raw, str):
        try:
            return _attributes(json.loads(raw))
        except json.JSONDecodeError:
            return []
    return []


def _unwrap_otlp_value(val: Any) -> common_pb2.AnyValue:
    """Handle attribute values already shaped like OTLP {stringValue: ...}.

    OTLP/JSON uses camelCase keys (stringValue); the protobuf JSON mapping also
    permits snake_case (string_value). Accept both.
    """
    if isinstance(val, dict):
        typed = {"stringValue", "intValue", "doubleValue", "boolValue",
                 "arrayValue", "kvlistValue", "bytesValue",
                 "string_value", "int_value", "double_value", "bool_value",
                 "array_value", "kvlist_value", "bytes_value"} & set(val.keys())
        if typed:
            key = next(iter(typed))
            inner = val[key]
            # intValue is a string in OTLP/JSON ("123") — coerce back to int.
            if key in ("intValue", "int_value") and isinstance(inner, str):
                try:
                    inner = int(inner)
                except ValueError:
                    pass
            return _any_value(inner)
    return _any_value(val)


def row_to_span(row: dict[str, Any]) -> trace_pb2.Span:
    span = trace_pb2.Span()
    span.trace_id = _hex_to_bytes(row.get("trace_id"), 16)
    span.span_id = _hex_to_bytes(row.get("span_id"), 8)
    parent = _hex_to_bytes(row.get("parent_span_id"), 8)
    if parent:
        span.parent_span_id = parent
    span.name = str(row.get("name") or "")

    kind = row.get("kind")
    if isinstance(kind, str):
        span.kind = _KIND_NAMES.get(kind.upper(), 0)
    elif isinstance(kind, int):
        span.kind = kind

    span.start_time_unix_nano = _to_unix_nano(row.get("start_time"))
    span.end_time_unix_nano = _to_unix_nano(row.get("end_time"))

    status_code = row.get("status_code")
    if isinstance(status_code, str):
        span.status.code = _STATUS_NAMES.get(status_code.upper(), 0)
    elif isinstance(status_code, int):
        span.status.code = status_code
    if row.get("status_message"):
        span.status.message = str(row["status_message"])

    span.attributes.extend(_attributes(row.get("attributes")))
    return span


def _resource_key(row: dict[str, Any]) -> tuple:
    attrs = _attributes(row.get("resource_attributes"))
    return tuple(sorted((kv.key, kv.value.SerializeToString()) for kv in attrs))


def build_request(rows: list[dict[str, Any]]) -> trace_service_pb2.ExportTraceServiceRequest:
    """Group a batch of rows into ResourceSpans by their resource attributes."""
    by_resource: dict[tuple, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        by_resource[_resource_key(row)].append(row)

    req = trace_service_pb2.ExportTraceServiceRequest()
    for _, group in by_resource.items():
        rs = req.resource_spans.add()
        res = resource_pb2.Resource()
        res.attributes.extend(_attributes(group[0].get("resource_attributes")))
        rs.resource.CopyFrom(res)
        scope_spans = rs.scope_spans.add()  # single default scope
        scope_spans.scope.CopyFrom(common_pb2.InstrumentationScope(name="json-otel-import"))
        scope_spans.spans.extend(row_to_span(r) for r in group)
    return req


# --------------------------------------------------------------------------- #
# POST to LangSmith OTLP endpoint
# --------------------------------------------------------------------------- #
def post_batch(session: requests.Session, args: argparse.Namespace,
               req: trace_service_pb2.ExportTraceServiceRequest) -> None:
    payload = req.SerializeToString()
    delay = 1.0
    for attempt in range(1, args.max_retries + 1):
        try:
            resp = session.post(args.endpoint, data=payload, timeout=args.timeout)
        except requests.RequestException as exc:
            if attempt == args.max_retries:
                raise
            logger.warning("POST failed (%s); retrying in %.1fs [%d/%d]", exc, delay, attempt, args.max_retries)
        else:
            if resp.status_code < 300:
                return
            if resp.status_code not in (429, 500, 502, 503, 504) or attempt == args.max_retries:
                raise RuntimeError(f"OTLP export failed {resp.status_code}: {resp.text[:500]}")
            logger.warning("HTTP %s; retrying in %.1fs [%d/%d]", resp.status_code, delay, attempt, args.max_retries)
        time.sleep(delay)
        delay = min(delay * 2, 30.0)  # bounded exponential backoff


def chunked(it: Iterable[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    batch: list[dict[str, Any]] = []
    for item in it:
        batch.append(item)
        if len(batch) >= size:
            yield batch
            batch = []
    if batch:
        yield batch


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)s %(message)s")

    api_key = os.environ.get("LANGSMITH_API_KEY")
    if not api_key and not args.dry_run:
        logger.error("LANGSMITH_API_KEY is not set.")
        return 2

    session = requests.Session()
    session.headers.update({
        "x-api-key": api_key or "",            # never logged
        "Langsmith-Project": args.langsmith_project,
        "Content-Type": "application/x-protobuf",
    })

    total_spans = total_batches = failed = 0
    logger.info("Importing from %s -> project %r (%s)%s",
                "stdin" if args.input == "-" else args.input,
                args.langsmith_project, args.endpoint,
                " [dry-run]" if args.dry_run else "")

    for batch in chunked(read_spans(args), args.batch_size):
        req = build_request(batch)
        total_batches += 1
        total_spans += len(batch)
        if args.dry_run:
            logger.info("Batch %d: %d spans, %d resource groups (not sent)",
                        total_batches, len(batch), len(req.resource_spans))
            continue
        try:
            post_batch(session, args, req)
            logger.info("Batch %d: sent %d spans (%d total)", total_batches, len(batch), total_spans)
        except Exception as exc:  # noqa: BLE001 - report and keep going
            failed += 1
            logger.error("Batch %d failed: %s", total_batches, exc)

    logger.info("Done. %d spans across %d batches, %d failed batches.",
                total_spans, total_batches, failed)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
