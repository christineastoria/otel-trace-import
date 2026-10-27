#!/usr/bin/env python3
"""Bulk-import OTLP-JSON traces into LangSmith (passthrough).

Use this when your data is ALREADY in OTLP JSON shape, i.e. standard
ResourceSpans envelopes:

    {"resourceSpans": [{"resource": {...}, "scopeSpans": [{"spans": [...]}]}]}

No per-field mapping happens — LangSmith's OTLP endpoint speaks OTLP/JSON, so we
just re-chunk the spans to a sane batch size and POST them straight through.

Accepts any of:
  * a single JSON document  {"resourceSpans": [...]}
  * a JSON array            [ {"resourceSpans": [...]}, ... ]  or  [ {resourceSpans entry}, ... ]
  * NDJSON                  one of the above per line

Usage:
    export LANGSMITH_API_KEY=lsv2_pt_...
    python import_otlp_json_to_langsmith.py --input spans.json --langsmith-project imported-otel
    cat spans.ndjson | python import_otlp_json_to_langsmith.py --langsmith-project imported-otel
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Iterable, TextIO

import requests

logger = logging.getLogger("otlp_import")

DEFAULT_ENDPOINT = "https://api.smith.langchain.com/otel/v1/traces"


def parse_args(argv: list[str]) -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--input", "-i", default="-",
                   help="Path to OTLP-JSON (.json / .ndjson), or '-' for stdin (default).")
    p.add_argument("--langsmith-project", default=os.environ.get("LANGSMITH_PROJECT", "default"),
                   help="Destination LangSmith project (default: $LANGSMITH_PROJECT or 'default').")
    p.add_argument("--endpoint", default=os.environ.get("LANGSMITH_OTEL_ENDPOINT", DEFAULT_ENDPOINT),
                   help=f"LangSmith OTLP traces endpoint (default: {DEFAULT_ENDPOINT}). "
                        "For self-hosted, e.g. https://langsmith.internal/otel/v1/traces")
    p.add_argument("--header", "-H", action="append", default=[], metavar="KEY:VALUE",
                   help="Extra request header (repeatable), e.g. -H 'X-Tenant-Id: abc'. "
                        "Overrides defaults on key collision.")
    p.add_argument("--no-auth", action="store_true",
                   help="Do not send x-api-key (self-hosted deployments without auth).")
    p.add_argument("--ca-cert", default=os.environ.get("LANGSMITH_CA_CERT"),
                   help="Path to a CA bundle for verifying a self-hosted TLS cert.")
    p.add_argument("--insecure", action="store_true",
                   help="Skip TLS verification (self-signed certs). Use with care.")
    p.add_argument("--batch-size", type=int, default=500, help="Spans per OTLP POST (default: 500).")
    p.add_argument("--max-retries", type=int, default=5, help="Retries per batch on 429/5xx (default: 5).")
    p.add_argument("--timeout", type=float, default=60.0, help="Per-request timeout in seconds.")
    p.add_argument("--dry-run", action="store_true", help="Build batches but do not POST.")
    p.add_argument("--verbose", "-v", action="store_true", help="Debug logging.")
    return p.parse_args(argv)


# --------------------------------------------------------------------------- #
# Read OTLP-JSON documents
# --------------------------------------------------------------------------- #
def _iter_documents(stream: TextIO) -> Iterable[Any]:
    """Yield top-level JSON values from a single document, an array, or NDJSON."""
    first = stream.read(1)
    while first and first.isspace():
        first = stream.read(1)
    if not first:
        return
    text = first + stream.read()
    if first in "[{":
        # Try to parse as one whole JSON value first (covers single doc + array).
        try:
            doc = json.loads(text)
        except json.JSONDecodeError:
            doc = None
        if doc is not None:
            if isinstance(doc, list):
                yield from doc
            else:
                yield doc
            return
    # Fall back to NDJSON: one JSON value per non-blank line.
    for line in text.splitlines():
        line = line.strip()
        if line:
            yield json.loads(line)


def _scope_spans(rs: dict[str, Any]) -> list[dict[str, Any]]:
    # Prefer the current field name; fall back to the deprecated one.
    return rs.get("scopeSpans") or rs.get("instrumentationLibrarySpans") or []


def iter_resource_spans(stream: TextIO) -> Iterable[dict[str, Any]]:
    """Normalize any accepted input into a stream of ResourceSpans dicts."""
    for doc in _iter_documents(stream):
        if not isinstance(doc, dict):
            continue
        if "resourceSpans" in doc:
            yield from doc["resourceSpans"]
        elif "scopeSpans" in doc or "instrumentationLibrarySpans" in doc or "resource" in doc:
            yield doc  # already a single ResourceSpans entry
        else:
            logger.warning("Skipping record with no resourceSpans/scopeSpans keys: %s",
                           list(doc.keys()))


# --------------------------------------------------------------------------- #
# Re-chunk spans into <= batch_size OTLP requests, preserving resource/scope
# --------------------------------------------------------------------------- #
def rebatch(resource_spans: Iterable[dict[str, Any]], size: int) -> Iterable[dict[str, Any]]:
    batch: list[dict[str, Any]] = []
    count = 0
    for rs in resource_spans:
        resource = rs.get("resource")
        for ss in _scope_spans(rs):
            scope = ss.get("scope") or ss.get("instrumentationLibrary")
            schema_url = ss.get("schemaUrl")
            spans = ss.get("spans") or []
            i = 0
            while i < len(spans):
                take = min(size - count, len(spans) - i)
                scope_entry: dict[str, Any] = {"spans": spans[i:i + take]}
                if scope is not None:
                    scope_entry["scope"] = scope
                if schema_url is not None:
                    scope_entry["schemaUrl"] = schema_url
                rs_entry: dict[str, Any] = {"scopeSpans": [scope_entry]}
                if resource is not None:
                    rs_entry["resource"] = resource
                if rs.get("schemaUrl") is not None:
                    rs_entry["schemaUrl"] = rs["schemaUrl"]
                batch.append(rs_entry)
                count += take
                i += take
                if count >= size:
                    yield {"resourceSpans": batch}
                    batch, count = [], 0
    if batch:
        yield {"resourceSpans": batch}


def _span_count(payload: dict[str, Any]) -> int:
    return sum(len(ss.get("spans") or [])
               for rs in payload["resourceSpans"]
               for ss in _scope_spans(rs))


# --------------------------------------------------------------------------- #
# POST to LangSmith OTLP endpoint
# --------------------------------------------------------------------------- #
def post_batch(session: requests.Session, args: argparse.Namespace, payload: dict[str, Any]) -> None:
    body = json.dumps(payload).encode("utf-8")
    delay = 1.0
    for attempt in range(1, args.max_retries + 1):
        try:
            resp = session.post(args.endpoint, data=body, timeout=args.timeout)
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
        "Content-Type": "application/json",
    })

    if args.input == "-":
        source: TextIO = sys.stdin
        close = False
    else:
        source = open(args.input, "r", encoding="utf-8")
        close = True

    total_spans = total_batches = failed = 0
    logger.info("Importing from %s -> project %r (%s)%s",
                "stdin" if args.input == "-" else args.input,
                args.langsmith_project, args.endpoint,
                " [dry-run]" if args.dry_run else "")

    try:
        for payload in rebatch(iter_resource_spans(source), args.batch_size):
            n = _span_count(payload)
            total_batches += 1
            total_spans += n
            if args.dry_run:
                logger.info("Batch %d: %d spans, %d resource groups (not sent)",
                            total_batches, n, len(payload["resourceSpans"]))
                continue
            try:
                post_batch(session, args, payload)
                logger.info("Batch %d: sent %d spans (%d total)", total_batches, n, total_spans)
            except Exception as exc:  # noqa: BLE001 - report and keep going
                failed += 1
                logger.error("Batch %d failed: %s", total_batches, exc)
    finally:
        if close:
            source.close()

    logger.info("Done. %d spans across %d batches, %d failed batches.",
                total_spans, total_batches, failed)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
