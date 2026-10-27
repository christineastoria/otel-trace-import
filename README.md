# Importing OpenTelemetry traces into LangSmith

Send OTel traces to a **self-hosted LangSmith** two ways:

- **Real-time (recommended, going forward):** your apps export to an OpenTelemetry Collector, and the Collector forwards to LangSmith.
- **Batch backfill (one-time):** convert existing trace data (e.g. a BigQuery export) to OTLP and send it.

Docs: https://docs.langchain.com/langsmith/trace-with-opentelemetry

## Files

```
otel-import/
├── otel-collector-langsmith.yaml      # Collector config: real-time + batch file import
├── import_otlp_json_to_langsmith.py   # direct sender for data already in OTLP-JSON
├── bigquery_rows_to_otlp.py           # convert flat span rows (BigQuery export) → OTLP, send
└── traces/                            # drop OTLP-JSON files here for the Collector to pick up
```

## Endpoint & headers (self-hosted)

- **Traces endpoint:** `https://<your-langsmith-host>/api/v1/otel/v1/traces`
  (base is `<host>/api/v1/otel`; the `/v1/traces` signal path is appended)
- **Headers:**
  - `x-api-key: <LANGSMITH_API_KEY>`
  - `Langsmith-Project: <project-name>` — self-hosted uses this, **not** the agent headers
- **Payload:** accepts OTLP/HTTP `application/json`, `application/protobuf`, and `application/x-protobuf`; `Content-Encoding: gzip` is supported.

## Going forward: real-time via the Collector

Run one Collector as the central forwarding point. Apps push OTLP to it; it batches, retries, and exports to LangSmith. Add or remove destinations without touching app code.

1. Edit `otel-collector-langsmith.yaml` — set your host in `traces_endpoint` and your `Langsmith-Project`.
2. Start the Collector:

   ```bash
   export LANGSMITH_API_KEY=lsv2_pt_...
   docker run --rm \
     -v "$PWD/otel-collector-langsmith.yaml:/etc/otelcol-contrib/config.yaml" \
     -v "$PWD/traces:/data/traces" \
     -e LANGSMITH_API_KEY \
     -p 4317:4317 -p 4318:4318 \
     otel/opentelemetry-collector-contrib:latest
   ```

3. Point your apps at the Collector: OTLP HTTP `http://<collector-host>:4318/v1/traces` or gRPC `:4317`.

For an internal CA or self-signed cert, uncomment the `tls:` block in the config.

## One-time backfill: BigQuery batch data

BigQuery stores spans **flat** — one row per span, columns like `trace_id`, `span_id`, `name`, `attributes`. That holds the same information as OTLP but is **not** OTLP format, so it needs converting first.

**Step 1 — export rows to JSON.** Newline-delimited JSON, one span per line:

```bash
bq extract --destination_format=NEWLINE_DELIMITED_JSON \
  'your_dataset.otel_spans' gs://your-bucket/spans-*.json
# then download the files locally
```

**Step 2 — pick a path:**

- **Direct send** — `bigquery_rows_to_otlp.py` reads the flat rows, builds OTLP, and POSTs to LangSmith. Simplest for a one-off:

  ```bash
  export LANGSMITH_API_KEY=lsv2_pt_...
  python bigquery_rows_to_otlp.py \
    -i spans.jsonl \
    --endpoint https://<your-host>/api/v1/otel/v1/traces \
    --langsmith-project backfill \
    --dry-run            # drop to send
  ```

  Override field names if your schema differs, e.g. `--col-start-time startTimeUnixNano`.

- **Through the Collector** — if your data is already OTLP-JSON (nested `resourceSpans` envelopes), copy the files into `traces/` and start the Collector above; the `otlpjsonfile` receiver ingests them. This reuses the same pipeline as real-time.

For a quick direct send of data that is already OTLP-JSON, `import_otlp_json_to_langsmith.py` re-chunks and POSTs it without the Collector.

### Backfill tips

- **Import complete traces together.** A child span whose parent never arrives is dropped after the parent-linking window (`REDIS_RUNS_EXPIRY_SECONDS`, default 12h). Group spans by trace and send each trace's spans within that window.
- **Keep batches moderate** (the Collector `batch` processor is set to 500 spans). The ingest endpoint has a 60s request timeout; gzip large payloads.

## Limits (self-hosted, tunable via env vars)

| Limit | Default | Env var |
|---|---|---|
| Request timeout | 60 s | `middleware.Timeout` (code) |
| Max field size — inputs / outputs / events / error / extra | 25 MB (→ HTTP 422 if exceeded) | `MAX_FIELD_SIZE_BYTES` |
| Max tags size (truncated from end) | 10 KB | `MAX_TAGS_SIZE_BYTES` |
| Max attachments per span | 2 | `OTEL_ATTACHMENT_MAX_PER_SPAN` |
| Parallel run-submit concurrency (non-batch path) | 20 | `RunsSemaphore` (code) |
| Redis ingest chunk size | 40 runs | `REDIS_TRANSACTION_RUNS_CHUNK_SIZE` |
| Parent/child linking window | 12 h | `REDIS_RUNS_EXPIRY_SECONDS` |
| S3 offload threshold | 0 KB (offload all, if blob storage on) | `MIN_BLOB_STORAGE_SIZE_KB` |

Notes:
- **No per-request body size limit** on the OTLP endpoint, and **no app-level rate limit** on ingest — large or fast backfills are bounded by the 60s timeout and the concurrency semaphore, not a hard cap. Rate limiting, if any, lives at your nginx/load balancer.
- A tenant over its usage limit gets HTTP **429**; oversized fields get HTTP **422** (`ErrPartTooLarge`).

Source: `smith-go/otel/otel.go`, `smith-go/runs/runs.go`, `smith-go/config/config.go`, `smith-go/ingestion/ingestion.go`.
