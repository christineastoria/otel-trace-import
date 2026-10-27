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
  - `x-api-key: <LANGSMITH_API_KEY>` — [how to create an API key](https://docs.langchain.com/langsmith/create-account-api-key)
  - `Langsmith-Project: <project-name>` — should map to your agent name
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

> ⚠️ **Assumption:** We don't yet know the exact schema of your BigQuery table — its column names, types, or which span attributes it carries. The converter assumes a *conventional* OTel span layout (the structural columns below). **Confirm these against your real table** and remap any that differ with the `--col-*` flags — no code changes needed. Paste one real row and we can lock the mapping precisely.

### Structural span fields the converter maps (from columns)

| OTLP span field | Assumed column (`--col-…`) | Accepted input shapes |
|---|---|---|
| `trace_id` | `trace_id` | 32-char hex string, or bytes |
| `span_id` | `span_id` | 16-char hex string, or bytes |
| `parent_span_id` | `parent_span_id` | hex string, bytes, or null/empty (= root span) |
| `name` | `name` | string |
| `kind` | `kind` | int `0–5`, or `SPAN_KIND_*` / `INTERNAL`/`SERVER`/`CLIENT`/… |
| `start_time_unix_nano` | `start_time` | ISO-8601 string, `TIMESTAMP`, int unix **nanos**, or float unix **seconds** |
| `end_time_unix_nano` | `end_time` | same as `start_time` |
| `status.code` | `status_code` | int `0–2`, or `STATUS_CODE_*` / `UNSET`/`OK`/`ERROR` (nullable) |
| `status.message` | `status_message` | string (nullable) |
| span `attributes` | `attributes` | object/map, array of `{key, value}` (camelCase or snake_case), or JSON string |
| resource `attributes` | `resource_attributes` | same shapes; also groups spans into `resourceSpans` |

### Span attributes LangSmith reads (populate these for rich runs)

Whatever keys live inside your `attributes` / `resource_attributes` pass through untouched. LangSmith then recognizes the keys below and maps them onto run fields — so the richer your attributes, the better the imported traces render. Full reference: [Supported OpenTelemetry attribute and event mapping](https://docs.langchain.com/langsmith/trace-with-opentelemetry#supported-opentelemetry-attribute-and-event-mapping).

**Run type / span kind**
| Attribute | → |
|---|---|
| `langsmith.span.kind` | run type: `llm`/`chain`/`tool`/`retriever`/`embedding`/`prompt`/`parser` |
| `gen_ai.operation.name` | `chat`/`completion` → llm, `embedding` → embedding |
| `gen_ai.tool.name` | tool name + run type `tool` |
| `traceloop.span.kind`, `traceloop.llm.request.type`, `openinference.span.kind` | run type |
| `llm.prompt_template.variables` | run type `prompt` |

**Core LangSmith attributes**
| Attribute | → |
|---|---|
| `langsmith.trace.name` | run name (overrides span name) |
| `langsmith.trace.id` / `langsmith.span.id` / `langsmith.span.parent_id` | trace id / run id / parent run id |
| `langsmith.span.dotted_order` | tree position |
| `langsmith.trace.session_id` / `langsmith.trace.session_name` | session (project) id / name |
| `langsmith.span.tags` | tags (comma-separated) |
| `langsmith.metadata.{key}` | `metadata.{key}` |
| `langsmith.attachments` | trace attachments (≤ 2 per span, needs blob storage) |

**GenAI content & params**
| Attribute | → |
|---|---|
| `gen_ai.system` | `metadata.ls_provider` |
| `gen_ai.prompt` / `gen_ai.completion` | `inputs` / `outputs` |
| `gen_ai.prompt.{n}.role`/`.content`, `gen_ai.completion.{n}.role`/`.content` | `inputs`/`outputs` messages |
| `gen_ai.input.messages` / `gen_ai.output.messages` | `inputs.messages` / `outputs.messages` |
| `gen_ai.request.model` / `gen_ai.response.model` | `invocation_params.model` |
| `gen_ai.request.*` (`temperature`, `top_p`, `top_k`, `max_tokens`, `frequency_penalty`, `presence_penalty`, `seed`, `stop_sequences`, `encoding_formats`) | `invocation_params.*` |

**Token usage**
| Attribute | → |
|---|---|
| `gen_ai.usage.input_tokens` / `output_tokens` / `total_tokens` | `usage_metadata.*` |
| `gen_ai.usage.cache_read.input_tokens` / `cache_write.input_tokens` | `input_token_details.cache_read` / `cache_creation` |
| `gen_ai.usage.reasoning.output_tokens` | `output_token_details.reasoning` |
| deprecated `prompt_tokens`/`completion_tokens` + OpenLLMetry `cache_*_input_tokens` spellings | same targets (precedence: current → deprecated dotted → OpenLLMetry underscore) |

**Other instrumentation libraries** (also supported)
| Source | Attributes → |
|---|---|
| TraceLoop | `traceloop.entity.input`/`.output` → inputs/outputs; `traceloop.entity.name` → run name; `traceloop.association.properties.{key}` → `metadata.{key}` |
| OpenInference | `input.value`/`output.value` → inputs/outputs; `llm.system` → `ls_provider`; `llm.model_name` → `ls_model_name`; `tool.name` → run name; `metadata` → metadata |
| OpenLLMetry (`llm.*`) | `llm.input_messages`/`llm.output_messages`; `llm.token_count.prompt`/`.completion`/`.total`; `llm.invocation_parameters`; `llm.request.functions` |
| Retriever | `retrieval.documents.{n}.document.content`/`.metadata` → `outputs.documents[n]` |
| Tool | `tools` / `tool_arguments` → `invocation_params.*` |
| Logfire | `prompt` → inputs; `all_messages_events`/`events` → outputs/inputs |

**OTLP events** (not attributes — on the span's events list): `gen_ai.content.prompt`/`.completion`, `gen_ai.{system,user}.message` → input messages, `gen_ai.{assistant,tool}.message`/`gen_ai.choice` → output messages, and `exception` → sets status `error` + extracts message/stacktrace.

**Not mapped from attributes:** cost (no attribute — derived from model + tokens), and start/end time (taken from the OTLP span timing, not an attribute).

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
