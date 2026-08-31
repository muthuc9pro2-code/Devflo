# Devflo

Devflo is a developer incident-analysis platform. A user uploads diagnostic
artifacts from a production incident (logs, stack traces, OTel-style traces,
screenshots) and Devflo turns them into a structured, evidence-backed
root-cause analysis, with an optional LLM-generated explanation layered on
top of the deterministic findings.

## Pipeline

```
upload / staging
  → bounded streaming parsing & normalization
  → evidence extraction & persistence
  → identity resolution / timeline reconstruction / correlation
  → deterministic root-cause prioritization
  → optional source-code correlation
  → bounded Gemini explanation (optional, additive)
```

The deterministic stages (parsing through root-cause prioritization) are the
system of record. Gemini is only asked to explain an already-complete
deterministic result — it never decides root cause on its own, and its
context is a bounded, structured summary rather than raw diagnostic dumps.
If Gemini is unconfigured or unavailable, the deterministic result still completes.

## Supported diagnostic input

Structured/plain-text logs (generic, JSON, syslog), stack traces, web
server, container, database, cloud gateway, CI/CD, browser, message broker,
and serverless log shapes, OpenTelemetry-style traces, and screenshots
(via OCR). Diagnostic artifacts can optionally be paired with a source ZIP
or a GitHub repository URL, which Devflo indexes and uses to correlate
stack frames against real source locations.

## Design properties

- **Bounded-memory ingestion** — diagnostics are streamed and parsed in
  fixed-size batches rather than loaded whole, up to a 1 GiB combined
  diagnostic budget, with a coarse raw-request ceiling ahead of multipart
  parsing.
- **Durable checkpoints** — each artifact tracks `processed_bytes` /
  `last_processed_line`, so a resumed or redispatched run continues from
  where it left off instead of reparsing from the start.
- **Asynchronous processing via Celery** — each artifact is processed as
  its own task, with cancellation, orphan/stale-analysis recovery (a
  periodic heartbeat-based scan), and worker-crash-safe redelivery.
- **Deterministic-first analysis** — identity resolution, timelines, and
  correlation/root-cause scoring are computed deterministically before any
  LLM is involved.
- **Per-user ownership isolation** — JWT access/refresh tokens in HttpOnly
  cookies; every analysis is scoped to the authenticated user who created it.
- **Small-deployment admission control** — one account may have at most
  three nonterminal investigations (pending/processing) at once, matching
  the two-worker interview deployment without introducing distributed
  quota infrastructure.

## Tech stack

**Backend:** Python 3.12+, FastAPI, SQLAlchemy, MySQL (PyMySQL), Alembic,
Celery, Redis, RapidOCR (screenshot OCR), google-genai (Gemini).

**Frontend:** React + Vite.

## Local development

Backend:

```
cd backend
uv sync
cp .env.example .env   # fill in real values
uv run alembic upgrade head
uv run uvicorn app.main:app --reload
```

Celery worker and beat scheduler (separate processes, both require Redis
and MySQL running):

```
uv run celery -A app.core.celery_app worker --loglevel=info
uv run celery -A app.core.celery_app beat --loglevel=info
```

Frontend:

```
cd frontend
npm install
npm run dev
```

## Tests & benchmarking

The backend test suite (`backend/tests/unit`, run with `uv run pytest`)
covers the parser, evidence pipeline, correlation engine, cancellation and
recovery lifecycle, and the API layer. `backend/scripts/` contains
standalone A/B and profiling scripts used during performance work on
parsing, source indexing, and correlation — not part of the test suite.

## Deployment status

Not yet containerized or deployed. `backend/Dockerfile` and
`docker-compose.yml` exist as empty placeholders, and there is no CI/CD
workflow configured. Several resource limits (e.g. OCR/artifact-fan-out
bounds, Celery worker concurrency) are sized in code comments for a small,
single-machine target (~2 vCPU) rather than for horizontal scaling or high
availability.
