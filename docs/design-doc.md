# Project 2 — Daily API-Based Data Collector
## Design Doc

**Status:** implemented and tagged as `v0.2-api-collector`. Extended in project 3 with an S3 raw-layer upload, see [`data-lake-design.md`](data-lake-design.md).
**Last updated:** 2026-10-14

---

## 1. Problem and Goal

Project 1 worked with a static CSV. This project practices the step where data arrives repeatedly from an external, live source. The goal isn't just to write an API call — it's to achieve three properties in the loading process:

- **Idempotency** — running the script multiple times, even for the same day, does not create duplicates and does not cause inconsistency.
- **Fault tolerance** — the external API being unavailable, returning a bad response, or hitting a rate limit does not silently collapse the process; the failure is visible and interpretable.
- **Observability** — it must be possible to reconstruct after the fact when a given run happened, what it did, and with what outcome.

## 2. Scope

**In scope for this project:**
- A single data source (Alpha Vantage), daily-frequency stock price queries
- Manually triggered Python script
- PostgreSQL target system, with upsert logic
- Structured logging and basic error handling

**Explicitly out of scope (left for later projects):**
- Automatic scheduling (cron / Airflow) — that's the topic of project 6, but it's worth writing the script now so a scheduler can trivially call it later (see point 6)
- Cloud storage (S3) — that's project 3 (since implemented, see [`data-lake-design.md`](data-lake-design.md))
- Fine-tuning scaling to multiple tickers/sources — if it comes up, note it in the "Future extension points" section, but it doesn't block the current project

## 3. Architecture Overview

```
Alpha Vantage API  →  Python client (call + validation)  →  Upsert logic  →  PostgreSQL (daily_prices table)
                                                                                        │
                                                              structured logging (at every step)
```

The process is one-directional and batch-oriented: a single run loads data for a given day (or a backfill interval), then stops. There is no long-running process, no in-memory state carried between runs — all state lives in the database, which is what makes idempotency possible.

## 4. Components in Detail

### 4.1 API Client Layer

- Responsibility: issuing the HTTP call, interpreting the response, detecting errors.
- Alpha Vantage **returns 200 OK even on error** — the error is signaled by a `"Note"` or `"Information"` key in the response body, not by the HTTP status code. The client must explicitly check for these keys in every response before passing the data on for processing.
- Rate limit handling: before every call, the script queries the number of successful calls made so far today from the `api_calls` table (see 4.2), and only issues a new call if it's under the limit — we don't wait for the API to reject us.
- **Granularity:** daily (`TIME_SERIES_DAILY`) endpoint only. Introducing intraday (hourly/minute-level) data was a consciously rejected option: the `TIME_SERIES_INTRADAY` endpoint returns one month of data per call, so backfilling a year of history for a single ticker would consume around 12 calls — a significant chunk of the 25-calls-per-day quota. If intraday needs ever arise, that should be a separate project/extension, not part of this project's scope.
- Market calendar: on weekends/holidays, missing data is an **expected state**, not an error. The validation logic must handle this separately from actual errors.
- **Success criterion:** what matters isn't whether the response is syntactically valid JSON/dict — a quota breach or a bad request also returns valid JSON, just with a `"Note"` or `"Information"` key instead of the expected `"Time Series (Daily)"`. So success/failure is decided by the **content** of the response (presence of the expected key), not by its format.

### 4.2 Database Layer — Schema

```sql
CREATE TABLE daily_prices (
    ticker      TEXT NOT NULL,
    trade_date  DATE NOT NULL,
    open        NUMERIC(12,4),
    high        NUMERIC(12,4),
    low         NUMERIC(12,4),
    close       NUMERIC(12,4),
    volume      BIGINT,
    loaded_at   TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (ticker, trade_date)
);
```

The composite `(ticker, trade_date)` primary key is what makes the `INSERT ... ON CONFLICT (ticker, trade_date) DO UPDATE` pattern possible. The `loaded_at` column serves an audit/lineage purpose, not a business one.

**Call log table (`api_calls`)** — the state needed to enforce the daily quota is stored in the same Postgres database, in a dedicated table, not in a separate JSON file:

```sql
CREATE TABLE api_calls (
    id            BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    called_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    ticker        TEXT,
    success       BOOLEAN NOT NULL,
    error_reason  TEXT
);
```

- The PK is a generated identifier, not `called_at` — so even a future finer granularity (e.g. multiple calls within the same second) won't cause a collision.
- `called_at` is of type `timestamptz`: stored internally in UTC, converted to local time only at display time — this is exactly the "local time matters at display, not at storage" principle, applied automatically.
- There's no separate counter column. The count of today's successful calls (`SELECT COUNT(*) FROM api_calls WHERE success AND called_at::date = ...`) is negligible in cost at this data volume (max. 25 rows/day), and it avoids the risk of keeping a counter column in sync.
- The price upsert and the call log entry happen **in the same transaction** — the two are guaranteed to succeed or fail together.

### 4.3 Upsert Logic

Load strategy: **upsert**, not delete-then-insert. The latter would cause race conditions and unnecessary write load in the case of concurrent or interrupted runs.

### 4.4 Error Handling and Logging

- Structured logging via Python's `logging` module (not `print`): timestamp, level, context. This is a human-readable, event-level log — it does not replace, and is not replaced by, the `api_calls` table, which the application logic actually relies on.
- Explicit separation of error types:
  - network error → retry, with exponential backoff
  - rate limit reached → planned stop, not an immediate retry; since project 4 the script then exits with code 1, so a scheduler does not count a run without fresh data as a success (see `serverless-etl-design.md`, section 12)
  - data format error → not retryable, requires human intervention, logged with emphasis
- **Testing during development:** a single, previously saved API response is stored as a JSON file (e.g. under `tests/fixtures/`), so the parsing and upsert logic can be tested without network access or API quota. This is the JSON file's only role in this project — the call counter and log live exclusively in the `api_calls` table; there is no second, parallel state store.

### 4.5 Future Scheduling (preparation, not implementation)

The script is designed to have a single, parameterizable entry point (e.g. date or ticker list as parameters), so a future cron or Airflow task can call it unchanged. This decision costs nothing extra now, but avoids a rewrite when project 6 comes around.

## 5. Key Design Decisions

| Decision | Alternatives | Rationale |
|---|---|---|
| Upsert (`ON CONFLICT`) | delete-then-insert; application-level existence check | Atomic, race-condition-free, a single round trip |
| Composite PK (`ticker`, `trade_date`) | surrogate ID + unique constraint | The business-level uniqueness is expressed directly, no need for a separate unique index |
| Explicit error-type separation | a single generic `except` block | Different errors require different responses (retry vs. stop vs. human intervention) |
| PostgreSQL (not SQLite) | continuing with SQLite | Closer to real-world conditions, also exercises concurrent access and a proper type system |
| Daily granularity (`TIME_SERIES_DAILY`) | Introducing intraday (hourly/minute) data | Intraday backfill would consume ~12 calls/ticker/year, a significant share of the 25-calls-per-day quota; the schema and current principles are built around daily granularity |
| Call counter in its own `api_calls` table | JSON file as state store | Single-source-of-truth principle: the metadata ends up in the database anyway, no need to maintain a parallel state store; this also lets call logging and the price upsert happen in one transaction |
| Generated (`IDENTITY`) primary key on `api_calls` | `called_at` timestamp as PK | The timestamp is an *attribute* of the event, not its *identifier* — concurrent calls would cause a collision |
| Success based on content (expected key present in the response) | Success based on format (valid JSON/dict) | Alpha Vantage also returns valid JSON on a quota breach, just with a different key — format-based checking would produce a false positive |
| Daily counter via aggregation (`COUNT(*)`), not a separate counter column | Denormalized counter column | At a maximum of 25 rows/day the aggregation cost is negligible; a counter column would introduce a sync-risk that nothing at this data volume justifies |

## 6. Non-Functional Requirements

- **Idempotency:** repeated processing of the same input does not change the final result.
- **Fault tolerance:** a transient network error does not cause data loss or an inconsistent state.
- **Observability:** every run is logged with enough detail to reconstruct after the fact what happened.
- **Extensibility:** the code structure should not preclude scheduling, multiple tickers, or other data sources later on.

## 7. Milestones

1. Create the PostgreSQL schema; obtain an Alpha Vantage API key and test it manually (`curl`)
2. API client layer: a single call, with error handling, no database
3. Wire up parsing and upsert logic
4. End-to-end test: first run, then re-run for the same day (idempotency check)

## 8. Risks and Open Questions

- *Closed:* the current Alpha Vantage rate limit has been verified — 25 requests/day on the free tier, with an unlimited quota available for verified open-source/educational projects (not used here; the design assumes the 25/day cap).
- *Risk (accepted):* the exact reset time of the daily limit is not officially documented by Alpha Vantage. The design uses the UTC date in the `called_at` field, which is the industry default; if the actual reset differs by a few hours, the impact would at most be a self-correcting, one-time edge case — not considered a blocking risk.
- *Risk:* if the project later expands to multiple tickers or other data sources, the current simple counting based on the `api_calls` table may not be sufficient (e.g. with per-source quotas) — at that point it would be worth extracting a dedicated "rate limiter" component.

## 9. Future Extension Points

- Project 3 (done): the raw API response also lands in S3, partitioned by run date. See [`data-lake-design.md`](data-lake-design.md).
- Project 6: this script becomes one task in the Airflow DAG.

## 10. Implementation Notes

The code follows this design. A few names changed during implementation, and one planned helper was folded into its caller:

| Design | Implementation (`src/API_call.py`) |
|---|---|
| `call_alpha_vantage` | `fetch_daily_prices` (urllib, 10 s timeout, 3 attempts with exponential backoff) |
| `daily_calls` | `calls_made_today` |
| `is_callable` (pure helper) | not a separate function: the limit check is a single comparison in `collect()` |
| `write_api_data` | `upsert_daily_prices` |
| `validate_call` | `validate_response` |

## 11. Lessons Learned

- The API integration and the database insertion came together faster than expected.
- The harder part was the function structure: deciding how functions depend on and call each other, so that each has a single, clear responsibility.
- Next time more time goes into the design phase: listing every piece the pipeline needs and how they connect before writing code, instead of discovering the structure while coding.
- Upsert semantics and transactions were new. The reasoning is clear now, but they need more practice.
