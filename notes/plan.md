# reckon — monthly multi-source AI usage tracker

> Personal tool. One command, one output: monthly token usage and
> unsubsidized cost across every coding agent + provider the user runs.

---

## 1. Scope

**Three things only:**

1. See token usage across every model the user has touched.
2. Show unsubsidized cost (LiteLLM list prices applied locally; the user's
   actual subscription/bundle discounts are *not* applied — that's the
   point of "unsubsidized").
3. Aggregate by calendar month.

**Sources (6):**

| Source     | Where it lives                                                        | Format            |
|------------|-----------------------------------------------------------------------|-------------------|
| Claude     | `~/.claude/projects/<encoded-cwd>/*.jsonl`                            | JSONL             |
| Codex      | `~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl`                        | JSONL, cumulative |
| Gemini     | `~/.gemini/tmp/<hash>/chats/session-*.json`                           | JSON              |
| Pi         | `~/.pi/agent/sessions/<encoded-cwd>/*.jsonl`                          | JSONL             |
| OpenCode   | `~/.local/share/opencode/opencode.db` (legacy `storage/message/*` fb) | SQLite            |
| OpenRouter | `https://openrouter.ai/api/v1/{credits,activity}`                     | JSON over HTTPS   |

**Not in scope:** daily/session/blocks/live/statusline views, MCP server,
TUI, 5-hour billing windows, anything ccusage has that isn't on the
three-bullet list above.

---

## 2. Output

One command:

```
reckon
```

…prints something like:

```
Month     Source      Model                              In        Out      Cache   Reason     Cost
─────────────────────────────────────────────────────────────────────────────────────────────────────
2026-05   claude      anthropic/claude-opus-4.7    1,204,883   542,113  8,201,442      0    $42.18
2026-05   claude      anthropic/claude-sonnet-4.6    312,001    98,440  1,002,914      0     $4.71
2026-05   codex       openai/gpt-5.2                  84,222    52,883        —    42,118   $9.04
2026-05   pi          anthropic/claude-haiku-4-5     220,001    14,003  3,512,000      0     $0.83
2026-05   openrouter  google/gemini-2.5-pro           41,003    18,200        —      0     $0.46
2026-05   TOTAL                                    1,862,110   725,639 12,716,356  42,118   $57.22
─────────────────────────────────────────────────────────────────────────────────────────────────────
2026-04   claude      anthropic/claude-opus-4.7      902,144   411,002  6,001,000      0    $31.40
...
```

Flags:

- `--month YYYY-MM` — single month instead of all.
- `--since YYYY-MM` / `--until YYYY-MM` — bounded range.
- `--source claude,codex,…` — restrict sources.
- `--by source|model|provider|project` — what to break each month down by
  (default `source,model`).
- `--json` — emit the rows as JSON for piping.
- `--no-color`.

That's the whole CLI. Cost column is unsubsidized USD; "—" means the
source doesn't expose that token class.

---

## 3. Network policy

- **No telemetry, ever.** Nothing reckon does sends data anywhere on the
  user's behalf except…
- **OpenRouter** — only when the user provides a key, only `/credits` and
  `/activity` (the user's own data).
- **LiteLLM pricing auto-refresh** — fetch
  `https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json`
  on first run and every 7 days, cached at `~/.cache/reckon/pricing.json`.
  `--offline` disables the refresh; a vendored snapshot at
  `crates/reckon-core/assets/pricing-fallback.json` is always available
  as fallback so a fresh install with `--offline` still costs correctly
  for the major models.

That's the entire allowed network surface. No analytics, no update checks.

---

## 4. Async runtime: asupersync

No tokio, no anything in the tokio ecosystem (hyper / reqwest / axum /
tower / async-std / smol). Verified by a CI grep over `cargo tree`.

Used parts:

- `asupersync::Runtime::native()` to drive `main`.
- `Cx`, `scope!`, structured concurrency — one task per source, joined at
  the report layer.
- `asupersync::channel::mpsc` — bounded sink, normalised `UsageEvent`s flow
  to the aggregator.
- `asupersync::http::HttpClientBuilder` with `tls` + `tls-webpki-roots` —
  for OpenRouter and pricing refresh.

Top-level shape:

```rust
fn main() -> anyhow::Result<()> {
    let args = Cli::parse();
    let runtime = asupersync::Runtime::native()?;
    runtime.block_on(|cx| reckon::run(cx, args))
}
```

```toml
asupersync = { version = "0.2", features = ["tls", "tls-webpki-roots", "proc-macros"] }
```

---

## 5. Internal data model

Every reader normalises to one struct:

```rust
struct UsageEvent {
    source: Source,                 // Claude | Codex | Gemini | Pi | OpenCode | OpenRouter
    month: YearMonth,               // (i32, u8) — derived from ts in user's local TZ? no: UTC
    model: ModelSlug,               // canonical "vendor/model" form, OpenRouter-style
    provider: String,               // "anthropic", "openai", "google", "openrouter"
    project: Option<String>,
    tokens: TokenCounts,            // input, output, cache_read, cache_write, reasoning
    dedup_key: String,              // per-source unique; see §6
}
```

**Model slug form: OpenRouter `vendor/model`.** A small mapping table in
`reckon-core/src/model_map.rs` folds each source's native identifier onto
the OpenRouter slug:

- Claude `claude-opus-4-7-20251015` → `anthropic/claude-opus-4.7`
- Codex `gpt-5.2-2025-...` → `openai/gpt-5.2`
- Pi `claude-haiku-4-5` + `provider=anthropic` → `anthropic/claude-haiku-4.5`
- OpenRouter rows are already in slug form; use as-is.
- Unknown slugs pass through verbatim; cost defaults to 0 with a warning
  printed once per unknown model.

---

## 6. Per-source reader notes

### Claude
- Parse `assistant` lines where `message.usage` exists.
- Tokens: `input_tokens`, `output_tokens`, `cache_creation_input_tokens`
  (→ `cache_write`), `cache_read_input_tokens` (→ `cache_read`).
- `dedup_key = requestId` (same request can appear in multiple project
  folders).
- Month from `timestamp` (UTC).

### Codex
- Cumulative model. Per-session state machine:
  - On `payload.type == "session_meta"` → reset running totals.
  - On `payload.type == "turn_context"` → switch active `model`.
  - On `payload.type == "token_count"` → delta = current − previous; emit
    one `UsageEvent` for the delta; update previous. Clamp negatives to 0
    (model changes can reset counters).
- Tokens: `input_tokens`, `cached_input_tokens` (→ `cache_read`),
  `output_tokens`, `reasoning_output_tokens` (→ `reasoning`).
- `dedup_key = "{session_id}:{turn_index}"`.
- Sessions whose first `token_count` is missing entirely → skipped (pre
  2025-09-06 builds).

### Gemini
- Read `~/.gemini/tmp/<hash>/chats/session-*.json`; per-message
  `usageMetadata.{promptTokenCount, candidatesTokenCount,
  cachedContentTokenCount, thoughtsTokenCount}` and the message's model.
- Timestamps come from the session filename + per-message offset, or from
  the matching `~/.gemini/tmp/<hash>/logs.json` entry when present.
- Project column shows `<hash[..8]>` truncated. We don't try to demangle
  the hash — `~/.gemini/projects.json` may map it but only sometimes.
- `dedup_key = "{session_filename}:{message_index}"`.

### Pi
- Verified format (see fixture): assistant `message` records carry
  `provider`, `model`, `usage.{input,output,cacheRead,cacheWrite,totalTokens}`,
  and a `usage.cost` block we *ignore* (it bakes in Pi's discounts).
- Read `~/.pi/agent/sessions/session-index.sqlite` to enumerate sessions;
  fall back to a `walkdir` over `sessions/--*-/*.jsonl` if the index is
  missing or older than any of the JSONL files in `sessions/`.
- `dedup_key = "{session_id}:{message_id}"`.
- `provider` is always present on assistant messages — confirmed against
  real data on this machine.

### OpenCode
- Open the SQLite DB at `~/.local/share/opencode/opencode.db` with
  `SQLITE_OPEN_READONLY`. Use a prepared statement that streams rows in
  `created` order with `LIMIT/OFFSET` so the 1.6 GB DB doesn't blow up
  memory.
- If `opencode` is actively writing and the DB is locked, just bail with
  a one-line warning — we'll handle it later.
- Schema (from upstream `drizzle-orm` migrations): pull `tokens_input`,
  `tokens_output`, `tokens_reasoning`, `tokens_cache_read`,
  `tokens_cache_write`, `model_id`, `provider_id`, `created`,
  `session_id`, `id` from `message`.
- `dedup_key = "{message.id}"` (it's a UUID).
- Legacy `storage/message/<sid>/*.json` fallback only kicks in if the DB
  is missing.

### OpenRouter
- HTTPS GET to `/api/v1/activity` once per request, with optional `date`
  filter. We page over the last 30 UTC days (one request per day) to get
  all of them; cheap, ~30 round-trips.
- Auth: read key from (in order) `RECKON_OPENROUTER_KEY`,
  `OPENROUTER_API_KEY`, `~/.config/reckon/config.toml`. **Must be a
  Management key** — `/activity` rejects regular inference keys. On 401
  we print a clear "Management API key required" message with a link to
  https://openrouter.ai/settings/keys.
- Map per-row fields:
  `model` → already slug form; `prompt_tokens` → input;
  `completion_tokens` → output; `reasoning_tokens` → reasoning;
  `usage` → known cost in USD (we *keep* it alongside our computed
  unsubsidized cost — useful for sanity-checking).
- `dedup_key = "openrouter:{date}:{endpoint_id}"`.

---

## 7. Pricing & cost computation

LiteLLM JSON exposes per-model:
`input_cost_per_token`, `output_cost_per_token`,
`cache_read_input_token_cost`, `cache_creation_input_token_cost`,
sometimes `output_cost_per_reasoning_token`.

```
cost_usd =   input        * input_cost_per_token
          +  output       * output_cost_per_token
          +  cache_read   * cache_read_input_token_cost
          +  cache_write  * cache_creation_input_token_cost
          +  reasoning    * (output_cost_per_reasoning_token ?? output_cost_per_token)
```

When the LiteLLM key is missing, the canonical OpenRouter slug is tried
against OpenRouter's `/api/v1/models` snapshot (also vendored). If still
unknown, cost is 0 and the model is reported in a "Unknown pricing for: …"
trailing line.

Refresh: pull LiteLLM JSON on first run and every 7 days. Cache at
`~/.cache/reckon/pricing.json`. `--offline` disables refresh; vendored
fallback at build time guarantees zero-network correctness for major
models.

---

## 8. Cache / index

A small SQLite cache at `~/.cache/reckon/index.sqlite`:

```sql
CREATE TABLE source_files (
    source         TEXT NOT NULL,
    path           TEXT NOT NULL,
    mtime_ns       INTEGER NOT NULL,
    size_bytes     INTEGER NOT NULL,
    last_offset    INTEGER NOT NULL,   -- byte offset for JSONL tail-resume
    PRIMARY KEY (source, path)
);

CREATE TABLE events (
    source         TEXT NOT NULL,
    dedup_key      TEXT NOT NULL,
    month          TEXT NOT NULL,      -- 'YYYY-MM'
    model          TEXT NOT NULL,
    provider       TEXT NOT NULL,
    project        TEXT,
    input          INTEGER NOT NULL DEFAULT 0,
    output         INTEGER NOT NULL DEFAULT 0,
    cache_read     INTEGER NOT NULL DEFAULT 0,
    cache_write    INTEGER NOT NULL DEFAULT 0,
    reasoning      INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (source, dedup_key)
);

CREATE INDEX events_month ON events (month);
```

Cold scan parses everything → bulk inserts. Warm runs `stat()` each file;
if `(mtime, size)` unchanged → skip. JSONL files resume from
`last_offset`. OpenCode SQLite is queried by `created > last_max_created`.
OpenRouter is always re-fetched (the upstream `/activity` window only
goes back 30 days).

Cost is computed at *display* time from `events` × current pricing, not
stored. That way refreshing the pricing snapshot re-prices history
without re-scanning.

---

## 9. Crate layout

```
reckon/
├── Cargo.toml
├── notes/plan.md
├── crates/
│   ├── reckon-core/        # types, model_map, pricing, cost math, cache schema
│   ├── reckon-readers/     # one module per source, all behind a Reader trait
│   ├── reckon-cli/         # clap front-end, monthly aggregator, table renderer
│   └── xtask/              # `cargo xtask vendor-pricing` to refresh fallback snapshot
└── tests/fixtures/         # anonymised real samples, 1-2 per source
```

Reader trait (with asupersync's `Cx`):

```rust
pub trait Reader: Send + Sync {
    fn source(&self) -> Source;
    async fn scan(&self, cx: &Cx, sink: &Sink) -> Outcome<(), ReaderError>;
}
```

`Sink` wraps an `asupersync::channel::mpsc::Sender<UsageEvent>` and
already knows how to upsert into the index.

---

## 10. Testing

- Fixtures: 1–2 anonymised real sessions per source, checked into
  `tests/fixtures/`. Sanitiser script replaces UUIDs and trims content.
- `insta` snapshots of the rendered monthly table for a known fixture set.
- Codex cumulative→delta has its own `proptest` (negative deltas clamp,
  session boundaries reset).
- Cost math has a unit test that pins a few known (tokens, model) pairs
  to expected cents.
- All reader tests run on `asupersync::LabRuntime` for determinism.

---

## 11. Milestones (much shorter now)

| # | Slice                                                          |
|---|----------------------------------------------------------------|
| 1 | Core types, Claude reader, vendored pricing, `reckon` prints one month — matches ccusage to 1¢ on a real week |
| 2 | Pi + Codex readers (cumulative-delta + provider field handling) |
| 3 | Gemini + OpenCode (SQLite) readers                              |
| 4 | OpenRouter `/activity` + 401-message UX                         |
| 5 | Cache/index for warm runs                                       |
| 6 | Pricing auto-refresh + `--offline`                              |
| 7 | `--month`, `--since`, `--until`, `--source`, `--by`, `--json`   |

---

## 12. Decisions captured

- **Open Q1 (monthly only):** confirmed.
- **Open Q2 (model-slug form):** `vendor/model`, OpenRouter style.
- **Open Q3 (OpenCode live writer):** read-only open; if locked, warn and
  move on. No `VACUUM INTO` tmpfile for v1.
- **Open Q4 (Gemini hash demangling):** show first 8 chars of the hash;
  no decode attempt. If `~/.gemini/projects.json` ever lists the hash,
  use it opportunistically.
- **Open Q5 (Pi `provider` field):** verified always present on
  assistant messages (fixture: `claude-haiku-4-5` + `provider: anthropic`).
- **Open Q6 (call home):** never. Only network is OpenRouter (user's own
  data) and LiteLLM pricing auto-refresh (every 7 days, disable with
  `--offline`).
