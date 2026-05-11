# reckon

Monthly token-usage and unsubsidized-cost tracker across every AI coding
agent on your machine — plus OpenRouter.

> **Status:** under construction. The shape below is the design; the
> implementation is being built out under `bn next` / `bn list`. See
> [`notes/plan.md`](notes/plan.md) for the full spec.

## What it does

`reckon` reads the session logs that Claude Code, Codex CLI, Gemini CLI,
Pi, and OpenCode write to disk, fetches your OpenRouter activity, and
prints one table:

- **Monthly aggregation** across all sources.
- **Per-model breakdown** (canonical `vendor/model` slugs).
- **Unsubsidized cost** computed locally from LiteLLM list prices —
  *not* your bundle/subscription rate. Tells you what your usage would
  cost if you paid retail.

That's the whole tool. No daily/session/blocks/statusline/MCP modes.
If you want those, use [`ccusage`](https://github.com/ryoppippi/ccusage).

## Install

```bash
cargo install reckon
# or, from source:
git clone https://github.com/bobisme/reckon
cd reckon && cargo install --path crates/reckon-cli
```

## Quick start

```bash
reckon                          # every month present, newest first
reckon --month 2026-05          # just one month
reckon --since 2026-01          # year to date
reckon --source claude,codex    # restrict sources
reckon --json | jq              # machine-readable
```

Example output:

```
Month     Source      Model                              In        Out      Cache   Reason     Cost
─────────────────────────────────────────────────────────────────────────────────────────────────────
2026-05   claude      anthropic/claude-opus-4.7    1,204,883   542,113  8,201,442      0    $42.18
2026-05   claude      anthropic/claude-sonnet-4.6    312,001    98,440  1,002,914      0     $4.71
2026-05   codex       openai/gpt-5.2                  84,222    52,883        —    42,118   $9.04
2026-05   pi          anthropic/claude-haiku-4-5     220,001    14,003  3,512,000      0     $0.83
2026-05   openrouter  google/gemini-2.5-pro           41,003    18,200        —      0     $0.46
2026-05   TOTAL                                    1,862,110   725,639 12,716,356  42,118   $57.22
```

OpenRouter balance, when a key is configured:

```
OpenRouter balance: $43.17 (used $156.83 of $200.00 purchased)
```

## Sources

| Source       | Where it reads                                                  |
|--------------|-----------------------------------------------------------------|
| Claude Code  | `~/.claude/projects/<encoded-cwd>/*.jsonl`                      |
| Codex CLI    | `~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl`                  |
| Gemini CLI   | `~/.gemini/tmp/<hash>/chats/session-*.json`                     |
| Pi           | `~/.pi/agent/sessions/<encoded-cwd>/*.jsonl` + index SQLite     |
| OpenCode     | `~/.local/share/opencode/opencode.db`                           |
| OpenRouter   | `https://openrouter.ai/api/v1/{activity,credits}` (your data)   |

Sources are auto-detected: if the directory or key isn't present, that
source is silently skipped. Force a subset with `--source`.

## Flags

| Flag                            | Effect                                                                 |
|---------------------------------|------------------------------------------------------------------------|
| `--month YYYY-MM`               | Single month (mutex with `--since`/`--until`)                          |
| `--since YYYY-MM`               | Inclusive lower bound                                                  |
| `--until YYYY-MM`               | Inclusive upper bound                                                  |
| `--source a,b,c`                | Restrict to a comma-separated list of sources                          |
| `--by source,model,...`         | Group-by dimensions (default `source,model`; also `provider`, `project`) |
| `--json`                        | One JSON array on stdout; warnings on stderr                           |
| `--no-color` / `--color=always` | Override the TTY auto-detection                                        |
| `--offline`                     | Skip the weekly pricing refresh; use the vendored snapshot             |
| `--verbose`                     | Log which sources were detected and skipped                            |

## Cost methodology

reckon computes **unsubsidized** USD. That means:

1. Tokens are extracted from the source's own session log.
2. Each model is mapped to a canonical OpenRouter-style slug
   (`anthropic/claude-opus-4.7`, `openai/gpt-5.2`, etc.).
3. List prices come from [LiteLLM's `model_prices_and_context_window.json`](https://github.com/BerriAI/litellm/blob/main/model_prices_and_context_window.json).
4. Cost is `Σ tokens_class × per-token-price`, including separate rates
   for cache reads, cache writes, and reasoning tokens where the model
   exposes them.

OpenRouter rows have a true USD `usage` field; reckon keeps that
alongside the computed unsubsidized number for sanity-checking but
displays the unsubsidized form so all sources are directly comparable.

Pi's pre-computed `usage.cost` block is **ignored** — it bakes in Pi's
own discounts.

## Pricing data lifecycle

- **First run**: uses the LiteLLM snapshot vendored at
  `crates/reckon-core/assets/pricing-fallback.json` (no network).
- **After 7 days**: a background fetch refreshes
  `~/.cache/reckon/pricing.json`. The refresh never blocks rendering;
  if it fails, the previous file (or the vendored fallback) is used.
- **`--offline`**: disables the refresh entirely. New models may price
  at $0 with a warning.
- **`cargo xtask vendor-pricing`**: re-snapshots the vendored fallback
  pre-release.

reckon never sends usage data anywhere. The only network calls are
the pricing fetch (LiteLLM GitHub raw) and the OpenRouter API (your
key, your data). No telemetry, ever.

## OpenRouter setup

The `/activity` endpoint requires a **Management API key**, not a
regular inference key. Create one at
<https://openrouter.ai/settings/keys> in the "Management Keys" section.

Then provide it via any of:

```bash
export RECKON_OPENROUTER_KEY=sk-or-mgmt-...   # preferred
export OPENROUTER_API_KEY=sk-or-mgmt-...
# or ~/.config/reckon/config.toml:
#   [openrouter]
#   key = "sk-or-mgmt-..."
```

Without a key, the OpenRouter source is skipped silently.

## Configuration file (optional)

`~/.config/reckon/config.toml`

```toml
[openrouter]
key = "sk-or-mgmt-..."

[paths]
# Override any source's root. Useful for testing or non-default installs.
claude_home   = "~/.claude"
codex_home    = "~/.codex"
gemini_home   = "~/.gemini"
pi_home       = "~/.pi"
opencode_home = "~/.local/share/opencode"
```

Environment variables (`CLAUDE_HOME`, `CODEX_HOME`, etc.) override the
config file.

## Architecture notes

- **Runtime:** [`asupersync`](https://crates.io/crates/asupersync) for
  structured async (no tokio). One region; one task per source; bounded
  mpsc to a single aggregator.
- **Cache:** `~/.cache/reckon/index.sqlite` stores parsed events keyed
  by `(source, dedup_key)`. Warm runs `stat()` each file and resume
  JSONL tails from `last_offset`. OpenCode uses a `created > last_max`
  cursor. OpenRouter is always re-fetched (rolling 30-day window).
- **Cost** is computed at display time from the events table × current
  pricing, so refreshing the pricing snapshot re-prices history without
  re-scanning logs.
- **Pricing** is loaded from the union of the cached file and the
  vendored fallback, cached taking precedence on key collisions.

See [`notes/plan.md`](notes/plan.md) for the full implementation plan
and milestone breakdown.

## Why "reckon"?

It reckons your spend. And it's short.

## License

TBD.
