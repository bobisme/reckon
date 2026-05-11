# reckon

Project type: cli
Tools: `bones`, `maw`, `seal`, `rite`, `vessel`

<!-- Add project-specific context below: architecture, conventions, key files, etc. -->

## About reckon

reckon is a personal CLI that reads local session logs from every AI
coding agent on this machine (Claude Code, Codex CLI, Gemini CLI, Pi,
OpenCode) plus OpenRouter's HTTP API, normalises everything into a
single `UsageEvent`, and prints one monthly table with token counts and
**unsubsidized** USD cost (LiteLLM list prices applied locally — not
the user's actual subscription/bundle rates).

**Scope is deliberately narrow.** Three features only:
1. Token usage per model.
2. Unsubsidized cost.
3. Monthly aggregation.

We do **not** clone the rest of ccusage (no daily/session/blocks/live
modes, no statusline, no MCP). If you find yourself adding one of
those, stop and check with the user first.

### Architecture invariants

- **Runtime:** `asupersync` (no tokio, no hyper, no reqwest, no
  tower/axum/async-std/smol). CI greps `cargo tree` to enforce this.
- **One canonical type:** every Reader emits `UsageEvent` (see
  `crates/reckon-core`). Aggregation and rendering are downstream.
- **Network policy:** the only outbound traffic is (a) OpenRouter API
  with the user's own Management key, and (b) LiteLLM pricing JSON,
  refreshed weekly and skippable with `--offline`. No telemetry, ever.
- **Cost at display time, not at write time.** The events table never
  stores USD — refreshing the pricing snapshot re-prices history for
  free.
- **Vendored pricing fallback:** `crates/reckon-core/assets/pricing-fallback.json`
  is checked in so a fresh clone with `--offline` produces correct
  numbers for major models.

### Key files & locations

- `notes/plan.md` — design bible. Source paths, field maps, dedup
  keys, milestone list. Treat as authoritative when bones disagree.
- `README.md` — user-facing description of how the tool will work.
- `crates/reckon-core/` — types, model_map, pricing, cost math, cache.
- `crates/reckon-readers/` — one module per source (claude/codex/
  gemini/pi/opencode/openrouter). Each implements the `Reader` trait.
- `crates/reckon-cli/` — clap front-end, aggregator, table renderer.
- `crates/xtask/` — `cargo xtask vendor-pricing` and other maintenance.

### Working on this project

Use bones. The pile is fully spec'd: `bn list` shows 41 open, `bn next`
returns the single ready bone. Every task bone has a **Scope** section
(specific files/types/field maps) and an **Acceptance** section
(concrete pass/fail criteria). Don't widen the scope of a bone without
asking — `notes/plan.md` is the contract.

Source-of-truth for what's in scope, in priority order:
1. The user's direct instructions in chat.
2. `notes/plan.md`.
3. The Scope/Acceptance of the bone you're on.

Anything not in those three is out of scope for v1.


<!-- edict:managed-start -->## Edict Workflow

### How to Make Changes

1. **Create a bone** to track your work: `maw exec default -- bn create --title "..." --description "..."`
2. **Create a workspace** for your changes: `maw ws create <bone-id> --from main --description "<bone-title>"` — use the bone ID as workspace name; this gives you `ws/<bone-id>/`
3. **Edit files in your workspace** (`ws/<name>/`), never in `ws/default/`
4. **Merge when done**: `maw ws merge <name> --into default --destroy --message "feat: <bone-title>"` (use conventional commit prefix: `feat:`, `fix:`, `chore:`, etc.; swap `default` for a change id when merging back into a tracked change)
5. **Close the bone**: `maw exec default -- bn done <id>`

Do not create git branches manually — `maw ws create` handles branching for you. See [worker-loop.md](.agents/edict/worker-loop.md) for the full triage → start → work → finish cycle.

**All tools have `--help`** with usage examples. When unsure, run `<tool> --help` or `<tool> <command> --help`.

### Directory Structure (maw v2)

This project uses a **bare repo** layout. Source files live in workspaces under `ws/`, not at the project root.

```
project-root/          ← bare repo (no source files here)
├── ws/
│   ├── default/       ← main working copy (AGENTS.md, .bones/, src/, etc.)
│   ├── bn-1abc/       ← agent workspace (named after bone ID)
│   └── bn-2def/       ← another agent workspace
├── .manifold/         ← maw metadata/artifacts
├── .git/              ← git data (core.bare=true)
└── AGENTS.md          ← stub redirecting to ws/default/AGENTS.md
```

**Key rules:**
- `ws/default/` is the main workspace — bones, config, and project files live here
- **Never merge or destroy the default workspace.** It is where other branches merge INTO, not something you merge.
- Agent workspaces (`ws/<name>/`) are isolated Git worktrees managed by maw
- Use `maw exec <ws> -- <command>` to run commands in a workspace context
- Use `maw exec default -- bn ...` for bones commands (always in default workspace)
- Use `maw exec <ws> -- seal ...` for review commands (always in the review's workspace)
- Never run `bn` or `seal` directly — always go through `maw exec`

### Bones Quick Reference

| Operation | Command |
|-----------|---------|
| Triage (scores) | `maw exec default -- bn triage` |
| Next bone | `maw exec default -- bn next` |
| Next N bones | `maw exec default -- bn next N` (e.g., `bn next 4` for dispatch) |
| Show bone | `maw exec default -- bn show <id>` |
| Create | `maw exec default -- bn create --title "..." --description "..."` |
| Start work | `maw exec default -- bn do <id>` |
| Add comment | `maw exec default -- bn bone comment add <id> "message"` |
| Close | `maw exec default -- bn done <id>` |
| Add dependency | `maw exec default -- bn triage dep add <blocker> --blocks <blocked>` |
| Search | `maw exec default -- bn search <query>` |

Identity resolved from `$AGENT` env. No flags needed in agent loops.

### Workspace Quick Reference

| Operation | Command |
|-----------|---------|
| Create workspace | `maw ws create <bone-id> --from main --description "<title>"` |
| List workspaces | `maw ws list` |
| Check merge readiness | `maw ws merge <name> --into default --check` |
| Merge to main | `maw ws merge <name> --into default --destroy --message "feat: <bone-title>"` |
| Destroy (no merge) | `maw ws destroy <name>` |
| Run command in workspace | `maw exec <name> -- <command>` |
| Diff workspace vs epoch | `maw ws diff <name>` |
| Check workspace overlap | `maw ws overlap <name1> <name2>` |
| View workspace history | `maw ws history <name>` |
| Sync stale workspace | `maw ws sync <name>` |
| Inspect merge conflicts | `maw ws conflicts <name>` |
| Undo local workspace changes | `maw ws undo <name>` |
| List recovery snapshots | `maw ws recover` |
| Recover destroyed workspace | `maw ws recover <name> --to <new-name>` |
| Search recovery snapshots | `maw ws recover --search <pattern>` |
| Show file from snapshot | `maw ws recover <name> --show <path>` |

**Inspecting a workspace:**
```bash
maw exec <name> -- git status             # what changed (unstaged)
maw exec <name> -- git log --oneline -5   # recent commits
maw ws diff <name>                        # diff vs epoch (maw-native)
```

**Lead agent merge workflow** — after a worker finishes a bone:
1. `maw ws list` — look for `active (+N to merge)` entries
2. `maw ws merge <name> --into default --check` — verify no conflicts
3. `maw ws merge <name> --into default --destroy --message "feat: <bone-title>"` — merge and clean up (use conventional commit prefix)

**Workspace safety:**
- Never merge or destroy `default`.
- Always `maw ws merge <name> --into default --check` before `--destroy`.
- Commit workspace changes with `maw exec <name> -- git add -A && maw exec <name> -- git commit -m "..."`.
- **No work is ever lost in maw.** Recovery snapshots are created automatically on every destroy. If a workspace was destroyed and you suspect code is missing, ALWAYS run `maw ws recover` before concluding work was lost. Never reopen a bone or start over without checking recovery first.

### Protocol Quick Reference

Use these commands at protocol transitions to check state and get exact guidance. Each command outputs instructions for the next steps.

| Step | Command | Who | Purpose |
|------|---------|-----|---------|
| Resume | `edict protocol resume --agent $AGENT` | Worker | Detect in-progress work from previous session |
| Start | `edict protocol start <bone-id> --agent $AGENT` | Worker | Verify bone is ready, get start commands |
| Review | `edict protocol review <bone-id> --agent $AGENT` | Worker | Verify work is complete, get review commands |
| Finish | `edict protocol finish <bone-id> --agent $AGENT` | Worker | Verify review approved, get close/cleanup commands |
| Merge | `edict protocol merge <workspace> --agent $AGENT` | Lead | Check preconditions, detect conflicts, get merge steps |
| Cleanup | `edict protocol cleanup --agent $AGENT` | Worker | Check for held resources to release |

All commands support JSON output with `--format json` for parsing. If a command is unavailable or fails (exit code 1), fall back to manual steps documented in [start](.agents/edict/start.md), [review-request](.agents/edict/review-request.md), and [finish](.agents/edict/finish.md).

### Bones Conventions

- Create a bone before starting work. Update state: `open` → `doing` → `done`.
- Post progress comments during work for crash recovery.
- **Run checks before committing**: `just check`. Fix any failures before proceeding.
- After finishing a bone, follow [finish.md](.agents/edict/finish.md). **Workers: do NOT push** — the lead handles merges and pushes.
- **Install locally** after releasing: `just install`
### Identity

Your agent name is set by the hook or script that launched you. Use `$AGENT` in commands.
For manual sessions, use `<project>-dev` (e.g., `myapp-dev`).

### Claims

When working on a bone, stake claims to prevent conflicts:

```bash
rite claims stake --agent $AGENT "bone://<project>/<id>" -m "<id>"
rite claims stake --agent $AGENT "workspace://<project>/<ws>" -m "<id>"
rite claims release --agent $AGENT --all  # when done
```

### Reviews

Use `@<project>-<role>` mentions to request reviews:

```bash
maw exec $WS -- seal reviews request <review-id> --reviewers $PROJECT-security --agent $AGENT
rite send --agent $AGENT $PROJECT "Review requested: <review-id> @$PROJECT-security" -L review-request
```

The @mention triggers the auto-spawn hook for the reviewer.

### Bus Communication

Agents communicate via rite channels. You don't need to be expert on everything — ask the right project.

| Operation | Command |
|-----------|---------|
| Send message | `rite send --agent $AGENT <channel> "message" [-L label]` |
| Check inbox | `rite inbox --agent $AGENT --channels <ch> [--mark-read]` |
| Wait for reply | `rite wait -c <channel> --mention -t 120` |
| Browse history | `rite history <channel> -n 20` |
| Search messages | `rite search "query" -c <channel>` |

**Conversations**: After sending a question, use `rite wait -c <channel> --mention -t <seconds>` to block until the other agent replies. This enables back-and-forth conversations across channels.

**Project experts**: Each `<project>-dev` is the expert on their project. When stuck on a companion tool (rite, maw, seal, vessel, bn), post a question to its project channel instead of guessing.

### Cross-Project Communication

**Don't suffer in silence.** If a tool confuses you or behaves unexpectedly, post to its project channel.

1. Find the project: `rite history projects -n 50` (the #projects channel has project registry entries)
2. Post question or feedback: `rite send --agent $AGENT <project> "..." -L feedback`
3. For bugs, create bones in their repo first
4. **Always create a local tracking bone** so you check back later:
   ```bash
   maw exec default -- bn create --title "[tracking] <summary>" --tag tracking --kind task
   ```

See [cross-channel.md](.agents/edict/cross-channel.md) for the full workflow.

### Session Search (optional)

Use `cass search "error or problem"` to find how similar issues were solved in past sessions.


### Design Guidelines


- [CLI tool design for humans, agents, and machines](.agents/edict/design/cli-conventions.md)



### Workflow Docs


- [Find work from inbox and bones](.agents/edict/triage.md)

- [Claim bone, create workspace, announce](.agents/edict/start.md)

- [Change bone state (open/doing/done)](.agents/edict/update.md)

- [Close bone, merge workspace, release claims](.agents/edict/finish.md)

- [Full triage-work-finish lifecycle](.agents/edict/worker-loop.md)

- [Turn specs/PRDs into actionable bones](.agents/edict/planning.md)

- [Explore unfamiliar code before planning](.agents/edict/scout.md)

- [Create and validate proposals before implementation](.agents/edict/proposal.md)

- [Request a review](.agents/edict/review-request.md)

- [Handle reviewer feedback (fix/address/defer)](.agents/edict/review-response.md)

- [Reviewer agent loop](.agents/edict/review-loop.md)

- [Merge a worker workspace (protocol merge + conflict recovery)](.agents/edict/merge-check.md)

- [Validate toolchain health](.agents/edict/preflight.md)

- [Ask questions, report bugs, and track responses across projects](.agents/edict/cross-channel.md)

- [Report bugs/features to other projects](.agents/edict/report-issue.md)

- [groom](.agents/edict/groom.md)

<!-- edict:managed-end -->
