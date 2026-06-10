mod pricing_refresh;
mod render;
mod report;

use std::collections::HashSet;
use std::env;
use std::fmt::Write as _;
use std::io::{self, IsTerminal};
use std::path::PathBuf;

use asupersync::Cx;
use clap::{Parser, Subcommand, ValueEnum};
use reckon_core::{
    is_pricing_cache_stale, load_pricing_fallback, load_pricing_from_cache, resolve_tz, ModelSlug,
    Source, YearMonth,
};
use reckon_readers::claude::ClaudeReader;
use reckon_readers::codex::CodexReader;
use reckon_readers::gemini::GeminiReader;
use reckon_readers::opencode::OpenCodeReader;
use reckon_readers::openrouter::{self, OpenRouterReader};
use reckon_readers::pi::PiReader;
use reckon_readers::{run_readers_with_cache, Reader};

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
enum ColorMode {
    Auto,
    Always,
}

use report::BySpec;

#[derive(Parser)]
#[command(name = "reckon")]
#[command(about = "Monthly AI usage tracker with unsubsidized cost breakdown")]
#[command(long_about = None)]
// These bools are independent CLI flags parsed by clap, not a state machine;
// collapsing them into enums would obscure the flag surface, not clarify it.
#[allow(clippy::struct_excessive_bools)]
struct Cli {
    /// Maintenance subcommands. Omit to print the usage report (the default).
    #[command(subcommand)]
    command: Option<Command>,

    /// Disable automatic pricing refresh from `LiteLLM`
    ///
    /// When set, pricing loads from ~/.cache/reckon/pricing.json if present,
    /// otherwise falls back to the vendored snapshot. No network requests are made.
    /// Note: newer models may be priced at $0 if not in cache or fallback.
    #[arg(long, global = true)]
    offline: bool,

    /// Output as JSON
    #[arg(long)]
    json: bool,

    /// Restrict which Readers run (comma-separated)
    ///
    /// Valid values: claude, codex, gemini, opencode, openrouter, pi.
    /// Default: each source whose data directory exists on disk (or for `OpenRouter`, whose key resolves).
    #[arg(long, value_delimiter = ',')]
    source: Option<Vec<String>>,

    /// Force color output even when output is not a terminal.
    #[arg(long, value_enum, default_value_t = ColorMode::Auto)]
    color: ColorMode,

    /// Disable ANSI color output.
    #[arg(long, conflicts_with = "color")]
    no_color: bool,

    /// Comma-separated breakdown dimensions: source, model, provider, project.
    ///
    /// Controls which columns appear in the output. Month is always implicit.
    /// If omitted: defaults to no breakdown (a compact Month/Total/Cost table)
    /// unless `--source` is passed, in which case it defaults to "source,model".
    #[arg(long, value_parser = parse_by_spec)]
    by: Option<BySpec>,

    /// Show only a single month (YYYY-MM)
    #[arg(long, conflicts_with_all = ["since", "until"])]
    month: Option<YearMonth>,

    /// Start of date range, inclusive (YYYY-MM)
    #[arg(long)]
    since: Option<YearMonth>,

    /// End of date range, inclusive (YYYY-MM)
    #[arg(long)]
    until: Option<YearMonth>,

    /// Time zone used for month bucketing.
    ///
    /// Accepts `local` (default; reads the system zone), `utc`, or any IANA
    /// name (e.g., `America/New_York`). `local` matches ccusage's bucketing on
    /// this machine; pass `utc` to reproduce reckon's pre-bn-wyu behavior.
    #[arg(long, default_value = "local")]
    tz: String,

    /// Show every per-category token column (In, Out, Cache Wr, Cache Rd,
    /// Reasoning). Default hides them and shows only the Total column.
    #[arg(long)]
    all_columns: bool,
}

#[derive(Subcommand)]
enum Command {
    /// Force-refresh the `LiteLLM` pricing cache now, ignoring its staleness.
    ///
    /// Fetches synchronously, overwrites ~/.cache/reckon/pricing.json, and
    /// prints how many models were written. Unlike the report path's
    /// background refresh, the new prices are on disk before this returns.
    Refresh,
}

fn parse_by_spec(s: &str) -> Result<BySpec, String> {
    BySpec::parse(s)
}

/// Re-bucket each event's `month` from its `timestamp_secs` under the active
/// TZ. Events whose `timestamp_secs` is 0 (legacy cache rows persisted before
/// schema v3) are left at their stored UTC month so they don't all collapse to
/// 1970-01.
fn rebucket_events(events: &mut [reckon_core::UsageEvent], tz: &jiff::tz::TimeZone) {
    for event in events {
        if event.timestamp_secs != 0 {
            event.month = YearMonth::from_timestamp(event.timestamp_secs, tz);
        }
    }
}

#[derive(Debug)]
struct MonthFilter {
    since: YearMonth,
    until: YearMonth,
}

impl MonthFilter {
    fn matches(&self, month: YearMonth) -> bool {
        month >= self.since && month <= self.until
    }
}

fn resolve_month_filter(
    month: Option<YearMonth>,
    since: Option<YearMonth>,
    until: Option<YearMonth>,
) -> Result<Option<MonthFilter>, String> {
    if let Some(m) = month {
        return Ok(Some(MonthFilter { since: m, until: m }));
    }
    match (since, until) {
        (None, None) => Ok(None),
        (Some(s), Some(u)) => {
            if s > u {
                Err("--since must be <= --until".into())
            } else {
                Ok(Some(MonthFilter { since: s, until: u }))
            }
        }
        (Some(s), None) => Ok(Some(MonthFilter {
            since: s,
            until: YearMonth::new(9999, 12),
        })),
        (None, Some(u)) => Ok(Some(MonthFilter {
            since: YearMonth::new(1, 1),
            until: u,
        })),
    }
}

const ANSI_COLOR_RESET: &str = "\x1b[0m";
const ANSI_YELLOW: &str = "\x1b[33m";

fn cache_path() -> PathBuf {
    let base = env::var("XDG_CACHE_HOME").map_or_else(
        |_| {
            let mut p = PathBuf::from(env::var("HOME").expect("HOME not set"));
            p.push(".cache");
            p
        },
        PathBuf::from,
    );
    base.join("reckon").join("index.sqlite")
}

fn pricing_cache_path() -> PathBuf {
    let home = env::var("HOME").unwrap_or_else(|_| ".".to_string());
    PathBuf::from(home).join(".cache/reckon/pricing.json")
}

fn home_dir() -> PathBuf {
    env::var("HOME").map_or_else(|_| PathBuf::from("."), PathBuf::from)
}

/// Synchronously fetch `LiteLLM` pricing and overwrite the cache, then report the
/// model count. Refreshing is inherently online, so `--offline` is rejected
/// rather than silently no-op'd.
fn run_refresh(offline: bool) -> Result<(), Box<dyn std::error::Error>> {
    if offline {
        eprintln!("error: `reckon refresh` needs network access; drop --offline");
        std::process::exit(2);
    }

    let path = pricing_cache_path();
    let count = pricing_refresh::fetch_and_cache_pricing(&path)?;
    println!("Refreshed pricing: {count} models -> {}", path.display());
    Ok(())
}

fn source_is_available(source: Source) -> bool {
    let home = home_dir();
    match source {
        Source::Claude => {
            let path = env::var("CLAUDE_HOME")
                .map_or_else(|_| home.join(".claude"), PathBuf::from);
            path.exists()
        }
        Source::Codex => {
            let path = home.join(".codex");
            path.exists()
        }
        Source::Gemini => {
            let path = home.join(".gemini");
            path.exists()
        }
        Source::Pi => {
            let path = home.join(".pi");
            path.exists()
        }
        Source::OpenCode => {
            let path = home.join(".local/share/opencode/opencode.db");
            path.exists()
        }
        Source::OpenRouter => {
            openrouter::resolve_key().is_some()
        }
    }
}

fn parse_source(name: &str) -> Result<Source, String> {
    match name.to_lowercase().as_str() {
        "claude" => Ok(Source::Claude),
        "codex" => Ok(Source::Codex),
        "gemini" => Ok(Source::Gemini),
        "opencode" => Ok(Source::OpenCode),
        "openrouter" => Ok(Source::OpenRouter),
        "pi" => Ok(Source::Pi),
        _ => Err(name.to_string()),
    }
}

fn format_source_error(unknown: &str) -> String {
    let valid = ["claude", "codex", "gemini", "opencode", "openrouter", "pi"];
    format!(
        "unknown source: {} (valid: {})",
        unknown,
        valid.join(", ")
    )
}

fn format_unknown_model_warning(models: &HashSet<ModelSlug>) -> String {
    let mut slugs: Vec<String> = models
        .iter()
        .map(|model| model.as_str().to_string())
        .collect();
    slugs.sort_unstable();

    let listed_count = slugs.len().min(10);
    let mut message = format!(
        "warning: priced at $0 (no pricing data): {}",
        slugs
            .iter()
            .take(listed_count)
            .cloned()
            .collect::<Vec<_>>()
            .join(", "),
    );

    if slugs.len() > 10 {
        let _ = write!(message, " (and {} more)", slugs.len() - 10);
    }

    message
}

const fn should_use_color(
    is_tty: bool,
    color: ColorMode,
    no_color_flag: bool,
    no_color_set: bool,
) -> bool {
    if no_color_flag {
        return false;
    }

    match color {
        ColorMode::Always => true,
        ColorMode::Auto => is_tty && !no_color_set,
    }
}

fn colorize(text: &str, ansi_code: &str, enabled: bool) -> String {
    if enabled {
        format!("{ansi_code}{text}{ANSI_COLOR_RESET}")
    } else {
        text.to_string()
    }
}

fn color_warning(text: &str, use_color: bool) -> String {
    colorize(text, ANSI_YELLOW, use_color)
}

// Length is dominated by the inline runtime-setup + reader-wiring async block;
// it reads as one linear pipeline, so splitting it would add indirection
// without improving clarity.
#[allow(clippy::too_many_lines)]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();

    if matches!(args.command, Some(Command::Refresh)) {
        return run_refresh(args.offline);
    }

    let month_filter = match resolve_month_filter(args.month, args.since, args.until) {
        Ok(f) => f,
        Err(msg) => {
            eprintln!("error: {msg}");
            std::process::exit(1);
        }
    };

    let tz = match resolve_tz(&args.tz) {
        Ok(tz) => tz,
        Err(error) => {
            eprintln!("error: {error}");
            std::process::exit(1);
        }
    };

    let requested_sources = args.source.as_ref().map_or_else(
        || {
            let all_sources = [
                Source::Claude,
                Source::Codex,
                Source::Gemini,
                Source::OpenCode,
                Source::OpenRouter,
                Source::Pi,
            ];
            all_sources
                .iter()
                .filter(|s| source_is_available(**s))
                .copied()
                .collect()
        },
        |source_names| {
            let mut sources = Vec::new();
            for name in source_names {
                match parse_source(name) {
                    Ok(source) => sources.push(source),
                    Err(unknown) => {
                        eprintln!("{}", format_source_error(&unknown));
                        std::process::exit(1);
                    }
                }
            }
            sources
        },
    );

    let runtime = asupersync::runtime::RuntimeBuilder::new().build()?;
    let handle = runtime.handle();
    let join = handle.spawn(async move {
        let cx = Cx::current().expect("no async context");

        let pricing_path = pricing_cache_path();
        let pricing = load_pricing_from_cache(&pricing_path).unwrap_or_else(load_pricing_fallback);

        let use_color = should_use_color(
            io::stdout().is_terminal(),
            args.color,
            args.no_color,
            env::var("NO_COLOR").is_ok(),
        );

        if !args.offline && is_pricing_cache_stale(&pricing_path) {
            let path_for_fetch = pricing_path.clone();
            let _refresh_task = std::thread::spawn(move || {
                if let Err(e) = pricing_refresh::fetch_and_cache_pricing(&path_for_fetch) {
                    eprintln!(
                        "{}",
                        color_warning(
                            &format!("Warning: failed to refresh pricing cache: {e}"),
                            use_color
                        )
                    );
                }
            });
        }

        let requested_sources_clone: Vec<Source> = requested_sources.clone();
        let mut readers: Vec<Box<dyn Reader>> = Vec::new();
        for source in requested_sources {
            match source {
                Source::Claude => readers.push(Box::new(ClaudeReader::new())),
                Source::Codex => readers.push(Box::new(CodexReader::new())),
                Source::Gemini => readers.push(Box::new(GeminiReader::new())),
                Source::OpenCode => readers.push(Box::new(OpenCodeReader::new())),
                Source::OpenRouter => readers.push(Box::new(OpenRouterReader::new())),
                Source::Pi => readers.push(Box::new(PiReader::new())),
            }
        }

        let mut events = run_readers_with_cache(&cx, readers, &cache_path()).await;

        rebucket_events(&mut events, &tz);

        // When --source is explicit, restrict the display to those sources
        // too. Without this, cache-replayed events from prior full scans of
        // other sources would still appear, defeating the filter.
        if args.source.is_some() {
            let allowed: std::collections::HashSet<Source> =
                requested_sources_clone.iter().copied().collect();
            events.retain(|e| allowed.contains(&e.source));
        }

        if let Some(ref filter) = month_filter {
            events.retain(|e| filter.matches(e.month));
        }

        let balance = openrouter::fetch_balance().ok().flatten();

        if events.is_empty() {
            println!("No usage events found.");
            return;
        }

        // `--by` defaults to the full breakdown only when the user is also
        // filtering by source (their intent is "show me what's inside that
        // source"). Otherwise default to no breakdown — a compact monthly
        // summary table — which the user can override explicitly with `--by`.
        let by = args.by.clone().unwrap_or_else(|| {
            if args.source.is_some() {
                report::BySpec::default()
            } else {
                report::BySpec(Vec::new())
            }
        });

        let aggregated = report::aggregate(&events, &by);
        let costs = report::aggregate_cost(&events, &by, &pricing);
        let unknown_models = report::unknown_model_slugs(&aggregated, &pricing);
        if args.json {
            render::print_json(&events, &pricing, balance.as_ref(), &by);
        } else {
            render::print_table(&aggregated, &costs, balance.as_ref(), use_color, &by, args.all_columns);
        }

        if !unknown_models.is_empty() {
            eprintln!(
                "{}",
                color_warning(&format_unknown_model_warning(&unknown_models), use_color)
            );
        }
    });
    runtime.block_on(join);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unknown_model_warning_is_single_and_sorted() {
        let mut models = HashSet::new();
        models.insert(ModelSlug::new("zeta/model"));
        models.insert(ModelSlug::new("alpha/model"));
        models.insert(ModelSlug::new("beta/model"));

        assert_eq!(
            format_unknown_model_warning(&models),
            "warning: priced at $0 (no pricing data): alpha/model, beta/model, zeta/model"
        );
    }

    #[test]
    fn unknown_model_warning_caps_at_ten_items_and_includes_remainder_count() {
        let mut models = HashSet::new();
        for i in 1..=12 {
            models.insert(ModelSlug::new(format!("vendor/model-{i:02}")));
        }

        assert_eq!(
            format_unknown_model_warning(&models),
            "warning: priced at $0 (no pricing data): vendor/model-01, vendor/model-02, vendor/model-03, vendor/model-04, vendor/model-05, vendor/model-06, vendor/model-07, vendor/model-08, vendor/model-09, vendor/model-10 (and 2 more)"
        );
    }

    #[test]
    fn parse_source_accepts_valid_sources() {
        assert_eq!(parse_source("claude").unwrap(), Source::Claude);
        assert_eq!(parse_source("codex").unwrap(), Source::Codex);
        assert_eq!(parse_source("gemini").unwrap(), Source::Gemini);
        assert_eq!(parse_source("opencode").unwrap(), Source::OpenCode);
        assert_eq!(parse_source("openrouter").unwrap(), Source::OpenRouter);
        assert_eq!(parse_source("pi").unwrap(), Source::Pi);
    }

    #[test]
    fn parse_source_is_case_insensitive() {
        assert_eq!(parse_source("Claude").unwrap(), Source::Claude);
        assert_eq!(parse_source("CODEX").unwrap(), Source::Codex);
        assert_eq!(parse_source("GeMiNi").unwrap(), Source::Gemini);
    }

    #[test]
    fn parse_source_rejects_unknown_sources() {
        assert!(parse_source("foo").is_err());
        assert!(parse_source("unknown").is_err());
        assert!(parse_source("openai").is_err());
    }

    #[test]
    fn format_source_error_lists_all_valid_sources() {
        let error = format_source_error("foo");
        assert!(error.contains("unknown source: foo"));
        assert!(error.contains("claude"));
        assert!(error.contains("codex"));
        assert!(error.contains("gemini"));
        assert!(error.contains("opencode"));
        assert!(error.contains("openrouter"));
        assert!(error.contains("pi"));
    }

    #[test]
    fn should_use_color_respects_tty_and_no_color_flag() {
        assert!(should_use_color(true, ColorMode::Auto, false, false));
        assert!(!should_use_color(false, ColorMode::Auto, false, false));
        assert!(!should_use_color(true, ColorMode::Always, true, false));
        assert!(should_use_color(false, ColorMode::Always, false, false));
        assert!(!should_use_color(true, ColorMode::Auto, false, true));
    }

    #[test]
    fn should_use_color_preserves_manual_override() {
        assert!(should_use_color(false, ColorMode::Always, false, true));
        assert!(!should_use_color(false, ColorMode::Always, true, false));
    }

    #[test]
    fn parse_by_spec_invalid_exits_with_error() {
        assert!(parse_by_spec("foo").is_err());
        assert!(parse_by_spec("source,foo").is_err());
    }

    #[test]
    fn parse_by_spec_valid_cases() {
        assert!(parse_by_spec("source").is_ok());
        assert!(parse_by_spec("model").is_ok());
        assert!(parse_by_spec("source,model").is_ok());
        assert!(parse_by_spec("source,model,project").is_ok());
        assert!(parse_by_spec("source,model,provider,project").is_ok());
    }

    #[test]
    fn yearmonth_from_str_valid() {
        let ym: YearMonth = "2026-05".parse().unwrap();
        assert_eq!(ym.year, 2026);
        assert_eq!(ym.month, 5);
    }

    #[test]
    fn yearmonth_from_str_january() {
        let ym: YearMonth = "2025-01".parse().unwrap();
        assert_eq!(ym.year, 2025);
        assert_eq!(ym.month, 1);
    }

    #[test]
    fn yearmonth_from_str_invalid_format() {
        assert!("2026".parse::<YearMonth>().is_err());
        assert!("not-a-date".parse::<YearMonth>().is_err());
        assert!("2026-13".parse::<YearMonth>().is_err());
        assert!("2026-00".parse::<YearMonth>().is_err());
    }

    #[test]
    fn month_filter_single_month() {
        let f = resolve_month_filter(
            Some(YearMonth::new(2026, 5)),
            None,
            None,
        )
        .unwrap()
        .unwrap();
        assert!(f.matches(YearMonth::new(2026, 5)));
        assert!(!f.matches(YearMonth::new(2026, 4)));
        assert!(!f.matches(YearMonth::new(2026, 6)));
    }

    #[test]
    fn month_filter_range() {
        let f = resolve_month_filter(
            None,
            Some(YearMonth::new(2026, 1)),
            Some(YearMonth::new(2026, 4)),
        )
        .unwrap()
        .unwrap();
        assert!(f.matches(YearMonth::new(2026, 1)));
        assert!(f.matches(YearMonth::new(2026, 2)));
        assert!(f.matches(YearMonth::new(2026, 4)));
        assert!(!f.matches(YearMonth::new(2025, 12)));
        assert!(!f.matches(YearMonth::new(2026, 5)));
    }

    #[test]
    fn month_filter_since_only() {
        let f = resolve_month_filter(
            None,
            Some(YearMonth::new(2026, 3)),
            None,
        )
        .unwrap()
        .unwrap();
        assert!(!f.matches(YearMonth::new(2026, 2)));
        assert!(f.matches(YearMonth::new(2026, 3)));
        assert!(f.matches(YearMonth::new(2030, 12)));
    }

    #[test]
    fn month_filter_until_only() {
        let f = resolve_month_filter(
            None,
            None,
            Some(YearMonth::new(2026, 6)),
        )
        .unwrap()
        .unwrap();
        assert!(f.matches(YearMonth::new(2020, 1)));
        assert!(f.matches(YearMonth::new(2026, 6)));
        assert!(!f.matches(YearMonth::new(2026, 7)));
    }

    #[test]
    fn month_filter_none_means_all() {
        assert!(resolve_month_filter(None, None, None).unwrap().is_none());
    }

    #[test]
    fn month_filter_since_after_until_errors() {
        let result = resolve_month_filter(
            None,
            Some(YearMonth::new(2026, 4)),
            Some(YearMonth::new(2026, 1)),
        );
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("--since must be <= --until"));
    }

    fn synth_event(dedup: &str, ts_secs: i64) -> reckon_core::UsageEvent {
        reckon_core::UsageEvent {
            source: Source::Claude,
            month: YearMonth::from_utc(ts_secs),
            timestamp_secs: ts_secs,
            model: ModelSlug::new("anthropic/claude-opus-4.7"),
            provider: "anthropic".into(),
            project: None,
            tokens: reckon_core::TokenCounts {
                input: 10,
                output: 1,
                cache_read: 0,
                cache_write: 0,
                reasoning: 0,
            },
            dedup_key: dedup.into(),
            known_cost_usd: None,
            byok_usage_inference: None,
        }
    }

    /// Integration anchor for bn-wyu: a UTC May-1 03:00 event re-buckets to
    /// April under America/Los_Angeles (UTC-7 PDT) and stays in May under
    /// utc. Mirrors the user-visible smoke test in the bone description.
    #[test]
    fn rebucket_events_shifts_across_tz_boundary() {
        let ts = 1_777_597_200_i64; // 2026-05-01T03:00:00Z
        let mut events = vec![synth_event("evt-1", ts)];

        let utc = resolve_tz("utc").expect("utc");
        rebucket_events(&mut events, &utc);
        assert_eq!(events[0].month, YearMonth::new(2026, 5));

        let pacific = resolve_tz("America/Los_Angeles").expect("pacific");
        rebucket_events(&mut events, &pacific);
        assert_eq!(events[0].month, YearMonth::new(2026, 4));
    }

    #[test]
    fn rebucket_events_leaves_legacy_rows_alone() {
        let mut events = vec![reckon_core::UsageEvent {
            source: Source::Claude,
            month: YearMonth::new(2026, 5),
            timestamp_secs: 0,
            model: ModelSlug::new("anthropic/claude-opus-4.7"),
            provider: "anthropic".into(),
            project: None,
            tokens: reckon_core::TokenCounts::default(),
            dedup_key: "legacy".into(),
            known_cost_usd: None,
            byok_usage_inference: None,
        }];

        let pacific = resolve_tz("America/Los_Angeles").expect("pacific");
        rebucket_events(&mut events, &pacific);

        // Without timestamp_secs there is nothing to project. The pre-computed
        // month must survive so legacy cache rows don't all collapse to 1970.
        assert_eq!(events[0].month, YearMonth::new(2026, 5));
    }
}
