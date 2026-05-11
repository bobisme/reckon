mod pricing_refresh;
mod render;
mod report;

use std::collections::HashSet;
use std::env;
use std::fmt::Write as _;
use std::path::PathBuf;

use asupersync::Cx;
use clap::Parser;
use reckon_core::{
    load_pricing_from_cache, load_pricing_fallback, is_pricing_cache_stale, ModelSlug, Source,
};
use reckon_readers::claude::ClaudeReader;
use reckon_readers::codex::CodexReader;
use reckon_readers::gemini::GeminiReader;
use reckon_readers::opencode::OpenCodeReader;
use reckon_readers::openrouter::{self, OpenRouterReader};
use reckon_readers::pi::PiReader;
use reckon_readers::{run_readers_with_cache, Reader};

#[derive(Parser)]
#[command(name = "reckon")]
#[command(about = "Monthly AI usage tracker with unsubsidized cost breakdown")]
#[command(long_about = None)]
struct Cli {
    /// Disable automatic pricing refresh from `LiteLLM`
    ///
    /// When set, pricing loads from ~/.cache/reckon/pricing.json if present,
    /// otherwise falls back to the vendored snapshot. No network requests are made.
    /// Note: newer models may be priced at $0 if not in cache or fallback.
    #[arg(long)]
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
}

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();

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

        if !args.offline && is_pricing_cache_stale(&pricing_path) {
            let path_for_fetch = pricing_path.clone();
            let _refresh_task = std::thread::spawn(move || {
                if let Err(e) = pricing_refresh::fetch_and_cache_pricing(&path_for_fetch) {
                    eprintln!("Warning: failed to refresh pricing cache: {e}");
                }
            });
        }

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

        let events = run_readers_with_cache(&cx, readers, &cache_path()).await;

        let balance = openrouter::fetch_balance().ok().flatten();

        if events.is_empty() {
            println!("No usage events found.");
            return;
        }

        let aggregated = report::aggregate(&events);
        let unknown_models = report::unknown_model_slugs(&aggregated, &pricing);
        if args.json {
            render::print_json(&events, &pricing, balance.as_ref());
        } else {
            render::print_table(&aggregated, &pricing, balance.as_ref());
        }

        if !unknown_models.is_empty() {
            eprintln!("{}", format_unknown_model_warning(&unknown_models));
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
}
