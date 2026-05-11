mod render;
mod report;
mod pricing_refresh;

use std::collections::HashSet;
use std::env;
use std::fmt::Write as _;
use std::path::PathBuf;

use asupersync::Cx;
use clap::Parser;
use reckon_core::{
    load_pricing_from_cache, load_pricing_fallback, is_pricing_cache_stale, ModelSlug,
};
use reckon_readers::claude::ClaudeReader;
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

fn format_unknown_model_warning(models: &HashSet<ModelSlug>) -> String {
    let mut slugs: Vec<String> = models.iter().map(|model| model.as_str().to_string()).collect();
    slugs.sort_unstable();

    let listed_count = slugs.len().min(10);
    let mut message = format!(
        "warning: priced at $0 (no pricing data): {}",
        slugs.iter().take(listed_count).cloned().collect::<Vec<_>>().join(", "),
    );

    if slugs.len() > 10 {
        let _ = write!(message, " (and {} more)", slugs.len() - 10);
    }

    message
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Cli::parse();
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

        let readers: Vec<Box<dyn Reader>> = vec![Box::new(ClaudeReader::new())];
        let events = run_readers_with_cache(&cx, readers, &cache_path()).await;

        if events.is_empty() {
            println!("No usage events found.");
            return;
        }

        let aggregated = report::aggregate(events);
        let unknown_models = report::unknown_model_slugs(&aggregated, &pricing);
        render::print_table(&aggregated, &pricing);

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
}
