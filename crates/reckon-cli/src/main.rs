mod render;
mod report;
mod pricing_refresh;

use std::env;
use std::path::PathBuf;

use asupersync::Cx;
use reckon_core::{load_pricing_from_cache, load_pricing_fallback, is_pricing_cache_stale};
use reckon_readers::claude::ClaudeReader;
use reckon_readers::{Reader, run_readers_with_cache};

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

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = asupersync::runtime::RuntimeBuilder::new().build()?;
    let handle = runtime.handle();
    let join = handle.spawn(async move {
        let cx = Cx::current().expect("no async context");

        let pricing_path = pricing_cache_path();
        let pricing = load_pricing_from_cache(&pricing_path)
            .unwrap_or_else(load_pricing_fallback);

        if is_pricing_cache_stale(&pricing_path) {
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

        let mut unknown_models = Vec::new();
        render::print_table(&aggregated, &pricing, &mut unknown_models);

        for model in &unknown_models {
            eprintln!("Unknown pricing for: {model}");
        }
    });
    runtime.block_on(join);
    Ok(())
}
