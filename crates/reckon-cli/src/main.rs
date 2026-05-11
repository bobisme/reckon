mod render;
mod report;

use asupersync::Cx;
use reckon_core::load_pricing_fallback;
use reckon_readers::claude::ClaudeReader;
use reckon_readers::{Reader, run_readers};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let runtime = asupersync::runtime::RuntimeBuilder::new().build()?;
    let handle = runtime.handle();
    let join = handle.spawn(async move {
        let cx = Cx::current().expect("no async context");

        let readers: Vec<Box<dyn Reader>> = vec![Box::new(ClaudeReader::new())];
        let events = run_readers(&cx, readers).await;

        if events.is_empty() {
            println!("No usage events found.");
            return;
        }

        let pricing = load_pricing_fallback();
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
