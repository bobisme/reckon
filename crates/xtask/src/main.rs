use std::fs;
use std::path::Path;

const LITELLM_PRICING_URL: &str = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    match args.next().as_deref() {
        Some("vendor-pricing") => vendor_pricing(),
        Some(cmd) => {
            eprintln!("unknown command: {cmd}");
            eprintln!("usage: cargo xtask <command>");
            eprintln!("commands:");
            eprintln!("  vendor-pricing   Refresh the vendored pricing snapshot");
            Err("unknown command".into())
        }
        None => {
            eprintln!("usage: cargo xtask <command>");
            eprintln!("commands:");
            eprintln!("  vendor-pricing   Refresh the vendored pricing snapshot");
            Err("missing command".into())
        }
    }
}

fn vendor_pricing() -> Result<(), Box<dyn std::error::Error>> {
    eprintln!("Fetching pricing from {LITELLM_PRICING_URL}");
    let response = minreq::get(LITELLM_PRICING_URL).send()?;

    if response.status_code < 200 || response.status_code >= 300 {
        return Err(format!("HTTP {}: {LITELLM_PRICING_URL}", response.status_code).into());
    }

    let body = response.as_str()?;

    // Parse to ensure valid JSON
    let parsed: serde_json::Value = serde_json::from_str(body)?;

    // Pretty-print with 4-space indentation for readable diffs
    let pretty = serde_json::to_string_pretty(&parsed)?;

    // Determine output path: relative to Cargo.toml's package directory
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let output_path = Path::new(manifest_dir)
        .join("..")
        .join("reckon-core")
        .join("assets")
        .join("pricing-fallback.json");

    eprintln!("Writing to {}", output_path.display());

    // Ensure directory exists
    if let Some(parent) = output_path.parent() {
        fs::create_dir_all(parent)?;
    }

    // Write atomically with temp file
    let temp_path = output_path.with_extension("tmp");
    fs::write(&temp_path, pretty)?;
    fs::rename(&temp_path, &output_path)?;

    eprintln!("Done!");
    Ok(())
}
