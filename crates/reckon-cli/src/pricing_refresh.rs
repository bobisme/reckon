use std::fs;
use std::path::Path;

const LITELLM_PRICING_URL: &str = "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";

/// Fetch pricing from `LiteLLM` and write atomically to the cache path.
/// Returns Ok(()) on success. On failure, returns an error (caller should log).
pub fn fetch_and_cache_pricing(cache_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    if let Some(parent) = cache_path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)?;
    }

    let response = minreq::get(LITELLM_PRICING_URL).send()?;

    if response.status_code < 200 || response.status_code >= 300 {
        return Err(format!("HTTP {}: {}", response.status_code, LITELLM_PRICING_URL).into());
    }

    let body = response.as_str()?;

    let temp_path = cache_path.with_extension("tmp");
    fs::write(&temp_path, body)?;
    fs::rename(&temp_path, cache_path)?;

    Ok(())
}
