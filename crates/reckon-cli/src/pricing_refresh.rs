use std::fs;
use std::path::Path;

const LITELLM_PRICING_URL: &str =
    "https://raw.githubusercontent.com/BerriAI/litellm/main/model_prices_and_context_window.json";

/// Fetch pricing from `LiteLLM` and write atomically to the cache path.
/// Returns the number of model entries written on success (`LiteLLM`'s
/// `sample_spec` meta entry is excluded). On failure, returns an error
/// (caller should log).
pub fn fetch_and_cache_pricing(cache_path: &Path) -> Result<usize, Box<dyn std::error::Error>> {
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

    Ok(model_count(body))
}

/// Count model entries in a `LiteLLM` pricing payload, excluding the
/// `sample_spec` documentation entry. Returns 0 if the body isn't a JSON
/// object (the file is still written; the count is only cosmetic).
fn model_count(body: &str) -> usize {
    serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(body)
        .map_or(0, |map| {
            map.keys().filter(|k| k.as_str() != "sample_spec").count()
        })
}

#[cfg(test)]
mod tests {
    use super::model_count;

    #[test]
    fn model_count_excludes_sample_spec() {
        let body = r#"{
            "sample_spec": {"note": "docs"},
            "gpt-5.5": {"input_cost_per_token": 0.1},
            "claude-opus-4.8": {"input_cost_per_token": 0.2}
        }"#;
        assert_eq!(model_count(body), 2);
    }

    #[test]
    fn model_count_is_zero_for_non_object() {
        assert_eq!(model_count("[]"), 0);
        assert_eq!(model_count("not json"), 0);
    }
}
