use std::collections::HashMap;
use std::fs;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::{ModelSlug, TokenCounts};

#[derive(Debug, Clone, Copy, PartialEq, Serialize)]
pub struct Pricing {
    pub input_per_token: f64,
    pub output_per_token: f64,
    pub cache_read_per_token: f64,
    pub cache_write_per_token: f64,
    pub reasoning_per_token: Option<f64>,
}

#[derive(Deserialize)]
struct LiteLLMEntry {
    input_cost_per_token: Option<f64>,
    output_cost_per_token: Option<f64>,
    cache_read_input_token_cost: Option<f64>,
    cache_creation_input_token_cost: Option<f64>,
    output_cost_per_reasoning_token: Option<f64>,
}

/// # Panics
///
/// Panics if the file cannot be read or parsed as JSON.
#[must_use]
pub fn load_pricing(path: &Path) -> HashMap<ModelSlug, Pricing> {
    let body = fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("failed to read pricing file {}: {error}", path.display()));
    load_pricing_from_str(&body)
}

/// Check if the pricing cache file is stale (older than 7 days or missing).
#[must_use]
pub fn is_pricing_cache_stale(path: &Path) -> bool {
    match fs::metadata(path) {
        Ok(metadata) => {
            if let Ok(modified) = metadata.modified()
                && let Ok(elapsed) = modified.elapsed()
            {
                return elapsed.as_secs() > 7 * 24 * 60 * 60;
            }
            false
        }
        Err(_) => true,
    }
}

/// Load pricing from cache, merging with fallback (cached takes precedence).
/// Returns None if the cache file doesn't exist; returns Some with cached+fallback if it does.
#[must_use]
pub fn load_pricing_from_cache(path: &Path) -> Option<HashMap<ModelSlug, Pricing>> {
    if !path.exists() {
        return None;
    }

    let cached = fs::read_to_string(path)
        .map_or_else(|_| HashMap::new(), |body| load_pricing_from_str(&body));

    let mut merged = load_pricing_fallback();
    merged.extend(cached);
    Some(merged)
}

#[must_use]
pub fn load_pricing_fallback() -> HashMap<ModelSlug, Pricing> {
    load_pricing_from_str(include_str!("../assets/pricing-fallback.json"))
}

/// # Panics
///
/// Panics if `body` is not valid `LiteLLM` pricing JSON.
#[must_use]
pub fn load_pricing_from_str(body: &str) -> HashMap<ModelSlug, Pricing> {
    let raw: HashMap<String, LiteLLMEntry> =
        serde_json::from_str(body).expect("failed to parse pricing JSON");

    let mut out = HashMap::new();
    for (raw_slug, entry) in raw {
        if let Some(canonical) = canonical_slug(&raw_slug) {
            let Some(input_per_token) = entry.input_cost_per_token else {
                continue;
            };
            let Some(output_per_token) = entry.output_cost_per_token else {
                continue;
            };

            let pricing = Pricing {
                input_per_token,
                output_per_token,
                cache_read_per_token: entry.cache_read_input_token_cost.unwrap_or(0.0),
                cache_write_per_token: entry.cache_creation_input_token_cost.unwrap_or(0.0),
                reasoning_per_token: entry.output_cost_per_reasoning_token,
            };

            out.insert(canonical, pricing);
        }
    }

    out
}

#[must_use]
#[expect(clippy::cast_precision_loss)]
pub fn cost(tokens: &TokenCounts, pricing: &Pricing) -> f64 {
    let reasoning_per_token = pricing
        .reasoning_per_token
        .unwrap_or(pricing.output_per_token);

    let acc = (tokens.input as f64).mul_add(pricing.input_per_token, 0.0);
    let acc = (tokens.output as f64).mul_add(pricing.output_per_token, acc);
    let acc = (tokens.cache_read as f64).mul_add(pricing.cache_read_per_token, acc);
    let acc = (tokens.cache_write as f64).mul_add(pricing.cache_write_per_token, acc);
    (tokens.reasoning as f64).mul_add(reasoning_per_token, acc)
}

fn canonical_slug(raw: &str) -> Option<ModelSlug> {
    if raw.is_empty() || raw == "sample_spec" {
        return None;
    }

    if raw.contains('/') {
        return canonical_slug_from_slash(raw);
    }

    // Dotted keys like `eu.anthropic.claude-opus-4-7` are bedrock-style
    // "region.vendor.model" — strip the region and re-split into vendor/model.
    // For any other dotted key (e.g. `gpt-5.5`, `gemini-3.1-pro-preview`,
    // `anthropic.claude-3-opus-...`) the dot is part of the model name itself,
    // so we index by the raw key. Readers either emit the raw key as-is or
    // map it via `model_map` to a slash-form vendor/model slug; we cover both
    // shapes by inserting only the raw key here and letting the slash-prefixed
    // duplicates in pricing-fallback (`openrouter/<vendor>/<model>`, etc.)
    // populate the slash form.
    if let Some(rest) = strip_region_prefix(raw) {
        return canonical_slug_from_dotted_region(rest);
    }

    Some(ModelSlug::new(raw))
}

fn canonical_slug_from_slash(raw: &str) -> Option<ModelSlug> {
    let mut current = raw;
    loop {
        if let Some((
            "openrouter" | "gmi" | "azure" | "deepinfra" | "github_copilot" | "perplexity"
            | "vercel_ai_gateway",
            rest,
        )) = current.split_once('/')
        {
            current = rest;
            continue;
        }
        break;
    }

    let parts: Vec<&str> = current.split('/').collect();
    if parts.len() < 2 {
        return None;
    }

    let vendor = parts[parts.len() - 2];
    let model = parts[parts.len() - 1];

    Some(ModelSlug::new(format!(
        "{}/{}",
        normalize_vendor(vendor),
        model
    )))
}

/// Given the portion of a bedrock-style key after the region prefix
/// (e.g. `anthropic.claude-opus-4-7` from `eu.anthropic.claude-opus-4-7`),
/// split into `vendor/model` form. Returns None if the input has no dot.
fn canonical_slug_from_dotted_region(rest: &str) -> Option<ModelSlug> {
    let (vendor, model) = rest.split_once('.')?;
    Some(ModelSlug::new(format!(
        "{}/{}",
        normalize_vendor(vendor),
        model
    )))
}

/// If `raw` starts with a known region prefix followed by `.`, return the
/// remainder. Otherwise return None.
fn strip_region_prefix(raw: &str) -> Option<&str> {
    let (prefix, rest) = raw.split_once('.')?;
    if is_region_prefix(prefix) {
        Some(rest)
    } else {
        None
    }
}

fn is_region_prefix(prefix: &str) -> bool {
    matches!(
        prefix,
        "us" | "eu"
            | "au"
            | "apac"
            | "global"
            | "ap"
            | "ca"
            | "jp"
            | "uk"
            | "south"
            | "north"
            | "us-east"
            | "us-west"
    )
}

fn normalize_vendor(vendor: &str) -> &str {
    match vendor {
        "gemini" => "google",
        _ => vendor,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::Path;
    use std::time::SystemTime;

    fn fixture() -> HashMap<ModelSlug, Pricing> {
        load_pricing(
            Path::new(env!("CARGO_MANIFEST_DIR"))
                .join("assets/pricing-fallback.json")
                .as_ref(),
        )
    }

    #[test]
    fn pricing_load_has_many_rows() {
        let pricing = fixture();
        assert!(
            pricing.len() >= 150,
            "pricing snapshot too small: {}",
            pricing.len()
        );
    }

    #[test]
    fn cost_uses_expected_formula_with_reasoning_fallback() {
        let pricing = fixture();

        let opus = pricing
            .get(&ModelSlug::new("anthropic/claude-opus-4.7"))
            .expect("missing anthropic/claude-opus-4.7");
        let opus_cost = super::cost(
            &TokenCounts {
                input: 1_000,
                output: 2_000,
                cache_read: 3_000,
                cache_write: 4_000,
                reasoning: 5_000,
            },
            opus,
        );
        assert!((opus_cost - 0.2065).abs() <= 0.001);

        let gpt_5 = pricing
            .get(&ModelSlug::new("openai/gpt-5.2"))
            .expect("missing openai/gpt-5.2");
        let gpt_cost = super::cost(
            &TokenCounts {
                input: 500,
                output: 1_000,
                cache_read: 250,
                cache_write: 0,
                reasoning: 0,
            },
            gpt_5,
        );
        assert!((gpt_cost - 0.01491875).abs() <= 0.001);

        let gemini = pricing
            .get(&ModelSlug::new("google/gemini-2.5-pro"))
            .expect("missing google/gemini-2.5-pro");
        let gemini_cost = super::cost(
            &TokenCounts {
                input: 500,
                output: 1_000,
                cache_read: 0,
                cache_write: 0,
                reasoning: 2_000,
            },
            gemini,
        );
        assert!((gemini_cost - 0.030625).abs() <= 0.001);
    }

    #[test]
    fn canonical_slug_supports_openrouter_and_aliases() {
        let slug = canonical_slug("openrouter/openai/gpt-5.2").expect("expected canonical slug");
        assert_eq!(slug.as_str(), "openai/gpt-5.2");

        let slug =
            canonical_slug("deepinfra/google/gemini-2.5-pro").expect("expected canonical slug");
        assert_eq!(slug.as_str(), "google/gemini-2.5-pro");

        // Region-prefixed bedrock-style keys still split into vendor/model.
        let slug =
            canonical_slug("global.anthropic.claude-opus-4-7").expect("expected canonical slug");
        assert_eq!(slug.as_str(), "anthropic/claude-opus-4-7");

        let slug =
            canonical_slug("eu.anthropic.claude-opus-4-7").expect("expected canonical slug");
        assert_eq!(slug.as_str(), "anthropic/claude-opus-4-7");
    }

    #[test]
    fn canonical_slug_uses_raw_for_non_region_dotted() {
        // Non-region dotted keys: the dot is part of the model name, not
        // a vendor separator. Index by the raw key so it matches what
        // readers/model_map emit.
        let slug = canonical_slug("gpt-5.5").expect("expected canonical slug");
        assert_eq!(slug.as_str(), "gpt-5.5");

        let slug = canonical_slug("gemini-3.1-pro-preview").expect("expected canonical slug");
        assert_eq!(slug.as_str(), "gemini-3.1-pro-preview");

        // `anthropic.<model>` is bedrock-style but without a region prefix;
        // treat it as raw since the reader path for Claude routes to
        // `anthropic/claude-...` (slash form) which is populated by other
        // entries in pricing-fallback (openrouter/anthropic/..., etc.).
        let slug = canonical_slug("anthropic.claude-opus-4-7").expect("expected canonical slug");
        assert_eq!(slug.as_str(), "anthropic.claude-opus-4-7");
    }

    #[test]
    fn canonical_slug_uses_raw_for_no_separator() {
        // Keys with neither `/` nor `.` (the bug case) must produce a slug.
        let slug = canonical_slug("gemini-3-flash-preview").expect("expected canonical slug");
        assert_eq!(slug.as_str(), "gemini-3-flash-preview");

        let slug = canonical_slug("gemini-3-pro-preview").expect("expected canonical slug");
        assert_eq!(slug.as_str(), "gemini-3-pro-preview");
    }

    #[test]
    fn pricing_fallback_resolves_gemini_3_and_gpt_5_5() {
        let pricing = load_pricing_fallback();
        assert!(
            pricing.contains_key(&ModelSlug::new("gemini-3-flash-preview")),
            "gemini-3-flash-preview must be resolvable from pricing-fallback"
        );
        assert!(
            pricing.contains_key(&ModelSlug::new("gpt-5.5")),
            "gpt-5.5 must be resolvable from pricing-fallback"
        );
    }

    #[test]
    fn missing_cache_file_is_stale() {
        let path = Path::new("/tmp/nonexistent/pricing-test-missing-12345.json");
        assert!(is_pricing_cache_stale(path));
    }

    #[test]
    fn recent_cache_file_is_not_stale() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("pricing.json");
        fs::write(&path, "{}").expect("write test file");
        assert!(!is_pricing_cache_stale(&path));
    }

    #[test]
    fn old_cache_file_is_stale() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("pricing.json");
        fs::write(&path, "{}").expect("write test file");

        let eight_days_ago = SystemTime::now() - std::time::Duration::from_secs(8 * 24 * 60 * 60);
        filetime::set_file_mtime(&path, filetime::FileTime::from_system_time(eight_days_ago))
            .expect("set mtime");

        assert!(is_pricing_cache_stale(&path));
    }

    #[test]
    fn missing_cache_returns_none() {
        let path = Path::new("/tmp/nonexistent/pricing-test-load-12345.json");
        assert_eq!(load_pricing_from_cache(path), None);
    }

    #[test]
    fn load_pricing_from_cache_merges_with_fallback() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("pricing.json");

        let cached = r#"{"anthropic/claude-opus-4.7": {"input_cost_per_token": 0.01, "output_cost_per_token": 0.03}}"#;
        fs::write(&path, cached).expect("write test file");

        let merged = load_pricing_from_cache(&path).expect("load pricing");

        assert!(merged.contains_key(&ModelSlug::new("anthropic/claude-opus-4.7")));
        let fallback_pricing = load_pricing_fallback();
        for (model, _) in fallback_pricing {
            assert!(
                merged.contains_key(&model),
                "fallback model {model} not in merged pricing"
            );
        }
    }

    #[test]
    fn cache_takes_precedence_over_fallback() {
        let dir = tempfile::tempdir().expect("temp dir");
        let path = dir.path().join("pricing.json");

        let cached = r#"{"anthropic/claude-opus-4.7": {"input_cost_per_token": 99.99, "output_cost_per_token": 88.88}}"#;
        fs::write(&path, cached).expect("write test file");

        let merged = load_pricing_from_cache(&path).expect("load pricing");
        let model = ModelSlug::new("anthropic/claude-opus-4.7");
        let price = merged.get(&model).expect("model not found");
        assert_eq!(price.input_per_token, 99.99);
        assert_eq!(price.output_per_token, 88.88);
    }
}
