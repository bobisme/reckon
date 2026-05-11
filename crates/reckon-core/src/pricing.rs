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

#[derive(Debug, Clone)]
struct CanonicalModelSlug {
    vendor: String,
    model: String,
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
        if let Some(model) = canonical_slug(&raw_slug) {
            let Some(input_per_token) = entry.input_cost_per_token else {
                continue;
            };
            let Some(output_per_token) = entry.output_cost_per_token else {
                continue;
            };

            let canonical = ModelSlug::new(format!("{}/{}", model.vendor, model.model));
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

fn canonical_slug(raw: &str) -> Option<CanonicalModelSlug> {
    if raw.is_empty() || raw == "sample_spec" {
        return None;
    }

    if raw.contains('/') {
        return canonical_slug_from_slash(raw);
    }

    if raw.contains('.') {
        return canonical_slug_from_dot(raw);
    }

    None
}

fn canonical_slug_from_slash(raw: &str) -> Option<CanonicalModelSlug> {
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

    Some(CanonicalModelSlug {
        vendor: normalize_vendor(vendor).to_string(),
        model: model.to_string(),
    })
}

fn canonical_slug_from_dot(raw: &str) -> Option<CanonicalModelSlug> {
    let mut parts = raw.split('.');
    let first = parts.next()?;
    let second = parts.next()?;
    let rest: Vec<&str> = parts.collect();

    if rest.is_empty() {
        return Some(CanonicalModelSlug {
            vendor: normalize_vendor(first).to_string(),
            model: second.to_string(),
        });
    }

    if is_region_prefix(first) {
        let mut joined = vec![second];
        joined.extend(rest);
        let dotted = joined.join(".");
        let (vendor, model) = dotted.split_once('.')?;
        return Some(CanonicalModelSlug {
            vendor: normalize_vendor(vendor).to_string(),
            model: model.to_string(),
        });
    }

    Some(CanonicalModelSlug {
        vendor: normalize_vendor(first).to_string(),
        model: core::iter::once(second)
            .chain(rest)
            .collect::<Vec<_>>()
            .join("."),
    })
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
    use std::path::Path;

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
        assert_eq!(format!("{}/{}", slug.vendor, slug.model), "openai/gpt-5.2");

        let slug =
            canonical_slug("deepinfra/google/gemini-2.5-pro").expect("expected canonical slug");
        assert_eq!(
            format!("{}/{}", slug.vendor, slug.model),
            "google/gemini-2.5-pro"
        );

        let slug = canonical_slug("anthropic.claude-opus-4.7").expect("expected canonical slug");
        assert_eq!(
            format!("{}/{}", slug.vendor, slug.model),
            "anthropic/claude-opus-4.7"
        );

        let slug =
            canonical_slug("global.anthropic.claude-opus-4-7").expect("expected canonical slug");
        assert_eq!(
            format!("{}/{}", slug.vendor, slug.model),
            "anthropic/claude-opus-4-7"
        );
    }
}
