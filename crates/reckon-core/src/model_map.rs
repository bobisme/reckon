use crate::{ModelSlug, Source};

#[must_use]
pub fn canonical(source: Source, raw: &str, provider: Option<&str>) -> ModelSlug {
    if source == Source::Claude {
        if let Some(slug) = claude_canonical(raw) {
            return slug;
        }
    } else if source == Source::Codex {
        if let Some(slug) = openai_canonical(raw) {
            return slug;
        }
    } else if source == Source::Gemini {
        if let Some(slug) = gemini_canonical(raw) {
            return slug;
        }
    } else if source == Source::OpenCode && let Some(p) = provider {
        return opencode_canonical(p, raw);
    } else if source == Source::Pi && let Some(p) = provider {
        return pi_canonical(p, raw);
    }
    ModelSlug(raw.into())
}

fn claude_canonical(raw: &str) -> Option<ModelSlug> {
    let families: &[(&str, &str)] = &[
        ("claude-opus-4-7", "anthropic/claude-opus-4.7"),
        ("claude-opus-4-6", "anthropic/claude-opus-4.6"),
        ("claude-sonnet-4-6", "anthropic/claude-sonnet-4.6"),
        ("claude-haiku-4-5", "anthropic/claude-haiku-4.5"),
    ];
    for &(prefix, canonical) in families {
        if raw == prefix || raw.starts_with(&format!("{prefix}-")) {
            return Some(ModelSlug(canonical.into()));
        }
    }
    None
}

fn openai_canonical(raw: &str) -> Option<ModelSlug> {
    let families: &[(&str, &str)] = &[
        ("gpt-5.2", "gpt-5.2"),
        ("gpt-4.1", "gpt-4.1"),
        ("o1", "o1"),
        ("o3-mini", "o3-mini"),
    ];
    for &(prefix, canonical) in families {
        if raw == prefix || raw.starts_with(&format!("{prefix}-")) {
            return Some(ModelSlug(format!("openai/{canonical}")));
        }
    }
    None
}

fn gemini_canonical(raw: &str) -> Option<ModelSlug> {
    let families: &[(&str, &str)] = &[
        ("gemini-2.5-pro", "google/gemini-2.5-pro"),
        ("gemini-2.5-flash", "google/gemini-2.5-flash"),
        ("gemini-1.5-pro", "google/gemini-1.5-pro"),
        ("gemini-1.5-flash", "google/gemini-1.5-flash"),
    ];
    for &(prefix, canonical) in families {
        if raw == prefix || raw.starts_with(&format!("{prefix}-")) {
            return Some(ModelSlug(canonical.into()));
        }
    }
    None
}

fn opencode_canonical(provider: &str, raw: &str) -> ModelSlug {
    if raw.contains('/') {
        ModelSlug(raw.into())
    } else {
        ModelSlug(format!("{provider}/{raw}"))
    }
}

fn pi_canonical(provider: &str, raw: &str) -> ModelSlug {
    let normalized = normalize_version_hyphens(raw);
    ModelSlug(format!("{provider}/{normalized}"))
}

fn normalize_version_hyphens(raw: &str) -> String {
    let mut result = String::new();
    let bytes = raw.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if i > 0 && i + 1 < bytes.len() && bytes[i] == b'-' {
            let prev_is_digit = bytes[i - 1].is_ascii_digit();
            let next_is_digit = bytes[i + 1].is_ascii_digit();
            if prev_is_digit && next_is_digit {
                result.push('.');
                i += 1;
                continue;
            }
        }
        result.push(bytes[i] as char);
        i += 1;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn claude_opus_with_date_suffix() {
        let slug = canonical(Source::Claude, "claude-opus-4-7-20251015", None);
        assert_eq!(slug.as_str(), "anthropic/claude-opus-4.7");
    }

    #[test]
    fn claude_opus_bare() {
        let slug = canonical(Source::Claude, "claude-opus-4-7", None);
        assert_eq!(slug.as_str(), "anthropic/claude-opus-4.7");
    }

    #[test]
    fn claude_sonnet_with_date() {
        let slug = canonical(Source::Claude, "claude-sonnet-4-6-20250514", None);
        assert_eq!(slug.as_str(), "anthropic/claude-sonnet-4.6");
    }

    #[test]
    fn claude_haiku_with_date() {
        let slug = canonical(Source::Claude, "claude-haiku-4-5-20251001", None);
        assert_eq!(slug.as_str(), "anthropic/claude-haiku-4.5");
    }

    #[test]
    fn claude_unknown_model_passes_through() {
        let slug = canonical(Source::Claude, "claude-future-9-9", None);
        assert_eq!(slug.as_str(), "claude-future-9-9");
    }

    #[test]
    fn openai_gpt_5_2_with_date() {
        let slug = canonical(Source::Codex, "gpt-5.2-2025-01-15", None);
        assert_eq!(slug.as_str(), "openai/gpt-5.2");
    }

    #[test]
    fn openai_gpt_4_1_with_date() {
        let slug = canonical(Source::Codex, "gpt-4.1-2025-06-01", None);
        assert_eq!(slug.as_str(), "openai/gpt-4.1");
    }

    #[test]
    fn openai_o1_with_date() {
        let slug = canonical(Source::Codex, "o1-2024-12-17", None);
        assert_eq!(slug.as_str(), "openai/o1");
    }

    #[test]
    fn openai_o3_mini_with_date() {
        let slug = canonical(Source::Codex, "o3-mini-2025-05-10", None);
        assert_eq!(slug.as_str(), "openai/o3-mini");
    }

    #[test]
    fn openai_unknown_slug_passes_through() {
        let slug = canonical(Source::Codex, "gpt-future-9-9", None);
        assert_eq!(slug.as_str(), "gpt-future-9-9");
    }

    #[test]
    fn pi_anthropic_haiku_normalizes_version() {
        let slug = canonical(Source::Pi, "claude-haiku-4-5", Some("anthropic"));
        assert_eq!(slug.as_str(), "anthropic/claude-haiku-4.5");
    }

    #[test]
    fn pi_anthropic_sonnet_normalizes_version() {
        let slug = canonical(Source::Pi, "claude-sonnet-4-6", Some("anthropic"));
        assert_eq!(slug.as_str(), "anthropic/claude-sonnet-4.6");
    }

    #[test]
    fn pi_openrouter_passthrough() {
        let slug = canonical(Source::Pi, "google/gemini-2.5-pro", Some("openrouter"));
        assert_eq!(slug.as_str(), "openrouter/google/gemini-2.5-pro");
    }

    #[test]
    fn pi_without_provider_passes_through() {
        let slug = canonical(Source::Pi, "claude-haiku-4-5", None);
        assert_eq!(slug.as_str(), "claude-haiku-4-5");
    }

    #[test]
    fn non_claude_source_passes_through() {
        let slug = canonical(Source::Pi, "claude-haiku-4-5", Some("anthropic"));
        assert_eq!(slug.as_str(), "anthropic/claude-haiku-4.5");
    }

    #[test]
    fn openrouter_slug_passes_through() {
        let slug = canonical(Source::OpenRouter, "google/gemini-2.5-pro", None);
        assert_eq!(slug.as_str(), "google/gemini-2.5-pro");
    }

    #[test]
    fn gemini_2_5_pro() {
        let slug = canonical(Source::Gemini, "gemini-2.5-pro", None);
        assert_eq!(slug.as_str(), "google/gemini-2.5-pro");
    }

    #[test]
    fn gemini_2_5_flash_with_suffix() {
        let slug = canonical(Source::Gemini, "gemini-2.5-flash-exp-04101", None);
        assert_eq!(slug.as_str(), "google/gemini-2.5-flash");
    }

    #[test]
    fn gemini_1_5_pro_with_suffix() {
        let slug = canonical(Source::Gemini, "gemini-1.5-pro-002", None);
        assert_eq!(slug.as_str(), "google/gemini-1.5-pro");
    }

    #[test]
    fn opencode_slashed_model_id() {
        let slug = canonical(Source::OpenCode, "openai/gpt-5.2", Some("openrouter"));
        assert_eq!(slug.as_str(), "openai/gpt-5.2");
    }

    #[test]
    fn opencode_bare_model_id_with_provider() {
        let slug = canonical(Source::OpenCode, "claude-opus-4.7", Some("anthropic"));
        assert_eq!(slug.as_str(), "anthropic/claude-opus-4.7");
    }

    #[test]
    fn opencode_without_provider_passes_through() {
        let slug = canonical(Source::OpenCode, "gpt-5.2", None);
        assert_eq!(slug.as_str(), "gpt-5.2");
    }
}
