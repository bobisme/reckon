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
    } else if source == Source::OpenCode
        && let Some(p) = provider
    {
        return opencode_canonical(p, raw);
    } else if source == Source::Pi
        && let Some(p) = provider
    {
        return pi_canonical(p, raw);
    }
    ModelSlug(raw.into())
}

pub(crate) fn claude_canonical(raw: &str) -> Option<ModelSlug> {
    // Order matters: longer prefixes must come first because matching uses
    // `starts_with`. The 4-5 families don't share a prefix with 4-6/4-7, so
    // order between major versions doesn't collide.
    let families: &[(&str, &str)] = &[
        ("claude-opus-4-7", "anthropic/claude-opus-4.7"),
        ("claude-opus-4-6", "anthropic/claude-opus-4.6"),
        ("claude-opus-4-5", "anthropic/claude-opus-4.5"),
        ("claude-sonnet-4-6", "anthropic/claude-sonnet-4.6"),
        ("claude-sonnet-4-5", "anthropic/claude-sonnet-4.5"),
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
    // OpenAI-family pricing keys with `openai/` prefix in pricing-fallback
    // map to `openai/<family>`. Keys that appear in pricing-fallback as bare
    // (no `openai/` prefix, no `/`) — like `gpt-5.4`, `gpt-5.5` — must map
    // to the raw key so it matches the pricing index. Order matters: longer
    // prefixes must come first so `gpt-5.4-mini` matches `gpt-5.4` not
    // `gpt-5`.
    let families: &[(&str, &str)] = &[
        ("gpt-5.5-pro", "gpt-5.5-pro"),
        ("gpt-5.5", "gpt-5.5"),
        ("gpt-5.4-pro", "gpt-5.4-pro"),
        ("gpt-5.4-mini", "gpt-5.4-mini"),
        ("gpt-5.4-nano", "gpt-5.4-nano"),
        ("gpt-5.4", "gpt-5.4"),
        ("gpt-5.3-codex", "gpt-5.3-codex"),
        ("gpt-5.3-chat-latest", "gpt-5.3-chat-latest"),
        ("gpt-5.2", "openai/gpt-5.2"),
        ("gpt-4.1", "openai/gpt-4.1"),
        ("o1", "openai/o1"),
        ("o3-mini", "openai/o3-mini"),
    ];
    for &(prefix, canonical) in families {
        if raw == prefix || raw.starts_with(&format!("{prefix}-")) {
            return Some(ModelSlug(canonical.into()));
        }
    }
    None
}

fn gemini_canonical(raw: &str) -> Option<ModelSlug> {
    // Gemini-3 entries in pricing-fallback live under bare keys without a
    // `google/` prefix, so map to the raw form. Gemini-2.5 / 1.5 retain the
    // `google/` prefix because pricing-fallback also indexes slash-prefixed
    // duplicates (vertex_ai/..., openrouter/google/..., gemini/...). Order
    // matters: longer prefixes must come first.
    let families: &[(&str, &str)] = &[
        ("gemini-3.1-pro-preview", "gemini-3.1-pro-preview"),
        ("gemini-3.1-flash-image-preview", "gemini-3.1-flash-image-preview"),
        ("gemini-3.1-flash-lite-preview", "gemini-3.1-flash-lite-preview"),
        ("gemini-3.1-flash-live-preview", "gemini-3.1-flash-live-preview"),
        ("gemini-3-pro-image-preview", "gemini-3-pro-image-preview"),
        ("gemini-3-pro-preview", "gemini-3-pro-preview"),
        ("gemini-3-flash-preview", "gemini-3-flash-preview"),
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
        return ModelSlug(raw.into());
    }
    // OpenCode reports older Anthropic models with the date-less `claude-*-4-5`
    // form (dashes). Route those through the shared claude family map so they
    // canonicalize to `anthropic/claude-*-4.5` (dot) which is what pricing has.
    if provider == "anthropic"
        && let Some(slug) = claude_canonical(raw)
    {
        return slug;
    }
    ModelSlug(format!("{provider}/{raw}"))
}

fn pi_canonical(provider: &str, raw: &str) -> ModelSlug {
    let normalized_provider = normalize_pi_provider(provider);
    // When the (normalized) provider is anthropic, route through the shared
    // claude family map so `claude-haiku-4-5` becomes `anthropic/claude-haiku-4.5`
    // (matches what pricing-fallback knows).
    if normalized_provider == "anthropic"
        && let Some(slug) = claude_canonical(raw)
    {
        return slug;
    }
    let normalized_model = normalize_version_hyphens(raw);
    ModelSlug(format!("{normalized_provider}/{normalized_model}"))
}

/// Pi sometimes records the *client tool* name (e.g. `google-gemini-cli`,
/// `openai-codex`) in the `provider` field instead of the underlying vendor.
/// Map those back to canonical vendor names so the resulting slug matches what
/// pricing knows.
fn normalize_pi_provider(provider: &str) -> &str {
    match provider {
        "google-gemini-cli" => "google",
        "openai-codex" => "openai",
        other => other,
    }
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

    #[test]
    fn gemini_3_flash_preview_bare() {
        let slug = canonical(Source::Gemini, "gemini-3-flash-preview", None);
        assert_eq!(slug.as_str(), "gemini-3-flash-preview");
    }

    #[test]
    fn gemini_3_flash_preview_with_suffix() {
        let slug = canonical(Source::Gemini, "gemini-3-flash-preview-001", None);
        assert_eq!(slug.as_str(), "gemini-3-flash-preview");
    }

    #[test]
    fn gemini_3_pro_preview_bare() {
        let slug = canonical(Source::Gemini, "gemini-3-pro-preview", None);
        assert_eq!(slug.as_str(), "gemini-3-pro-preview");
    }

    #[test]
    fn gemini_3_1_pro_preview_with_suffix() {
        let slug = canonical(Source::Gemini, "gemini-3.1-pro-preview-001", None);
        assert_eq!(slug.as_str(), "gemini-3.1-pro-preview");
    }

    #[test]
    fn openai_gpt_5_5_with_date() {
        let slug = canonical(Source::Codex, "gpt-5.5-2026-04-01", None);
        assert_eq!(slug.as_str(), "gpt-5.5");
    }

    #[test]
    fn openai_gpt_5_4_bare() {
        let slug = canonical(Source::Codex, "gpt-5.4", None);
        assert_eq!(slug.as_str(), "gpt-5.4");
    }

    #[test]
    fn openai_gpt_5_4_mini_with_date() {
        let slug = canonical(Source::Codex, "gpt-5.4-mini-2026-03-17", None);
        assert_eq!(slug.as_str(), "gpt-5.4-mini");
    }

    #[test]
    fn opencode_anthropic_old_family_normalizes() {
        // OpenCode emits `claude-opus-4-5` (dashes) for older Anthropic models.
        // Pricing has `anthropic/claude-opus-4.5` (dot), so we must route the
        // raw model through claude_canonical when provider is "anthropic".
        let slug = canonical(Source::OpenCode, "claude-opus-4-5", Some("anthropic"));
        assert_eq!(slug.as_str(), "anthropic/claude-opus-4.5");

        let slug = canonical(Source::OpenCode, "claude-sonnet-4-5", Some("anthropic"));
        assert_eq!(slug.as_str(), "anthropic/claude-sonnet-4.5");

        let slug = canonical(Source::OpenCode, "claude-haiku-4-5", Some("anthropic"));
        assert_eq!(slug.as_str(), "anthropic/claude-haiku-4.5");
    }

    #[test]
    fn opencode_unknown_anthropic_falls_back_to_prefix() {
        // Models not in the claude family map should still produce a slash
        // slug — falling back to `{provider}/{raw}`.
        let slug = canonical(Source::OpenCode, "claude-future-9-9", Some("anthropic"));
        assert_eq!(slug.as_str(), "anthropic/claude-future-9-9");
    }

    #[test]
    fn pi_google_gemini_cli_provider_normalizes() {
        let slug = canonical(
            Source::Pi,
            "gemini-3-pro-preview",
            Some("google-gemini-cli"),
        );
        assert_eq!(slug.as_str(), "google/gemini-3-pro-preview");
    }

    #[test]
    fn pi_openai_codex_provider_normalizes() {
        let slug = canonical(Source::Pi, "gpt-5.3-codex", Some("openai-codex"));
        assert_eq!(slug.as_str(), "openai/gpt-5.3-codex");
    }

    #[test]
    fn pi_anthropic_haiku_still_normalizes_no_regression() {
        // Confirm the haiku-4-5 -> anthropic/claude-haiku-4.5 path still
        // works after routing pi-anthropic through claude_canonical.
        let slug = canonical(Source::Pi, "claude-haiku-4-5", Some("anthropic"));
        assert_eq!(slug.as_str(), "anthropic/claude-haiku-4.5");
    }
}
