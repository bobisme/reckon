use crate::{ModelSlug, Source};

#[must_use]
pub fn canonical(source: Source, raw: &str, _provider: Option<&str>) -> ModelSlug {
    if source == Source::Claude
        && let Some(slug) = claude_canonical(raw)
    {
        return slug;
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
    fn non_claude_source_passes_through() {
        let slug = canonical(Source::Pi, "claude-haiku-4-5", Some("anthropic"));
        assert_eq!(slug.as_str(), "claude-haiku-4-5");
    }

    #[test]
    fn openrouter_slug_passes_through() {
        let slug = canonical(Source::OpenRouter, "google/gemini-2.5-pro", None);
        assert_eq!(slug.as_str(), "google/gemini-2.5-pro");
    }
}
