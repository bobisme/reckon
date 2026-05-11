use crate::{ModelSlug, Source};

#[must_use]
pub fn canonical(_source: Source, raw: &str, _provider: Option<&str>) -> ModelSlug {
    ModelSlug(raw.into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn passthrough_returns_input_verbatim() {
        let slug = canonical(Source::Claude, "claude-opus-4-7", None);
        assert_eq!(slug.as_str(), "claude-opus-4-7");
    }

    #[test]
    fn passthrough_with_provider() {
        let slug = canonical(Source::Pi, "claude-haiku-4-5", Some("anthropic"));
        assert_eq!(slug.as_str(), "claude-haiku-4-5");
    }

    #[test]
    fn passthrough_openrouter_slug() {
        let slug = canonical(Source::OpenRouter, "google/gemini-2.5-pro", None);
        assert_eq!(slug.as_str(), "google/gemini-2.5-pro");
    }
}
