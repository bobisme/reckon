use std::path::{Path, PathBuf};
use std::{env, fs};

use asupersync::http::h1::http_client::HttpClient;
use asupersync::http::HttpClientBuilder;
use serde::Deserialize;

/// Create an HTTP client configured for use with the `OpenRouter` API.
///
/// TLS uses Mozilla root certificates (compiled in via `tls-webpki-roots`).
/// The `User-Agent` is set to `reckon/<version>`.
#[must_use]
pub fn build_http_client() -> HttpClient {
    HttpClientBuilder::new()
        .user_agent(format!("reckon/{}", env!("CARGO_PKG_VERSION")))
        .build()
}

/// Resolve the `OpenRouter` API key using the standard lookup chain.
///
/// Priority (highest to lowest):
/// 1. `RECKON_OPENROUTER_KEY` environment variable
/// 2. `OPENROUTER_API_KEY` environment variable
/// 3. `openrouter.key` field in `~/.config/reckon/config.toml`
///
/// Returns `None` if no key is found in any location.
#[must_use]
pub fn resolve_key() -> Option<String> {
    resolve_key_inner(|k| env::var(k).ok(), default_config_path().as_deref())
}

/// Mask an `OpenRouter` API key for safe display in logs and error messages.
///
/// Returns a string of the form `sk-or-...XXXX` where `XXXX` is the last
/// four characters of the key.
#[must_use]
pub fn mask_key(k: &str) -> String {
    let tail = if k.len() >= 4 { &k[k.len() - 4..] } else { k };
    format!("sk-or-...{tail}")
}

fn default_config_path() -> Option<PathBuf> {
    env::var("HOME")
        .ok()
        .map(|home| PathBuf::from(home).join(".config").join("reckon").join("config.toml"))
}

fn resolve_key_inner(
    get_env: impl Fn(&str) -> Option<String>,
    config_path: Option<&Path>,
) -> Option<String> {
    if let Some(key) = get_env("RECKON_OPENROUTER_KEY").filter(|k| !k.is_empty()) {
        return Some(key);
    }
    if let Some(key) = get_env("OPENROUTER_API_KEY").filter(|k| !k.is_empty()) {
        return Some(key);
    }
    key_from_config(config_path)
}

#[derive(Deserialize)]
struct ConfigFile {
    openrouter: Option<OpenRouterConfig>,
}

#[derive(Deserialize)]
struct OpenRouterConfig {
    key: Option<String>,
}

fn key_from_config(path: Option<&Path>) -> Option<String> {
    let path = path?;
    let content = fs::read_to_string(path).ok()?;
    let cfg: ConfigFile = toml::from_str(&content).ok()?;
    cfg.openrouter?.key.filter(|k| !k.is_empty())
}

#[cfg(test)]
mod tests {
    use std::io::Write as _;

    use tempfile::NamedTempFile;

    use super::*;

    fn env_from<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        |key| {
            pairs
                .iter()
                .find(|(k, _)| *k == key)
                .map(|(_, v)| (*v).to_string())
        }
    }

    #[test]
    fn only_openrouter_api_key_set() {
        let get = env_from(&[("OPENROUTER_API_KEY", "sk-or-testkey1234")]);
        assert_eq!(resolve_key_inner(get, None), Some("sk-or-testkey1234".into()));
    }

    #[test]
    fn reckon_key_wins_over_openrouter_key() {
        let get = env_from(&[
            ("RECKON_OPENROUTER_KEY", "sk-or-reckon9999"),
            ("OPENROUTER_API_KEY", "sk-or-generic0000"),
        ]);
        assert_eq!(resolve_key_inner(get, None), Some("sk-or-reckon9999".into()));
    }

    #[test]
    fn no_env_falls_back_to_config_file() {
        let mut tmp = NamedTempFile::new().expect("tempfile");
        write!(tmp, "[openrouter]\nkey = \"sk-or-fromfile5678\"\n").expect("write");
        let path = tmp.path().to_owned();
        let result = resolve_key_inner(|_| None, Some(&path));
        assert_eq!(result, Some("sk-or-fromfile5678".into()));
    }

    #[test]
    fn env_takes_priority_over_config_file() {
        let mut tmp = NamedTempFile::new().expect("tempfile");
        write!(tmp, "[openrouter]\nkey = \"sk-or-fromfile5678\"\n").expect("write");
        let path = tmp.path().to_owned();
        let get = env_from(&[("OPENROUTER_API_KEY", "sk-or-fromenv1111")]);
        let result = resolve_key_inner(get, Some(&path));
        assert_eq!(result, Some("sk-or-fromenv1111".into()));
    }

    #[test]
    fn missing_config_file_returns_none() {
        let result = resolve_key_inner(|_| None, Some(Path::new("/nonexistent/config.toml")));
        assert!(result.is_none());
    }

    #[test]
    fn empty_env_values_are_skipped() {
        let get = env_from(&[("RECKON_OPENROUTER_KEY", ""), ("OPENROUTER_API_KEY", "sk-or-nonempty")]);
        assert_eq!(resolve_key_inner(get, None), Some("sk-or-nonempty".into()));
    }

    #[test]
    fn mask_key_shows_last_four_chars() {
        assert_eq!(mask_key("sk-or-abcdefgh"), "sk-or-...efgh");
    }

    #[test]
    fn mask_key_short_input() {
        assert_eq!(mask_key("abc"), "sk-or-...abc");
    }

    #[test]
    fn mask_key_exactly_four_chars() {
        assert_eq!(mask_key("1234"), "sk-or-...1234");
    }
}
