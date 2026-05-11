use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, fs};

#[cfg(test)]
use std::io::{BufRead, BufReader, Read};
#[cfg(test)]
use std::net::{SocketAddr, TcpListener, TcpStream};
#[cfg(test)]
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
#[cfg(test)]
use std::sync::{Condvar, Mutex};
#[cfg(test)]
use std::thread;
#[cfg(test)]
use std::time::Duration;

use asupersync::http::HttpClientBuilder;
use asupersync::http::h1::http_client::{ClientError, HttpClient};
use asupersync::http::h1::types::{Method, Response};
use asupersync::sync::{AcquireError, Semaphore};
use asupersync::{CancelReason, Cx, Outcome};
use async_trait::async_trait;
use futures::stream::{FuturesUnordered, StreamExt};
use reckon_core::{ModelSlug, OpenRouterSummary, Source, TokenCounts, UsageEvent, YearMonth};
use serde::Deserialize;
use serde::de::{self, Visitor};

use crate::{CacheStrategy, Reader, ReaderError, Sink, SinkError};

const OPENROUTER_AUTH_ERROR_MESSAGE: &str = "OpenRouter rejected this key ({}). The /activity or /credits endpoint requires a Management API key, not an inference key. Create one at https://openrouter.ai/settings/keys under Management Keys.";
const OPENROUTER_BASE_URL: &str = "https://openrouter.ai";
const OPENROUTER_ACTIVITY_DAYS: usize = 30;
const OPENROUTER_MAX_IN_FLIGHT: usize = 4;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenRouterErrorKind {
    Auth,
    Network,
    Upstream,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenRouterExitCode {
    Success,
    Auth,
    Network,
    Other,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpenRouterError {
    pub kind: OpenRouterErrorKind,
    pub status: Option<u16>,
    pub message: String,
}

impl OpenRouterExitCode {
    #[must_use]
    pub const fn as_i32(self) -> i32 {
        match self {
            Self::Success => 0,
            Self::Auth => 2,
            Self::Network => 3,
            Self::Other => 1,
        }
    }
}

impl OpenRouterError {
    #[must_use]
    const fn new(kind: OpenRouterErrorKind, status: Option<u16>, message: String) -> Self {
        Self {
            kind,
            status,
            message,
        }
    }

    #[must_use]
    pub const fn exit_code(&self) -> OpenRouterExitCode {
        match self.kind {
            OpenRouterErrorKind::Auth => OpenRouterExitCode::Auth,
            OpenRouterErrorKind::Network => OpenRouterExitCode::Network,
            OpenRouterErrorKind::Upstream => OpenRouterExitCode::Other,
        }
    }
}

#[must_use]
pub fn management_api_key_error(key: &str) -> String {
    OPENROUTER_AUTH_ERROR_MESSAGE.replace("{}", &mask_key(key))
}

#[must_use]
pub fn classify_openrouter_response(key: &str, response: &Response) -> Option<OpenRouterError> {
    if response.is_success() {
        return None;
    }

    if response.status == 401 {
        Some(OpenRouterError::new(
            OpenRouterErrorKind::Auth,
            Some(response.status),
            management_api_key_error(key),
        ))
    } else {
        let body = response
            .text()
            .map_or_else(|_| String::new(), ToOwned::to_owned);
        Some(OpenRouterError::new(
            OpenRouterErrorKind::Upstream,
            Some(response.status),
            body,
        ))
    }
}

#[must_use]
pub fn classify_openrouter_network_error(error: &ClientError) -> OpenRouterError {
    OpenRouterError::new(OpenRouterErrorKind::Network, None, error.to_string())
}

/// Create an HTTP client configured for use with the `OpenRouter` API.
#[must_use]
pub fn build_http_client() -> HttpClient {
    HttpClientBuilder::new()
        .user_agent(format!("reckon/{}", env!("CARGO_PKG_VERSION")))
        .build()
}

/// Resolve the `OpenRouter` API key using the standard lookup chain.
#[must_use]
pub fn resolve_key() -> Option<String> {
    resolve_key_inner(|k| env::var(k).ok(), default_config_path().as_deref())
}

/// Mask an `OpenRouter` API key for safe display in logs and error messages.
#[must_use]
pub fn mask_key(k: &str) -> String {
    let tail = if k.len() >= 4 { &k[k.len() - 4..] } else { k };
    format!("sk-or-...{tail}")
}

pub struct OpenRouterReader {
    client: Arc<HttpClient>,
    base_url: String,
    key: Option<String>,
    today_override: Option<UtcDate>,
}

impl Default for OpenRouterReader {
    fn default() -> Self {
        Self::new()
    }
}

impl OpenRouterReader {
    #[must_use]
    pub fn new() -> Self {
        Self {
            client: Arc::new(build_http_client()),
            base_url: OPENROUTER_BASE_URL.into(),
            key: resolve_key(),
            today_override: None,
        }
    }

    #[must_use]
    pub fn with_base_url_and_key(base_url: impl Into<String>, key: impl Into<String>) -> Self {
        Self {
            client: Arc::new(build_http_client()),
            base_url: base_url.into(),
            key: Some(key.into()),
            today_override: None,
        }
    }

    #[cfg(test)]
    #[must_use]
    fn with_base_url_key_and_today(
        base_url: impl Into<String>,
        key: impl Into<String>,
        today: UtcDate,
    ) -> Self {
        Self {
            client: Arc::new(build_http_client()),
            base_url: base_url.into(),
            key: Some(key.into()),
            today_override: Some(today),
        }
    }

    fn today_utc(&self) -> Result<UtcDate, ReaderError> {
        if let Some(today) = self.today_override {
            return Ok(today);
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|error| {
                ReaderError::new(format!("system clock before unix epoch: {error}"))
            })?;
        Ok(UtcDate::from_epoch_seconds(
            i64::try_from(now.as_secs()).expect("unix seconds overflow"),
        ))
    }
}

#[async_trait]
impl Reader for OpenRouterReader {
    fn source(&self) -> Source {
        Source::OpenRouter
    }

    async fn scan(&self, cx: &Cx, sink: &Sink) -> Outcome<(), ReaderError> {
        let Some(key) = self.key.clone().filter(|key| !key.is_empty()) else {
            return Outcome::ok(());
        };

        let today = match self.today_utc() {
            Ok(today) => today,
            Err(error) => return Outcome::Err(error),
        };
        let client = Arc::clone(&self.client);
        let semaphore = Arc::new(Semaphore::new(OPENROUTER_MAX_IN_FLIGHT));
        let mut requests = FuturesUnordered::new();

        for date in date_window(today, OPENROUTER_ACTIVITY_DAYS) {
            let cx = cx.clone();
            let client = Arc::clone(&client);
            let semaphore = Arc::clone(&semaphore);
            let base_url = self.base_url.clone();
            let key = key.clone();
            requests.push(async move {
                let _permit = semaphore
                    .acquire(&cx, 1)
                    .await
                    .map_err(OpenRouterScanError::Acquire)?;
                fetch_activity_day(&cx, &client, &base_url, &key, date).await
            });
        }

        while let Some(result) = requests.next().await {
            let rows = match result {
                Ok(rows) => rows,
                Err(OpenRouterScanError::Acquire(AcquireError::Cancelled)) => {
                    return Outcome::Cancelled(
                        cx.cancel_reason().unwrap_or_else(CancelReason::shutdown),
                    );
                }
                Err(OpenRouterScanError::Acquire(error)) => {
                    return Outcome::Err(ReaderError::new(format!(
                        "openrouter semaphore: {error}"
                    )));
                }
                Err(OpenRouterScanError::Http(error)) if error.is_cancelled() => {
                    return Outcome::Cancelled(
                        cx.cancel_reason().unwrap_or_else(CancelReason::shutdown),
                    );
                }
                Err(OpenRouterScanError::Http(error)) => {
                    let mapped = classify_openrouter_network_error(&error);
                    return Outcome::Err(ReaderError::new(mapped.message));
                }
                Err(OpenRouterScanError::Api(error)) => {
                    return Outcome::Err(ReaderError::new(error.message));
                }
                Err(OpenRouterScanError::Json(error)) => {
                    return Outcome::Err(ReaderError::new(error));
                }
            };

            for event in rows {
                match sink.send(cx, event).await {
                    Ok(()) => {}
                    Err(SinkError::Cancelled) => {
                        return Outcome::Cancelled(
                            cx.cancel_reason().unwrap_or_else(CancelReason::shutdown),
                        );
                    }
                    Err(err) => return Outcome::Err(ReaderError::new(err.to_string())),
                }
            }
        }

        Outcome::ok(())
    }

    fn cache_strategy(&self) -> CacheStrategy {
        CacheStrategy::NeverCache
    }
}

#[derive(Debug)]
enum OpenRouterScanError {
    Acquire(AcquireError),
    Http(ClientError),
    Api(OpenRouterError),
    Json(String),
}

async fn fetch_activity_day(
    cx: &Cx,
    client: &HttpClient,
    base_url: &str,
    key: &str,
    date: UtcDate,
) -> Result<Vec<UsageEvent>, OpenRouterScanError> {
    let url = format!(
        "{}/api/v1/activity?date={}",
        base_url.trim_end_matches('/'),
        date.ymd()
    );
    let response = client
        .request(
            cx,
            Method::Get,
            &url,
            vec![("Authorization".into(), format!("Bearer {key}"))],
            Vec::new(),
        )
        .await
        .map_err(OpenRouterScanError::Http)?;

    if let Some(error) = classify_openrouter_response(key, &response) {
        return Err(OpenRouterScanError::Api(error));
    }

    let payload: ActivityResponse = response.json().map_err(|error| {
        OpenRouterScanError::Json(format!(
            "parsing OpenRouter activity for {}: {error}",
            date.ymd()
        ))
    })?;

    Ok(payload
        .data
        .into_iter()
        .map(|row| row.into_usage_event(date))
        .collect())
}

fn default_config_path() -> Option<PathBuf> {
    env::var("HOME").ok().map(|home| {
        PathBuf::from(home)
            .join(".config")
            .join("reckon")
            .join("config.toml")
    })
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

fn key_from_config(path: Option<&Path>) -> Option<String> {
    let path = path?;
    let content = fs::read_to_string(path).ok()?;
    let cfg: ConfigFile = toml::from_str(&content).ok()?;
    cfg.openrouter?.key.filter(|k| !k.is_empty())
}

fn date_window(today: UtcDate, days: usize) -> Vec<UtcDate> {
    let start = today.days_since_epoch - i64::try_from(days).expect("days overflow") + 1;
    (0..days)
        .map(|offset| {
            UtcDate::from_days_since_epoch(start + i64::try_from(offset).expect("offset overflow"))
        })
        .collect()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct UtcDate {
    year: i32,
    month: u8,
    day: u8,
    days_since_epoch: i64,
}

impl UtcDate {
    const fn from_epoch_seconds(seconds: i64) -> Self {
        Self::from_days_since_epoch(seconds.div_euclid(86_400))
    }

    const fn from_days_since_epoch(days_since_epoch: i64) -> Self {
        let (year, month, day) = civil_from_days(days_since_epoch);
        Self {
            year,
            month,
            day,
            days_since_epoch,
        }
    }

    fn ymd(self) -> String {
        format!("{:04}-{:02}-{:02}", self.year, self.month, self.day)
    }

    fn year_month(self) -> YearMonth {
        YearMonth::new(self.year, self.month)
    }
}

#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "UTC civil-date conversion uses bounded calendrical arithmetic"
)]
const fn civil_from_days(days_since_epoch: i64) -> (i32, u8, u8) {
    let z = days_since_epoch + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u32;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    let year = y + if month <= 2 { 1 } else { 0 };
    (year as i32, month as u8, day as u8)
}

#[derive(Deserialize)]
struct ConfigFile {
    openrouter: Option<OpenRouterConfig>,
}

#[derive(Deserialize)]
struct OpenRouterConfig {
    key: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ActivityResponse {
    #[serde(default)]
    data: Vec<ActivityRow>,
}

#[derive(Debug, Deserialize)]
struct ActivityRow {
    model: String,
    provider_name: String,
    #[serde(default)]
    prompt_tokens: u64,
    #[serde(default)]
    completion_tokens: u64,
    #[serde(default)]
    reasoning_tokens: u64,
    #[serde(default)]
    usage: Option<f64>,
    #[serde(default)]
    byok_usage_inference: Option<bool>,
    endpoint_id: EndpointId,
}

impl ActivityRow {
    fn into_usage_event(self, date: UtcDate) -> UsageEvent {
        UsageEvent {
            source: Source::OpenRouter,
            month: date.year_month(),
            model: ModelSlug::new(self.model),
            provider: self.provider_name,
            project: None,
            tokens: TokenCounts {
                input: self.prompt_tokens,
                output: self.completion_tokens,
                cache_read: 0,
                cache_write: 0,
                reasoning: self.reasoning_tokens,
            },
            dedup_key: format!("openrouter:{}:{}", date.ymd(), self.endpoint_id.0),
            known_cost_usd: self.usage,
            byok_usage_inference: self.byok_usage_inference,
        }
    }
}

#[derive(Debug)]
struct EndpointId(String);

impl<'de> Deserialize<'de> for EndpointId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        struct EndpointIdVisitor;

        impl Visitor<'_> for EndpointIdVisitor {
            type Value = EndpointId;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("a string or number endpoint id")
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(EndpointId(value.to_string()))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(EndpointId(value))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(EndpointId(value.to_string()))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(EndpointId(value.to_string()))
            }
        }

        deserializer.deserialize_any(EndpointIdVisitor)
    }
}

#[derive(Deserialize)]
struct CreditsResponse {
    data: CreditsData,
}

#[derive(Deserialize)]
struct CreditsData {
    total_credits: f64,
    total_usage: f64,
}

/// Fetch the current `OpenRouter` credit balance via the Management API.
///
/// # Errors
///
/// Returns an error on HTTP failures or if the key is rejected.
pub fn fetch_balance() -> Result<Option<OpenRouterSummary>, Box<dyn std::error::Error>> {
    let Some(key) = resolve_key() else {
        return Ok(None);
    };

    let url = "https://openrouter.ai/api/v1/credits";

    let response = minreq::get(url)
        .with_header("Authorization", format!("Bearer {key}"))
        .send()?;

    if response.status_code == 401 {
        return Err(management_api_key_error(&key).into());
    }

    if response.status_code < 200 || response.status_code >= 300 {
        return Err(format!("OpenRouter /credits returned {}", response.status_code).into());
    }

    let text = response.as_str()?;
    let parsed: CreditsResponse = serde_json::from_str(text)?;

    let now = SystemTime::now();
    let duration = now.duration_since(SystemTime::UNIX_EPOCH)?;
    let secs = duration.as_secs();
    let nanos = duration.subsec_nanos();
    let ts_str = format!("{secs}.{nanos:09}Z");

    Ok(Some(OpenRouterSummary {
        total_credits: parsed.data.total_credits,
        total_usage: parsed.data.total_usage,
        fetched_at: ts_str,
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::io::Write as _;

    use asupersync::runtime::RuntimeBuilder;
    use tempfile::NamedTempFile;

    use super::*;
    use crate::run_readers;

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
        assert_eq!(
            resolve_key_inner(get, None),
            Some("sk-or-testkey1234".into())
        );
    }

    #[test]
    fn reckon_key_wins_over_openrouter_key() {
        let get = env_from(&[
            ("RECKON_OPENROUTER_KEY", "sk-or-reckon9999"),
            ("OPENROUTER_API_KEY", "sk-or-generic0000"),
        ]);
        assert_eq!(
            resolve_key_inner(get, None),
            Some("sk-or-reckon9999".into())
        );
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
        let get = env_from(&[
            ("RECKON_OPENROUTER_KEY", ""),
            ("OPENROUTER_API_KEY", "sk-or-nonempty"),
        ]);
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

    #[test]
    fn date_window_covers_last_thirty_days_inclusive() {
        let dates = date_window(UtcDate::from_days_since_epoch(20_000), 30);
        assert_eq!(dates.len(), 30);
        assert_eq!(dates.first().expect("first").days_since_epoch, 19_971);
        assert_eq!(dates.last().expect("last").days_since_epoch, 20_000);
    }

    #[test]
    fn reader_emits_activity_rows_for_last_thirty_days_and_preserves_extra_fields() {
        let today = UtcDate::from_days_since_epoch(20_000);
        let dates = date_window(today, OPENROUTER_ACTIVITY_DAYS);
        let mut fixtures = HashMap::new();
        for (index, date) in dates.iter().enumerate() {
            let body = if index == 7 {
                "{\"data\":[]}".to_string()
            } else {
                format!(
                    "{{\"data\":[{{\"model\":\"google/gemini-2.5-pro\",\"provider_name\":\"openrouter\",\"prompt_tokens\":{},\"completion_tokens\":{},\"reasoning_tokens\":{},\"usage\":0.5,\"byok_usage_inference\":true,\"endpoint_id\":\"ep-{}\"}}]}}",
                    index + 1,
                    index + 2,
                    index + 3,
                    index
                )
            };
            fixtures.insert(date.ymd(), body);
        }

        let server = StubServer::start(fixtures, None);
        let reader = OpenRouterReader::with_base_url_key_and_today(
            server.base_url(),
            "sk-or-test1234",
            today,
        );

        let events = run_reader(reader);
        assert_eq!(events.len(), 29);

        let first = events
            .iter()
            .find(|event| event.dedup_key.ends_with(":ep-0"))
            .expect("first event");
        assert_eq!(first.source, Source::OpenRouter);
        assert_eq!(first.month, dates[0].year_month());
        assert_eq!(first.model.as_str(), "google/gemini-2.5-pro");
        assert_eq!(first.provider, "openrouter");
        assert_eq!(first.tokens.input, 1);
        assert_eq!(first.tokens.output, 2);
        assert_eq!(first.tokens.reasoning, 3);
        assert_eq!(first.tokens.cache_read, 0);
        assert_eq!(first.tokens.cache_write, 0);
        assert_eq!(first.known_cost_usd, Some(0.5));
        assert_eq!(first.byok_usage_inference, Some(true));
        assert_eq!(
            first.dedup_key,
            format!("openrouter:{}:ep-0", dates[0].ymd())
        );

        let empty_date = dates[7].ymd();
        assert!(
            events
                .iter()
                .all(|event| !event.dedup_key.contains(&empty_date))
        );
    }

    #[test]
    fn reader_limits_activity_requests_to_four_in_flight() {
        let today = UtcDate::from_days_since_epoch(20_123);
        let fixtures = date_window(today, OPENROUTER_ACTIVITY_DAYS)
            .into_iter()
            .map(|date| {
                (
                    date.ymd(),
                    "{\"data\":[{\"model\":\"google/gemini-2.5-pro\",\"provider_name\":\"openrouter\",\"prompt_tokens\":1,\"completion_tokens\":1,\"reasoning_tokens\":0,\"usage\":0.1,\"byok_usage_inference\":false,\"endpoint_id\":\"ep\"}]}".to_string(),
                )
            })
            .collect();
        let server = StubServer::start(fixtures, Some(OPENROUTER_MAX_IN_FLIGHT));
        let reader = OpenRouterReader::with_base_url_key_and_today(
            server.base_url(),
            "sk-or-test1234",
            today,
        );

        let events = run_reader(reader);
        assert_eq!(events.len(), OPENROUTER_ACTIVITY_DAYS);
        assert_eq!(server.max_active(), OPENROUTER_MAX_IN_FLIGHT);
        assert!(server.max_active() <= OPENROUTER_MAX_IN_FLIGHT);
    }

    fn run_reader(reader: OpenRouterReader) -> Vec<UsageEvent> {
        let runtime = RuntimeBuilder::new().build().expect("runtime");
        let handle = runtime.handle();
        let join = handle.spawn(async move {
            let cx = Cx::current().expect("cx");
            run_readers(&cx, vec![Box::new(reader)]).await
        });
        runtime.block_on(join)
    }

    struct StubServer {
        addr: SocketAddr,
        max_active: Arc<AtomicUsize>,
        shutdown: Arc<AtomicBool>,
        thread: Option<thread::JoinHandle<()>>,
    }

    impl StubServer {
        fn start(fixtures: HashMap<String, String>, gate_target: Option<usize>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind stub server");
            listener
                .set_nonblocking(true)
                .expect("set nonblocking listener");
            let addr = listener.local_addr().expect("local addr");
            let fixtures = Arc::new(fixtures);
            let shutdown = Arc::new(AtomicBool::new(false));
            let active = Arc::new(AtomicUsize::new(0));
            let max_active = Arc::new(AtomicUsize::new(0));
            let gate = Arc::new(Gate::new(gate_target));
            let thread_shutdown = Arc::clone(&shutdown);
            let thread_fixtures = Arc::clone(&fixtures);
            let thread_active = Arc::clone(&active);
            let thread_max_active = Arc::clone(&max_active);
            let thread_gate = Arc::clone(&gate);
            let thread = thread::spawn(move || {
                while !thread_shutdown.load(Ordering::SeqCst) {
                    match listener.accept() {
                        Ok((stream, _)) => {
                            let fixtures = Arc::clone(&thread_fixtures);
                            let active = Arc::clone(&thread_active);
                            let max_active = Arc::clone(&thread_max_active);
                            let gate = Arc::clone(&thread_gate);
                            thread::spawn(move || {
                                handle_connection(stream, &fixtures, &active, &max_active, &gate);
                            });
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(_) => break,
                    }
                }
            });

            Self {
                addr,
                max_active,
                shutdown,
                thread: Some(thread),
            }
        }

        fn base_url(&self) -> String {
            format!("http://{}", self.addr)
        }

        fn max_active(&self) -> usize {
            self.max_active.load(Ordering::SeqCst)
        }
    }

    impl Drop for StubServer {
        fn drop(&mut self) {
            self.shutdown.store(true, Ordering::SeqCst);
            let _ = TcpStream::connect(self.addr);
            if let Some(thread) = self.thread.take() {
                thread.join().expect("join stub server thread");
            }
        }
    }

    struct Gate {
        target: Option<usize>,
        state: Mutex<bool>,
        cvar: Condvar,
    }

    impl Gate {
        fn new(target: Option<usize>) -> Self {
            Self {
                target,
                state: Mutex::new(false),
                cvar: Condvar::new(),
            }
        }

        fn wait_if_needed(&self, active: usize) {
            let Some(target) = self.target else { return };
            let mut reached = self.state.lock().expect("gate mutex poisoned");
            if !*reached && active >= target {
                *reached = true;
                self.cvar.notify_all();
                return;
            }
            if !*reached {
                let _ = self
                    .cvar
                    .wait_timeout(reached, Duration::from_millis(250))
                    .expect("gate wait");
            }
        }
    }

    fn handle_connection(
        mut stream: TcpStream,
        fixtures: &HashMap<String, String>,
        active: &AtomicUsize,
        max_active: &AtomicUsize,
        gate: &Gate,
    ) {
        let current = active.fetch_add(1, Ordering::SeqCst) + 1;
        let _ = max_active.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |prev| {
            (current > prev).then_some(current)
        });
        gate.wait_if_needed(current);

        let result = (|| -> std::io::Result<()> {
            let mut reader = BufReader::new(stream.try_clone()?);
            let mut request_line = String::new();
            reader.read_line(&mut request_line)?;
            let path = request_line
                .split_whitespace()
                .nth(1)
                .unwrap_or("/")
                .to_string();
            loop {
                let mut line = String::new();
                reader.read_line(&mut line)?;
                if line == "\r\n" || line.is_empty() {
                    break;
                }
            }

            let body = extract_date(&path)
                .and_then(|date| fixtures.get(&date).cloned())
                .unwrap_or_else(|| "{\"data\":[]}".to_string());
            let response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(response.as_bytes())?;
            stream.flush()?;
            let mut drain = [0_u8; 64];
            let _ = stream.read(&mut drain);
            Ok(())
        })();

        active.fetch_sub(1, Ordering::SeqCst);
        if result.is_err() {
            let _ = stream.shutdown(std::net::Shutdown::Both);
        }
    }

    fn extract_date(path: &str) -> Option<String> {
        path.split("date=").nth(1).map(|s| s.to_string())
    }
}
