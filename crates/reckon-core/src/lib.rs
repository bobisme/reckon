pub mod model_map;
pub mod pricing;

pub use pricing::{Pricing, cost, load_pricing};

use std::fmt;
use std::ops::AddAssign;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Source {
    Claude,
    Codex,
    Gemini,
    Pi,
    OpenCode,
    OpenRouter,
}

impl fmt::Display for Source {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Claude => f.write_str("claude"),
            Self::Codex => f.write_str("codex"),
            Self::Gemini => f.write_str("gemini"),
            Self::Pi => f.write_str("pi"),
            Self::OpenCode => f.write_str("opencode"),
            Self::OpenRouter => f.write_str("openrouter"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct ModelSlug(pub String);

impl ModelSlug {
    #[must_use]
    pub fn new(s: impl Into<String>) -> Self {
        Self(s.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ModelSlug {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct YearMonth {
    pub year: i32,
    pub month: u8,
}

impl YearMonth {
    /// Create a `YearMonth` from year and month (1-12).
    ///
    /// # Panics
    ///
    /// Panics if `month` is not in 1..=12.
    #[must_use]
    pub fn new(year: i32, month: u8) -> Self {
        assert!((1..=12).contains(&month), "month must be 1-12, got {month}");
        Self { year, month }
    }

    #[must_use]
    pub fn from_utc(ts: i64) -> Self {
        const SECS_PER_DAY: i64 = 86_400;
        let days = ts.div_euclid(SECS_PER_DAY);
        // Civil date from days since Unix epoch (1970-01-01).
        // Algorithm from Howard Hinnant's `chrono`-compatible date library.
        let z = days + 719_468;
        let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
        let doe = (z - era * 146_097) as u32;
        let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
        let y = yoe as i64 + era * 400;
        let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
        let mp = (5 * doy + 2) / 153;
        let m = if mp < 10 { mp + 3 } else { mp - 9 };
        let y = if m <= 2 { y + 1 } else { y };
        #[expect(clippy::cast_possible_truncation)]
        Self {
            year: y as i32,
            month: m as u8,
        }
    }

    #[must_use]
    pub fn next(self) -> Self {
        if self.month == 12 {
            Self { year: self.year + 1, month: 1 }
        } else {
            Self { year: self.year, month: self.month + 1 }
        }
    }

    #[must_use]
    pub fn prev(self) -> Self {
        if self.month == 1 {
            Self { year: self.year - 1, month: 12 }
        } else {
            Self { year: self.year, month: self.month - 1 }
        }
    }
}

impl fmt::Display for YearMonth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:04}-{:02}", self.year, self.month)
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TokenCounts {
    pub input: u64,
    pub output: u64,
    pub cache_read: u64,
    pub cache_write: u64,
    pub reasoning: u64,
}

impl TokenCounts {
    #[must_use]
    pub fn total(&self) -> u64 {
        self.input + self.output + self.cache_read + self.cache_write + self.reasoning
    }
}

impl AddAssign for TokenCounts {
    fn add_assign(&mut self, rhs: Self) {
        self.input += rhs.input;
        self.output += rhs.output;
        self.cache_read += rhs.cache_read;
        self.cache_write += rhs.cache_write;
        self.reasoning += rhs.reasoning;
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UsageEvent {
    pub source: Source,
    pub month: YearMonth,
    pub model: ModelSlug,
    pub provider: String,
    pub project: Option<String>,
    pub tokens: TokenCounts,
    pub dedup_key: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn year_boundary_utc() {
        // 2025-12-31T23:59:59Z
        let ts_dec = 1_767_225_599;
        let ym_dec = YearMonth::from_utc(ts_dec);
        assert_eq!(ym_dec, YearMonth::new(2025, 12));

        // 2026-01-01T00:00:00Z
        let ts_jan = 1_767_225_600;
        let ym_jan = YearMonth::from_utc(ts_jan);
        assert_eq!(ym_jan, YearMonth::new(2026, 1));

        assert_ne!(ym_dec, ym_jan);
    }

    #[test]
    fn year_rollover_next_prev() {
        let dec = YearMonth::new(2025, 12);
        let jan = dec.next();
        assert_eq!(jan, YearMonth::new(2026, 1));
        assert_eq!(jan.prev(), dec);
    }

    #[test]
    fn usage_event_serde_roundtrip() {
        let event = UsageEvent {
            source: Source::Claude,
            month: YearMonth::new(2026, 5),
            model: ModelSlug::new("anthropic/claude-opus-4.7"),
            provider: "anthropic".into(),
            project: Some("my-project".into()),
            tokens: TokenCounts {
                input: 1000,
                output: 500,
                cache_read: 200,
                cache_write: 100,
                reasoning: 50,
            },
            dedup_key: "req-12345".into(),
        };

        let json = serde_json::to_string(&event).expect("serialize");
        let back: UsageEvent = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(event, back);
    }

    #[test]
    fn token_counts_total_and_add_assign() {
        let mut a = TokenCounts { input: 10, output: 20, cache_read: 5, cache_write: 3, reasoning: 2 };
        assert_eq!(a.total(), 40);

        let b = TokenCounts { input: 1, output: 2, cache_read: 3, cache_write: 4, reasoning: 5 };
        a += b;
        assert_eq!(a, TokenCounts { input: 11, output: 22, cache_read: 8, cache_write: 7, reasoning: 7 });
        assert_eq!(a.total(), 55);
    }

    #[test]
    fn source_display() {
        assert_eq!(Source::Claude.to_string(), "claude");
        assert_eq!(Source::OpenRouter.to_string(), "openrouter");
        assert_eq!(Source::OpenCode.to_string(), "opencode");
    }

    #[test]
    fn yearmonth_display() {
        assert_eq!(YearMonth::new(2026, 1).to_string(), "2026-01");
        assert_eq!(YearMonth::new(2026, 12).to_string(), "2026-12");
    }

    #[test]
    fn epoch_zero() {
        let ym = YearMonth::from_utc(0);
        assert_eq!(ym, YearMonth::new(1970, 1));
    }

    #[test]
    fn negative_timestamp() {
        // 1969-12-31T23:59:59Z
        let ym = YearMonth::from_utc(-1);
        assert_eq!(ym, YearMonth::new(1969, 12));
    }
}

#[cfg(test)]
mod proptest_roundtrip {
    use super::*;
    use proptest::prelude::*;

    fn arb_source() -> impl Strategy<Value = Source> {
        prop_oneof![
            Just(Source::Claude),
            Just(Source::Codex),
            Just(Source::Gemini),
            Just(Source::Pi),
            Just(Source::OpenCode),
            Just(Source::OpenRouter),
        ]
    }

    fn arb_yearmonth() -> impl Strategy<Value = YearMonth> {
        (1970..2100i32, 1..=12u8).prop_map(|(y, m)| YearMonth::new(y, m))
    }

    fn arb_token_counts() -> impl Strategy<Value = TokenCounts> {
        (any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>(), any::<u64>()).prop_map(|(i, o, cr, cw, r)| TokenCounts {
            input: i,
            output: o,
            cache_read: cr,
            cache_write: cw,
            reasoning: r,
        })
    }

    proptest! {
        #[test]
        fn usage_event_roundtrips(
            source in arb_source(),
            month in arb_yearmonth(),
            model in "[a-z0-9/._-]{1,40}",
            provider in "[a-z]{1,20}",
            project in proptest::option::of("[a-z0-9_-]{1,30}"),
            tokens in arb_token_counts(),
            dedup_key in "[a-z0-9:_-]{1,60}",
        ) {
            let event = UsageEvent {
                source,
                month,
                model: ModelSlug::new(model),
                provider,
                project,
                tokens,
                dedup_key,
            };
            let json = serde_json::to_string(&event).expect("serialize");
            let back: UsageEvent = serde_json::from_str(&json).expect("deserialize");
            prop_assert_eq!(event, back);
        }
    }
}
