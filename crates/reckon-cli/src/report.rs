use std::collections::{BTreeMap, HashMap, HashSet};
use std::str::FromStr;

use reckon_core::{cost, ModelSlug, Pricing, Source, TokenCounts, UsageEvent, YearMonth};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Dimension {
    Source,
    Model,
    Provider,
    Project,
}

impl FromStr for Dimension {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "source" => Ok(Self::Source),
            "model" => Ok(Self::Model),
            "provider" => Ok(Self::Provider),
            "project" => Ok(Self::Project),
            other => Err(format!(
                "unknown dimension '{other}'; valid: source, model, provider, project"
            )),
        }
    }
}

#[derive(Debug, Clone)]
pub struct BySpec(pub Vec<Dimension>);

impl BySpec {
    pub fn parse(s: &str) -> Result<Self, String> {
        let dims = s
            .split(',')
            .map(str::trim)
            .filter(|d| !d.is_empty())
            .map(str::parse::<Dimension>)
            .collect::<Result<Vec<_>, _>>()?;
        if dims.is_empty() {
            return Err("--by requires at least one dimension".into());
        }
        Ok(Self(dims))
    }

    pub fn has(&self, d: &Dimension) -> bool {
        self.0.contains(d)
    }

}

impl Default for BySpec {
    fn default() -> Self {
        Self(vec![Dimension::Source, Dimension::Model])
    }
}

#[derive(PartialEq, Eq, PartialOrd, Ord, Hash, Clone, Debug)]
pub struct AggregateKey {
    pub month: YearMonth,
    pub source: Option<Source>,
    pub model: Option<ModelSlug>,
    pub provider: Option<String>,
    pub project: Option<String>,
}

pub fn aggregate(events: &[UsageEvent], by: &BySpec) -> BTreeMap<AggregateKey, TokenCounts> {
    let mut seen = HashSet::new();
    let mut map: BTreeMap<AggregateKey, TokenCounts> = BTreeMap::new();
    for event in events {
        if !seen.insert(event.dedup_key.clone()) {
            continue;
        }
        let key = AggregateKey {
            month: event.month,
            source: by.has(&Dimension::Source).then_some(event.source),
            model: by.has(&Dimension::Model).then_some(event.model.clone()),
            provider: by.has(&Dimension::Provider).then(|| event.provider.clone()),
            project: by
                .has(&Dimension::Project)
                .then(|| event.project.clone().unwrap_or_default()),
        };
        *map.entry(key).or_default() += event.tokens;
    }
    map
}

/// Aggregate the per-row USD cost the same way `aggregate` sums tokens.
///
/// Cost depends on the model, so it must be computed per-event before the
/// model dimension is dropped from the aggregation key. Without this, any
/// `--by` spec that omits `model` (including the compact default with no
/// breakdown) would render every row as `$0.00`.
pub fn aggregate_cost(
    events: &[UsageEvent],
    by: &BySpec,
    pricing: &HashMap<ModelSlug, Pricing>,
) -> BTreeMap<AggregateKey, f64> {
    let mut seen = HashSet::new();
    let mut map: BTreeMap<AggregateKey, f64> = BTreeMap::new();
    for event in events {
        if !seen.insert(event.dedup_key.clone()) {
            continue;
        }
        let key = AggregateKey {
            month: event.month,
            source: by.has(&Dimension::Source).then_some(event.source),
            model: by.has(&Dimension::Model).then_some(event.model.clone()),
            provider: by.has(&Dimension::Provider).then(|| event.provider.clone()),
            project: by
                .has(&Dimension::Project)
                .then(|| event.project.clone().unwrap_or_default()),
        };
        let c = pricing.get(&event.model).map_or(0.0, |p| cost(&event.tokens, p));
        *map.entry(key).or_default() += c;
    }
    map
}

pub fn month_totals(map: &BTreeMap<AggregateKey, TokenCounts>) -> BTreeMap<YearMonth, TokenCounts> {
    let mut totals: BTreeMap<YearMonth, TokenCounts> = BTreeMap::new();
    for (key, tokens) in map {
        *totals.entry(key.month).or_default() += *tokens;
    }
    totals
}

pub fn unknown_model_slugs(
    aggregated: &BTreeMap<AggregateKey, TokenCounts>,
    pricing: &HashMap<ModelSlug, Pricing>,
) -> HashSet<ModelSlug> {
    aggregated
        .keys()
        .filter_map(|key| {
            key.model.as_ref().and_then(|model| {
                if pricing.contains_key(model) {
                    None
                } else {
                    Some(model.clone())
                }
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn event(month: u8, source: Source, model: &str, input: u64, dedup: &str) -> UsageEvent {
        UsageEvent {
            source,
            month: YearMonth::new(2026, month),
            timestamp_secs: 0,
            model: ModelSlug::new(model),
            provider: "test".into(),
            project: None,
            tokens: TokenCounts {
                input,
                output: 10,
                cache_read: 0,
                cache_write: 0,
                reasoning: 0,
            },
            dedup_key: dedup.into(),
            known_cost_usd: None,
            byok_usage_inference: None,
        }
    }

    fn default_by() -> BySpec {
        BySpec::default()
    }

    fn key(month: u8, source: Source, model: &str) -> AggregateKey {
        AggregateKey {
            month: YearMonth::new(2026, month),
            source: Some(source),
            model: Some(ModelSlug::new(model)),
            provider: None,
            project: None,
        }
    }

    #[test]
    fn dedup_by_key() {
        let events = vec![
            event(5, Source::Claude, "anthropic/claude-opus-4.7", 100, "req-1"),
            event(5, Source::Claude, "anthropic/claude-opus-4.7", 100, "req-1"),
        ];
        let agg = aggregate(&events, &default_by());
        assert_eq!(agg[&key(5, Source::Claude, "anthropic/claude-opus-4.7")].input, 100);
    }

    #[test]
    fn aggregate_sums_tokens() {
        let events = vec![
            event(5, Source::Claude, "anthropic/claude-opus-4.7", 100, "req-1"),
            event(5, Source::Claude, "anthropic/claude-opus-4.7", 200, "req-2"),
        ];
        let agg = aggregate(&events, &default_by());
        let k = key(5, Source::Claude, "anthropic/claude-opus-4.7");
        assert_eq!(agg[&k].input, 300);
        assert_eq!(agg[&k].output, 20);
    }

    #[test]
    fn month_totals_sum_correctly() {
        let events = vec![
            event(5, Source::Claude, "anthropic/claude-opus-4.7", 100, "req-1"),
            event(5, Source::Claude, "anthropic/claude-sonnet-4.6", 50, "req-2"),
            event(4, Source::Claude, "anthropic/claude-opus-4.7", 200, "req-3"),
        ];
        let agg = aggregate(&events, &default_by());
        let totals = month_totals(&agg);
        assert_eq!(totals[&YearMonth::new(2026, 5)].input, 150);
        assert_eq!(totals[&YearMonth::new(2026, 4)].input, 200);
    }

    #[test]
    fn unknown_model_slugs_collect_unknown_models() {
        let events = vec![
            event(5, Source::Claude, "known/model", 100, "req-1"),
            event(5, Source::Claude, "vendor/unknown-a", 100, "req-2"),
            event(5, Source::Claude, "vendor/unknown-b", 100, "req-3"),
        ];
        let agg = aggregate(&events, &default_by());

        let mut pricing = HashMap::new();
        pricing.insert(
            ModelSlug::new("known/model"),
            Pricing {
                input_per_token: 0.0,
                output_per_token: 0.0,
                cache_read_per_token: 0.0,
                cache_write_per_token: 0.0,
                reasoning_per_token: None,
            },
        );

        let unknown = unknown_model_slugs(&agg, &pricing);
        let mut expected = HashSet::new();
        expected.insert(ModelSlug::new("vendor/unknown-a"));
        expected.insert(ModelSlug::new("vendor/unknown-b"));

        assert_eq!(unknown, expected);
        assert_eq!(unknown.len(), 2);
    }

    #[test]
    fn by_source_collapses_model() {
        let by = BySpec::parse("source").expect("valid");
        let events = vec![
            event(5, Source::Claude, "anthropic/claude-opus-4.7", 100, "req-1"),
            event(5, Source::Claude, "anthropic/claude-sonnet-4.6", 200, "req-2"),
            event(5, Source::Gemini, "google/gemini-2.5-pro", 50, "req-3"),
        ];
        let agg = aggregate(&events, &by);
        assert_eq!(agg.len(), 2, "two sources, no model split");
        let claude_key = AggregateKey {
            month: YearMonth::new(2026, 5),
            source: Some(Source::Claude),
            model: None,
            provider: None,
            project: None,
        };
        assert_eq!(agg[&claude_key].input, 300);
    }

    #[test]
    fn by_model_collapses_source() {
        let by = BySpec::parse("model").expect("valid");
        let events = vec![
            event(5, Source::Claude, "anthropic/claude-opus-4.7", 100, "req-1"),
            event(5, Source::OpenRouter, "anthropic/claude-opus-4.7", 50, "req-2"),
        ];
        let agg = aggregate(&events, &by);
        assert_eq!(agg.len(), 1, "same model from two sources collapses");
        let model_key = AggregateKey {
            month: YearMonth::new(2026, 5),
            source: None,
            model: Some(ModelSlug::new("anthropic/claude-opus-4.7")),
            provider: None,
            project: None,
        };
        assert_eq!(agg[&model_key].input, 150);
    }

    #[test]
    fn parse_invalid_dimension_errors() {
        let err = BySpec::parse("foo").unwrap_err();
        assert!(err.contains("foo"), "error mentions the bad value");
        assert!(err.contains("valid"), "error mentions valid options");
    }

    #[test]
    fn parse_empty_string_errors() {
        assert!(BySpec::parse("").is_err());
    }

    #[test]
    fn parse_multi_dimension() {
        let by = BySpec::parse("source,model,project").expect("valid");
        assert!(by.has(&Dimension::Source));
        assert!(by.has(&Dimension::Model));
        assert!(by.has(&Dimension::Project));
        assert!(!by.has(&Dimension::Provider));
    }
}
