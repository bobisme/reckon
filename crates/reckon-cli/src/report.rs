use std::collections::{BTreeMap, HashMap, HashSet};

use reckon_core::{ModelSlug, Pricing, Source, TokenCounts, UsageEvent, YearMonth};

pub type AggregateKey = (YearMonth, Source, ModelSlug);

pub fn aggregate(events: Vec<UsageEvent>) -> BTreeMap<AggregateKey, TokenCounts> {
    let mut seen = HashSet::new();
    let mut map: BTreeMap<AggregateKey, TokenCounts> = BTreeMap::new();
    for event in events {
        if !seen.insert(event.dedup_key) {
            continue;
        }
        *map.entry((event.month, event.source, event.model))
            .or_default() += event.tokens;
    }
    map
}

pub fn month_totals(map: &BTreeMap<AggregateKey, TokenCounts>) -> BTreeMap<YearMonth, TokenCounts> {
    let mut totals: BTreeMap<YearMonth, TokenCounts> = BTreeMap::new();
    for ((month, _, _), tokens) in map {
        *totals.entry(*month).or_default() += *tokens;
    }
    totals
}

pub fn unknown_model_slugs(
    aggregated: &BTreeMap<AggregateKey, TokenCounts>,
    pricing: &HashMap<ModelSlug, Pricing>,
) -> HashSet<ModelSlug> {
    aggregated
        .keys()
        .filter_map(|(_, _, model)| {
            if pricing.contains_key(model) {
                None
            } else {
                Some(model.clone())
            }
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
        }
    }

    #[test]
    fn dedup_by_key() {
        let events = vec![
            event(5, Source::Claude, "anthropic/claude-opus-4.7", 100, "req-1"),
            event(5, Source::Claude, "anthropic/claude-opus-4.7", 100, "req-1"),
        ];
        let agg = aggregate(events);
        let key = (
            YearMonth::new(2026, 5),
            Source::Claude,
            ModelSlug::new("anthropic/claude-opus-4.7"),
        );
        assert_eq!(agg[&key].input, 100);
    }

    #[test]
    fn aggregate_sums_tokens() {
        let events = vec![
            event(5, Source::Claude, "anthropic/claude-opus-4.7", 100, "req-1"),
            event(5, Source::Claude, "anthropic/claude-opus-4.7", 200, "req-2"),
        ];
        let agg = aggregate(events);
        let key = (
            YearMonth::new(2026, 5),
            Source::Claude,
            ModelSlug::new("anthropic/claude-opus-4.7"),
        );
        assert_eq!(agg[&key].input, 300);
        assert_eq!(agg[&key].output, 20);
    }

    #[test]
    fn month_totals_sum_correctly() {
        let events = vec![
            event(5, Source::Claude, "anthropic/claude-opus-4.7", 100, "req-1"),
            event(
                5,
                Source::Claude,
                "anthropic/claude-sonnet-4.6",
                50,
                "req-2",
            ),
            event(4, Source::Claude, "anthropic/claude-opus-4.7", 200, "req-3"),
        ];
        let agg = aggregate(events);
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
        let agg = aggregate(events);

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
}
