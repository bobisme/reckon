use std::collections::{BTreeMap, HashMap, HashSet};

use reckon_core::{
    cost, ModelSlug, OpenRouterSummary, Pricing, Source, TokenCounts, UsageEvent, YearMonth,
};
use serde_json::Value;
use tabled::builder::Builder;
use tabled::settings::object::Columns;
use tabled::settings::{Alignment, Style};

use crate::report::{month_totals, AggregateKey, BySpec, Dimension};

const ANSI_COLOR_RESET: &str = "\x1b[0m";
const ANSI_BLUE: &str = "\x1b[36m";
const ANSI_GREEN: &str = "\x1b[32m";

fn colorize(text: &str, ansi_code: &str, enabled: bool) -> String {
    if enabled {
        format!("{ansi_code}{text}{ANSI_COLOR_RESET}")
    } else {
        text.to_string()
    }
}

pub fn format_table(
    aggregated: &BTreeMap<AggregateKey, TokenCounts>,
    pricing: &HashMap<ModelSlug, Pricing>,
    balance: Option<&OpenRouterSummary>,
    by: &BySpec,
) -> String {
    let totals = month_totals(aggregated);
    let months: Vec<YearMonth> = totals.keys().copied().collect();

    let mut header: Vec<String> = vec!["Month".into()];
    if by.has(&Dimension::Source) {
        header.push("Source".into());
    }
    if by.has(&Dimension::Model) {
        header.push("Model".into());
    }
    if by.has(&Dimension::Provider) {
        header.push("Provider".into());
    }
    if by.has(&Dimension::Project) {
        header.push("Project".into());
    }
    let num_dim_cols = header.len() - 1;
    let token_col_start = header.len();
    header.extend(["In", "Out", "Cache", "Reason", "Cost"].map(String::from));
    let total_cols = header.len();

    let mut builder = Builder::default();
    builder.push_record(header);

    for month in months.iter().rev() {
        let mut month_entries: Vec<_> = aggregated
            .iter()
            .filter(|(key, _)| key.month == *month)
            .collect();
        month_entries.sort_by_key(|(key, _)| (*key).clone());

        for (key, tokens) in &month_entries {
            let model_cost = key
                .model
                .as_ref()
                .and_then(|m| pricing.get(m))
                .map_or(0.0, |p| cost(tokens, p));
            builder.push_record(build_data_row(by, *month, key, tokens, model_cost));
        }

        let total_tokens = &totals[month];
        let total_cost: f64 = month_entries
            .iter()
            .map(|(key, tokens)| {
                key.model
                    .as_ref()
                    .and_then(|m| pricing.get(m))
                    .map_or(0.0, |p| cost(tokens, p))
            })
            .sum();
        builder.push_record(build_total_row(*month, num_dim_cols, total_tokens, total_cost));
    }

    let mut table = builder.build();
    table.with(Style::blank());
    for col in token_col_start..total_cols {
        table.modify(Columns::single(col), Alignment::right());
    }

    let mut out = table.to_string();
    if let Some(summary) = balance {
        out.push('\n');
        out.push_str(&fmt_balance(summary));
    }
    out
}

pub fn print_table(
    aggregated: &BTreeMap<AggregateKey, TokenCounts>,
    pricing: &HashMap<ModelSlug, Pricing>,
    balance: Option<&OpenRouterSummary>,
    use_color: bool,
    by: &BySpec,
) {
    let table_str = format_table(aggregated, pricing, None, by);
    println!("{}", colorize(&table_str, ANSI_BLUE, use_color));
    if let Some(summary) = balance {
        println!(
            "{}",
            colorize(&fmt_balance(summary), ANSI_GREEN, use_color)
        );
    }
}

fn build_data_row(
    by: &BySpec,
    month: YearMonth,
    key: &AggregateKey,
    tokens: &TokenCounts,
    cost_usd: f64,
) -> Vec<String> {
    let mut row = vec![month.to_string()];
    if by.has(&Dimension::Source) {
        row.push(key.source.as_ref().map_or_else(String::new, ToString::to_string));
    }
    if by.has(&Dimension::Model) {
        row.push(key.model.as_ref().map_or_else(String::new, ToString::to_string));
    }
    if by.has(&Dimension::Provider) {
        row.push(key.provider.as_deref().unwrap_or("").to_string());
    }
    if by.has(&Dimension::Project) {
        row.push(key.project.as_deref().unwrap_or("").to_string());
    }
    row.push(fmt_thousands(tokens.input));
    row.push(fmt_thousands(tokens.output));
    row.push(fmt_thousands(tokens.cache_read));
    row.push(fmt_thousands(tokens.reasoning));
    row.push(fmt_cost(cost_usd));
    row
}

fn build_total_row(
    month: YearMonth,
    num_dim_cols: usize,
    tokens: &TokenCounts,
    cost_usd: f64,
) -> Vec<String> {
    let mut row = vec![month.to_string()];
    for i in 0..num_dim_cols {
        row.push(if i == 0 { "TOTAL".into() } else { String::new() });
    }
    row.push(fmt_thousands(tokens.input));
    row.push(fmt_thousands(tokens.output));
    row.push(fmt_thousands(tokens.cache_read));
    row.push(fmt_thousands(tokens.reasoning));
    row.push(fmt_cost(cost_usd));
    row
}

fn fmt_thousands(n: u64) -> String {
    if n == 0 {
        return "0".into();
    }
    let s = n.to_string();
    let mut result = String::with_capacity(s.len() + s.len() / 3);
    for (i, ch) in s.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.push(',');
        }
        result.push(ch);
    }
    result.chars().rev().collect()
}

fn fmt_cost(usd: f64) -> String {
    format!("${usd:.2}")
}

fn fmt_balance(summary: &OpenRouterSummary) -> String {
    let remaining = summary.total_credits - summary.total_usage;
    format!(
        "OpenRouter balance: ${:.2} (used ${:.2} of ${:.2} purchased)",
        remaining, summary.total_usage, summary.total_credits
    )
}

#[derive(Default)]
struct JsonAggregate {
    tokens: TokenCounts,
    provider: String,
    project: String,
    known_cost_usd: Option<f64>,
}

fn aggregate_for_json(
    events: &[UsageEvent],
    by: &BySpec,
) -> (
    BTreeMap<AggregateKey, JsonAggregate>,
    BTreeMap<YearMonth, TokenCounts>,
) {
    let mut seen = HashSet::new();
    let mut aggregated: BTreeMap<AggregateKey, JsonAggregate> = BTreeMap::new();
    let mut totals: BTreeMap<YearMonth, TokenCounts> = BTreeMap::new();

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
        let bucket = aggregated.entry(key).or_default();
        bucket.tokens += event.tokens;
        if bucket.provider.is_empty() {
            bucket.provider.clone_from(&event.provider);
        }
        if bucket.project.is_empty() {
            bucket.project = event.project.clone().unwrap_or_default();
        }

        if event.source == Source::OpenRouter
            && let Some(known) = event.known_cost_usd
        {
            bucket.known_cost_usd = Some(bucket.known_cost_usd.unwrap_or(0.0) + known);
        }

        *totals.entry(event.month).or_default() += event.tokens;
    }

    (aggregated, totals)
}

fn insert_token_fields(obj: &mut serde_json::Map<String, Value>, tokens: &TokenCounts, cost_usd: f64) {
    obj.insert("input".into(), Value::Number(tokens.input.into()));
    obj.insert("output".into(), Value::Number(tokens.output.into()));
    obj.insert("cache_read".into(), Value::Number(tokens.cache_read.into()));
    obj.insert("cache_write".into(), Value::Number(tokens.cache_write.into()));
    obj.insert("reasoning".into(), Value::Number(tokens.reasoning.into()));
    obj.insert(
        "cost_usd".into(),
        serde_json::Number::from_f64(cost_usd).map_or(Value::Null, Value::Number),
    );
}

fn build_json_data_row(
    by: &BySpec,
    month: YearMonth,
    key: &AggregateKey,
    bucket: &JsonAggregate,
    model_cost: f64,
) -> Value {
    let mut obj = serde_json::Map::new();
    obj.insert("month".into(), Value::String(month.to_string()));
    if by.has(&Dimension::Source) {
        obj.insert(
            "source".into(),
            key.source.as_ref().map_or(Value::Null, |s| Value::String(s.to_string())),
        );
    }
    if by.has(&Dimension::Model) {
        obj.insert(
            "model".into(),
            key.model.as_ref().map_or(Value::Null, |m| Value::String(m.to_string())),
        );
    }
    if by.has(&Dimension::Provider) {
        obj.insert("provider".into(), Value::String(bucket.provider.clone()));
    }
    if by.has(&Dimension::Project) {
        obj.insert("project".into(), Value::String(bucket.project.clone()));
    }
    insert_token_fields(&mut obj, &bucket.tokens, model_cost);
    if let Some(known) = bucket.known_cost_usd {
        obj.insert(
            "known_cost_usd".into(),
            serde_json::Number::from_f64(known).map_or(Value::Null, Value::Number),
        );
    }
    Value::Object(obj)
}

pub fn format_json(
    events: &[UsageEvent],
    pricing: &HashMap<ModelSlug, Pricing>,
    balance: Option<&OpenRouterSummary>,
    by: &BySpec,
) -> String {
    let (aggregated, totals) = aggregate_for_json(events, by);
    let months: Vec<YearMonth> = totals.keys().copied().collect();
    let mut rows: Vec<Value> = Vec::new();

    for month in months.iter().rev() {
        let mut month_entries: Vec<_> = aggregated
            .iter()
            .filter(|(key, _)| key.month == *month)
            .collect();
        month_entries.sort_by_key(|(key, _)| (*key).clone());

        for (key, bucket) in &month_entries {
            let model_cost = key
                .model
                .as_ref()
                .and_then(|m| pricing.get(m))
                .map_or(0.0, |p| cost(&bucket.tokens, p));
            rows.push(build_json_data_row(by, *month, key, bucket, model_cost));
        }

        let total_tokens = &totals[month];
        let total_cost: f64 = month_entries
            .iter()
            .map(|(key, bucket)| {
                key.model
                    .as_ref()
                    .and_then(|m| pricing.get(m))
                    .map_or(0.0, |p| cost(&bucket.tokens, p))
            })
            .sum();

        let mut total_obj = serde_json::Map::new();
        total_obj.insert("month".into(), Value::String(month.to_string()));
        total_obj.insert("source".into(), Value::String("TOTAL".into()));
        insert_token_fields(&mut total_obj, total_tokens, total_cost);
        rows.push(Value::Object(total_obj));
    }

    if let Some(summary) = balance {
        rows.push(serde_json::json!({"openrouter_balance": summary}));
    }

    serde_json::to_string_pretty(&rows).expect("json serialization")
}

pub fn print_json(
    events: &[UsageEvent],
    pricing: &HashMap<ModelSlug, Pricing>,
    balance: Option<&OpenRouterSummary>,
    by: &BySpec,
) {
    println!("{}", format_json(events, pricing, balance, by));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::report::aggregate;

    #[test]
    fn fmt_thousands_cases() {
        assert_eq!(fmt_thousands(0), "0");
        assert_eq!(fmt_thousands(999), "999");
        assert_eq!(fmt_thousands(1_000), "1,000");
        assert_eq!(fmt_thousands(1_234_567), "1,234,567");
    }

    #[test]
    fn fmt_cost_cases() {
        assert_eq!(fmt_cost(0.0), "$0.00");
        assert_eq!(fmt_cost(42.183), "$42.18");
        assert_eq!(fmt_cost(0.005), "$0.01");
    }

    #[test]
    fn balance_line_format() {
        let summary = OpenRouterSummary {
            total_credits: 100.0,
            total_usage: 25.5,
            fetched_at: "2026-05-11T12:00:00Z".to_string(),
        };
        assert_eq!(
            fmt_balance(&summary),
            "OpenRouter balance: $74.50 (used $25.50 of $100.00 purchased)"
        );
    }

    fn fixture_events() -> Vec<UsageEvent> {
        vec![
            UsageEvent {
                source: Source::Claude,
                month: YearMonth::new(2026, 4),
                model: ModelSlug::new("anthropic/claude-opus-4.7"),
                provider: "anthropic".into(),
                project: None,
                tokens: TokenCounts { input: 50_000, output: 12_000, cache_read: 80_000, cache_write: 5_000, reasoning: 0 },
                dedup_key: "claude-apr-1".into(),
                known_cost_usd: None,
                byok_usage_inference: None,
            },
            UsageEvent {
                source: Source::Claude,
                month: YearMonth::new(2026, 5),
                model: ModelSlug::new("anthropic/claude-sonnet-4.6"),
                provider: "anthropic".into(),
                project: None,
                tokens: TokenCounts { input: 120_000, output: 30_000, cache_read: 200_000, cache_write: 15_000, reasoning: 0 },
                dedup_key: "claude-may-1".into(),
                known_cost_usd: None,
                byok_usage_inference: None,
            },
            UsageEvent {
                source: Source::Codex,
                month: YearMonth::new(2026, 4),
                model: ModelSlug::new("openai/codex-mini"),
                provider: "openai".into(),
                project: None,
                tokens: TokenCounts { input: 10_000, output: 3_000, cache_read: 0, cache_write: 0, reasoning: 5_000 },
                dedup_key: "codex-apr-1".into(),
                known_cost_usd: None,
                byok_usage_inference: None,
            },
            UsageEvent {
                source: Source::Gemini,
                month: YearMonth::new(2026, 5),
                model: ModelSlug::new("google/gemini-2.5-pro"),
                provider: "google".into(),
                project: None,
                tokens: TokenCounts { input: 75_000, output: 20_000, cache_read: 0, cache_write: 0, reasoning: 40_000 },
                dedup_key: "gemini-may-1".into(),
                known_cost_usd: None,
                byok_usage_inference: None,
            },
            UsageEvent {
                source: Source::Pi,
                month: YearMonth::new(2026, 4),
                model: ModelSlug::new("anthropic/claude-sonnet-4.6"),
                provider: "anthropic".into(),
                project: None,
                tokens: TokenCounts { input: 8_000, output: 2_500, cache_read: 15_000, cache_write: 1_000, reasoning: 0 },
                dedup_key: "pi-apr-1".into(),
                known_cost_usd: None,
                byok_usage_inference: None,
            },
            UsageEvent {
                source: Source::OpenCode,
                month: YearMonth::new(2026, 5),
                model: ModelSlug::new("anthropic/claude-sonnet-4.6"),
                provider: "anthropic".into(),
                project: None,
                tokens: TokenCounts { input: 45_000, output: 11_000, cache_read: 60_000, cache_write: 4_000, reasoning: 0 },
                dedup_key: "opencode-may-1".into(),
                known_cost_usd: None,
                byok_usage_inference: None,
            },
            UsageEvent {
                source: Source::OpenRouter,
                month: YearMonth::new(2026, 5),
                model: ModelSlug::new("anthropic/claude-opus-4.7"),
                provider: "anthropic".into(),
                project: None,
                tokens: TokenCounts { input: 25_000, output: 8_000, cache_read: 0, cache_write: 0, reasoning: 0 },
                dedup_key: "openrouter-may-1".into(),
                known_cost_usd: Some(1.65),
                byok_usage_inference: None,
            },
        ]
    }

    fn fixture_pricing() -> HashMap<ModelSlug, Pricing> {
        let mut p = HashMap::new();
        p.insert(ModelSlug::new("anthropic/claude-opus-4.7"), Pricing {
            input_per_token: 0.000_015,
            output_per_token: 0.000_075,
            cache_read_per_token: 0.000_001_5,
            cache_write_per_token: 0.000_018_75,
            reasoning_per_token: None,
        });
        p.insert(ModelSlug::new("anthropic/claude-sonnet-4.6"), Pricing {
            input_per_token: 0.000_003,
            output_per_token: 0.000_015,
            cache_read_per_token: 0.000_000_3,
            cache_write_per_token: 0.000_003_75,
            reasoning_per_token: None,
        });
        p.insert(ModelSlug::new("google/gemini-2.5-pro"), Pricing {
            input_per_token: 0.000_001_25,
            output_per_token: 0.000_010,
            cache_read_per_token: 0.0,
            cache_write_per_token: 0.0,
            reasoning_per_token: Some(0.000_010),
        });
        p
    }

    #[test]
    fn snapshot_table_default() {
        let events = fixture_events();
        let pricing = fixture_pricing();
        let by = BySpec::default();
        let aggregated = aggregate(&events, &by);
        let out = format_table(&aggregated, &pricing, None, &by);
        insta::assert_snapshot!(out);
    }

    #[test]
    fn snapshot_table_with_balance() {
        let events = fixture_events();
        let pricing = fixture_pricing();
        let by = BySpec::default();
        let aggregated = aggregate(&events, &by);
        let balance = OpenRouterSummary {
            total_credits: 100.0,
            total_usage: 25.50,
            fetched_at: "2026-05-11T12:00:00Z".into(),
        };
        let out = format_table(&aggregated, &pricing, Some(&balance), &by);
        insta::assert_snapshot!(out);
    }

    #[test]
    fn snapshot_table_by_source() {
        let events = fixture_events();
        let pricing = fixture_pricing();
        let by = BySpec::parse("source").expect("valid");
        let aggregated = aggregate(&events, &by);
        let out = format_table(&aggregated, &pricing, None, &by);
        insta::assert_snapshot!(out);
    }

    #[test]
    fn snapshot_json_default() {
        let events = fixture_events();
        let pricing = fixture_pricing();
        let by = BySpec::default();
        let out = format_json(&events, &pricing, None, &by);
        insta::assert_snapshot!(out);
    }

    #[test]
    fn snapshot_json_with_balance() {
        let events = fixture_events();
        let pricing = fixture_pricing();
        let by = BySpec::default();
        let balance = OpenRouterSummary {
            total_credits: 100.0,
            total_usage: 25.50,
            fetched_at: "2026-05-11T12:00:00Z".into(),
        };
        let out = format_json(&events, &pricing, Some(&balance), &by);
        insta::assert_snapshot!(out);
    }

    #[test]
    fn snapshot_json_by_source() {
        let events = fixture_events();
        let pricing = fixture_pricing();
        let by = BySpec::parse("source").expect("valid");
        let out = format_json(&events, &pricing, None, &by);
        insta::assert_snapshot!(out);
    }
}
