use std::collections::{BTreeMap, HashMap, HashSet};

use reckon_core::{
    ModelSlug, OpenRouterSummary, Pricing, Source, TokenCounts, UsageEvent, YearMonth, cost,
};
use serde::Serialize;
use tabled::settings::object::Columns;
use tabled::settings::{Alignment, Style};
use tabled::{Table, Tabled};

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

use crate::report::{AggregateKey, month_totals};

#[derive(Tabled)]
struct Row {
    #[tabled(rename = "Month")]
    month: String,
    #[tabled(rename = "Source")]
    source: String,
    #[tabled(rename = "Model")]
    model: String,
    #[tabled(rename = "In")]
    input: String,
    #[tabled(rename = "Out")]
    output: String,
    #[tabled(rename = "Cache")]
    cache: String,
    #[tabled(rename = "Reason")]
    reason: String,
    #[tabled(rename = "Cost")]
    cost_usd: String,
}

pub fn print_table(
    aggregated: &BTreeMap<AggregateKey, TokenCounts>,
    pricing: &HashMap<ModelSlug, Pricing>,
    balance: Option<&OpenRouterSummary>,
    use_color: bool,
) {
    let totals = month_totals(aggregated);
    let months: Vec<YearMonth> = totals.keys().copied().collect();

    let mut rows = Vec::new();
    for month in months.iter().rev() {
        let month_rows: Vec<_> = aggregated
            .range(
                (*month, Source::Claude, ModelSlug::new(""))
                    ..=(*month, Source::OpenRouter, ModelSlug::new("\u{10FFFF}")),
            )
            .collect();

        for ((_, source, model), tokens) in &month_rows {
            let model_cost = pricing.get(model).map_or(0.0, |p| cost(tokens, p));

            rows.push(Row {
                month: month.to_string(),
                source: source.to_string(),
                model: model.to_string(),
                input: fmt_thousands(tokens.input),
                output: fmt_thousands(tokens.output),
                cache: fmt_thousands(tokens.cache_read),
                reason: fmt_thousands(tokens.reasoning),
                cost_usd: fmt_cost(model_cost),
            });
        }

        let total_tokens = &totals[month];
        let total_cost: f64 = month_rows
            .iter()
            .map(|((_, _, model), tokens)| pricing.get(model).map_or(0.0, |p| cost(tokens, p)))
            .sum();

        rows.push(Row {
            month: month.to_string(),
            source: "TOTAL".into(),
            model: String::new(),
            input: fmt_thousands(total_tokens.input),
            output: fmt_thousands(total_tokens.output),
            cache: fmt_thousands(total_tokens.cache_read),
            reason: fmt_thousands(total_tokens.reasoning),
            cost_usd: fmt_cost(total_cost),
        });
    }

    let mut table = Table::new(rows);
    table
        .with(Style::blank())
        .modify(Columns::new(3..=6), Alignment::right())
        .modify(Columns::single(7), Alignment::right());

    println!("{}", colorize(&table.to_string(), ANSI_BLUE, use_color));

    if let Some(summary) = balance {
        println!(
            "{}",
            colorize(&fmt_balance(summary), ANSI_GREEN, use_color)
        );
    }
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

#[derive(Serialize)]
struct JsonRow {
    month: String,
    source: String,
    model: String,
    provider: String,
    project: String,
    input: u64,
    output: u64,
    cache_read: u64,
    cache_write: u64,
    reasoning: u64,
    cost_usd: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    known_cost_usd: Option<f64>,
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

        let key = (event.month, event.source, event.model.clone());
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

pub fn print_json(
    events: &[UsageEvent],
    pricing: &HashMap<ModelSlug, Pricing>,
    balance: Option<&OpenRouterSummary>,
) {
    let (aggregated, totals) = aggregate_for_json(events);
    let months: Vec<YearMonth> = totals.keys().copied().collect();

    let mut output: Vec<serde_json::Value> = Vec::new();
    for month in months.iter().rev() {
        let month_rows: Vec<_> = aggregated
            .range(
                (*month, Source::Claude, ModelSlug::new(""))
                    ..=(*month, Source::OpenRouter, ModelSlug::new("\u{10FFFF}")),
            )
            .collect();

        for ((_, source, model), bucket) in &month_rows {
            let model_cost = pricing.get(model).map_or(0.0, |p| cost(&bucket.tokens, p));
            let known_cost_usd = if *source == Source::OpenRouter {
                bucket.known_cost_usd
            } else {
                None
            };

            output.push(
                serde_json::to_value(JsonRow {
                    month: month.to_string(),
                    source: source.to_string(),
                    model: model.to_string(),
                    provider: bucket.provider.clone(),
                    project: bucket.project.clone(),
                    input: bucket.tokens.input,
                    output: bucket.tokens.output,
                    cache_read: bucket.tokens.cache_read,
                    cache_write: bucket.tokens.cache_write,
                    reasoning: bucket.tokens.reasoning,
                    cost_usd: model_cost,
                    known_cost_usd,
                })
                .expect("json row serialization"),
            );
        }

        let total_tokens = &totals[month];
        let total_cost: f64 = month_rows
            .iter()
            .map(|((_, _, model), bucket)| {
                pricing.get(model).map_or(0.0, |p| cost(&bucket.tokens, p))
            })
            .sum();

        output.push(
            serde_json::to_value(JsonRow {
                month: month.to_string(),
                source: "TOTAL".into(),
                model: String::new(),
                provider: String::new(),
                project: String::new(),
                input: total_tokens.input,
                output: total_tokens.output,
                cache_read: total_tokens.cache_read,
                cache_write: total_tokens.cache_write,
                reasoning: total_tokens.reasoning,
                cost_usd: total_cost,
                known_cost_usd: None,
            })
            .expect("json row serialization"),
        );
    }

    if let Some(summary) = balance {
        output.push(serde_json::json!({"openrouter_balance": summary}));
    }

    println!(
        "{}",
        serde_json::to_string_pretty(&output).expect("json serialization")
    );
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
