use std::collections::{BTreeMap, HashMap};

use reckon_core::{cost, ModelSlug, OpenRouterSummary, Pricing, Source, TokenCounts, YearMonth};
use serde::Serialize;
use tabled::settings::object::Columns;
use tabled::settings::{Alignment, Style};
use tabled::{Table, Tabled};

use crate::report::{month_totals, AggregateKey};

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

    println!("{table}");

    if let Some(summary) = balance {
        println!("{}", fmt_balance(summary));
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
    input: u64,
    output: u64,
    cache_read: u64,
    reasoning: u64,
    cost_usd: f64,
}

pub fn print_json(
    aggregated: &BTreeMap<AggregateKey, TokenCounts>,
    pricing: &HashMap<ModelSlug, Pricing>,
    balance: Option<&OpenRouterSummary>,
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

            rows.push(JsonRow {
                month: month.to_string(),
                source: source.to_string(),
                model: model.to_string(),
                input: tokens.input,
                output: tokens.output,
                cache_read: tokens.cache_read,
                reasoning: tokens.reasoning,
                cost_usd: model_cost,
            });
        }

        let total_tokens = &totals[month];
        let total_cost: f64 = month_rows
            .iter()
            .map(|((_, _, model), tokens)| pricing.get(model).map_or(0.0, |p| cost(tokens, p)))
            .sum();

        rows.push(JsonRow {
            month: month.to_string(),
            source: "TOTAL".into(),
            model: String::new(),
            input: total_tokens.input,
            output: total_tokens.output,
            cache_read: total_tokens.cache_read,
            reasoning: total_tokens.reasoning,
            cost_usd: total_cost,
        });
    }

    let mut output = serde_json::json!(rows);

    if let Some(summary) = balance {
        output.as_array_mut().unwrap_or(&mut Vec::new());
        let output_obj = serde_json::json!({
            "rows": rows,
            "openrouter_summary": summary
        });
        println!("{}", serde_json::to_string_pretty(&output_obj).expect("json serialization"));
        return;
    }

    println!("{}", serde_json::to_string_pretty(&output).expect("json serialization"));
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
