use std::collections::{BTreeMap, HashMap};

use reckon_core::{ModelSlug, Pricing, Source, TokenCounts, YearMonth, cost};
use tabled::settings::object::Columns;
use tabled::settings::{Alignment, Style};
use tabled::{Table, Tabled};

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
    unknown_models: &mut Vec<String>,
) {
    let totals = month_totals(aggregated);
    let months: Vec<YearMonth> = totals.keys().copied().collect();

    let mut rows = Vec::new();
    for month in months.iter().rev() {
        let month_rows: Vec<_> = aggregated
            .range((*month, Source::Claude, ModelSlug::new(""))..=(*month, Source::OpenRouter, ModelSlug::new("\u{10FFFF}")))
            .collect();

        for ((_, source, model), tokens) in &month_rows {
            let model_cost = pricing.get(model).map_or_else(
                || {
                    if !unknown_models.contains(&model.0) {
                        unknown_models.push(model.0.clone());
                    }
                    0.0
                },
                |p| cost(tokens, p),
            );

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
            .map(|((_, _, model), tokens)| {
                pricing.get(model).map_or(0.0, |p| cost(tokens, p))
            })
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
}
