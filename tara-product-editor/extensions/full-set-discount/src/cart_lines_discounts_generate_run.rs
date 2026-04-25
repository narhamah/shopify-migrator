use super::schema;
use shopify_function::prelude::*;
use shopify_function::Result;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use crate::schema::cart_lines_discounts_generate_run::input::cart::lines::Merchandise;

const MESSAGE: &str = "Consultation full set";

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct FunctionConfiguration {
    mode: Option<String>,
    discount_percent: Option<f64>,
    required_handles: Option<Vec<String>>,
    required_variant_ids: Option<Vec<String>>,
    bundle_handle: Option<String>,
    max_sets: Option<f64>,
}

#[derive(Debug, Clone)]
struct QualifiedLine {
    line_id: String,
    quantity: i32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OfferMode {
    BundleProduct,
    MultiItem,
}

fn to_serde_json_value(value: &JsonValue) -> serde_json::Value {
    match value {
        JsonValue::Null => serde_json::Value::Null,
        JsonValue::String(value) => serde_json::Value::String(value.clone()),
        JsonValue::Number(value) => {
            serde_json::Number::from_f64(*value)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null)
        }
        JsonValue::Boolean(value) => serde_json::Value::Bool(*value),
        JsonValue::Object(map) => serde_json::Value::Object(
            map.iter()
                .map(|(key, value)| (key.clone(), to_serde_json_value(value)))
                .collect(),
        ),
        JsonValue::Array(values) => serde_json::Value::Array(
            values.iter().map(to_serde_json_value).collect(),
        ),
    }
}

impl OfferMode {
    fn parse(value: Option<&str>) -> Option<Self> {
        match value.unwrap_or_default().trim() {
            "bundle_product" => Some(Self::BundleProduct),
            "multi_item" => Some(Self::MultiItem),
            _ => None,
        }
    }
}

fn parse_configuration(
    input: &schema::cart_lines_discounts_generate_run::Input,
) -> Option<FunctionConfiguration> {
    let metafield = input.discount().configuration()?;
    serde_json::from_value::<FunctionConfiguration>(to_serde_json_value(metafield.json_value())).ok()
}

fn normalized_percent(value: Option<f64>) -> Option<f64> {
    let percent = value?;
    if percent.is_finite() && percent > 0.0 {
        Some(percent)
    } else {
        None
    }
}

fn normalize_non_empty_strings(values: Option<Vec<String>>) -> Vec<String> {
    values
        .unwrap_or_default()
        .into_iter()
        .filter_map(|value| {
            let trimmed = value.trim();
            if trimmed.is_empty() {
                None
            } else {
                Some(trimmed.to_string())
            }
        })
        .collect()
}

fn match_bundle_targets(
    input: &schema::cart_lines_discounts_generate_run::Input,
    config: &FunctionConfiguration,
) -> Vec<QualifiedLine> {
    let required_variants = normalize_non_empty_strings(config.required_variant_ids.clone());
    let bundle_handle = config
        .bundle_handle
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let required_variant = match required_variants.first() {
        Some(value) => value.as_str(),
        None => return vec![],
    };

    input
        .cart()
        .lines()
        .iter()
        .find_map(|line| {
            if *line.quantity() < 1 {
                return None;
            }
            let (variant_id, handle) = match line.merchandise() {
                Merchandise::ProductVariant(variant) => (
                    variant.id().to_string(),
                    variant.product().handle().to_string(),
                ),
                _ => return None,
            };
            if variant_id != required_variant {
                return None;
            }
            if let Some(expected_handle) = bundle_handle {
                if handle != expected_handle {
                    return None;
                }
            }
            Some(QualifiedLine {
                line_id: line.id().to_string(),
                quantity: 1,
            })
        })
        .into_iter()
        .collect()
}

fn match_multi_item_targets(
    input: &schema::cart_lines_discounts_generate_run::Input,
    config: &FunctionConfiguration,
) -> Vec<QualifiedLine> {
    let required_variants = normalize_non_empty_strings(config.required_variant_ids.clone());
    if required_variants.is_empty() {
        return vec![];
    }

    let required_handles: HashSet<String> = normalize_non_empty_strings(config.required_handles.clone())
        .into_iter()
        .collect();

    let mut lines_by_variant: HashMap<String, Vec<QualifiedLine>> = HashMap::new();
    for line in input.cart().lines() {
        if *line.quantity() < 1 {
            continue;
        }
        let (variant_id, handle) = match line.merchandise() {
            Merchandise::ProductVariant(variant) => (
                variant.id().to_string(),
                variant.product().handle().to_string(),
            ),
            _ => continue,
        };
        if !required_variants.iter().any(|value| value == &variant_id) {
            continue;
        }
        if !required_handles.is_empty() && !required_handles.contains(&handle) {
            continue;
        }
        lines_by_variant
            .entry(variant_id)
            .or_default()
            .push(QualifiedLine {
                line_id: line.id().to_string(),
                quantity: *line.quantity(),
            });
    }

    let mut matched_targets = Vec::new();
    for required_variant in required_variants {
        let Some(available_lines) = lines_by_variant.get(&required_variant) else {
            return vec![];
        };
        let Some(target) = available_lines.first() else {
            return vec![];
        };
        if target.quantity < 1 {
            return vec![];
        }
        matched_targets.push(QualifiedLine {
            line_id: target.line_id.clone(),
            quantity: 1,
        });
    }

    matched_targets
}

fn build_operation(
    percent: f64,
    matched_targets: Vec<QualifiedLine>,
) -> schema::CartOperation {
    schema::CartOperation::ProductDiscountsAdd(schema::ProductDiscountsAddOperation {
        selection_strategy: schema::ProductDiscountSelectionStrategy::All,
        candidates: vec![schema::ProductDiscountCandidate {
            targets: matched_targets
                .into_iter()
                .map(|target| {
                    schema::ProductDiscountCandidateTarget::CartLine(schema::CartLineTarget {
                        id: target.line_id,
                        quantity: Some(target.quantity),
                    })
                })
                .collect(),
            message: Some(MESSAGE.to_string()),
            value: schema::ProductDiscountCandidateValue::Percentage(schema::Percentage {
                value: Decimal(percent),
            }),
            associated_discount_code: None,
        }],
    })
}

#[shopify_function]
fn cart_lines_discounts_generate_run(
    input: schema::cart_lines_discounts_generate_run::Input,
) -> Result<schema::CartLinesDiscountsGenerateRunResult> {
    let has_product_discount_class = input
        .discount()
        .discount_classes()
        .contains(&schema::DiscountClass::Product);

    if !has_product_discount_class {
        return Ok(schema::CartLinesDiscountsGenerateRunResult { operations: vec![] });
    }

    let Some(config) = parse_configuration(&input) else {
        return Ok(schema::CartLinesDiscountsGenerateRunResult { operations: vec![] });
    };
    let Some(mode) = OfferMode::parse(config.mode.as_deref()) else {
        return Ok(schema::CartLinesDiscountsGenerateRunResult { operations: vec![] });
    };
    let Some(percent) = normalized_percent(config.discount_percent) else {
        return Ok(schema::CartLinesDiscountsGenerateRunResult { operations: vec![] });
    };
    if config.max_sets.unwrap_or(1.0) < 1.0 {
        return Ok(schema::CartLinesDiscountsGenerateRunResult { operations: vec![] });
    }

    let matched_targets = match mode {
        OfferMode::BundleProduct => match_bundle_targets(&input, &config),
        OfferMode::MultiItem => match_multi_item_targets(&input, &config),
    };

    if matched_targets.is_empty() {
        return Ok(schema::CartLinesDiscountsGenerateRunResult { operations: vec![] });
    }

    Ok(schema::CartLinesDiscountsGenerateRunResult {
        operations: vec![build_operation(percent, matched_targets)],
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cart_delivery_options_discounts_generate_run::cart_delivery_options_discounts_generate_run;
    use shopify_function::run_function_with_input;
    use shopify_function::wasm_api::Deserialize as ShopifyDeserialize;

    fn fixture_input(path: &str) -> String {
        let raw = match path {
            "bundle" => include_str!("../tests/fixtures/cart-lines-bundle-product-qualifies.json"),
            "partial" => include_str!("../tests/fixtures/cart-lines-multi-item-partial-set-fails.json"),
            "extras" => include_str!("../tests/fixtures/cart-lines-multi-item-qualifies-with-extras.json"),
            "wrong_variant" => include_str!("../tests/fixtures/cart-lines-multi-item-wrong-variant-fails.json"),
            "delivery" => include_str!("../tests/fixtures/delivery-options-shipping-discount.json"),
            _ => panic!("unknown fixture"),
        };
        let parsed: serde_json::Value = serde_json::from_str(raw).expect("fixture json");
        parsed["payload"]["input"].to_string()
    }

    fn deserialize_lines_input(payload: &str) -> schema::cart_lines_discounts_generate_run::Input {
        let parsed: serde_json::Value = serde_json::from_str(payload).expect("payload json");
        let context = shopify_function::wasm_api::Context::new_with_input(parsed);
        <schema::cart_lines_discounts_generate_run::Input as ShopifyDeserialize>::deserialize(
            &context.input_get().expect("context input"),
        )
        .expect("deserialized input")
    }

    #[test]
    fn multi_item_applies_only_to_one_qualified_set() {
        let payload = fixture_input("extras");
        let input = deserialize_lines_input(&payload);
        let metafield = input.discount().configuration().expect("configuration metafield");
        let config_json = to_serde_json_value(metafield.json_value());
        let config = serde_json::from_value::<FunctionConfiguration>(config_json.clone())
            .unwrap_or_else(|error| panic!("config parse failed: {error}; json={config_json:?}"));
        assert_eq!(config.mode.as_deref(), Some("multi_item"));
        assert_eq!(match_multi_item_targets(&input, &config).len(), 3);
        let result: schema::CartLinesDiscountsGenerateRunResult =
            run_function_with_input(cart_lines_discounts_generate_run, &payload)
            .expect("function result");

        assert_eq!(result.operations.len(), 1);
        let operation = match &result.operations[0] {
            schema::CartOperation::ProductDiscountsAdd(operation) => operation,
            _ => panic!("expected product discount operation"),
        };
        assert_eq!(operation.candidates.len(), 1);

        let candidate = &operation.candidates[0];
        assert_eq!(candidate.message, Some(MESSAGE.to_string()));
        assert_eq!(candidate.targets.len(), 3);

        let mut discounted_lines = candidate
            .targets
            .iter()
            .map(|target| match target {
                schema::ProductDiscountCandidateTarget::CartLine(line) => {
                    (line.id.to_string(), *line.quantity.as_ref().expect("quantity"))
                }
            })
            .collect::<Vec<_>>();
        discounted_lines.sort();

        assert_eq!(
            discounted_lines,
            vec![
                ("gid://shopify/CartLine/1".to_string(), 1),
                ("gid://shopify/CartLine/2".to_string(), 1),
                ("gid://shopify/CartLine/3".to_string(), 1),
            ]
        );
    }

    #[test]
    fn bundle_product_requires_exact_bundle_variant() {
        let payload = fixture_input("bundle");
        let input = deserialize_lines_input(&payload);
        let metafield = input.discount().configuration().expect("configuration metafield");
        let config_json = to_serde_json_value(metafield.json_value());
        let config = serde_json::from_value::<FunctionConfiguration>(config_json.clone())
            .unwrap_or_else(|error| panic!("config parse failed: {error}; json={config_json:?}"));
        assert_eq!(config.mode.as_deref(), Some("bundle_product"));
        assert_eq!(match_bundle_targets(&input, &config).len(), 1);
        let result: schema::CartLinesDiscountsGenerateRunResult =
            run_function_with_input(cart_lines_discounts_generate_run, &payload)
            .expect("function result");

        assert_eq!(result.operations.len(), 1);
        let operation = match &result.operations[0] {
            schema::CartOperation::ProductDiscountsAdd(operation) => operation,
            _ => panic!("expected product discount operation"),
        };
        let candidate = &operation.candidates[0];
        assert_eq!(candidate.targets.len(), 1);

        match &candidate.targets[0] {
            schema::ProductDiscountCandidateTarget::CartLine(line) => {
                assert_eq!(line.id, "gid://shopify/CartLine/1");
                assert_eq!(*line.quantity.as_ref().expect("quantity"), 1);
            }
        }
    }

    #[test]
    fn partial_and_wrong_variant_sets_do_not_discount() {
        for key in ["partial", "wrong_variant"] {
            let payload = fixture_input(key);
            let result: schema::CartLinesDiscountsGenerateRunResult =
                run_function_with_input(cart_lines_discounts_generate_run, &payload)
                .expect("function result");
            assert!(result.operations.is_empty(), "{key} should not qualify");
        }
    }

    #[test]
    fn delivery_target_is_intentionally_a_no_op() {
        let payload = fixture_input("delivery");
        let result: schema::CartDeliveryOptionsDiscountsGenerateRunResult = run_function_with_input(
            cart_delivery_options_discounts_generate_run,
            &payload,
        )
        .expect("function result");
        assert!(result.operations.is_empty());
    }
}
