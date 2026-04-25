use super::schema;
use shopify_function::prelude::*;
use shopify_function::Result;

#[shopify_function]
pub fn cart_delivery_options_discounts_generate_run(
    _input: schema::cart_delivery_options_discounts_generate_run::Input,
) -> Result<schema::CartDeliveryOptionsDiscountsGenerateRunResult> {
    Ok(schema::CartDeliveryOptionsDiscountsGenerateRunResult {
        operations: vec![],
    })
}
