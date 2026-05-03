# Pattern: Product Mapping Validation

## Description

Validate product configuration and mapping files before processing so all configured products map into the shared transaction domain.

## When to Use

Use this pattern when accepting an event, onboarding a product, loading a mapping file, or testing product configuration.

## Pattern

Reject products that are missing configuration, use the wrong domain, have no mapping key, have no publish destinations, or whose mapping does not cover all required domain fields.

## Example

```python
def validate_product_config(product: str, product_config: dict[str, Any]) -> None:
    if product_config.get("domain") != DOMAIN_NAME:
        raise PipelineError(f"product {product} must map to domain {DOMAIN_NAME}")
    if not product_config.get("mapping_key"):
        raise PipelineError(f"product {product} missing mapping_key")
    if "publish" not in product_config or not product_config["publish"].get("destinations"):
        raise PipelineError(f"product {product} missing publish destinations")
    normalize_rejection_policy(product, product_config)
```

```python
def validate_mapping(product: str, mapping_config: dict[str, Any]) -> dict[str, Any]:
    if mapping_config.get("domain") != DOMAIN_NAME:
        raise PipelineError(f"mapping for {product} must map to domain {DOMAIN_NAME}")

    layout_mapping = mapping_config.get("layout_mapping", {})
    if not layout_mapping:
        raise PipelineError(f"mapping for {product} missing layout_mapping")

    mapped_fields = set(layout_mapping.values())
    missing = [field for field in DOMAIN_REQUIRED_FIELDS if field not in mapped_fields]
    if missing:
        raise PipelineError(f"mapping for {product} missing domain fields: {', '.join(missing)}")
```

## Files Using This Pattern

- `aws_local.py` - validates product config, rejection policy, and mapping shape.
- `local_sfn_runner.py` - validates incoming events before pipeline steps.
- `glue_harmonization.py` - loads and validates mapping files before harmonization.
- `tests/test_pipeline.py` - tests configured products and mapping files.

## Related

- [Decision: Config Driven Product Mapping](../../decisions/004-config-driven-product-mapping.md)
- [Feature: Config-Driven Product Onboarding](../../intent/feature-product-configuration.md)
- [Feature: Business Data Lake Processing](../../intent/feature-business-data-lake-processing.md)

## Status

- **Created**: 2026-05-02
- **Status**: Active
