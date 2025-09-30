# CSV Export Rules

## Weight Handling
- Capture catalogue weight in kilograms; ignore zero or missing values.
- When the weight is below 1kg, convert to grams and export `Variant Weight` with `Variant Weight Unit = g` and `Variant Grams` with the gram value.
- For weights equal or above 1kg, keep the kilogram value, export `Variant Weight Unit = kg`, and format numbers with dot decimals and no thousand separators.
- Trim numeric strings and leave the unit blank when the weight is not available.

## Critical Metafields
- The importer maps the following Shopify metafields from the catalogue or product attributes:
  - `product.metafields.custom.unidade` from the catalogue unit (`unit`) or NF-e units.
  - `product.metafields.custom.catalogo` from the `catalogo` column.
  - `product.metafields.custom.dimensoes_do_produto` from `medidas_s_emb`.
  - `product.metafields.custom.capacidade` from `capacidade__ml_ou_peso_suportado`.
  - `product.metafields.custom.ncm` from catalogue data when NF-e is empty.
  - `product.metafields.custom.ipi` from the catalogue IPI column; exported for every product.
- All catalog texts are normalised: `_x000D_`, duplicated `\r\n`, and extra spaces are removed before exporting.
- Dynamic mapping is enabled in both Enhanced and Super configs with identical source columns.

## Composition Content
- Keep composition only inside the metafield (`product.metafields.custom.composicao`).
- Do not inject composition texts into `Body (HTML)` when the metafield is populated; descriptions rely on features/infAdProd only.

## Collections and Tags
- Populate the `Collection` column from the catalogue `collection` value; fallback to the primary `Product Type` when the catalogue is empty.
- Preserve category tags so automatic collection-by-tag flows remain compatible.

## Metrics Tracking
- Every run updates `<log_folder>/metrics.json` with the non-empty counts for the critical metafields above plus the total exported rows.
- The file keeps the latest 50 runs and can be used to monitor catalogue coverage.

## Version Parity
- Enhanced and Super pipelines share the same dynamic metafield mapping, weight rules, and collection logic. The UI version switch reuses the updated configs.
