# BOL Extraction Pipeline

Lakeflow Spark Declarative Pipeline (SDP) that parses scanned Bill of Lading
`.tif` files from a Unity Catalog volume and produces structured tables for
analyst review.

## Layers

| Table | Type | Purpose |
|---|---|---|
| `bol_parsed` | streaming | Auto Loader binaryFile + `ai_parse_document` (one row per `.tif`, raw VARIANT) |
| `bol_extracted` | streaming | `ai_extract` over a 16-field BOL schema, streams from `bol_parsed` (only new files pay extraction cost) |
| `bol_review` | materialized view | flat typed columns; `shipment_date` is a real `DATE` parsed from messy OCR formats; `bol_number_filename` derived from the file name; `bol_number` extracted from the document under strict labeling |

## Layout

```
.
├── databricks.yml                    # bundle config + variables + targets
├── resources/
│   └── bol_extraction.pipeline.yml   # pipeline definition
└── src/
    └── transformations/
        └── bol_extraction_pipeline.sql
```

## Variables (catalog / schema / volume flexibility)

Set in `databricks.yml`. Override per target or on the CLI.

| Variable | Default | Override example |
|---|---|---|
| `catalog` | `main` | `--var="catalog=my_catalog"` |
| `schema` | `bol_extraction` | `--var="schema=bol_v3"` |
| `source_volume_path` | `/Volumes/${var.catalog}/${var.schema}/bol/` | `--var="source_volume_path=/Volumes/.../other_bols/"` |

The SQL uses unqualified table names (`bol_parsed`, etc.) so they auto-resolve
to the target catalog/schema. Only the volume path needs explicit substitution
(via `${source_volume_path}` in the SQL, fed by the pipeline's `configuration`
block).

## Deploy and run

Before deploying, set the workspace `host` (and `root_path` for the prod target)
in `databricks.yml` to your Databricks workspace.

```bash
# Validate the bundle
databricks bundle validate --target dev

# Deploy to dev (uses dev defaults)
databricks bundle deploy --target dev

# Run the pipeline
databricks bundle run bol_extraction_pipeline --target dev

# Deploy to prod (or any other target you add)
databricks bundle deploy --target prod
databricks bundle run bol_extraction_pipeline --target prod
```

## Add a new environment

Add a target in `databricks.yml`:

```yaml
targets:
  staging:
    workspace:
      host: https://<your-workspace>.cloud.databricks.com
    variables:
      catalog: staging_catalog
      schema: bol_extraction
```

Then `databricks bundle deploy --target staging`.

## Adding new BOLs

Drop `.tif` files into the configured volume (default
`/Volumes/<catalog>/<schema>/bol/`) and re-run the pipeline. Auto Loader
processes only the new files.

## Natural-language exploration (optional)

Once the pipeline runs and `bol_review` is populated, you can create a
Databricks Genie space on top of `bol_review` (and optionally `bol_extracted`)
for natural-language exploration of the extracted data. Genie creation is
intentionally out of scope for this bundle so the pipeline stays reusable
across workspaces — create the space separately in your Databricks workspace
once the data is in place.

## Known data quality notes

- `total_weight_lbs` can misread thousands separators (e.g. `10,155` → `10.155`).
  Flag implausibly small weights for freight shipments.
- `freight_charge_terms` is enum-constrained (`Prepaid` / `Collect` / `Third Party`).
- A subset of BOLs may have very low-quality scans; those rows have null
  fields. `extraction_error` is non-null when extraction itself failed.
- Handwritten dates are noisy; `shipment_date_unparseable` flags rows where
  OCR returned a date string we could not parse, and `shipment_date_confidence`
  surfaces the parser's confidence in the matched element for the date.
- `bol_number_filename` is always populated from the filename; `bol_number`
  is only populated when the document explicitly labels the field
  (BOL #, B/L Number, Bill of Lading Number, House BOL #, Freight Bill No, etc.).
