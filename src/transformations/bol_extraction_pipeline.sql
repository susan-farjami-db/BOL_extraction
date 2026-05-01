-- BOL Extraction — Lakeflow Spark Declarative Pipeline
--
-- Catalog and schema are set by the bundle target (databricks.yml) — unqualified
-- table names below resolve into them automatically.
--
-- The source volume path is parameterized via ${source_volume_path} and set in
-- resources/bol_extraction.pipeline.yml from the var.source_volume_path bundle
-- variable.
--
-- Flow:
--   bronze (bol_parsed)    : Auto Loader streams new .tif files from the volume
--                            and runs ai_parse_document over each one.
--   silver (bol_extracted) : streams from bronze, runs ai_extract over the
--                            16-field BOL schema. Only new rows are extracted.
--   gold   (bol_review)    : materialized view, flat typed columns for analyst
--                            review and Genie. shipment_date parsed from messy
--                            OCR formats into a real DATE.

-- ---------------------------------------------------------------------------
-- Bronze: parse each .tif as it lands in the volume
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bol_parsed
COMMENT 'Bronze: ai_parse_document VARIANT output, one row per .tif file in the bol volume.'
AS
SELECT
  path,
  modificationTime AS source_modification_time,
  length AS file_size_bytes,
  ai_parse_document(
    content,
    map(
      'version', '2.0',
      -- Empty string disables LLM-generated element descriptions. The
      -- descriptions don't help downstream ai_extract and add cost + variance.
      'descriptionElementTypes', ''
    )
  ) AS parsed_doc,
  current_timestamp() AS parsed_at
FROM STREAM read_files(
  '${source_volume_path}',
  format => 'binaryFile'
);

-- ---------------------------------------------------------------------------
-- Silver: extract the 16 common BOL fields from the parsed VARIANT
-- ---------------------------------------------------------------------------
CREATE OR REFRESH STREAMING TABLE bol_extracted
COMMENT 'Silver: ai_extract over the BOL field schema. Streams from bol_parsed so new files only pay extraction cost once.'
AS
SELECT
  path,
  source_modification_time,
  file_size_bytes,
  parsed_doc,
  ai_extract(
    parsed_doc,
    '{
      "bol_number": {"type": "string", "description": "Bill of lading number specifically labeled on the document as BOL #, BOL Number, B/L Number, Bill of Lading Number, House BOL #, or Freight Bill No / Freight Bill Number. Do NOT return shipment IDs, shipment numbers, sales order numbers, customer IDs, PO numbers, or any other identifier here. Return null if none of these labels appear on the document."},
      "shipment_id": {"type": "string", "description": "Shipment ID or shipment number printed on the document, often labeled Shipment ID, Shipment Number, Shipment #, Sales Order #, Sales Order Number, or SO #. Do NOT return the BOL number, PRO number, PO number, or any other identifier here. Return null if no shipment ID is present."},
      "shipment_date": {"type": "string", "description": "Pickup date or date the document was issued, as written on the BOL"},
      "shipper_name": {"type": "string", "description": "Name of the party shipping the goods (the origin company)"},
      "shipper_address": {"type": "string", "description": "Full shipper address including street, city, state, and zip"},
      "consignee_name": {"type": "string", "description": "Name of the party receiving the goods (the destination company)"},
      "consignee_address": {"type": "string", "description": "Full consignee address including street, city, state, and zip"},
      "carrier_name": {"type": "string", "description": "Name of the trucking carrier, often Landstar or a Landstar agent"},
      "pro_number": {"type": "string", "description": "Carrier PRO number or internal tracking number"},
      "trailer_number": {"type": "string", "description": "Trailer or equipment identifier"},
      "pickup_number": {"type": "string", "description": "Pickup reference number if present"},
      "purchase_order_number": {"type": "string", "description": "Customer PO number or customer reference number"},
      "commodity_description": {"type": "string", "description": "Description of the goods being shipped"},
      "total_pieces": {"type": "integer", "description": "Total count of pieces, pallets, or handling units"},
      "total_weight_lbs": {"type": "number", "description": "Total weight in pounds. Preserve thousands separators verbatim and convert to a number."},
      "freight_charge_terms": {"type": "string", "enum": ["Prepaid", "Collect", "Third Party"], "description": "Freight payment terms. Must be exactly one of: Prepaid, Collect, Third Party. Do not return Incoterms or routing text."},
      "special_instructions": {"type": "string", "description": "Any special handling instructions or notes on the BOL"}
    }',
    map(
      'instructions',
      'These are scanned Bill of Lading documents from Landstar trucking. Extract each field exactly as it appears. Return null for any field that is not present on the document. Do not invent values. For freight_charge_terms return only one of Prepaid, Collect, or Third Party.'
    )
  ) AS extracted,
  current_timestamp() AS extracted_at
FROM STREAM bol_parsed;

-- ---------------------------------------------------------------------------
-- Gold: flat, typed columns for analyst review and Genie
-- ---------------------------------------------------------------------------
CREATE OR REFRESH MATERIALIZED VIEW bol_review
COMMENT 'Gold: one row per BOL with extraction flattened into typed columns. shipment_date is a real DATE; shipment_date_raw preserves the original OCR text. shipment_date_unparseable flags rows where OCR produced a value we could not parse; shipment_date_confidence is the highest ai_parse_document element confidence containing the raw date string.'
AS
WITH base AS (
  SELECT
    path,
    parsed_doc,
    extracted,
    file_size_bytes,
    source_modification_time,
    extracted_at,
    extracted:response:shipment_date::string AS shipment_date_raw,
    -- Shipment date: parse the OCR'd string (formats vary wildly across BOLs:
    -- "1/16/2026", "04/12/26", "Apr 11, 2018", "13-Apr-26 11:30", "4-13-2026",
    -- and OCR variants with stray spaces like "4 - 9 - 2026" or "1- 9- 2026").
    -- We strip internal whitespace first so dash patterns match space-padded text.
    -- Falls through to NULL if none match (e.g. 3-digit-year OCR garbage like
    -- "1- 9- 210" / "4 - 9 - 210", which is upstream OCR error on handwriting).
    COALESCE(
      try_to_date(regexp_replace(extracted:response:shipment_date::string, '\\s+', ''), 'M/d/yyyy'),
      try_to_date(regexp_replace(extracted:response:shipment_date::string, '\\s+', ''), 'M/d/yy'),
      try_to_date(regexp_replace(extracted:response:shipment_date::string, '\\s+', ''), 'M-d-yyyy'),
      try_to_date(regexp_replace(extracted:response:shipment_date::string, '\\s+', ''), 'M-d-yy'),
      try_to_date(regexp_replace(extracted:response:shipment_date::string, '\\s+', ''), 'yyyy-M-d'),
      try_to_date(regexp_replace(extracted:response:shipment_date::string, '\\s+', ''), 'd-MMM-yyyy'),
      try_to_date(regexp_replace(extracted:response:shipment_date::string, '\\s+', ''), 'd-MMM-yy'),
      try_to_date(substring(regexp_replace(extracted:response:shipment_date::string, '\\s+', ''), 1, 9), 'd-MMM-yy'),
      try_to_date(extracted:response:shipment_date::string, 'MMM d, yyyy'),
      try_to_date(extracted:response:shipment_date::string, 'MMMM d, yyyy'),
      try_to_date(extracted:response:shipment_date::string, 'd MMM yyyy'),
      try_to_date(extracted:response:shipment_date::string, 'yyyy/M/d')
    ) AS shipment_date_parsed
  FROM bol_extracted
)
SELECT
  regexp_extract(path, '([^/]+)$', 1)              AS file_name,
  path,
  -- Two distinct BOL columns:
  --   bol_number_filename — deterministic id parsed from the BL_<digits>.tif filename.
  --   bol_number          — value printed on the document, returned by ai_extract under
  --                         strict labeling (BOL #, B/L Number, Freight Bill No, etc.).
  -- The two are not always the same; the filename is an internal id while the doc value
  -- may be a customer-facing BOL number.
  regexp_extract(regexp_extract(path, '([^/]+)$', 1), 'BL_([A-Za-z0-9]+)', 1)
                                                   AS bol_number_filename,
  extracted:response:bol_number::string            AS bol_number,
  extracted:response:shipment_id::string           AS shipment_id,
  shipment_date_parsed                             AS shipment_date,
  shipment_date_raw,
  -- TRUE when OCR returned a non-empty date string we could not parse. Rows
  -- with this flag are good candidates for human review or re-scan.
  (shipment_date_parsed IS NULL
   AND shipment_date_raw IS NOT NULL
   AND length(trim(shipment_date_raw)) > 0)        AS shipment_date_unparseable,
  -- Highest ai_parse_document element confidence (0-1) for any element whose
  -- content contains the raw shipment_date string. Null when no element matches
  -- (typical when ai_extract normalized the value differently, or when the date
  -- string is OCR garbage). Lower values suggest a noisy scan.
  array_max(
    transform(
      filter(
        from_json(
          to_json(parsed_doc:document:elements),
          'array<struct<content:string,confidence:double>>'
        ),
        e -> e.content IS NOT NULL
             AND shipment_date_raw IS NOT NULL
             AND length(trim(shipment_date_raw)) > 0
             AND e.content LIKE concat('%', shipment_date_raw, '%')
      ),
      e -> e.confidence
    )
  )                                                AS shipment_date_confidence,
  extracted:response:shipper_name::string          AS shipper_name,
  extracted:response:shipper_address::string       AS shipper_address,
  extracted:response:consignee_name::string        AS consignee_name,
  extracted:response:consignee_address::string     AS consignee_address,
  extracted:response:carrier_name::string          AS carrier_name,
  extracted:response:pro_number::string            AS pro_number,
  extracted:response:trailer_number::string        AS trailer_number,
  extracted:response:pickup_number::string         AS pickup_number,
  extracted:response:purchase_order_number::string AS purchase_order_number,
  extracted:response:commodity_description::string AS commodity_description,
  extracted:response:total_pieces::int             AS total_pieces,
  extracted:response:total_weight_lbs::double      AS total_weight_lbs,
  extracted:response:freight_charge_terms::string  AS freight_charge_terms,
  extracted:response:special_instructions::string  AS special_instructions,
  extracted:error_message::string                  AS extraction_error,
  file_size_bytes,
  source_modification_time,
  extracted_at
FROM base;
