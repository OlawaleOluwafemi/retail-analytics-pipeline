{{ config(materialized='view') }}

SELECT
    transaction_id,
    customer_id,
    store_location,
    product_category,
    item_count,
    -- Cast decimal to float or keep as is; ClickHouse handles Decimal(38,9) fine, but casting keeps it lightweight
    CAST(purchase_amount AS Float64) as purchase_amount,
    -- Use the pre-parsed timestamp column Airbyte provided
    transaction_timestamp
FROM {{ source('airbyte_raw', 'raw_transactions') }}