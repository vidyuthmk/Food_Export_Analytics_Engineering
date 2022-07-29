WITH source AS(
    SELECT
        *,
        CAST(
            supplier_ids AS int64
        ) AS supplier_id,
        CURRENT_TIMESTAMP() AS ingestion_timestamp
    FROM
        {{ source(
            'northwind',
            'products'
        ) }}
)
SELECT
    *
EXCEPT(supplier_ids)
FROM
    source
WHERE
    supplier_ids NOT LIKE "%;%"
