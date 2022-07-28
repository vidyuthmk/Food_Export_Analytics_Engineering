WITH source AS (
    SELECT
        p.id AS product_id,
        p.product_code,
        p.product_name,
        p.description,
        s.company AS supplier_company,
        p.standard_cost,
        p.list_price,
        p.reorder_level,
        p.target_level,
        p.quantity_per_unit,
        p.discontinued,
        p.minimum_reorder_quantity,
        p.category,
        p.attachments,
        CURRENT_TIMESTAMP() AS insertion_timestamp
    FROM
        {{ ref('stg_food_exports_products') }} AS p
        LEFT JOIN {{ ref('stg_food_exports_suppliers') }} AS s
        ON p.supplier_id = s.id
),
unique_source AS(
    SELECT
        *,
        ROW_NUMBER() over(
            PARTITION BY product_id
        ) AS ROW_NUMBER
    FROM
        source
)
SELECT
    *
EXCEPT(ROW_NUMBER)
FROM
    unique_source
WHERE
    ROW_NUMBER = 1
