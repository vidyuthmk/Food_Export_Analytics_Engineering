WITH source AS (
    SELECT
        i.inventory_id AS unique_inventory_id,
        i.transaction_type,
        i.transaction_created_date,
        i.transaction_modified_date,
        i.product_id,
        i.quantity,
        i.purchase_order_id,
        i.customer_order_id,
        i.comments,
        p.product_id AS unique_product_id,
        p.product_code,
        p.product_name,
        p.description,
        p.supplier_company,
        p.standard_cost,
        p.list_price,
        p.reorder_level,
        p.target_level,
        p.quantity_per_unit,
        p.discontinued,
        p.minimum_reorder_quantity,
        p.category,
        p.attachments,
    FROM
        {{ ref('fact_inventory') }} AS i
        LEFT JOIN {{ ref('dim_products') }} AS p
        ON i.product_id = p.product_id
)
SELECT
    *
FROM
    source
