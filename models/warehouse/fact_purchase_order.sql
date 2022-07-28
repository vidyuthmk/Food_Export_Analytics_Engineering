{{ config(
    partition_by ={ 'field' :'creation_date',
    'data_type' :'date' }
) }}

WITH source AS (

    SELECT
        C.id AS customer_id,
        e.id AS employee_id,
        pod.purchase_order_id,
        prod.id AS product_id,
        pod.quantity,
        pod.unit_cost,
        pod.date_received,
        pod.posted_to_inventory,
        pod.inventory_id,
        p.supplier_id,
        p.created_by,
        p.submitted_date,
        DATE(
            p.creation_date
        ) AS creation_date,
        p.status_id,
        p.expected_date,
        p.shipping_fee,
        p.taxes,
        p.payment_date,
        p.payment_amount,
        p.payment_method,
        p.notes,
        p.approved_by,
        p.approved_date,
        p.submitted_by,
        CURRENT_TIMESTAMP() AS insertion_timestamp
    FROM
        {{ ref('stg_food_exports_purchase_order') }} AS p
        LEFT JOIN {{ ref('stg_food_exports_purchase_order_details') }} AS pod
        ON p.id = pod.purchase_order_id
        LEFT JOIN {{ ref('stg_food_exports_products') }} AS prod
        ON prod.id = pod.product_id
        LEFT JOIN {{ ref('stg_food_exports_order_details') }} AS od
        ON od.product_id = prod.id
        LEFT JOIN {{ ref('stg_food_exports_orders') }} AS o
        ON o.id = od.order_id
        LEFT JOIN {{ ref('stg_food_export_customer') }} AS C
        ON C.id = o.customer_id
        LEFT JOIN {{ ref('stg_food_exports_employees') }} AS e
        ON e.id = p.created_by
    WHERE
        C.id IS NOT NULL
),
unique_source AS (
    SELECT
        *,
        ROW_NUMBER() over(
            PARTITION BY customer_id,
            employee_id,
            product_id,
            inventory_id,
            supplier_id,
            purchase_order_id,
            creation_date
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
