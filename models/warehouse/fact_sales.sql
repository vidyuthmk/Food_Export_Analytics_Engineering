{{ config(
    partition_by ={ 'field' :'order_date',
    'data_type' :'date' }
) }}

WITH source AS (

    SELECT
        od.id AS order_id,
        od.product_id,
        o.customer_id,
        o.employee_id,
        o.shipper_id,
        od.quantity,
        od.unit_price,
        od.discount,
        od.status_id,
        od.date_allocated,
        od.purchase_order_id,
        od.inventory_id,
        DATE(
            o.order_date
        ) AS order_date,
        o.shipped_date,
        o.paid_date,
        CURRENT_TIMESTAMP() AS insertion_timestamp
    FROM
        {{ ref('stg_food_exports_order_details') }} AS od
        LEFT JOIN {{ ref('stg_food_exports_orders') }} AS o
        ON od.order_id = o.id
    WHERE
        od.id IS NOT NULL
),
unique_source AS (
    SELECT
        *,
        ROW_NUMBER() over (
            PARTITION BY order_id,
            product_id,
            customer_id,
            employee_id,
            shipper_id,
            order_date
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
