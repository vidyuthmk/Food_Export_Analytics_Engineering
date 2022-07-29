WITH source AS (
    SELECT
        C.customer_id AS unique_customer_id,
        C.company AS customer_company,
        C.last_name AS customer_last_name,
        C.first_name AS customer_first_name,
        C.email_address AS customer_email_address,
        C.job_title AS customer_job_title,
        C.business_phone AS customer_business_phone,
        C.home_phone AS customer_home_phone,
        C.mobile_phone AS customer_mobile_phone,
        C.fax_number AS customer_fax_number,
        C.address AS customer_address,
        C.city AS customer_city,
        C.state_province AS customer_state_province,
        C.zip_postal_code AS customer_zip_postal_code,
        C.country_region AS customer_country_region,
        C.web_page AS customer_web_page,
        C.notes AS customer_notes,
        C.attachments AS customer_attachments,
        e.employee_id AS unique_employee_id,
        e.company AS employee_company,
        e.last_name AS employee_last_name,
        e.first_name AS employee_first_name,
        e.email_address AS employee_email_address,
        e.job_title AS employee_job_title,
        e.business_phone AS employee_business_phone,
        e.home_phone AS employee_home_phone,
        e.mobile_phone AS employee_mobile_phone,
        e.fax_number AS employee_fax_number,
        e.address AS employee_address,
        e.city AS employee_city,
        e.state_province AS employee_state_province,
        e.zip_postal_code AS employee_zip_postal_code,
        e.country_region AS employee_country_region,
        e.web_page AS employee_web_page,
        e.notes AS employee_notes,
        e.attachments AS employee_attachments,
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
        fs.order_id,
        fs.shipper_id,
        fs.quantity,
        fs.unit_price,
        fs.discount,
        fs.status_id,
        fs.date_allocated,
        fs.purchase_order_id,
        fs.inventory_id,
        fs.order_date,
        fs.shipped_date,
        fs.paid_date
    FROM
        {{ ref('fact_sales') }} AS fs
        LEFT JOIN {{ ref('dim_customer') }} AS C
        ON C.customer_id = fs.customer_id
        LEFT JOIN {{ ref('dim_employee') }} AS e
        ON e.employee_id = fs.employee_id
        LEFT JOIN {{ ref('dim_products') }} AS p
        ON p.product_id = fs.product_id
)
SELECT
    *
FROM
    source
