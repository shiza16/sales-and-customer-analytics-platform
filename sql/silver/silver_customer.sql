INSERT INTO silver.customers (
    customer_id,
    customer_name,
    email,
    region,
    join_date,
    loyalty_points,
    insert_date,
    update_date
)
WITH cleaned AS (
    SELECT
        /* 1. Fix duplicated customer_id like C082C082 → C082 */
        SUBSTRING(TRIM(customer_id) FROM '(C[0-9]+)') AS customer_id,

        /* 2. Clean customer_name:
              - remove trailing numbers
              - remove special chars
              - empty / long → NA */
        CASE
            WHEN customer_name IS NULL OR TRIM(customer_name) = '' THEN 'NA'
            WHEN customer_name ~ '[!@#$%^&*()]' THEN 'NA'
            WHEN LENGTH(customer_name) > 100 THEN 'NA'
            ELSE TRIM(
                REGEXP_REPLACE(customer_name, '\s*\d+$', '')
            )
        END AS customer_name,

        /* 3. Clean email */
        CASE
            WHEN email IS NULL THEN NULL
            WHEN LENGTH(email) > 150 THEN NULL
            WHEN email ~* '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
                THEN LOWER(email)
            ELSE NULL
        END AS email,

        /* 4. Clean region → NA */
        CASE
            WHEN region IS NULL OR TRIM(region) = '' THEN 'NA'
            WHEN UPPER(TRIM(region)) = 'UNKNOWN' THEN 'NA'
            ELSE UPPER(TRIM(region))
        END AS region,

        /* 5. Clean join_date */
        CASE
            WHEN join_date ~ '^\d{4}-\d{2}-\d{2}$'
                THEN join_date::date
            WHEN join_date ~ '^\d{4}/\d{2}/\d{2}$'
                THEN TO_DATE(join_date, 'YYYY/MM/DD')
            WHEN join_date ~ '^\d{4}-\d{2}-\d{2}T'
                THEN LEFT(join_date, 10)::date
            ELSE NULL
        END AS join_date,

        /* 6. Clean loyalty_points */
        CASE
            WHEN loyalty_points IS NULL OR TRIM(loyalty_points) = '' THEN 0
            WHEN NULLIF(loyalty_points, '')::int < 0 THEN 0
            WHEN NULLIF(loyalty_points, '')::int > 1000000 THEN 1000000
            ELSE NULLIF(loyalty_points, '')::int
        END AS loyalty_points,


        insert_date,

        ROW_NUMBER() OVER (
            PARTITION BY SUBSTRING(TRIM(customer_id) FROM '(C[0-9]+)')
            ORDER BY insert_date DESC
        ) AS rn

    FROM raw.customers_raw
    WHERE customer_id IS NOT NULL
      AND TRIM(customer_id) <> ''
)

SELECT
    customer_id,
    customer_name,
    email,
    region,
    join_date,
    loyalty_points,
    insert_date,
    NOW() AS update_date
FROM cleaned
WHERE rn = 1 -- deduplicate, keep latest record

ON CONFLICT (customer_id)
DO UPDATE SET
    customer_name   = EXCLUDED.customer_name,
    email           = EXCLUDED.email,
    region          = EXCLUDED.region,
    join_date       = EXCLUDED.join_date,
    loyalty_points  = EXCLUDED.loyalty_points,
    update_date     = NOW();
