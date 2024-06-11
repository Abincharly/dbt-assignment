WITH catalog_sales AS (
    SELECT cs_bill_customer_sk, cs_item_sk, cs_sold_date_sk
    FROM {{ source('snowflake_sample_data', 'catalog_sales') }}
),
web_sales AS (
    SELECT  ws_bill_customer_sk, ws_item_sk, ws_sold_date_sk
    FROM {{ source('snowflake_sample_data', 'web_sales') }}
),
store_sales AS (
    SELECT  ss_customer_sk, ss_sold_date_sk, ss_addr_sk, ss_ext_sales_price
    FROM {{ source('snowflake_sample_data', 'store_sales') }}
),
item AS (
    SELECT i_item_sk, i_category, i_class
    FROM {{ source('snowflake_sample_data', 'item') }}
),
date_dim AS (
    SELECT d_date_sk, d_moy, d_year, d_month_seq
    FROM {{ source('snowflake_sample_data', 'date_dim') }}
),
customer_address AS (
    SELECT ca_address_sk, ca_county, ca_state
    FROM {{ source('snowflake_sample_data', 'customer_address') }}
),
store AS (
    SELECT s_county, s_state
    FROM {{ source('snowflake_sample_data', 'store') }}
),
customer AS (
    SELECT c_customer_sk, c_current_addr_sk
    FROM {{ source('snowflake_sample_data', 'customer') }}
),



catalog_sales_item AS (
    SELECT 
        DISTINCT catalog_sales.cs_bill_customer_sk AS customer_sk, catalog_sales.cs_item_sk,
        catalog_sales.cs_sold_date_sk, item.i_category, item.i_class
    FROM catalog_sales
    JOIN item
    ON catalog_sales.cs_item_sk = item.i_item_sk
    WHERE item.i_category = 'Women' 
    AND item.i_class = 'maternity'
),
catalog_sales_item_date_dim AS (
    SELECT 
        DISTINCT catalog_sales_item.customer_sk, catalog_sales_item.cs_sold_date_sk,
        date_dim.d_moy, date_dim.d_year
    FROM catalog_sales_item
    JOIN date_dim 
    ON catalog_sales_item.cs_sold_date_sk = date_dim.d_date_sk
    WHERE d_moy = 12 
    AND d_year = 1998
),
web_sales_item AS (
    SELECT 
        DISTINCT web_sales.ws_bill_customer_sk AS customer_sk, web_sales.ws_item_sk,
        web_sales.ws_sold_date_sk, item.i_category, item.i_class
    FROM web_sales
    JOIN item 
    ON web_sales.ws_item_sk = item.i_item_sk
    WHERE i_category = 'Women' 
    AND i_class = 'maternity'
),
web_sales_item_date_dim AS (
    SELECT 
        DISTINCT web_sales_item.customer_sk, web_sales_item.ws_sold_date_sk,
        date_dim.d_moy, date_dim.d_year
    FROM web_sales_item
    JOIN date_dim 
    ON web_sales_item.ws_sold_date_sk = date_dim.d_date_sk
    WHERE d_moy = 12 
    AND d_year = 1998
),
web_catalog AS (
    SELECT 
        cs.customer_sk 
    FROM catalog_sales_item_date_dim AS cs
    UNION
    SELECT 
        ws.customer_sk 
    FROM web_sales_item_date_dim AS ws
),



first_dms AS (
    SELECT DISTINCT d_month_seq + 1 AS dms_1 
    FROM date_dim
    WHERE d_year = 1998 
    AND d_moy = 12
),
third_dms AS (
    SELECT DISTINCT d_month_seq + 3 AS dms_3 
    FROM date_dim
    WHERE d_year = 1998 
    AND d_moy = 12
),
store_sales_date_dim AS (
    SELECT
        DISTINCT store_sales.ss_customer_sk, store_sales.ss_addr_sk
    FROM store_sales 
    JOIN date_dim 
    ON store_sales.ss_sold_date_sk = date_dim.d_date_sk
    JOIN first_dms
    ON date_dim.d_month_seq >= first_dms.dms_1
    JOIN third_dms
    ON date_dim.d_month_seq <= third_dms.dms_3
),
store_sales_date_dim_customer_address AS (
    SELECT 
        DISTINCT sd.ss_customer_sk, customer_address.ca_county, customer_address.ca_state
    FROM store_sales_date_dim sd
    JOIN customer_address 
    ON sd.ss_addr_sk = customer_address.ca_address_sk
),
in_store_purchases AS (
    SELECT 
    DISTINCT sdc.ss_customer_sk
    FROM store_sales_date_dim_customer_address sdc
    JOIN store 
    ON sdc.ca_county = store.s_county 
    AND sdc.ca_state = store.s_state
),



customer_web_catalog AS (
    SELECT DISTINCT customer_sk 
    FROM web_catalog
),
customer_in_store_purchases AS (
    SELECT DISTINCT ss_customer_sk 
    FROM in_store_purchases
),
store_sales_customer AS (
    SELECT 
        store_sales.ss_customer_sk, store_sales.ss_ext_sales_price,
        customer.c_current_addr_sk, customer.c_customer_sk
    FROM store_sales
    JOIN customer
    ON store_sales.ss_customer_sk = customer.c_customer_sk
    JOIN customer_web_catalog 
    ON customer.c_customer_sk = customer_web_catalog.customer_sk
    JOIN customer_in_store_purchases 
    ON store_sales.ss_customer_sk = customer_in_store_purchases.ss_customer_sk
),
store_sales_customer_customer_address AS(
    SELECT
        sc.ss_customer_sk AS customer_sk, SUM(sc.ss_ext_sales_price) AS revenue
    FROM store_sales_customer sc
    JOIN customer_address
    ON sc.c_current_addr_sk = customer_address.ca_address_sk
    GROUP BY customer_sk
),



revenue_segment AS (
    SELECT
        FLOOR(sca.revenue / 50) * 50 AS segment,
        COUNT(*) AS num_customers
    FROM
        store_sales_customer_customer_address sca
    GROUP BY
        segment
)


SELECT 
    segment, 
    num_customers
FROM 
    revenue_segment
ORDER BY
    segment, 
    num_customers
