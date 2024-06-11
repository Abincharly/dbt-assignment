WITH
store_sales AS (
    SELECT DISTINCT 
        ss_customer_sk, ss_sold_date_sk, ss_promo_sk, ss_item_sk
    FROM 
        {{ source('snowflake_sample_data', 'store_sales') }}
),
item AS (
    SELECT 
        i_item_sk, i_category
    FROM 
        {{ source('snowflake_sample_data', 'item') }}
),
date_dim AS (
    SELECT 
        d_date_sk, d_moy, d_year, d_month_seq
    FROM 
        {{ source('snowflake_sample_data', 'date_dim') }}
),
promotion AS (
    SELECT 
        p_promo_sk, p_response_target
    FROM 
        {{ source('snowflake_sample_data', 'promotion') }}
),



store_sales_date_dim AS (
    SELECT ss.ss_customer_sk, ss.ss_sold_date_sk, ss.ss_promo_sk, ss.ss_item_sk, dd.d_year, dd.d_moy, dd.d_month_seq
    FROM store_sales ss
    JOIN date_dim dd ON ss.ss_sold_date_sk = dd.d_date_sk
    WHERE dd.d_year = 2000   -- Assuming d_year = 2000
        AND dd.d_moy = 1    -- Assuming d_moy = 1
        AND dd.d_month_seq = 1200
),
store_sales_date_dim_promotion AS(
    SELECT sd.ss_customer_sk, sd.ss_item_sk, p.p_response_target
    FROM store_sales_date_dim sd
    JOIN promotion p ON sd.ss_promo_sk = p.p_promo_sk
    WHERE
        p.p_response_target = 1
),
store_sales_date_dim_promotion_item AS (
    SELECT sdp.ss_customer_sk, i.i_category
    FROM store_sales_date_dim_promotion sdp
    JOIN item i ON sdp.ss_item_sk = i.i_item_sk
    WHERE i.i_category IN ('Men', 'Children', 'Women')   --Assuming i_category IN ('Men', 'Children', 'Women')
),
promotion_sales AS(
    SELECT COUNT(*) promo_sales_count FROM store_sales_date_dim_promotion_item
),



store_sales_date_dim_item AS (
    SELECT sd.ss_customer_sk
    FROM store_sales_date_dim sd
    JOIN item i ON sd.ss_item_sk = i.i_item_sk 
    WHERE i.i_category IN ('Men', 'Children', 'Women')   --Assuming i_category IN ('Men', 'Children', 'Women')
),
all_sales AS (
    SELECT COUNT(*) total_sales_count FROM store_sales_date_dim_item
),

final_result AS(
SELECT
    promotion_sales.promo_sales_count,
    all_sales.total_sales_count,
    CASE
        WHEN total_sales_count > 0 THEN promotion_sales.promo_sales_count::float / all_sales.total_sales_count
        ELSE 0
    END AS promo_sales_ratio
FROM
    promotion_sales, all_sales
)


SELECT promo_sales_count, total_sales_count, promo_sales_ratio
FROM final_result


