/*

[WIP]

Create a plplgsql function.
- Inputs: total_current_size_gb and total_avg_size_gb_by_qtrly_rp_date
- Output columns: total_size_gb, future_date

future_date is a date that is 3, 6, 9, or 12 months from now.

*/

CREATE OR REPLACE FUNCTION funcs.total_size_gb_future_date(
    total_current_size_gb NUMERIC,
    total_avg_size_gb_by_qtrly_rp_date NUMERIC
) 
RETURNS TABLE (
    total_size_gb NUMERIC,
    future_date DATE
)
AS $$
    DECLARE
        months_ahead INTEGER;
    BEGIN
        FOR months_ahead IN 3, 6, 9, 12 LOOP
            future_date := CURRENT_DATE + months_ahead * INTERVAL '1 month';
            total_size_gb := total_current_size_gb + total_avg_size_gb_by_qtrly_rp_date * months_ahead;
            RETURN NEXT;
        END LOOP;
    END;
$$ LANGUAGE plpgsql;


DROP TABLE IF EXISTS metrics.alpaharank_de_eval_db_metrics;
CREATE VIEW metrics.alpaharank_de_eval_db_metrics (
    WITH mem_usage AS (
        SELECT
            schema_name,
            table_name,
            current_size_bytes / 1024 / 1024 / 1024 AS current_size_gb,
            avg_size_bytes_by_qtrly_rp_date / 1024 / 1024 / 1024 AS avg_size_gb_by_qtrly_rp_date
        FROM data_marts.alpaharank_de_eval_mem_usage
    ),

    -- mem_usage_projected AS (
    --     SELECT
    --         schema_name,
    --         table_name,
    --         current_size_gb,
    --         current_size_gb + avg_size_gb_by_qtrly_rp_date * 3 AS gb_in_3_months,
    --         current_size_gb + avg_size_gb_by_qtrly_rp_date * 6 AS gb_in_6_months,
    --         current_size_gb + avg_size_gb_by_qtrly_rp_date * 9 AS gb_in_9_months,
    --         current_size_gb + avg_size_gb_by_qtrly_rp_date * 12 AS gb_in_12_months
    --     FROM mem_usage
    -- ),

    -- mem_cost_projected AS (
    --     SELECT
    --         schema_name,
    --         table_name,
    --         current_size_gb,
    --         current_size_gb * 0.40 AS current_cost_per_month_usd,
    --         gb_in_3_months,
    --         gb_in_3_months * 0.40 AS cost_in_3_months_usd,
    --         gb_in_6_months,
    --         gb_in_6_months * 0.40 AS cost_in_6_months_usd,
    --         gb_in_9_months,
    --         gb_in_9_months * 0.40 AS cost_in_9_months_usd,
    --         gb_in_12_months
    --         gb_in_12_months * 0.40 AS cost_in_12_months_usd
    --     FROM mem_usage_projected
    -- )

    months_ahead AS (
        SELECT
            
    )

    SELECT
        SUM(current_size_gb) AS total_current_size_gb,
        SUM(avg_size_gb_by_qtrly_rp_date) AS total_avg_size_gb_by_qtrly_rp_date,
    FROM mem_usage

    
);
