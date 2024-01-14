-- DROP TABLE IF EXISTS metrics.alpaharank_de_eval_usage;
-- CREATE TABLE metrics.alpaharank_de_eval_usage (
--     schema_name TEXT,
--     table_name TEXT,
--     current_size_bytes NUMERIC,
--     avg_size_bytes_by_qtrly_rp_date NUMERIC,
-- )

-- INSERT INTO metrics.alpaharank_de_eval_usage (
--     WITH raw_fdic_fin AS (
--         SELECT 
--             repdte,
--             SUM(pg_column_size(t.*)) as bytes
--         FROM raw.fdic_financials as t
--         GROUP BY repdte
--     ),

--     raw_fdic_fin_avg_qtrly AS (
--         SELECT 
--             avg(bytes) as avg_bytes 
--         FROM raw_fdic_fin
--     ),

--     raw_fdic_inst AS ( -- average this one by scraped day
--         SELECT
--             scraped_at::DATE,
--             SUM(pg_column_size(t.*)) as bytes
--         FROM raw.fdic_institutions as t
--         GROUP BY scraped_at::DATE
--     ),

--     raw_fdic_inst_avg_scraped AS (
--         SELECT 
--             avg(bytes) as avg_bytes 
--         FROM raw_fdic_inst
--     ),


-- );


