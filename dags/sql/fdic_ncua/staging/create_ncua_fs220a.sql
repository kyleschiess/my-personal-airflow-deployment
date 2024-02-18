DROP TABLE IF EXISTS staging.ncua_fs220a{{ params.table_suffix }};
CREATE TABLE staging.ncua_fs220a{{ params.table_suffix }}  (
    -- pulling from raw.ncua_fs220
    cu_number NUMERIC, -- NCUA Charter Number
    cycle_date TEXT, -- `cycle_date`: Quarterly Report Date
    dda_div_shr_dft_cnt NUMERIC, -- `acct_452`: DDAs / Shares Draft Count
    total_int_inc_ytd NUMERIC, -- `acct_115`: Total Interest Income YTD
    total_int_exp_ytd NUMERIC, -- `acct_350`: Total Interest Expense YTD
    service_chg_ytd NUMERIC, -- `acct_131`: Service Charge YTD
    dim_id TEXT PRIMARY KEY, -- MD5 hash of cu_number, cycle_date, update_date, total_assets, and total_deposits
    loaded_at TIMESTAMP WITH TIME ZONE, -- The date the most up-to-date version of this record was loaded into the table
    scraped_at TIMESTAMP WITH TIME ZONE -- The date this record was scraped
);

INSERT INTO staging.ncua_fs220a{{ params.table_suffix }}  (
    WITH cte1 AS (
        SELECT
            cu_number,
            cycle_date,
            acct_452,
            acct_115,
            acct_350,
            acct_131,
            scraped_at,
            -- get scraped_at and convert to YYYYMMDD format
            to_char(scraped_at, 'YYYYMMDD') AS day_scraped_at
        FROM raw.ncua_fs220a
    ),

    -- staging will contain one version of each institution for whatever date it was scraped on
    cte2 AS (
        SELECT
            cu_number,
            cycle_date,
            acct_452,
            acct_115,
            acct_350,
            acct_131,
            day_scraped_at AS scraped_at
        FROM cte1
        GROUP BY
            cu_number,
            cycle_date,
            acct_452,
            acct_115,
            acct_350,
            acct_131,
            day_scraped_at
    )

    SELECT
        cu_number::NUMERIC,
        cycle_date,
        acct_452::NUMERIC as dda_div_shr_dft_cnt,
        acct_115::NUMERIC as total_int_inc_ytd,
        acct_350::NUMERIC as total_int_exp_ytd,
        acct_131::NUMERIC as service_chg_ytd,
        md5(
            cu_number::TEXT || cycle_date || acct_452 || acct_115 || acct_350 || acct_131 || scraped_at
        ) AS dim_id,
        NOW()::TIMESTAMP WITH TIME ZONE AS loaded_at,
        scraped_at::DATE AS scraped_at
    FROM cte2
);