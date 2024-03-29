DROP TABLE IF EXISTS staging.ncua_fs220{{ params.table_suffix }};
CREATE TABLE staging.ncua_fs220{{ params.table_suffix }}  (
    -- pulling from raw.ncua_fs220
    cu_number NUMERIC, -- NCUA Charter Number
    cycle_date TEXT, -- `cycle_date`: Quarterly Report Date
    update_date TEXT,  -- `update_date`: The date this record was last updated in the NCUA DB
    total_assets NUMERIC, -- `acct_010`: Total Assets
    total_deposits NUMERIC, -- `acct_018`: Total Deposits
    loans_for_investment NUMERIC, -- `acct_025B`: Loans for Investment
    current_members NUMERIC, -- `acct_083`: Current Members
    dda_div_shr_dft_amt NUMERIC, -- `acct_902`: DDAs / Shares Draft Amount
    dim_id TEXT PRIMARY KEY, -- MD5 hash of cu_number, cycle_date, update_date, total_assets, and total_deposits
    loaded_at TIMESTAMP WITH TIME ZONE, -- The date the most up-to-date version of this record was loaded into the table
    scraped_at TIMESTAMP WITH TIME ZONE -- The date this record was scraped
);

INSERT INTO staging.ncua_fs220{{ params.table_suffix }}  (
    WITH cte1 AS (
        SELECT
            cu_number,
            cycle_date,
            update_date,
            acct_010,
            acct_018,
            acct_025B,
            acct_083,
            acct_902,
            scraped_at,
            -- get scraped_at and convert to YYYYMMDD format
            to_char(scraped_at, 'YYYYMMDD') AS day_scraped_at
        FROM raw.ncua_fs220
    ),

    -- staging will contain one version of each institution for whatever date it was scraped on
    cte2 AS (
        SELECT
            cu_number,
            cycle_date,
            update_date,
            acct_010,
            acct_018,
            acct_025B,
            acct_083,
            acct_902,
            day_scraped_at AS scraped_at
        FROM cte1
        GROUP BY
            cu_number,
            cycle_date,
            update_date,
            acct_010,
            acct_018,
            acct_025B,
            acct_083,
            acct_902,
            day_scraped_at
    )

    SELECT
        cu_number::NUMERIC,
        cycle_date,
        update_date,
        acct_010::NUMERIC as total_assets,
        acct_018::NUMERIC as total_deposits,
        acct_025B::NUMERIC as loans_for_investment,
        acct_083::NUMERIC as current_members,
        acct_902::NUMERIC as dda_div_shr_dft_amt,
        md5(
            cu_number::TEXT || cycle_date || update_date || acct_010 || acct_018 || acct_025B || acct_083 || acct_902 || scraped_at
        ) AS dim_id,
        NOW()::TIMESTAMP WITH TIME ZONE AS loaded_at,
        scraped_at::DATE AS scraped_at
    FROM cte2
);