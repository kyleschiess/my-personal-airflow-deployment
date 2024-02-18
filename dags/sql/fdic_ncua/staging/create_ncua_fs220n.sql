DROP TABLE IF EXISTS staging.ncua_fs220n{{ params.table_suffix }};
CREATE TABLE staging.ncua_fs220n{{ params.table_suffix }}  (
    -- pulling from raw.ncua_fs220d
    cu_number NUMERIC, -- NCUA Charter Number
    cycle_date TEXT, -- `cycle_date`: Quarterly Report Date
    other_non_int_inc_ytd NUMERIC, -- `acct_is0020`: Other Non-Interest Income YTD
    dim_id TEXT PRIMARY KEY, -- MD5 hash of cu_number, cycle_date, other_non_int_inc_ytd
    loaded_at TIMESTAMP WITH TIME ZONE, -- The date the most up-to-date version of this record was loaded into the table
    scraped_at TIMESTAMP WITH TIME ZONE -- The date this record was scraped
);

INSERT INTO staging.ncua_fs220n{{ params.table_suffix }}  (
    WITH cte1 AS (
        SELECT
            cu_number,
            cycle_date,
            acct_is0020,
            scraped_at,
            -- get scraped_at and convert to YYYYMMDD format
            to_char(scraped_at, 'YYYYMMDD') AS day_scraped_at
        FROM raw.ncua_fs220n
    ),

    -- staging will contain one version of each institution for whatever date it was scraped on
    cte2 AS (
        SELECT
            cu_number,
            cycle_date,
            acct_is0020,
            day_scraped_at AS scraped_at
        FROM cte1
        GROUP BY
            cu_number,
            cycle_date,
            acct_is0020,
            day_scraped_at
    )

    SELECT
        cu_number::NUMERIC,
        cycle_date,
        acct_is0020::NUMERIC AS other_non_int_inc_ytd,
        md5(
            cu_number::TEXT || cycle_date || acct_is0020 || scraped_at
        ) AS dim_id,
        NOW()::TIMESTAMP WITH TIME ZONE AS loaded_at,
        scraped_at::DATE AS scraped_at
    FROM cte2
);