DROP TABLE IF EXISTS staging.ncua_fs220d{{ params.table_suffix }};
CREATE TABLE staging.ncua_fs220d{{ params.table_suffix }}  (
    -- pulling from raw.ncua_fs220d
    cu_number NUMERIC, -- NCUA Charter Number
    cycle_date TEXT, -- `cycle_date`: Quarterly Report Date
    website TEXT, -- `acct_891`: Website
    dim_id TEXT PRIMARY KEY, -- MD5 hash of cu_number, cycle_date, update_date, total_assets, and total_deposits
    loaded_at TIMESTAMP WITH TIME ZONE, -- The date the most up-to-date version of this record was loaded into the table
    scraped_at TIMESTAMP WITH TIME ZONE -- The date this record was scraped
);

INSERT INTO staging.ncua_fs220d{{ params.table_suffix }}  (
    WITH cte1 AS (
        SELECT
            cu_number,
            cycle_date,
            acct_891,
            scraped_at,
            -- get scraped_at and convert to YYYYMMDD format
            to_char(scraped_at, 'YYYYMMDD') AS day_scraped_at
        FROM raw.ncua_fs220d
    ),

    -- staging will contain one version of each institution for whatever date it was scraped on
    cte2 AS (
        SELECT
            cu_number,
            cycle_date,
            acct_891,
            day_scraped_at AS scraped_at
        FROM cte1
        GROUP BY
            cu_number,
            cycle_date,
            acct_891,
            day_scraped_at
    )

    SELECT
        cu_number::NUMERIC,
        cycle_date,
        acct_891 as website,
        md5(
            cu_number::TEXT || cycle_date || acct_891 || scraped_at
        ) AS dim_id,
        NOW()::TIMESTAMP WITH TIME ZONE AS loaded_at,
        scraped_at::DATE AS scraped_at
    FROM cte2
);