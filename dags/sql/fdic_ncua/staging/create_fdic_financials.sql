DROP TABLE IF EXISTS staging.fdic_financials{{ params.table_suffix }};
CREATE TABLE staging.fdic_financials{{ params.table_suffix }} (
    -- pulling from raw.fdic_financials
    cert NUMERIC, -- FDIC Certificate Number
    name TEXT, -- `namefull`: Institution Name
    total_assets NUMERIC, -- `asset`: Total Assets
    total_deposits NUMERIC, -- `dep`: Total Deposits
    report_date TEXT, -- `repdte`: Quarterly Report Date
    dim_id TEXT PRIMARY KEY, -- MD5 hash of cert, name, total_assets, total_deposits, and report_date
    loaded_at TIMESTAMP WITH TIME ZONE, -- The date the most up-to-date version of this record was loaded into the table
    scraped_at TIMESTAMP WITH TIME ZONE -- The date this record was scraped
);

INSERT INTO staging.fdic_financials{{ params.table_suffix }}  (
    WITH cte1 AS (
        SELECT
            cert,
            namefull,
            asset,
            dep,
            repdte,
            scraped_at,
            -- get scraped_at and convert to YYYYMMDD format
            to_char(scraped_at, 'YYYYMMDD') AS day_scraped_at
        FROM raw.fdic_financials
    ),

    -- staging will contain one version of each institution for whatever date it was scraped on
    cte2 AS (
        SELECT
            cert,
            namefull,
            asset,
            dep,
            repdte,
            day_scraped_at AS scraped_at
        FROM cte1
        GROUP BY
            cert,
            namefull,
            asset,
            dep,
            repdte,
            day_scraped_at
    )

    SELECT
        cert::NUMERIC,
        namefull AS name,
        asset::NUMERIC * 1000 AS total_assets, -- FDIC reports in thousands
        dep::NUMERIC * 1000 AS total_deposits,
        repdte AS report_date,
        md5(
            cert::TEXT || namefull || asset || dep || repdte || scraped_at
        ) AS dim_id,
        NOW()::TIMESTAMP WITH TIME ZONE AS loaded_at,
        scraped_at::DATE AS scraped_at
    FROM cte2
);