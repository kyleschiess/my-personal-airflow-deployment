DROP TABLE IF EXISTS staging.ncua_cu_branch_info{{ params.table_suffix }};
CREATE TABLE staging.ncua_cu_branch_info{{ params.table_suffix }}  (
    -- pulling from raw.ncua_cu_branch_info
    cu_number NUMERIC, -- NCUA Charter Number
    name TEXT, -- `cu_name`: Credit Union Name
    city TEXT, -- `physical_address_city`: City
    state_code TEXT, -- `physical_address_state_code`: State
    cycle_date TEXT, -- `cycle_date`: Quarterly Report Date
    dim_id TEXT PRIMARY KEY, -- MD5 hash of cu_number, name, city, state_code, and date_updated
    loaded_at TIMESTAMP WITH TIME ZONE, -- The date the most up-to-date version of this record was loaded into the table
    scraped_at TIMESTAMP WITH TIME ZONE -- The date this record was scraped
);

INSERT INTO staging.ncua_cu_branch_info{{ params.table_suffix }}  (
    WITH cte1 AS (
        SELECT
            cu_number,
            cu_name,
            physical_address_city,
            physical_address_state_code,
            cycle_date,
            scraped_at,
            -- get scraped_at and convert to YYYYMMDD format
            to_char(scraped_at, 'YYYYMMDD') AS day_scraped_at
        FROM raw.ncua_credit_union_branch_information
    ),

    -- staging will contain one version of each institution for whatever date it was scraped on
    cte2 AS (
        SELECT
            cu_number,
            cu_name,
            physical_address_city,
            physical_address_state_code,
            cycle_date,
            day_scraped_at AS scraped_at
        FROM cte1
        GROUP BY
            cu_number,
            cu_name,
            physical_address_city,
            physical_address_state_code,
            cycle_date,
            day_scraped_at
    )

    SELECT
        cu_number::NUMERIC,
        cu_name AS name,
        physical_address_city AS city,
        physical_address_state_code AS state_code,
        cycle_date,
        md5(
            cu_number::TEXT || cu_name || physical_address_city || physical_address_state_code || cycle_date || scraped_at
        ) AS dim_id,
        NOW()::TIMESTAMP WITH TIME ZONE AS loaded_at,
        scraped_at::DATE AS scraped_at
    FROM cte2
);