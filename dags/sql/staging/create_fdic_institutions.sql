DROP TABLE IF EXISTS staging.fdic_institutions{{ params.table_suffix }};
CREATE TABLE staging.fdic_institutions{{ params.table_suffix }}  (
    -- pulling from raw.fdic_institutions
    -- For this evaluation, I'm only pulling a few columns.
    -- In a real world scenario, I might pull all columns.
    cert NUMERIC, -- FDIC Certificate Number
    name TEXT, -- Institution Name
    city TEXT, -- City
    state TEXT, -- `stname`: State
    date_updated TEXT, -- `dateupdt`: The date this record was last updated in the FDIC API
    dim_id TEXT PRIMARY KEY, -- MD5 hash of cert, name, city, state, and date_updated
    loaded_at TIMESTAMP WITH TIME ZONE, -- The date the most up-to-date version of this record was loaded into the table
    scraped_at TIMESTAMP WITH TIME ZONE -- The date this record was scraped
);          

INSERT INTO staging.fdic_institutions{{ params.table_suffix }}  (
    WITH inst AS (
        SELECT
            cert,
            name,
            city,
            stname,
            dateupdt,
            scraped_at,
            -- get scraped_at and convert to YYYYMMDD format
            to_char(scraped_at, 'YYYYMMDD') AS day_scraped_at
        FROM raw.fdic_institutions
    ),

    -- staging will contain one version of each institution for whatever date it was scraped on
    inst_latest AS (
        SELECT
            cert,
            name,
            city,
            stname,
            dateupdt,
            day_scraped_at AS scraped_at
        FROM inst
        GROUP BY
            cert,
            name,
            city,
            stname,
            dateupdt,
            day_scraped_at
    )

    SELECT
        cert::NUMERIC,
        name,
        city,
        stname AS state,
        dateupdt AS date_updated,
        md5(
            cert::TEXT || COALESCE(name,'') || COALESCE(city,'') || COALESCE(stname,'') || COALESCE(dateupdt,'') || scraped_at
        ) AS dim_id,
        NOW()::TIMESTAMP WITH TIME ZONE AS loaded_at,
        scraped_at::DATE AS scraped_at
    FROM inst_latest
);