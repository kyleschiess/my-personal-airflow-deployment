DROP TABLE IF EXISTS data_marts.fdic_banks;
CREATE TABLE data_marts.fdic_banks (
    -- pulling from staging.fdic_institutions and staging.fdic_financials
    cert NUMERIC,
    name TEXT,
    city TEXT,
    state TEXT,
    state_code TEXT,
    website TEXT,
    domain TEXT,
    total_assets NUMERIC,
    total_deposits NUMERIC,
    report_date TEXT,
    dim_id TEXT GENERATED ALWAYS AS (
        md5(
            cert::TEXT || report_date
        )
    ) STORED PRIMARY KEY
);

INSERT INTO data_marts.fdic_banks (
    WITH fin AS (
        SELECT DISTINCT ON (cert, report_date)
            cert,
            name,
            total_assets,
            total_deposits,
            report_date,
            scraped_at
        FROM staging.fdic_financials
        ORDER BY cert, report_date, scraped_at DESC
    ),

    inst AS (
        SELECT DISTINCT ON (cert, date_updated::DATE)
            cert,
            name,
            city,
            state,
            website,
            date_updated,
            scraped_at
        FROM staging.fdic_institutions
        ORDER BY cert, date_updated::DATE DESC
    )

    SELECT
        fin.cert::NUMERIC,
        fin.name,
        inst.city,
        inst.state,
        utils.alpha2 AS state_code,
        inst.website,
        CASE
            WHEN
                inst.website IS NOT NULL THEN SUBSTRING(inst.website from '(?:.*://)?(?:www\.)?([^/?]*)')
                ELSE NULL
        END AS domain,
        fin.total_assets,
        fin.total_deposits,
        fin.report_date
    FROM fin
    LEFT JOIN inst 
        ON inst.cert = fin.cert
    LEFT JOIN utils.states AS utils
        ON utils.state = inst.state

);