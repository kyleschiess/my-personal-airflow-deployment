DROP TABLE IF EXISTS metrics.bank_cu_asset_dep;
CREATE TABLE metrics.bank_cu_asset_dep (
    -- pulling from data_marts.fdic_banks and data_marts.ncua_credit_unions
    inst_type TEXT,
    id_number NUMERIC,
    name TEXT,
    city TEXT,
    state TEXT,
    state_code TEXT,
    website TEXT,
    domain TEXT,
    total_assets NUMERIC,
    total_deposits NUMERIC,
    report_date DATE,
    asset_tier TEXT,
    deposits_growth_prev_qtr NUMERIC,
    deposits_growth_prev_qtr_tier TEXT
);

INSERT INTO metrics.bank_cu_asset_dep (
    WITH fdic AS (
        SELECT
            'bank' AS inst_type,
            cert AS id_number,
            name,
            city,
            state,
            state_code,
            website,
            domain,
            total_assets,
            total_deposits,
            report_date::DATE
        FROM data_marts.fdic_banks
    ),

    ncua AS (
        SELECT
            'credit union' AS inst_type,
            cu_number AS id_number,
            name,
            city,
            state,
            state_code,
            website,
            domain,
            total_assets,
            total_deposits,
            report_date::DATE
        FROM data_marts.ncua_credit_unions
    ),

    u_all AS (
        SELECT
            *
        FROM fdic
        UNION ALL
        SELECT
            *
        FROM ncua
    ),

    metrics AS (
        SELECT
            *,
            CASE
                WHEN total_assets < 500000000 THEN 'Less than $500M'
                WHEN total_assets < 1000000000 THEN '$500M to $1B'
                WHEN total_assets < 5000000000 THEN '$1B to $5B'
                WHEN total_assets < 10000000000 THEN '$5B to $10B'
                WHEN total_assets < 50000000000 THEN '$10B to $50B'
                WHEN total_assets < 100000000000 THEN '$50B to $100B'
                WHEN total_assets < 500000000000 THEN '$100B to $500B'
                WHEN total_assets < 1000000000000 THEN '$500B to $1T'
                WHEN total_assets < 3000000000000 THEN '$1T to $3T'
                ELSE 'Greater than $3T'
            END AS asset_tier,
            CASE
                WHEN total_deposits = 0 THEN 0
                ELSE (total_deposits - LAG(total_deposits, 1) OVER (PARTITION BY id_number, inst_type ORDER BY report_date)) / total_deposits
            END AS deposits_growth_prev_qtr
        FROM u_all
    )

    SELECT
        *,
        CASE
            WHEN deposits_growth_prev_qtr <= -0.05 THEN '>5% decline'
            WHEN deposits_growth_prev_qtr <= 0 THEN '0% to 5% decline'
            WHEN deposits_growth_prev_qtr <= 0.05 THEN '0% to 5% growth'
            WHEN deposits_growth_prev_qtr >= 0.05 THEN '>5% growth'
            ELSE NULL
        END AS deposits_growth_prev_qtr_tier
    FROM metrics
    ORDER BY inst_type, id_number, report_date
);