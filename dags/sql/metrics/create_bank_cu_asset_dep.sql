DROP TABLE IF EXISTS metrics.bank_cu_asset_dep;
CREATE TABLE metrics.bank_cu_asset_dep (
    -- pulling from data_marts.fdic_banks and data_marts.ncua_credit_unions
    type TEXT,
    id_number NUMERIC,
    name TEXT,
    city TEXT,
    state TEXT,
    state_code TEXT,
    total_assets NUMERIC,
    total_deposits NUMERIC,
    report_date DATE
);

INSERT INTO metrics.bank_cu_asset_dep (
    WITH fdic AS (
        SELECT
            'bank' AS type,
            cert AS id_number,
            name,
            city,
            state,
            state_code,
            total_assets,
            total_deposits,
            report_date::DATE
        FROM data_marts.fdic_banks
    ),

    ncua AS (
        SELECT
            'credit union' AS type,
            cu_number AS id_number,
            name,
            city,
            state,
            state_code,
            total_assets,
            total_deposits,
            report_date::DATE
        FROM data_marts.ncua_credit_unions
    )

    SELECT
        *
    FROM fdic
    UNION ALL
    SELECT
        *
    FROM ncua
);