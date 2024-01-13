CREATE SCHEMA metrics AUTHORIZATION alpharank_de_eval;

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
)