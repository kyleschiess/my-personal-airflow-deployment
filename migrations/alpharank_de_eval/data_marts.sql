CREATE TABLE data_marts.fdic_banks (
    -- pulling from staging.fdic_institutions and staging.fdic_financials
    cert NUMERIC,
    name TEXT,
    city TEXT,
    state TEXT,
    state_code TEXT,
    total_assets NUMERIC,
    total_deposits NUMERIC,
    report_date TEXT,
    dim_id TEXT GENERATED ALWAYS AS (
        md5(
            cert::TEXT || report_date
        )
    ) STORED PRIMARY KEY
)

CREATE TABLE data_marts.ncua_credit_unions (
    -- pulling from staging.ncua_cu_branch_info and staging.ncua_fs220
    cu_number NUMERIC,
    name TEXT,
    city TEXT,
    state TEXT,
    state_code TEXT,
    total_assets NUMERIC,
    total_deposits NUMERIC,
    report_date TEXT,
    dim_id TEXT GENERATED ALWAYS AS (
        md5(
            cu_number::TEXT || report_date
        )
    ) STORED PRIMARY KEY
)