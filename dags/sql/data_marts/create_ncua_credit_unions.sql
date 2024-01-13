DROP TABLE IF EXISTS data_marts.ncua_credit_unions;
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
);

INSERT INTO data_marts.ncua_credit_unions (
    WITH cubi AS (
        SELECT DISTINCT ON (cu_number)
            cu_number,
            name,
            city,
            state_code,
            cycle_date,
            scraped_at
        FROM staging.ncua_cu_branch_info
        ORDER BY cu_number, cycle_date::DATE DESC
    ),

    fs220 AS (
        SELECT DISTINCT ON (cu_number, cycle_date::DATE)
            cu_number,
            cycle_date,
            total_assets,
            total_deposits,
            scraped_at
        FROM staging.ncua_fs220
        ORDER BY cu_number, cycle_date::DATE DESC
    )

    SELECT
        f.cu_number::NUMERIC,
        c.name,
        c.city,
        utils.state,
        c.state_code,
        f.total_assets,
        f.total_deposits,
        f.cycle_date AS report_date
    FROM fs220 AS f
    LEFT JOIN cubi AS c
        ON c.cu_number = f.cu_number
    LEFT JOIN utils.states AS utils
        ON utils.alpha2 = c.state_code

);
