DROP TABLE IF EXISTS data_marts.ncua_credit_unions;
CREATE TABLE data_marts.ncua_credit_unions (
    -- pulling from staging.ncua_cu_branch_info and staging.ncua_fs220
    cu_number NUMERIC,
    name TEXT,
    city TEXT,
    state TEXT,
    state_code TEXT,
    website TEXT,
    domain TEXT,
    total_assets NUMERIC,
    total_deposits NUMERIC,
    loans_for_investment NUMERIC,
    current_members NUMERIC,
    dda_div_shr_dft_amt NUMERIC,
    dda_div_shr_dft_cnt NUMERIC,
    total_int_inc_ytd NUMERIC,
    total_int_exp_ytd NUMERIC,
    service_chg_ytd NUMERIC,
    other_non_int_inc_ytd NUMERIC,
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
            loans_for_investment,
            current_members,
            dda_div_shr_dft_amt,
            scraped_at
        FROM staging.ncua_fs220
        ORDER BY cu_number, cycle_date::DATE DESC
    ),

    fs220a AS (
        SELECT DISTINCT ON (cu_number, cycle_date::DATE)
            cu_number,
            cycle_date,
            dda_div_shr_dft_cnt,
            total_int_inc_ytd,
            total_int_exp_ytd,
            service_chg_ytd,
            scraped_at
        FROM staging.ncua_fs220a
        ORDER BY cu_number, cycle_date::DATE DESC
    ),

    fs220d AS (
        SELECT DISTINCT ON (cu_number, cycle_date::DATE)
            cu_number,
            cycle_date,
            website,
            scraped_at
        FROM staging.ncua_fs220d
        ORDER BY cu_number, cycle_date::DATE DESC
    ),

    fs220n AS (
        SELECT DISTINCT ON (cu_number, cycle_date::DATE)
            cu_number,
            cycle_date,
            other_non_int_inc_ytd,
            scraped_at
        FROM staging.ncua_fs220n
        ORDER BY cu_number, cycle_date::DATE DESC
    )

    SELECT
        f.cu_number::NUMERIC,
        c.name,
        c.city,
        utils.state,
        c.state_code,
        d.website,
        CASE
            WHEN
                d.website IS NOT NULL THEN SUBSTRING(d.website from '(?:.*://)?(?:www\.)?([^/?]*)')
                ELSE NULL
        END AS domain,
        f.total_assets,
        f.total_deposits,
        f.loans_for_investment,
        f.current_members,
        f.dda_div_shr_dft_amt,
        a.dda_div_shr_dft_cnt,
        a.total_int_inc_ytd,
        a.total_int_exp_ytd,
        a.service_chg_ytd,
        n.other_non_int_inc_ytd,
        f.cycle_date AS report_date
    FROM fs220 AS f
    LEFT JOIN cubi AS c
        ON c.cu_number = f.cu_number
    LEFT JOIN fs220a AS a
        ON a.cu_number = f.cu_number AND f.cycle_date::DATE = a.cycle_date::DATE
    LEFT JOIN fs220d AS d
        ON d.cu_number = f.cu_number AND f.cycle_date::DATE = d.cycle_date::DATE
    LEFT JOIN fs220n AS n
        ON n.cu_number = f.cu_number AND f.cycle_date::DATE = n.cycle_date::DATE
    LEFT JOIN utils.states AS utils
        ON utils.alpha2 = c.state_code

);
