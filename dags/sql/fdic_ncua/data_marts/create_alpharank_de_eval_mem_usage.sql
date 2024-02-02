DROP TABLE IF EXISTS data_marts.alpaharank_de_eval_mem_usage;
CREATE TABLE data_marts.alpaharank_de_eval_mem_usage (
    schema_name TEXT,
    table_name TEXT,
    current_size_bytes NUMERIC,
    avg_size_bytes_by_qtrly_rp_date NUMERIC,
)

CREATE TABLE data_marts.alpaharank_de_eval_mem_usage AS (
    ---- raw tables
    -- fdic
    WITH raw_fdic_fin AS (
        SELECT 
            repdte,
            SUM(pg_column_size(t.*)) as bytes
        FROM raw.fdic_financials as t
        GROUP BY repdte
    ),

    rff_avg_qtrly AS (
        SELECT 
            AVG(bytes) as avg_bytes 
        FROM raw_fdic_fin
    ),

    raw_fdic_inst AS (
        SELECT
            repdte,
            SUM(pg_column_size(t.*)) as bytes
        FROM raw.fdic_institutions as t
        GROUP BY repdte
    ),

    rfi_avg_qtrly AS (
        SELECT 
            AVG(bytes) as avg_bytes 
        FROM raw_fdic_inst
    ),

    -- ncua
    raw_ncua_cu_branch_info AS (
        SELECT
            cycle_date::DATE,
            SUM(pg_column_size(t.*)) as bytes
        FROM raw.ncua_credit_union_branch_information as t
        GROUP BY cycle_date::DATE
    ),

    rncbi_avg_qtrly AS (
        SELECT 
            AVG(bytes) as avg_bytes 
        FROM raw_ncua_cu_branch_info
    ),

    raw_ncua_acct_desc_avg_acct_name AS (
        SELECT
            acct_name,
            AVG(pg_column_size(t.*)) as bytes
        FROM raw.ncua_acct_desc as t
        GROUP BY acct_name
    ),

    rnad_avg AS (
        SELECT
            AVG(bytes) as bytes
        FROM raw.ncua_acct_desc
    ),

    raw_ncua_fs220 AS (
        SELECT
            cycle_date::DATE,
            SUM(pg_column_size(t.*)) as bytes
        FROM raw.ncua_fs220 as t
        GROUP BY cycle_date::DATE
    ),

    rfs220_avg_qtrly AS (
        SELECT 
            AVG(bytes) as avg_bytes 
        FROM raw_ncua_fs220
    ),

    ---- staging tables
    -- fdic
    staging_fdic_financials AS (
        SELECT 
            report_date,
            SUM(pg_column_size(t.*)) as bytes
        FROM staging.fdic_financials as t
        GROUP BY report_date
    ),

    sff_avg_qtrly AS (
        SELECT 
            AVG(bytes) as avg_bytes 
        FROM staging_fdic_financials
    ),

    staging_fdic_institutions AS (
        SELECT
            report_date,
            SUM(pg_column_size(t.*)) as bytes
        FROM staging.fdic_institutions as t
        GROUP BY report_date
    ),

    sfi_avg_qtrly AS (
        SELECT 
            AVG(bytes) as avg_bytes 
        FROM staging_fdic_institutions
    ),

    -- ncua
    staging_ncua_cu_branch_info AS (
        SELECT
            cycle_date::DATE,
            SUM(pg_column_size(t.*)) as bytes
        FROM staging.ncua_cu_branch_info as t
        GROUP BY cycle_date::DATE
    ),

    sncbi_avg_qtrly AS (
        SELECT 
            AVG(bytes) as avg_bytes 
        FROM staging_ncua_cu_branch_info
    ),

    staging_ncua_fs220 AS (
        SELECT
            cycle_date::DATE,
            SUM(pg_column_size(t.*)) as bytes
        FROM staging.ncua_fs220 as t
        GROUP BY cycle_date::DATE
    ),

    sfs220_avg_qtrly AS (
        SELECT 
            AVG(bytes) as avg_bytes 
        FROM staging_ncua_fs220
    ),

    ---- data marts
    -- fdic
    dm_fdic_banks AS (
        SELECT
            report_date,
            SUM(pg_column_size(t.*)) as bytes
        FROM data_marts.fdic_banks as t
        GROUP BY report_date
    ),

    dff_avg_qtrly AS (
        SELECT 
            AVG(bytes) as avg_bytes 
        FROM dm_fdic_banks
    ),

    -- ncua
    dm_ncua_credit_unions AS (
        SELECT
            report_date,
            SUM(pg_column_size(t.*)) as bytes
        FROM data_marts.ncua_credit_unions as t
        GROUP BY report_date
    ),

    dncu_avg_qtrly AS (
        SELECT 
            AVG(bytes) as avg_bytes 
        FROM dm_ncua_credit_unions
    ),

    ---- metrics.bank_cu_asset_dep
    metrics_bank_cu_asset_dep AS (
        SELECT
            report_date,
            SUM(pg_column_size(t.*)) as bytes
        FROM metrics.bank_cu_asset_dep as t
        GROUP BY report_date
    ),

    mbcad_avg_qtrly AS (
        SELECT 
            AVG(bytes) as avg_bytes 
        FROM metrics_bank_cu_asset_dep
    ),

    ---- metrics
    metrics_alpharank_de_eval_usage AS (
        SELECT
            'raw' as schema_name,
            'fdic_financials' as table_name,
            bytes as current_size_bytes,
            (SELECT avg_bytes FROM rff_avg_qtrly) as avg_size_bytes_by_qtrly_rp_date
        FROM raw_fdic_financials
        UNION ALL
        SELECT
            'raw' as schema_name,
            'fdic_institutions' as table_name,
            bytes as current_size_bytes,
            (SELECT avg_bytes FROM rfi_avg_qtrly) as avg_size_bytes_by_qtrly_rp_date
        FROM raw_fdic_institutions
        UNION ALL
        SELECT
            'raw' as schema_name,
            'ncua_credit_union_branch_information' as table_name,
            bytes as current_size_bytes,
            (SELECT avg_bytes FROM rncbi_avg_qtrly) as avg_size_bytes_by_qtrly_rp_date
        FROM raw_ncua_cu_branch_info
        UNION ALL
        SELECT
            'raw' as schema_name,
            'ncua_acct_desc' as table_name,
            bytes as current_size_bytes,
            (SELECT bytes FROM rnad_avg) as avg_size_bytes_by_qtrly_rp_date
        FROM raw_ncua_acct_desc
        UNION ALL
        SELECT
            'raw' as schema_name,
            'ncua_fs220' as table_name,
            bytes as current_size_bytes,
            (SELECT avg_bytes FROM rfs220_avg_qtrly) as avg_size_bytes_by_qtrly_rp_date
        FROM raw_ncua_fs220
        UNION ALL
        SELECT
            'staging' as schema_name,
            'fdic_financials' as table_name,
            bytes as current_size_bytes,
            (SELECT avg_bytes FROM sff_avg_qtrly) as avg_size_bytes_by_qtrly_rp_date
        FROM staging_fdic_financials
        UNION ALL
        SELECT
            'staging' as schema_name,
            'fdic_institutions' as table_name,
            bytes as current_size_bytes,
            (SELECT avg_bytes FROM sfi_avg_qtrly) as avg_size_bytes_by_qtrly_rp_date
        FROM staging_fdic_institutions
        UNION ALL
        SELECT
            'staging' as schema_name,
            'ncua_cu_branch_info' as table_name,
            bytes as current_size_bytes,
            (SELECT avg_bytes FROM sncbi_avg_qtrly) as avg_size_bytes_by_qtrly_rp_date
        FROM staging_ncua_cu_branch_info
        UNION ALL
        SELECT
            'staging' as schema_name,
            'ncua_fs220' as table_name,
            bytes as current_size_bytes,
            (SELECT avg_bytes FROM sfs220_avg_qtrly) as avg_size_bytes_by_qtrly_rp_date
        FROM staging_ncua_fs220
        UNION ALL
        SELECT
            'data_marts' as schema_name,
            'fdic_banks' as table_name,
            bytes as current_size_bytes,
            (SELECT avg_bytes FROM dff_avg_qtrly) as avg_size_bytes_by_qtrly_rp_date
        FROM dm_fdic_banks
        UNION ALL
        SELECT
            'data_marts' as schema_name,
            'ncua_credit_unions' as table_name,
            bytes as current_size_bytes,
            (SELECT avg_bytes FROM dncu_avg_qtrly) as avg_size_bytes_by_qtrly_rp_date
        FROM dm_ncua_credit_unions
        UNION ALL
        SELECT
            'metrics' as schema_name,
            'bank_cu_asset_dep' as table_name,
            bytes as current_size_bytes,
            (SELECT avg_bytes FROM mbcad_avg_qtrly) as avg_size_bytes_by_qtrly_rp_date
        FROM metrics_bank_cu_asset_dep
    )

    SELECT
        *
    FROM metrics_alpharank_de_eval_usage
);


