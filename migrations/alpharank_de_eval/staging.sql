-- postgres 16

-- create schemas
CREATE SCHEMA IF NOT EXISTS staging AUTHORIZATION alpharank_de_eval;

-- create tables

DROP TABLE IF EXISTS staging.fdic_institutions;
CREATE TABLE staging.fdic_institutions (
    -- pulling from raw.fdic_institutions
    -- For this evaluation, I'm only pulling a few columns.
    -- In a real world scenario, I might pull all columns.
    cert NUMERIC, -- FDIC Certificate Number
    name TEXT, -- Institution Name
    city TEXT, -- City
    state TEXT, -- `stname`: State
    date_updated TEXT, -- `dateupdt`: The date this record was last updated in the FDIC API
    dim_id TEXT GENERATED ALWAYS AS (
        md5(
            cert::TEXT || name || city || state || date_updated
        )
    ) STORED PRIMARY KEY, -- MD5 hash of cert, name, city, state, and date_updated
    loaded_at TIMESTAMP WITH TIME ZONE -- The date the most up-to-date version of this record was loaded into the table
    --created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


DROP TABLE IF EXISTS staging.fdic_financials;
CREATE TABLE staging.fdic_financials (
    -- pulling from raw.fdic_financials
    cert NUMERIC, -- FDIC Certificate Number
    name TEXT, -- `namefull`: Institution Name
    total_assets NUMERIC, -- `asset`: Total Assets
    total_deposits NUMERIC, -- `dep`: Total Deposits
    report_date TEXT, -- `repdte`: Quarterly Report Date
    dim_id TEXT GENERATED ALWAYS AS (
        md5(
            cert::TEXT || name || total_assets::TEXT || total_deposits::TEXT || report_date
        )
    ) STORED PRIMARY KEY, -- MD5 hash of cert, name, total_assets, total_deposits, and report_date
    loaded_at TIMESTAMP WITH TIME ZONE -- The date the most up-to-date version of this record was loaded into the table
    --created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


DROP TABLE IF EXISTS staging.ncua_cu_branch_info;
CREATE TABLE staging.ncua_cu_branch_info (
    -- pulling from raw.ncua_cu_branch_info
    cu_number NUMERIC, -- NCUA Charter Number
    name TEXT, -- `cu_name`: Credit Union Name
    city TEXT, -- `physical_address_city`: City
    state_code TEXT, -- `physical_address_state_code`: State
    cycle_date TEXT, -- `cycle_date`: The date this record was last updated in the NCUA DB
    dim_id TEXT GENERATED ALWAYS AS (
        md5(
            cu_number::TEXT || name || city || state_code || date_updated
        )
    ) STORED PRIMARY KEY, -- MD5 hash of cu_number, name, city, state_code, and date_updated
    loaded_at TIMESTAMP WITH TIME ZONE -- The date the most up-to-date version of this record was loaded into the table
    --created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


DROP TABLE IF EXISTS staging.ncua_acct_desc;
CREATE TABLE staging.ncua_acct_desc (
    -- pulling from raw.ncua_acct_desc
    cu_number NUMERIC, -- NCUA Charter Number
    name TEXT, -- `cu_name`: Credit Union Name
    acct_desc TEXT, -- `acct_desc`: Account Description
    dim_id TEXT GENERATED ALWAYS AS (
        md5(
            cu_number::TEXT || name || acct_desc
        )
    ) STORED PRIMARY KEY, -- MD5 hash of cu_number, name, and acct_desc
    loaded_at TIMESTAMP WITH TIME ZONE -- The date the most up-to-date version of this record was loaded into the table
    --created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);


DROP TABLE IF EXISTS staging.ncua_fs220;
CREATE TABLE staging.ncua_fs220 (
    -- pulling from raw.ncua_fs220
    cu_number NUMERIC, -- NCUA Charter Number
    cycle_date TEXT, -- `cycle_date`: Quarterly Report Date
    update_date TEXT,  -- `update_date`: The date this record was last updated in the NCUA DB
    -- right now, we care about acct_010 and acct_018
    total_assets NUMERIC, -- `acct_010`: Total Assets
    total_deposits NUMERIC, -- `acct_018`: Total Deposits
    dim_id TEXT GENERATED ALWAYS AS (
        md5(
            cu_number::TEXT || cycle_date || update_date || total_assets::TEXT || total_deposits::TEXT
        )
    ) STORED PRIMARY KEY, -- MD5 hash of cu_number, cycle_date, update_date, total_assets, and total_deposits
    loaded_at TIMESTAMP WITH TIME ZONE -- The date the most up-to-date version of this record was loaded into the table
    --created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
