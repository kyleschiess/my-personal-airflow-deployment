DROP TABLE IF EXISTS staging.contributorsspider{{ params.table_suffix }};
CREATE TABLE staging.contributorsspider{{ params.table_suffix }} (
    repo_name TEXT,
    source_repo_link TEXT,
    profile_link TEXT,
    commits_to_source_repo NUMERIC,
    dim_id TEXT PRIMARY KEY,
    loaded_at TIMESTAMP WITH TIME ZONE,
    scraped_at TIMESTAMP WITH TIME ZONE
);

INSERT INTO staging.contributorsspider{{ params.table_suffix }} (
    WITH c AS (
        SELECT
            repo_name,
            source_repo_link,
            profile_link,
            text_to_num(SPLIT_PART(commits_to_source_repo,' ',1)) AS commits_to_source_repo,
            NOW()::TIMESTAMP WITH TIME ZONE AS loaded_at,
            scraped_at,
            ROW_NUMBER() OVER (PARTITION BY source_repo_link, profile_link ORDER BY scraped_at DESC) AS rn
        FROM raw.contributorsspider
    )	

    SELECT
        repo_name,
        source_repo_link,
        profile_link,
        commits_to_source_repo,
        md5(
            source_repo_link || profile_link
        ) AS dim_id,
        loaded_at,
        scraped_at
    FROM c
    WHERE rn = 1
);
