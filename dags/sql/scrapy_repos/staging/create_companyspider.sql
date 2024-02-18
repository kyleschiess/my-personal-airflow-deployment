DROP TABLE IF EXISTS staging.companyspider{{ params.table_suffix }};
CREATE TABLE staging.companyspider{{ params.table_suffix }} (
    name TEXT,
	website TEXT,
	domain TEXT,
	github_url TEXT,
	linkedin_url TEXT,
	facebook_url TEXT,
	twitter_url TEXT,
    dim_id TEXT PRIMARY KEY,
    loaded_at TIMESTAMP WITH TIME ZONE,
    scraped_at TIMESTAMP WITH TIME ZONE
);

INSERT INTO staging.companyspider{{ params.table_suffix }} (
    WITH c AS (
        SELECT
            name,
            website,
            domain,
            github_url,
            linkedin_url,
            facebook_url,
            twitter_url,
            NOW()::TIMESTAMP WITH TIME ZONE AS loaded_at,
            scraped_at,
            ROW_NUMBER() OVER (PARTITION BY domain ORDER BY scraped_at DESC) AS rn
        FROM raw.companyspider
    )	

    SELECT
        name,
        website,
        domain,
        github_url,
        linkedin_url,
        facebook_url,
        twitter_url,
        md5(
            domain
        ) AS dim_id,
        loaded_at,
        scraped_at
    FROM c
    WHERE rn = 1
);
