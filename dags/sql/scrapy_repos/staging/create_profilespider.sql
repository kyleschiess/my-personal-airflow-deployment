DROP TABLE IF EXISTS staging.profilespider{{ params.table_suffix }};
CREATE TABLE staging.profilespider{{ params.table_suffix }} (
    link TEXT,
	number_of_repos NUMERIC,
	number_of_stars NUMERIC,
	number_of_followers NUMERIC,
	number_following NUMERIC,
	number_of_contributions_past_year NUMERIC,
	company_name TEXT,
	email_domain TEXT,
	organizations JSONB,
    dim_id TEXT PRIMARY KEY,
    loaded_at TIMESTAMP WITH TIME ZONE,
    scraped_at TIMESTAMP WITH TIME ZONE
);

INSERT INTO staging.profilespider{{ params.table_suffix }} (
    WITH c AS (
        SELECT
            link,
            utils.text_to_num(number_of_repos) AS number_of_repos,
            utils.text_to_num(number_of_stars) AS number_of_stars,
            utils.text_to_num(number_of_followers) AS number_of_followers,
            utils.text_to_num(number_following) AS number_following,
            utils.text_to_num(number_of_contributions_past_year) AS number_of_contributions_past_year,
            company_name,
            email_domain,
            organizations,
            NOW()::TIMESTAMP WITH TIME ZONE AS loaded_at,
            scraped_at,
            ROW_NUMBER() OVER (PARTITION BY link ORDER BY scraped_at DESC) AS rn
        FROM raw.profilespider
    )	

    SELECT
        link,
        number_of_repos,
        number_of_stars,
        number_of_followers,
        number_following,
        number_of_contributions_past_year,
        company_name,
        email_domain,
        organizations,
        md5(
            link
        ) AS dim_id,
        loaded_at,
        scraped_at
    FROM c
    WHERE rn = 1
);
