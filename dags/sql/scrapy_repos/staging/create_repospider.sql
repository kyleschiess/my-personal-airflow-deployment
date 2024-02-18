DROP TABLE IF EXISTS staging.repospider{{ params.table_suffix }};
CREATE TABLE staging.repospider{{ params.table_suffix }} (
    name TEXT,
	link TEXT,
	description TEXT,
	project_link TEXT,
	tags JSONB,
	languages JSONB,
	pull_requests NUMERIC,
	commits NUMERIC,
	stargazers NUMERIC,
	forks NUMERIC,
	contributors NUMERIC,
	last_updated TIMESTAMP WITH TIME ZONE,
    dim_id TEXT PRIMARY KEY,
    loaded_at TIMESTAMP WITH TIME ZONE,
    scraped_at TIMESTAMP WITH TIME ZONE
);

INSERT INTO staging.repospider{{ params.table_suffix }} (
    WITH c AS (
        SELECT
            name,
            link,
            description,
            project_link,
            tags,
            languages,
            text_to_num(pull_requests) AS pull_requests,
            text_to_num(commits) AS commits,
            text_to_num(stargazers) AS stargazers,
            text_to_num(forks) AS forks,
            text_to_num(contributors) AS contributors,
            CASE
                WHEN last_updated != 'null' THEN last_updated::TIMESTAMP WITH TIME ZONE
                ELSE NULL
            END AS last_updated,
            NOW()::TIMESTAMP WITH TIME ZONE AS loaded_at,
            scraped_at,
            ROW_NUMBER() OVER (PARTITION BY link ORDER BY scraped_at DESC) AS rn
        FROM raw.repospider
    )	

    SELECT
        name,
        link,
        description,
        project_link,
        tags,
        languages,
        pull_requests,
        commits,
        stargazers,
        forks,
        contributors,
        last_updated,
        md5(
            link
        ) AS dim_id,
        loaded_at,
        scraped_at
    FROM c
    WHERE rn = 1
);
