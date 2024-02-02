DROP TABLE IF EXISTS staging.projectspider{{ params.table_suffix }};
CREATE TABLE staging.projectspider{{ params.table_suffix }} (
    section TEXT,
	project_title TEXT,
	project_link TEXT,
	project_description TEXT,
    dim_id TEXT PRIMARY KEY,
    loaded_at TIMESTAMP WITH TIME ZONE,
    scraped_at TIMESTAMP WITH TIME ZONE
);

INSERT INTO staging.projectspider{{ params.table_suffix }} (
    WITH c AS (
        SELECT
            section,
            project_title,
            project_link,
            project_description,
            NOW()::TIMESTAMP WITH TIME ZONE AS loaded_at,
            scraped_at,
            ROW_NUMBER() OVER (PARTITION BY project_link ORDER BY scraped_at DESC) AS rn
        FROM raw.projectspider
    )	

    SELECT
        section,
        project_title,
        project_link,
        project_description,
        md5(
            project_link
        ) AS dim_id,
        loaded_at,
        scraped_at
    FROM c
    WHERE rn = 1
);
