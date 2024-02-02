DROP TABLE IF EXISTS maps.company_to_contributor;
CREATE TABLE maps.company_to_contributor (
    company_name TEXT,
    domain TEXT,
    github_profile_link TEXT,
    contrib_on_repo_link TEXT
);

INSERT INTO maps.company_to_contributor (
    WITH en AS (
        SELECT
            (outputs->0->>'name') AS out_name,
            (meta->>'og_company_name') AS og_company_name,
            (outputs->0->>'domain') AS out_domain,
            ROW_NUMBER() OVER (PARTITION BY (outputs->0->>'domain') ORDER BY created_at DESC) as rn
        FROM raw.enrichment_data
        WHERE 
            (meta->>'og_company_name') IS NOT NULL AND
            (outputs->0->>'domain') IS NOT NULL
    ),

    prof AS (
        SELECT
            company_name,
            link
        FROM staging.profilespider
        WHERE
            company_name IS NOT NULL
    ),

    contribs AS (
        SELECT
            source_repo_link,
            profile_link
        FROM staging.contributorsspider
    )

    SELECT
        e.out_name AS company_name,
        e.out_domain AS domain,
        p.link AS github_profile_link,
        c.source_repo_link AS contrib_on_repo_link
    FROM en AS e
    LEFT JOIN prof AS p ON
        p.company_name = e.og_company_name
    LEFT JOIN contribs AS c ON
        c.profile_link = p.link
    WHERE e.rn = 1
);