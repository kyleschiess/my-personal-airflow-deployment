DROP TABLE IF EXISTS data_marts.company_contrib_to_oss;
CREATE TABLE data_marts.company_contrib_to_oss (
    company_name TEXT,
    domain TEXT,
    linkedin_url TEXT,
    repo_name TEXT,
    repo_description TEXT,
    repo_link TEXT,
    repo_num_of_contribs NUMERIC,
    repo_num_of_commits NUMERIC,
    comp_num_of_contribs_on_repo NUMERIC,
    comp_num_of_commits_on_repo NUMERIC,
    scraped_at DATE
);

INSERT INTO data_marts.company_contrib_to_oss (
    WITH repo AS (
        SELECT
            name,
            link,
            project_link,
            description,
            contributors,
            commits,
            scraped_at::DATE as scraped_at
        FROM staging.repospider
    ),

    contrib AS (
        SELECT
            profile_link,
            source_repo_link,
            commits_to_source_repo,
            scraped_at::DATE as scraped_at
        FROM staging.contributorsspider
    ),

    comp AS (
        SELECT
            domain,
            linkedin_url,
            scraped_at::DATE as scraped_at
        FROM staging.companyspider
    ),

    j1 AS (
        SELECT
            company_name,
            domain,
            contrib_on_repo_link,
            COUNT(*) AS comp_num_of_contribs_on_repo
        FROM maps.company_to_contributor
        GROUP BY
            company_name,
            domain,
            contrib_on_repo_link
    ),

    j2 AS (
        SELECT
            m.company_name,
            m.domain,
            c.source_repo_link,
            SUM(commits_to_source_repo) AS comp_num_of_commits_on_repo
        FROM contrib AS c
        LEFT JOIN maps.company_to_contributor AS m ON
            m.contrib_on_repo_link = c.source_repo_link AND m.github_profile_link = c.profile_link
        GROUP BY
            m.company_name,
            m.domain,
            c.source_repo_link
        
    ),

    main AS (
        SELECT
            j1.company_name,
            j1.domain,
            c.linkedin_url,
            r.name AS repo_name,
            COALESCE(p.project_description, r.description) AS repo_description,
            r.link AS repo_link,
            r.contributors AS repo_num_of_contribs,
            r.commits AS repo_num_of_commits,
            j1.comp_num_of_contribs_on_repo,
            j2.comp_num_of_commits_on_repo,
            r.scraped_at
        FROM repo AS r
        LEFT JOIN staging.projectspider AS p ON
            p.project_link = r.project_link
        LEFT JOIN j1 ON
            j1.contrib_on_repo_link = r.link
        LEFT JOIN j2 ON
            j2.domain = j1.domain AND j2.source_repo_link = r.link
        LEFT JOIN comp AS c ON
            c.domain = j1.domain
        WHERE j1.company_name IS NOT NULL
        
    )


    SELECT
        *
    FROM main

);