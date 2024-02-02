DROP TABLE IF EXISTS metrics.oss_repos_agg;
CREATE TABLE metrics.oss_repos_agg (
    repo_name TEXT,
	repo_description TEXT,
	repo_link TEXT,
    total_num_of_commits NUMERIC,
    num_of_commits_from_top10_contribs_w_comp NUMERIC,
	total_num_of_contribs NUMERIC,
	num_of_top10_contribs_w_comp NUMERIC
);

INSERT INTO metrics.oss_repos_agg (
    WITH metrics AS (
        SELECT
            repo_name,
            repo_description,
            repo_link,
            repo_num_of_commits AS total_num_of_commits,
            repo_num_of_contribs AS total_num_of_contribs,
            SUM(comp_num_of_contribs_on_repo) AS num_of_top10_contribs_w_comp,
            SUM(comp_num_of_commits_on_repo) AS num_of_commits_from_top10_contribs_w_comp
        FROM data_marts.company_contrib_to_oss
        GROUP BY
            repo_name,
            repo_description,
            repo_link,
            repo_num_of_commits,
            repo_num_of_contribs
            
    )

    SELECT
        repo_name,
        repo_description,
        repo_link,
        total_num_of_commits,
        total_num_of_contribs,
        num_of_top10_contribs_w_comp,
        num_of_commits_from_top10_contribs_w_comp
    FROM metrics
);