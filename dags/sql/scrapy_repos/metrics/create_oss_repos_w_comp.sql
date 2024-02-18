DROP TABLE IF EXISTS metrics.oss_repos_w_comp;
CREATE TABLE metrics.oss_repos_w_comp (
    company_name TEXT,
    domain TEXT,
    linkedin_url TEXT,
    repo_name TEXT,
	repo_description TEXT,
	repo_link TEXT,
    repo_total_num_of_commits NUMERIC,
    num_of_top10_contrib_commits_from_comp NUMERIC,
    pct_top10_contrib_commits_from_comp NUMERIC,
	repo_total_num_of_contribs NUMERIC,
	num_of_top10_contribs_from_comp NUMERIC,
    pct_top10_contribs_from_comp NUMERIC,
    scraped_at DATE
);

INSERT INTO metrics.oss_repos_w_comp (
    SELECT
        company_name,
        domain,
        linkedin_url,
        repo_name,
        repo_description,
        repo_link,
        repo_num_of_commits AS repo_total_num_of_commits,
        comp_num_of_commits_on_repo AS num_of_top10_contrib_commits_from_comp,
        (comp_num_of_commits_on_repo / repo_num_of_commits) AS pct_top10_contrib_commits_from_comp,
        repo_num_of_contribs AS repo_total_num_of_contribs,
        comp_num_of_contribs_on_repo AS num_of_top10_contribs_from_comp,
        (comp_num_of_contribs_on_repo / repo_num_of_contribs) AS pct_top10_contribs_from_comp,
        scraped_at::DATE
    FROM data_marts.company_contrib_to_oss
);