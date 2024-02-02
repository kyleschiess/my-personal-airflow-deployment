CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS data_marts;
CREATE SCHEMA IF NOT EXISTS metrics;

DROP TABLE IF EXISTS raw.projectspider;
CREATE TABLE raw.projectspider (
	id TEXT GENERATED ALWAYS AS (
		md5(
			job_id || project_link
		)
	) STORED PRIMARY KEY,
	job_id TEXT,
	section TEXT,
	project_title TEXT,
	project_link TEXT,
	project_description TEXT,
	scraped_at timestamp with time zone not null default now()
);


DROP TABLE IF EXISTS raw.repospider;
CREATE TABLE raw.repospider (
	id TEXT GENERATED ALWAYS AS (
		md5(
			job_id || link
		)
	) STORED PRIMARY KEY,
	job_id TEXT,
	name TEXT,
	link TEXT,
	description TEXT,
	project_link TEXT,
	tags JSONB,
	languages JSONB,
	pull_requests TEXT,
	commits TEXT,
	stargazers TEXT,
	forks TEXT,
	contributors TEXT,
	last_updated TEXT,
	scraped_at timestamp with time zone not null default now()
);


DROP TABLE IF EXISTS raw.contributorsspider;
CREATE TABLE raw.contributorsspider (
	id TEXT GENERATED ALWAYS AS (
		md5(
			job_id || source_repo_link || profile_link
		)
	) STORED PRIMARY KEY,
	job_id TEXT,
	repo_name TEXT,
	source_repo_link TEXT,
	profile_link TEXT,
	commits_to_source_repo TEXT,
	scraped_at timestamp with time zone not null default now()
);


DROP TABLE IF EXISTS raw.profilespider;
CREATE TABLE raw.profilespider (
	id TEXT GENERATED ALWAYS AS (
		md5(
			job_id || link
		)
	) STORED PRIMARY KEY,
	job_id TEXT,
	link TEXT,
	number_of_repos TEXT,
	number_of_stars TEXT,
	number_of_followers TEXT,
	number_following TEXT,
	number_of_contributions_past_year TEXT,
	contrib_pct_code_review TEXT,
	contrib_pct_commits TEXT,
	contrib_pct_issues TEXT,
	contrib_pct_pull_requests TEXT,
	company_name TEXT,
	email_domain TEXT,
	organizations JSONB,
	scraped_at timestamp with time zone not null default now()
);


DROP TABLE IF EXISTS raw.companyspider;
CREATE TABLE raw.companyspider (
	id TEXT GENERATED ALWAYS AS (
		md5(
			job_id || website
		)
	) STORED PRIMARY KEY,
	job_id TEXT,
	name TEXT,
	website TEXT,
	domain TEXT,
	github_url TEXT,
	linkedin_url TEXT,
	facebook_url TEXT,
	twitter_url TEXT,
	scraped_at timestamp with time zone not null default now()
);


DROP TABLE IF EXISTS raw.enrichment_data;
CREATE TABLE raw.enrichment_data (
	id SERIAL PRIMARY KEY,
	source TEXT,
	inputs JSONB,
	outputs JSON,
	meta JSONB,
	created_at timestamp with time zone not null default now()
);


CREATE OR REPLACE FUNCTION text_to_num (
	input_text TEXT
)
RETURNS NUMERIC
AS $$
	DECLARE
		fmt_text TEXT;
		num NUMERIC;
		has_k BOOL;
	BEGIN
		num = NULL;
		IF input_text IS NOT NULL AND input_text != 'null' THEN
			-- Replace commas and remove non-numeric characters
		    fmt_text := regexp_replace(input_text, '[^0-9.]', '', 'g');
		
		    -- Convert to numeric
		    num := fmt_text::NUMERIC;
		   
		   	-- Check if the input_text contains the specified substring
	    	has_k := POSITION('k' IN input_text) > 0;
	    	
	    	IF has_k = TRUE THEN
	    		num := num * 1000;
	    	END IF;
	    END IF;
    
    	RETURN num;
	END;
$$ LANGUAGE plpgsql;