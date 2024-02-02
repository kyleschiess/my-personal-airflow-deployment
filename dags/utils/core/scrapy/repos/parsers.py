import json

import urllib
from scrapy import Selector, Field, Item

class ProjectItem(Item):
    job_id = Field()
    section = Field()
    project_title = Field()
    project_link = Field()
    project_description = Field()
    item_type = Field()

def parse_project_spider(response):
    # get the react partial containing the README.adoc
    react_partial = response.xpath('//script[@data-target="react-partial.embeddedData"]')
    react_json = json.loads(react_partial[1].xpath('./text()').get())
    toc = react_json['props']['initialPayload']['overview']['overviewFiles'][0]['headerInfo']['toc']

    # get header hrefs from the table of contents
    header_hrefs = ['#' + item['anchor'] for item in toc if item['level'] == 2]

    # remove the #related-resources and #license hrefs
    remove_header_hrefs = ['#related-resources', '#license']
    header_hrefs = [href for href in header_hrefs if href not in remove_header_hrefs]

    #print('ProjectspiderSpider header_hrefs: ', header_hrefs)

    # loop through headers to get the divs for each section
    article_html = react_json['props']['initialPayload']['overview']['overviewFiles'][0]['richText']
    article_sel = Selector(text=article_html)

    for header_href in header_hrefs:
        # get the child li of the div
        section_li_list = article_sel.xpath(f'//a[@href="{header_href}" and contains(@id,"user-content")]/parent::h2/parent::div/child::div/child::div/child::ul/child::li')

        # loop through the li list to get details on the projects
        for li_r in section_li_list:
            project_item = ProjectItem()
            project_item['item_type'] = 'repos.items.ProjectItem'

            # find child p
            li_p = li_r.xpath('./p')
            project_description = li_p.xpath('./text()').get()

            # find child a
            li_a = li_p.xpath('./child::a')
            project_title = li_a.xpath('./text()').get()
            project_link = li_a.xpath('./@href').get()

            # add to project_item
            project_item['section'] = header_href
            project_item['project_title'] = project_title
            project_item['project_description'] = project_description
            project_item['project_link'] = project_link

            #print('ProjectspiderSpider project_title: ', project_title)
            
            yield project_item

class RepoItem(Item):
    job_id = Field()
    name = Field()
    link = Field()
    pull_requests = Field()
    description = Field()
    project_link = Field()
    tags = Field()
    languages = Field()
    commits = Field()
    stargazers = Field()
    last_updated = Field()
    forks = Field()
    contributors = Field()
    item_type = Field()

def parse_repo_spider(response, meta):
    # initialize the item
    repo_item = RepoItem()

    # get the current url
    repo_item['link'] = meta['link']

    # get the repo name from the url using urllib
    url_path = urllib.parse.urlparse(meta['link']).path
    repo_name = url_path.split('/')[2]
    repo_item['name'] = repo_name

    # get the repo description
    about_text = response.xpath('//h2[contains(text(),"About")]/parent::div/child::p/text()').get()
    repo_item['description'] = about_text

    # get the project link
    repo_item['project_link'] = meta['project_link']

    # get the tags
    tags = response.xpath('//a[contains(@class,"topic-tag")]/text()').getall()
    repo_item['tags'] = tags

    # get the languages
    languages_anchor_list = response.xpath('//h2[contains(text(),"Languages")]/parent::div/child::ul/child::li/a').getall()
    languages = []
    for a in languages_anchor_list:
        a_r = Selector(text=a)
        
        # first span is the name of the language
        a_spans = a_r.xpath('//span/text()').getall()
        language_name = a_spans[0]
        language_pct = a_spans[1]

        # add to languages list
        languages.append({
            'name': language_name,
            'pct': language_pct
        })
    repo_item['languages'] = languages

    # get the pull requests
    pull_requests = response.xpath('//span[contains(@id, "pull-requests-repo-tab-count")]/text()').get()
    repo_item['pull_requests'] = pull_requests

    # get the stargazers
    stargazers = response.xpath('//a[contains(@href,"/stargazers")]/strong/text()').get()
    repo_item['stargazers'] = stargazers

    # get the commits
    react_partial = response.xpath('//script[@data-target="react-partial.embeddedData"]')
    try:
        react_json = json.loads(react_partial[1].xpath('./text()').get())
        commits = react_json['props']['initialPayload']['overview']['commitCount']
        repo_item['commits'] = commits
    except:
        repo_item['commits'] = None

    # get the forks
    forks = response.xpath('//a[contains(@href,"/forks")]/strong/text()').get()
    repo_item['forks'] = forks

    # get the contributors
    contributors = response.xpath('//a[contains(@href,"/graphs/contributors")]/span/text()').get()
    repo_item['contributors'] = contributors

    # get the last updated
    if 'datetime' in response.xpath('//relative-time').attrib:
        last_updated = response.xpath('//relative-time').attrib['datetime']
        repo_item['last_updated'] = last_updated
    else:
        repo_item['last_updated'] = None

    repo_item['item_type'] = 'repos.items.RepoItem'

    return repo_item

class ContributorItem(Item):
    job_id = Field()
    repo_name = Field()
    source_repo_link = Field()
    profile_link = Field()
    commits_to_source_repo = Field()
    item_type = Field()

def parse_contributor_spider(response, meta):
    #### initialize the item
    contrib_item = ContributorItem()
    contrib_item['item_type'] = 'repos.items.ContributorItem'
    contrib_item['repo_name'] = meta['repo_name']
    contrib_item['source_repo_link'] = meta['source_repo_link']

    contributors = response.xpath('//li[contains(@class,"contrib-person")]')
    for c in contributors:
        prof_anchors = c.xpath('.//a[contains(@data-hovercard-type,"user")]/@href')
        if isinstance(prof_anchors, list):
            prof_anchor = prof_anchors[0].get()
        else:
            prof_anchor = prof_anchors.get()
        contrib_url = 'https://github.com' + prof_anchor
        contrib_item['profile_link'] = contrib_url
        
        commits = c.xpath('.//a[contains(@href,"commits?author")]/text()').get()
        contrib_item['commits_to_source_repo'] = commits

        yield contrib_item


class ProfileItem(Item):
    job_id = Field()
    link = Field()
    number_of_repos = Field()
    number_of_stars = Field()
    number_of_followers = Field()
    number_following = Field()
    number_of_contributions_past_year = Field()
    contrib_pct_code_review = Field()
    contrib_pct_commits = Field()
    contrib_pct_issues = Field()
    contrib_pct_pull_requests = Field()
    company_name = Field()
    email_domain = Field()
    organizations = Field()
    item_type = Field()

def parse_profile_spider(response, meta):
    #### initialize the item
    prof_item = ProfileItem()
    prof_item['link'] = meta['link']
    prof_item['item_type'] = 'repos.items.ProfileItem'

    #### get number of repos
    repos = response.xpath('//a[contains(@href,"tab=repositories")]/span/text()').get()
    prof_item['number_of_repos'] = repos

    #### get number of stars
    stars = response.xpath('//a[contains(@href,"tab=stars")]/span/text()').get()
    prof_item['number_of_stars'] = stars

    #### get number of followers
    followers = response.xpath('//a[contains(@href,"tab=followers")]/span/text()').get()
    prof_item['number_of_followers'] = followers

    #### get number of following
    following = response.xpath('//a[contains(@href,"tab=following")]/span/text()').get()
    prof_item['number_following'] = following

    #### get number of contributions past year
    contrib_past_year = response.xpath('//h2[contains(text()," contributions in the last year")]/text()').get()
    prof_item['number_of_contributions_past_year'] = contrib_past_year

    #### get contribution percentages
    activity_graph = response.xpath('//svg[contains(@class,"js-activity-overview-graph")]')
    if activity_graph:
        contrib_pct_code_review = activity_graph.xpath('//text[contains(@class,"js-highlight-percent-top")]/text()')
        contrib_pct_commits = activity_graph.xpath('//text[contains(@class,"js-highlight-percent-left")]/text()')
        contrib_pct_issues = activity_graph.xpath('//text[contains(@class,"js-highlight-percent-right")]/text()')
        contrib_pct_pull_requests = activity_graph.xpath('//text[contains(@class,"js-highlight-percent-bottom")]/text()')
        prof_item['contrib_pct_code_review'] = contrib_pct_code_review
        prof_item['contrib_pct_commits'] = contrib_pct_commits
        prof_item['contrib_pct_issues'] = contrib_pct_issues
        prof_item['contrib_pct_pull_requests'] = contrib_pct_pull_requests
    else:
        prof_item['contrib_pct_code_review'] = None
        prof_item['contrib_pct_commits'] = None
        prof_item['contrib_pct_issues'] = None
        prof_item['contrib_pct_pull_requests'] = None

    #### get company name
    # inititalize orgs list
    orgs = []

    # get company name div
    company_name_div = response.xpath('//li[contains(@itemprop, "worksFor")]/span[@class="p-org"]/div')

    # get all children
    company_name_div_children = []
    try:
        company_name_div_children = company_name_div.xpath('./*')
    except:
        company_name_div_children = []

    # if company_name_div_children is empty, then get company name from company_name_div
    if len(company_name_div_children) == 0:
        company_name = company_name_div.xpath('./text()').get()
    else:
        # try if company name is in a link
        organizations = company_name_div.xpath('./a')
        # if organizations is a list
        if isinstance(organizations, list):
            company_name = organizations[0].xpath('./text()').get()
            for o in organizations:
                org_name = o.xpath('./text()').get()
                org_href = o.attrib['href']
                orgs.append({
                    'name': org_name,
                    'href': org_href,
                    'order': organizations.index(o) + 1
                })
        else:
            company_name = organizations.attrib['aria-label']
            orgs.append({
                'name': company_name,
                'href': organizations.attrib['href'],
                'order': 1
            })

    prof_item['company_name'] = company_name

    #### get organizations
    organizations = response.xpath('//h2[contains(text(),"Organizations")]/parent::div/child::a')
    if organizations:
        for o in organizations:
            org_name = o.attrib['aria-label']
            org_href = o.attrib['href']
            orgs.append({
                'name': org_name,
                'href': 'https://github.com' + org_href,
                'order': organizations.index(o) + 1
            })

    prof_item['organizations'] = orgs if orgs else None
    
    #### get email domain
    # can't see emails when not logged in
    prof_item['email_domain'] = None

    return prof_item


class CompanyItem(Item):
    job_id = Field()
    name = Field()
    website = Field()
    domain = Field()
    github_url = Field()
    linkedin_url = Field()
    facebook_url = Field()
    twitter_url = Field()
    item_type = Field()

def parse_company_spider(response, meta):
    domain = meta['domain']

    # initialize CompanyItem
    company_item = CompanyItem()
    company_item['item_type'] = 'repos.items.CompanyItem'

    # get the company name
    company_item['name'] = meta['og_company_name']

    # get the domain
    company_item['domain'] = domain

    # get the website
    company_item['website'] = meta['url']

    # check if company has a github page
    company_item['github_url'] = None
    github_link = response.xpath('//a[contains(@href,"github")]/@href')
    if github_link:
        company_item['github_url'] = github_link.get()

    # check if company has a linkedin page
    company_item['linkedin_url'] = None
    if domain == 'linkedin.com':
        company_item['linkedin_url'] = 'https://www.linkedin.com/company/linkedin'
    else:
        linkedin_link = response.xpath('//a[contains(@href,"linkedin")]/@href')
        if linkedin_link:
            company_item['linkedin_url'] = linkedin_link.get()

    # check if company has a facebook page
    company_item['facebook_url'] = None
    if domain == 'facebook.com':
        company_item['facebook_url'] = 'https://www.facebook.com/facebook'
    facebook_link = response.xpath('//a[contains(@href,"facebook")]/@href')
    if facebook_link:
        company_item['facebook_url'] = facebook_link.get()

    # check if company has a twitter page
    company_item['twitter_url'] = None
    if domain == 'twitter.com':
        company_item['twitter_url'] = 'https://twitter.com/twitter'
    twitter_link = response.xpath('//a[contains(@href,"twitter")]/@href')
    if twitter_link:
        company_item['twitter_url'] = twitter_link.get()

    return company_item

def parse(data, spider_name):
    
    parsed_data = []
    if spider_name == 'projectspider':
        response = Selector(text=data['body'])
        gen = parse_project_spider(response)
        for item in gen:
            parsed_data.append(dict(item))

    if spider_name == 'repospider':
        for d in data:
            response = Selector(text=d['body'])
            meta = {'link': d['url'], 'project_link': d['meta']['project_link']}
            parsed_data.append(parse_repo_spider(response, meta))

    if spider_name == 'contributorsspider':
        for d in data:
            response = Selector(text=d['body'])
            meta = d['meta']
            gen = parse_contributor_spider(response, meta)
            for item in gen:
                parsed_data.append(dict(item))

    if spider_name == 'profilespider':
        for d in data:
            response = Selector(text=d['body'])
            meta = {'link': d['url']}
            parsed_data.append(parse_profile_spider(response, meta))

    if spider_name == 'companyspider':
        for d in data:
            response = Selector(text=d['body'])
            meta = {'url': d['url'], 'domain': d['meta']['domain'], 'og_company_name': d['meta']['og_company_name']}
            parsed_data.append(parse_company_spider(response, meta))

    return parsed_data