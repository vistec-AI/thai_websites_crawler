# thai_website_crawlers

Scripts for crawling the 500 most visited websites in Thailand according to [Alexa](https://www.alexa.com/) for `th` and `en` parallel texts.

## Scripts
1. `get_alexa_rankings.py` - get top 500 most visited domains in Thailand according to [Alexa](https://www.alexa.com/)
2. `scrape_robots.py` - scrape all sitemaps from `https://domain/robots.txt`
3. `scrape_urls.py` - scrape all URLs from `https://domain/sitemap.xml`
4. `clean_urls.py` - send head requests to verify that URLs return `200`
5. `scrape_requests.py` - scrape URL contents and align by tags
6. `check_health.py` - simple dataset health checkup script for scraped datasets