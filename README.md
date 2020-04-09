# real-time-page-tracker

This repo contains the code used to run this service: https://covid-data.wmflabs.org/

The main components are:

* PageCrawler.py: Giving a set of Wikidata Item as seeds - where len(set(seeds) >0-  discover related wikidata items and using the [sitelinks](https://www.wikidata.org/wiki/Help:Sitelinks) returns a list of pages related with the seed(s). The output is written and sqlite db, in two tables: 
** pagesPerProjectTable: Information about the Wikipedia articles, such as: project,page,url,wikilink,wikidataItem
** itemsInfoTable: Information about the Wikidata Items and their relation with the seeds.

* getEdits.py: A rudimentary crawler to count the number of edits in each of the pages in pagesPerProjectTable. Save all edits with user,timestamp,page_title,project in 'revisions' table in sqlite. 

* app.py: It is Flask server to run https://covid-data.wmflabs.org/. It offers some endpoinds to download the data in JSON, and also some visualizations and statistics about the data. 

To understand the methodology behind the Crawler please follow [this notebook](https://paws-public.wmflabs.org/paws-public/User:Diego_(WMF)/CoronaAllRelatedPagesMarch30.ipynb).

TODOs:

* Replace/improve geEdits.py with direct connection to the [Wiki Replicas](https://wikitech.wikimedia.org/wiki/Help:Toolforge/Database).
* Add more interactive visualizations, 
* Move the seeds list to a configuration file.
* Clean the code ;)


