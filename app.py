from flask import Flask
import pandas as pd
import sqlite3
app = Flask(__name__)


@app.route('/')
def index():
	conn = sqlite3.connect('AllWikidataItems.sqlite')
	totalEdits = pd.read_sql(''' SELECT COUNT(*) as cnt  FROM  revisions ''', con=conn).iloc[0].cnt  
	editors = pd.read_sql(''' SELECT COUNT(DISTINCT(user)) as cnt  FROM  revisions ''', con=conn).iloc[0].cnt  
	pages = pd.read_sql(''' SELECT COUNT(*) as cnt  FROM  pagesPerProjectTable ''', con=conn).iloc[0].cnt 
	projects = pd.read_sql(''' SELECT COUNT(DISTINCT(project))  as cnt  FROM  pagesPerProjectTable ''', con=conn).iloc[0].cnt 
	updated = pd.to_datetime(pd.read_sql(''' SELECT max(revisions_update)  as cnt  FROM  updated ''', con=conn).iloc[0].cnt).strftime('%Y-%b-%d %H:%M:%S')

	data = {'pages':pages,'totalEdits':totalEdits,'editors':editors,'projects':projects,'updated':updated}
	return """
		<h1> General statistics about COVID-19 in Wikipedia projects </h1>
		There are {totalEdits} edits  done by {editors} editors in {projects} Wikipedia projects. <br> 
		<ul>
		<li><a href='/perProject'> Amount of edits per project </a>. </li>
		<li><a href='/pages'> List of all related pages  </a>.  </li>
		<li><a href='/pagesNoHumans'> List of all related pages excluding people (usually people is related with COVID-19 by their medical condition </a>.  </li>
		</ul>  

		This data was updated at {updated} UTC <br>
		To know more about the methodology to build the list of pages, <a href='https://paws-public.wmflabs.org/paws-public/User:Diego_(WMF)/CoronaAllRelatedPagesMarch30.ipynb'> 
		please go this notebook. </a> <br> All these results are based on public data. 
		
		""".format(**data)


@app.route('/perProject')
def perProject():
	conn = sqlite3.connect('AllWikidataItems.sqlite')
	revisions = pd.read_sql(''' SELECT COUNT(*) as cnt,project  FROM  revisions GROUP BY project''', con=conn)
	return revisions.to_html(index=False)

@app.route('/pagesNoHumans')
def pagesNoHumans():
	conn = sqlite3.connect('AllWikidataItems.sqlite')
	pages = pd.read_sql('''SELECT DISTINCT(pagesPerProjectTable.page), pagesPerProjectTable.project,pagesPerProjectTable.wikilink,  itemsInfoTable.Instace_Of_Label,
                pagesPerProjectTable.url    FROM 
            itemsInfoTable INNER JOIN pagesPerProjectTable ON pagesPerProjectTable.wikidataItem = itemsInfoTable.item_id
            WHERE itemsInfoTable.Instace_Of != 'Q5' 
            ''',con=conn).sort_values(by=['project','page'])
	return pages.to_html(index=False)


@app.route('/pages')
def pages():
	conn = sqlite3.connect('AllWikidataItems.sqlite')
	pages = pd.read_sql('''SELECT DISTINCT(pagesPerProjectTable.page), pagesPerProjectTable.wikidataItem, pagesPerProjectTable.project,pagesPerProjectTable.wikilink,  itemsInfoTable.Instace_Of_Label, pagesPerProjectTable.url, itemsInfoTable.connector_Label        FROM 
            itemsInfoTable INNER JOIN pagesPerProjectTable ON pagesPerProjectTable.wikidataItem = itemsInfoTable.item_id
                   ''',con=conn).sort_values(by=['project','page'])
	return pages.to_html(index=False)



