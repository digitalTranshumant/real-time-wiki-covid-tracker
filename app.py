from flask import Flask
import pandas as pd
import sqlite3
from flask import jsonify
from flask import request
import io
from flask import Response
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure


## Config
app = Flask(__name__)
pathToDB = '/home/dsaez/real-time-wiki-covid-tracker/AllWikidataItems.sqlite'

@app.route('/')
def index():
	conn = sqlite3.connect(pathToDB)
	totalEdits = totalEditsNoHumans() #removing duplicates, should be done in sqlite query, but I'm failing on this, so using this work around. TODO:
	 
	editors = getEditors(humans=False)
	pages = pd.read_sql(''' SELECT COUNT(*) as cnt  FROM  pagesPerProjectTable ''', con=conn).iloc[0].cnt 
	projects = numProjects()
	updated = pd.to_datetime(pd.read_sql(''' SELECT max(revisions_update)  as cnt  FROM  updated ''', con=conn).iloc[0].cnt)

	data = {'pages':pages,'totalEdits':totalEdits,'editors':editors,'projects':projects,'updated':updated}
	return """
		<h1> General statistics about COVID-19 in Wikipedia projects </h1>
		There are {totalEdits} edits  done by {editors} editors in {projects} Wikipedia projects. <br> 
		<ul>
		<li><a href='/perDayNoHumans'> Total daily edits </a>  (<a href='/perDay?data=True'>raw data</a>). </li>
		<li><a href='/perProject'> Amount of edits per project </a> (<a href='/perProject?data=True'>raw data</a>).</li> 
		<li><a href='/pagesNoHumans'> List of all related pages  </a>  (<a href='/pagesNoHumans?data=True'>raw data</a>). </li>

		<li><a href='/pages'> List of all related pages including Humans (Q5). Usually humans are related with COVID-19 by 'Medical Condition' or 'Cause of Death'</a> (<a href='/pages?data=True'>raw data</a>).  </li>
		</ul>  

		This data was updated at {updated} UTC <br>
		To know more about the methodology to build the list of pages, <a href='https://paws-public.wmflabs.org/paws-public/User:Diego_(WMF)/CoronaAllRelatedPagesMarch30.ipynb'> 
		please go this notebook. </a> <br> All these results are based on public data. Find the <a href='https://github.com/digitalTranshumant/real-time-wiki-covid-tracker'> code here. </a>
		
		""".format(**data)


@app.route('/perProject')
def perProject():
	dump = request.args.get('data',False)
	conn = sqlite3.connect(pathToDB)
	revisions = pd.read_sql(''' SELECT project,COUNT(*) as cnt  FROM  revisions GROUP BY project''', con=conn)
	if dump:
		return jsonify(dict(zip(revisions['project'],revisions['cnt'])))	
	return revisions.to_html(index=False)

@app.route('/pagesNoHumans')
def pagesNoHumans():
	dump = request.args.get('data',False)
	conn = sqlite3.connect(pathToDB)
	pages = pagesNoHumans()
	if dump:
		output = {}
		for project, data in pages.groupby('project'):
			output[project] = [row.to_json() for index,row in data.iterrows() ]
		return jsonify(output)
	pages['url'] =pages.url.apply(lambda x: '<a href="%s"> %s </a>' % (x,x))
	return pages.to_html(index=False,escape=False)


@app.route('/pages')
def pages():
	dump = request.args.get('data',False)
	conn = sqlite3.connect(pathToDB)
	pages = pd.read_sql('''SELECT DISTINCT pagesPerProjectTable.page, pagesPerProjectTable.wikidataItem, pagesPerProjectTable.project,pagesPerProjectTable.wikilink,  itemsInfoTable.Instace_Of_Label, pagesPerProjectTable.url, itemsInfoTable.connector_Label        FROM 
            itemsInfoTable INNER JOIN pagesPerProjectTable ON pagesPerProjectTable.wikidataItem = itemsInfoTable.item_id
                   ''',con=conn).sort_values(by=['project','page'])
	if dump:
		output = {}
		for project, data in pages.groupby('project'):
			output[project] = [row.to_json() for index,row in data.iterrows() ]
		return jsonify(output)
	pages['url'] =pages.url.apply(lambda x: '<a href="%s"> %s </a>' % (x,x))
	return pages.to_html(index=False,escape=False)

@app.route('/perDay')
def perDay():
	dump = request.args.get('data',False)
	days = getEditsPerDay()
	if dump:
		return jsonify(days)
	return days.to_html()

@app.route('/perDayNoHumans')
def perDayNoHumans():
	dump = request.args.get('data',False)
	days = getEditsPerDay()
	if dump:
		return jsonify(days)
	return days.to_html()



def getEditsPerDay(project=False,humans=True):
	conn = sqlite3.connect(pathToDB)
	if not project:
		revisions = pd.read_sql(''' SELECT timestamp,page,project  FROM  revisions''', con=conn)
	else:
		revisions = pd.read_sql(''' SELECT timestamp,page,project   FROM  revisions WHERE project = '%s' ''' % project, con=conn)
	if not humans:	
		pages = pagesNoHumans()
		revisions = pd.merge(revisions,pages,on=['page','project'])[['timestamp','project','page']].drop_duplicates() 		
	revisions['day'] = pd.to_datetime(revisions['timestamp']).dt.strftime('%Y-%m-%d')
	days = revisions[['day','timestamp']].groupby('day').agg('count')
	days.sort_index(inplace=True)
	return days
	
	
def pagesNoHumans():
	conn = sqlite3.connect(pathToDB)
	pages = pd.read_sql('''SELECT DISTINCT pagesPerProjectTable.page , pagesPerProjectTable.project,pagesPerProjectTable.wikilink,  itemsInfoTable.Instace_Of_Label,
                pagesPerProjectTable.url    FROM 
            itemsInfoTable INNER JOIN pagesPerProjectTable ON pagesPerProjectTable.wikidataItem = itemsInfoTable.item_id
            WHERE itemsInfoTable.Instace_Of != 'Q5' 
            ''',con=conn).sort_values(by=['project','page'])
	return pages

def totalEditsNoHumans(project=False):
	return  getEditsPerDay(project=project,humans=False).timestamp.sum()


def getEditors(project=False,humans=True):
	conn = sqlite3.connect(pathToDB)
	if not project:
		revisions = pd.read_sql(''' SELECT timestamp,page,project,user  FROM  revisions''', con=conn)		
	else:
		revisions = pd.read_sql(''' SELECT timestamp,page,project,user   FROM  revisions WHERE project = '%s' ''' % project, con=conn)
	if not humans:	
		pages = pagesNoHumans()
		revisions = pd.merge(revisions,pages,on=['page','project'])[['timestamp','project','page','user']]
		
	return revisions.user.unique().size

def numProjects(humans=True):
	conn = sqlite3.connect(pathToDB)
	revisions = pd.read_sql(''' SELECT timestamp,page,project,user  FROM  revisions''', con=conn)		
	if not humans:	
		pages = pagesNoHumans()
		revisions = pd.merge(revisions,pages,on=['page','project'])[['timestamp','project','page','user']]
		
	return revisions.project.unique().size

''
