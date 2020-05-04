from flask import Flask
import pandas as pd
import sqlite3
from flask import jsonify
from flask import request, render_template, send_from_directory
import io
from flask import Response
from matplotlib.backends.backend_agg import FigureCanvasAgg as FigureCanvas
from matplotlib.figure import Figure
import matplotlib.pyplot as plt
import io
import base64
from flask import Flask
from flask_caching import Cache
from threading import Thread

#testing plotly
import plotly
import plotly.graph_objs as go
import numpy as np
import json



#add scheduler to keep main page with fresh cache every all time
import requests
from apscheduler.schedulers.background import BackgroundScheduler

#save logs
import logging



# TODO: give editors per project
## Config
app = Flask(__name__)
# define the cache config keys, remember that it can be done in a settings file
app.config['CACHE_TYPE'] = 'simple'

# register the cache instance and binds it on to your app 
app.cache = Cache(app)   

#save logs
logging.basicConfig(filename='demo.log', level=logging.DEBUG)

#config sqlite DB
DB = 'AllWikidataItems.sqlite'
path ='/home/dsaez/real-time-wiki-covid-tracker/'
pathToDB = path+DB


#define views
@app.route('/')
@app.cache.cached(timeout=1200)
def index():
	conn = sqlite3.connect(pathToDB)
	totalEdits = totalEditsFunc() #
	#totalEditsHumans = totalEditsFunc(humans=True) #Removing this for performance issues.
	editors = getEditors(humans=False)
	pages = pd.read_sql(''' SELECT COUNT(*) as cnt  FROM  pagesPerProjectTable ''', con=conn).iloc[0].cnt 
	projects = numProjects()
	updated = pd.to_datetime(pd.read_sql(''' SELECT max(revisions_update)  as cnt  FROM  updated ''', con=conn).iloc[0].cnt)
	plot = plotTotalEdits()
	data = {'pages':pages,'totalEdits':totalEdits,'editors':editors,'projects':projects,'updated':updated,'plot':plot}
	return render_template('index.html',**data) #this has changed

@app.route('/perProject')
@app.cache.cached(timeout=1200)
def perProject():
	dump = request.args.get('data',False)
	conn = sqlite3.connect(pathToDB)
	revisions = pd.read_sql(''' SELECT project,COUNT(*) as cnt  FROM  revisions GROUP BY project''', con=conn)
	if dump:
		return jsonify(dict(zip(revisions['project'],revisions['cnt'])))	
	table = revisions.to_html(index=False,escape=False,table_id="example",classes=['table','thead-dark', 'table-striped','table-bordered','table-sm'])
	return render_template('tables.html',data=table,title='All related pages') #this has changed


@app.route('/pagesNoHumans')
@app.cache.cached(timeout=1200)
def pagesNoHumansRender():
	dump = request.args.get('data',False)
	conn = sqlite3.connect(pathToDB)
	pages = pagesNoHumans()
	if dump:
		output = {}
		for project, data in pages.groupby('project'):
			output[project] = [row.to_json() for index,row in data.iterrows() ]
		return jsonify(output)
	pages['url'] =pages.url.apply(lambda x: '<a href="%s"> %s </a>' % (x,x))
	table = pages.to_html(escape=False,table_id="example",classes=['table','thead-dark', 'table-striped','table-bordered','table-sm'])
	return render_template('tables.html',data=table,title='All related pages (not Including Q5)') #this has changed



@app.route('/pages')
@app.cache.cached(timeout=600)
def pages():
	dump = request.args.get('data',False)
	conn = sqlite3.connect(pathToDB)
	pages = pd.read_sql('''SELECT DISTINCT pagesPerProjectTable.page, pagesPerProjectTable.wikidataItem, pagesPerProjectTable.project,pagesPerProjectTable.wikilink,  itemsInfoTable.Instace_Of_Label as 'Page is About',
         itemsInfoTable.connector_Label as 'Relation with COVID'   , pagesPerProjectTable.url   FROM 
            itemsInfoTable INNER JOIN pagesPerProjectTable ON pagesPerProjectTable.wikidataItem = itemsInfoTable.item_id
                   ''',con=conn).sort_values(by=['project','page'])
	if dump:
		output = {}
		for project, data in pages.groupby('project'):
			output[project] = [row.to_json() for index,row in data.iterrows() ]
		return jsonify(output)
	pages['url'] =pages.url.apply(lambda x: '<a href="%s"> %s </a>' % (x,x))
	table = pages.to_html(escape=False,table_id="example",classes=['table','thead-dark', 'table-striped','table-bordered','table-sm'])
	return render_template('tables.html',data=table,title='All related pages') #this has changed


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
	project = request.args.get('project',False)
	days = getEditsPerDay(humans=False,project=project)
	if dump:
		return jsonify(days)
	table = days.to_html(escape=False,table_id="example",classes=['table','thead-dark', 'table-striped','table-bordered','table-sm'])
	return render_template('tables.html',data=table,title='Edits per day (all projects) (not Including Q5)') #this has changed

@app.route('/perProjectNoHumans')
def perProjectNoHumans():
	dump = request.args.get('data',False)
	if dump:
		df = getEditsPerProject()
		output = dict(zip(df.index,df['revisions']))
		return jsonify(output)
	table = getEditsPerProject().to_html(table_id="example",classes=['table','thead-dark', 'table-striped','table-bordered','table-sm'])
	return render_template('tables.html',data=table,title='Edits per Project (not Including Q5)') #this has changed

### Functions

def getEditsPerDay(project=False,humans=False):
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
	pages = pd.read_sql('''SELECT DISTINCT pagesPerProjectTable.page , pagesPerProjectTable.project,pagesPerProjectTable.wikilink,  itemsInfoTable.Instace_Of_Label as 'Page is About', 
             itemsInfoTable.connector_Label as 'Relation with COVID',    pagesPerProjectTable.url    FROM 
            itemsInfoTable INNER JOIN pagesPerProjectTable ON pagesPerProjectTable.wikidataItem = itemsInfoTable.item_id
            WHERE itemsInfoTable.Instace_Of != 'Q5' 
            ''',con=conn).sort_values(by=['project','page'])
	return pages

def totalEditsFunc(project=False,humans=False):
	conn = sqlite3.connect(pathToDB)
	return  pd.read_sql(''' SELECT timestamp,page,project  FROM  revisions GROUP BY timestamp,page,project''', con=conn).shape[0]

def getEditors(project=False,humans=True):
	conn = sqlite3.connect(pathToDB)
	if not project:
		revisions = pd.read_sql(''' SELECT page,project,user  FROM  revisions GROUP BY  page,project,user  ''', con=conn)		
	else:
		revisions = pd.read_sql(''' SELECT page,project,user   FROM  revisions WHERE project = '%s' GROUP BY  page,project,user  ''' % project, con=conn)
	if not humans:	
		pages = pagesNoHumans()
		pages = pages[['page','project']]
		revisions = pd.merge(revisions,pages,on=['page','project'])[['user']]	
	return revisions.user.unique().size

def numProjects(humans=True):
	conn = sqlite3.connect(pathToDB)
	revisions = pd.read_sql(''' SELECT timestamp,page,project,user  FROM  revisions ''', con=conn)		
	if not humans:	
		pages = pagesNoHumans()
		revisions = pd.merge(revisions,pages,on=['page','project'])[['timestamp','project','page','user']]	
	return revisions.project.unique().size

def getEditsPerProject(humans=False):
	conn = sqlite3.connect(pathToDB)
	revisions = pd.read_sql(''' SELECT timestamp,page,project   FROM  revisions ''' , con=conn)
	revisions = revisions.drop_duplicates()
	if not humans:	
		pages = pagesNoHumans()
		revisions = pd.merge(revisions,pages,on=['page','project'])[['timestamp','project','page']].drop_duplicates() 		
	revisions['day'] = pd.to_datetime(revisions['timestamp']).dt.strftime('%Y-%m-%d')
	projects = revisions[['project','timestamp']].groupby('project').agg('count')
	projects.sort_index(inplace=True)
	projects.rename(columns={'timestamp':'revisions'},inplace=True)
	return projects



#Send sqlite db as file
@app.route('/downloadSqlite')
def sqliteDownload():
    return send_from_directory(path, DB, as_attachment=True)


#plotly total edits
def plotTotalEdits():
    perDay = getEditsPerDay(humans=False)
    data = [
        go.Bar(
            x=perDay.index, # assign x as the dataframe column 'x'
            y=perDay['timestamp']
        )
    ]
    graphJSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)
    return graphJSON


### Cache alive
# add a function to keep generated a new call  and cache a version of pages every ten minutes
def refreshCache():
    requests.get('http://covid-data.wmflabs.org/')

scheduler = BackgroundScheduler(daemon=True)
scheduler.add_job(refreshCache, 'interval', seconds=300)
scheduler.start()


if __name__ == '__main__':
    app.debug = True
    app.run()
