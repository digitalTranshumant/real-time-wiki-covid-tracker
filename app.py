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


# TODO: give editors per project
## Config
app = Flask(__name__)
# define the cache config keys, remember that it can be done in a settings file
app.config['CACHE_TYPE'] = 'simple'

# register the cache instance and binds it on to your app 
app.cache = Cache(app)   

DB = 'AllWikidataItems.sqlite'
path ='/home/dsaez/real-time-wiki-covid-tracker/'
pathToDB = path+DB



@app.route('/')
@app.cache.cached(timeout=600)	
def fastIndex():
	data = getStatsIndex()
	return render_template('index.html',**data) #this has changed


@app.route('/oldindex')
@app.cache.cached(timeout=600)
def index():
	conn = sqlite3.connect(pathToDB)
	totalEdits = totalEditsFunc() #
	#totalEditsHumans = totalEditsFunc(humans=True) #Removing this for performance issues.
	editors = getEditors(humans=False)
	pages = pd.read_sql(''' SELECT COUNT(*) as cnt  FROM  pagesPerProjectTable ''', con=conn).iloc[0].cnt 
	projects = numProjects()
	updated = pd.to_datetime(pd.read_sql(''' SELECT max(revisions_update)  as cnt  FROM  updated ''', con=conn).iloc[0].cnt)
	plot = create_plot()
	data = {'pages':pages,'totalEdits':totalEdits,'editors':editors,'projects':projects,'updated':updated,'plot':plot}
	return render_template('index.html',**data) #this has changed

@app.route('/perProject')
@app.cache.cached(timeout=600)
def perProject():
	dump = request.args.get('data',False)
	conn = sqlite3.connect(pathToDB)
	revisions = pd.read_sql(''' SELECT project,COUNT(*) as cnt  FROM  revisions GROUP BY project''', con=conn)
	if dump:
		return jsonify(dict(zip(revisions['project'],revisions['cnt'])))	
	return revisions.to_html(index=False)


@app.route('/pagesNoHumans')
@app.cache.cached(timeout=600)
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
@app.cache.cached(timeout=600)
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
@app.cache.cached(timeout=600)
def perDay():
	dump = request.args.get('data',False)
	days = getEditsPerDay()
	if dump:
		return jsonify(days)
	return days.to_html()

@app.route('/perDayNoHumans')
@app.cache.cached(timeout=600)
def perDayNoHumans():
	dump = request.args.get('data',False)
	project = request.args.get('project',False)
	days = getEditsPerDay(humans=False,project=project)
	if dump:
		return jsonify(days)
	return days.to_html()

@app.route('/perProjectNoHumans')
@app.cache.cached(timeout=600)
def perProjectNoHumans():
	dump = request.args.get('data',False)
	if dump:
		return jsonify(getEditsPerProject())
	return getEditsPerProject().to_html()


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
	pages = pd.read_sql('''SELECT DISTINCT pagesPerProjectTable.page , pagesPerProjectTable.project,pagesPerProjectTable.wikilink,  itemsInfoTable.Instace_Of_Label,
                pagesPerProjectTable.url    FROM 
            itemsInfoTable INNER JOIN pagesPerProjectTable ON pagesPerProjectTable.wikidataItem = itemsInfoTable.item_id
            WHERE itemsInfoTable.Instace_Of != 'Q5' 
            ''',con=conn).sort_values(by=['project','page'])
	return pages

def totalEditsFunc(project=False,humans=False):
	return  getEditsPerDay(project=project,humans=humans).timestamp.sum()


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
	return projects


#writing this to get faster index stats / be careful when modifying other functions
def getStatsIndex():
	conn = sqlite3.connect(pathToDB)
	pages = pagesNoHumans()
	revisions = pd.read_sql(''' SELECT timestamp,page,project,user  FROM  revisions''', con=conn)
	revisions = pd.merge(revisions,pages,on=['page','project'])[['timestamp','project','page','user']]
	revisions.drop_duplicates(inplace=True)
	projects = revisions.project.unique().size
	editors = revisions.user.unique().size
	totalEdits = revisions.shape[0]
	updated = pd.to_datetime(revisions.timestamp.max())
	plot = create_plot()
	return {'totalEdits':totalEdits,'editors':editors,'projects':projects,'updated':updated,'plot':plot}




#Send sqlite db as file
@app.route('/downloadSqlite')
def sqliteDownload():
    return send_from_directory(path, DB, as_attachment=True)


#plot example
def create_plot():

    perDay = getEditsPerDay(humans=False)
    data = [
        go.Bar(
            x=perDay.index, # assign x as the dataframe column 'x'
            y=perDay['timestamp']
        )
    ]


    graphJSON = json.dumps(data, cls=plotly.utils.PlotlyJSONEncoder)

    return graphJSON



if __name__ == '__main__':
    app.debug = True
    app.run()
