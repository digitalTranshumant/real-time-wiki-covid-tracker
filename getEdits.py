#Download edits

import datetime
import mwapi
import pandas as pd
import pickle 
import pandas as pd
import sqlite3
from random import randint
conn = sqlite3.connect('/home/dsaez/real-time-wiki-covid-tracker/AllWikidataItems.sqlite')
now = datetime.datetime.now().strftime('%Y-%b-%d %H:%M:%S') 

defaultStartTime = '2020-01-01T00:00:00Z'

#def save revisions
def saveRevisionsPerDay(page_name,project,startdate):
 """
	    page_name: str, article title, ex: 'COVID-19'
	    project: str, project id, ex: 'es.wikipedia'
	    startdate: timestamp, counting from given day example '2020-01-01T00:00:00Z'"""
 row = []
 session = mwapi.Session("https://%s.org" % project, user_agent="dsaez@wikimedia.org - COVID-19 research")
 for response_doc in session.get(action='query', prop='revisions', titles=page_name, 
  rvprop=['ids', 'timestamp','user'], rvlimit=100, rvdir="newer", 
  formatversion=2, rvstart=startdate, continuation=True):
  for rev_doc in response_doc['query']['pages'][0]['revisions']:
   rev_id = rev_doc['revid']
   rev_timestamp = rev_doc['timestamp']
   rev_user = rev_doc['user']
   row.append([project,page_name,rev_timestamp,rev_user])
 df = pd.DataFrame(row,columns =['project','page','timestamp','user'])
 df.to_sql(name='revisions', if_exists='append',index_label = ['page','project','timestamp'], con=conn)


try: #if revision table already exists
	pagesPerProject= pd.read_sql(''' SELECT pagesPerProjectTable.project as project, pagesPerProjectTable.page as page ,MAX(revisions.timestamp) as timestamp FROM pagesPerProjectTable LEFT JOIN revisions ON
					 pagesPerProjectTable.project  = revisions.project AND
					pagesPerProjectTable.page = revisions.page GROUP BY pagesPerProjectTable.project,pagesPerProjectTable.page  ''',con=conn)
except:
	pagesPerProject= pd.read_sql(''' SELECT pagesPerProjectTable.project as project ,pagesPerProjectTable.page as page , '%s' as timestamp
						 FROM pagesPerProjectTable ''' % defaultStartTime, con=conn)


for index, row in pagesPerProject.iterrows():
	try:
		if not row['timestamp']: 
			row['timestamp']= defaultStartTime
		saveRevisionsPerDay(row['page'],row['project'],row['timestamp'])
	except:
		pass


updated = pd.DataFrame([now],columns=['revisions_update']) 
updated.to_sql(name='updated', if_exists='replace', con=conn)

