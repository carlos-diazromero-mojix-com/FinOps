from google.cloud import bigquery
client = bigquery.Client()
import pandas as pd
#import pandas_gbq
import numpy as np
import requests
import datetime as dt
import re
import json
from datetime import date, timedelta
from datetime import datetime


current_date = datetime.today()
dias_atras = 0
#dias_atras = int(dias_atras)
str_day = (current_date - timedelta(days = dias_atras)).strftime("%Y-%m-%d")
print(str_day)

df_logtable = pd.DataFrame()
sourceN = 0

### READ DATA SOURCES

## WORKLOG
table_name ='Worklogs'
query = """
 SELECT *
 FROM `saas-analytics-io.jira.%s`
 WHERE date(UPDATED) between '2022-01-01' and '%s'
       """%(table_name,str_day)

query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
#print(query)

try:
    df_worklog = query_job.to_dataframe()
    #print(df_worklog.info())
    #print(df_worklog.shape)

    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_worklog.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_worklog.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    sourceN = sourceN+1
except Exception as e:
    df_worklog = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e
    

## ISSUES
table_name ='Issues'
query = """
 SELECT *
 FROM `saas-analytics-io.jira.%s` 
 WHERE date(CREATED) between '2020-01-01' and '%s' """%(table_name,str_day)

query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query

try:
   # print(query)
    df_issues = query_job.to_dataframe()
    #print(df_issues.info())
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_issues.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_issues.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    sourceN = sourceN+1
except Exception as e:
    df_issues = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e 
    
    
## TIME IN STATUS

table_name ='TimeInStatus'

query = """
 SELECT *
 FROM `saas-analytics-io.jira.%s` 
"""%(table_name)
query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
#print(query)

try:
    df_timeinstatus = query_job.to_dataframe()
    #print(df_worklog.info())
    #print(df_worklog.shape)

    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_timeinstatus.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_timeinstatus.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    sourceN = sourceN+1
except Exception as e:
    df_customers = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e    
    
    
# DATA PROCESSING 
df_issue_ticket = df_issues[df_issues['ISSUE_KEY'].str.startswith('TICKET')]  
df_issue_ticket_s = df_issue_ticket[['ISSUE_ID','ISSUE_KEY','ISSUE_TYPE_NAME','ISSUE_STATUS_NAME','SUMMARY',
                        'PRIORITY','PROJECT_KEY','CURRENT_ASSIGNEE_NAME','CREATOR_NAME','REPORTER_NAME',
                       'ENVIRONMENT','CREATED','RESOLUTION','RESOLUTION_DATE','SECURITY_LEVEL_NAME','STATUS_CATEGORY_CHANGE_DATE',
                       'Platform_Version_12025','Progress___11891','Severity_Level_11918','Support_Level_11903','Tenant_12026','Tier_11886','Customer_Type_11954']]

df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('RED'),'TENANT']='RED'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('Eclipse'),'TENANT']='ECLIPSE'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('ECLIPSE'),'TENANT']='ECLIPSE'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('SESAME'),'TENANT']='SESAME'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('Sesame'),'TENANT']='SESAME'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('FedEx'),'TENANT']='FEDEX'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('HONDA'),'TENANT']='HONDA'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('Honda'),'TENANT']='HONDA'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('Ikea'),'TENANT']='IKEA'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('IKEA'),'TENANT']='IKEA'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('Mieloo'),'TENANT']='MIELOO'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('FALA'),'TENANT']='FALA'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('FALAPE'),'TENANT']='FALAPE'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('FALACO'),'TENANT']='FALACO'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('PEI'),'TENANT']='PEI'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('Perry Ellis'),'TENANT']='PEI'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('GREY'),'TENANT']='GREY'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('Grey'),'TENANT']='GREY'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('CENT'),'TENANT']='CENT'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('PERN'),'TENANT']='PERN'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('FISIA'),'TENANT']='FISIA'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('Fisia'),'TENANT']='FISIA'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('TOSCA'),'TENANT']='TOSCA'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('Tosca'),'TENANT']='TOSCA'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('CHIP'),'TENANT']='CHIP'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('Chipotle'),'TENANT']='CHIP'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('Raytheon'),'TENANT']='RAYTHEON'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('TRR'),'TENANT']='TRR'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('RLO2'),'TENANT']='RLO'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('CELINE'),'TENANT']='CELINE'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('Celine'),'TENANT']='CELINE'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('TORRA'),'TENANT']='TORRA'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('IVONNE'),'TENANT']='IVONNE'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('STUDIOF'),'TENANT']='STUDIOF'
df_issue_ticket_s.loc[df_issue_ticket_s['SUMMARY'].str.contains('Studio F'),'TENANT']='STUDIOF'


df_worklog_ticket = df_worklog[df_worklog['ISSUE_KEY'].str.startswith('TICKET')]
df_worklog_ticket_enh=pd.merge(df_worklog_ticket,df_issue_ticket_s[['ISSUE_KEY','ISSUE_TYPE_NAME','ISSUE_STATUS_NAME','SUMMARY','TENANT','Severity_Level_11918','Support_Level_11903','Customer_Type_11954','Tier_11886']],how='left', on ='ISSUE_KEY')
df_worklog_ticket_enh.loc[:,'LOGGED_HOURS']=df_worklog_ticket_enh['LOGGED_TIME']/60/60



df_timeinstatus_s = df_timeinstatus[df_timeinstatus['ISSUE_KEY'].str.startswith('TICKET')]
df_timeinstatus_s.loc[:,'DURATION_BUSINESS_DAYS_HOURS'] = df_timeinstatus_s['DURATION_IN_BUSINESS_DAYS_BH'].astype(int)/60/60
df_timeinstatus_s_pivot = pd.pivot_table(df_timeinstatus_s, values=['DURATION_BUSINESS_DAYS_HOURS'],index='ISSUE_KEY',columns='ISSUE_STATUS_NAME', aggfunc='sum')
df_timeinstatus_s_pivot = df_timeinstatus_s_pivot['DURATION_BUSINESS_DAYS_HOURS'].reset_index(drop = False)
df_timeinstatus_s_pivot.columns = df_timeinstatus_s_pivot.columns.str.replace(' ', '_')



#WRITE DATA IN BIGQUERY
print('Write Data in BigQuery')

try:
    table = "saas-analytics-io.processed.jira_TICKET"
    df_issue_ticket_s.to_gbq(table, if_exists='replace')
except Exception as e:
    print('Error in:',table,'ERROR CODE-->', e)    
try:
    table = "saas-analytics-io.processed.jira_TICKET_worklog"
    df_worklog_ticket_enh.to_gbq(table, if_exists='replace')
except Exception as e:
    print('Error in:',table,'ERROR CODE-->', e)
try:
    table = "saas-analytics-io.processed.jira_TICKET_timeinstatus"
    df_timeinstatus_s_pivot.to_gbq(table, if_exists='replace')
except Exception as e:
    print('Error in:',table,'ERROR CODE-->', e)