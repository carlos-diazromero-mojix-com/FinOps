# MAGIC %md
# MAGIC ## JIRA DATA 
# MAGIC - PR/IDEAS Trace to subtasks to get KPI to use in a dashboard in Looker
# MAGIC - Worklog in each issue to monitor developer hour and SP in a looker dashboard

# COMMAND ----------

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
#pd.set_option('display.width', 1000)
#pd.set_option("max_colwidth",10000)
#pd.set_option("max_rows",1000)
#pd.set_option("max_columns",100)

# COMMAND ----------

current_date = datetime.today()
dias_atras = 0
#dias_atras = int(dias_atras)
str_day = (current_date - timedelta(days = dias_atras)).strftime("%Y-%m-%d")
print(str_day)

# COMMAND ----------

df_logtable = pd.DataFrame()
sourceN = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Jira Sources

# COMMAND ----------

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

# COMMAND ----------

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

#df_issues.head()

# COMMAND ----------

table_name ='IssueSprints'

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
    df_issueSprints = query_job.to_dataframe()
    #print(df_worklog.info())
    #print(df_worklog.shape)

    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_issueSprints.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_issueSprints.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    sourceN = sourceN+1
except Exception as e:
    df_issueSprints = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e

# COMMAND ----------

table_name ='IssueLinks'

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
    df_issueLinks = query_job.to_dataframe()
    #print(df_worklog.info())
    #print(df_worklog.shape)

    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_issueLinks.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_issueLinks.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    sourceN = sourceN+1
except Exception as e:
    df_issueLinks = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e

# COMMAND ----------

table_name ='IssueFixVersions'

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
    df_IssueFixVersions = query_job.to_dataframe()
    #print(df_worklog.info())
    #print(df_worklog.shape)

    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_IssueFixVersions.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_IssueFixVersions.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    sourceN = sourceN+1
except Exception as e:
    df_IssueFixVersions = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e

# COMMAND ----------

table_name ='Versions'

query = """
 SELECT *
 FROM `saas-analytics-io.jira.%s` 
 WHERE VERSION_NAME like 'v%%' """%(table_name)

query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
#print(query)

try:
    df_versions = query_job.to_dataframe()
    #print(df_worklog.info())
    #print(df_worklog.shape)
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_versions.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_versions.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    sourceN = sourceN+1
except Exception as e:
    df_versions = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e

# COMMAND ----------

table_name ='Business_Unit_12031'

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
    df_businessUnit = query_job.to_dataframe()
    #print(df_worklog.info())
    #print(df_worklog.shape)

    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_businessUnit.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_businessUnit.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    sourceN = sourceN+1
except Exception as e:
    df_businessUnit = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e

# COMMAND ----------

table_name ='Customers_12132'

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
    df_customers = query_job.to_dataframe()
    #print(df_worklog.info())
    #print(df_worklog.shape)

    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_customers.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_customers.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    sourceN = sourceN+1
except Exception as e:
    df_customers = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e

# COMMAND ----------

table_name ='OKR_Category_12032'

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
    df_category = query_job.to_dataframe()
    #print(df_worklog.info())
    #print(df_worklog.shape)

    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_category.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_category.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    sourceN = sourceN+1
except Exception as e:
    df_category = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e

# COMMAND ----------

table_name ='Sprints'

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
    df_Sprints = query_job.to_dataframe()
    #print(df_worklog.info())
    #print(df_worklog.shape)

    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_Sprints.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_Sprints.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    sourceN = sourceN+1
except Exception as e:
    df_Sprints = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e

# COMMAND ----------

table_name ='Theme_12033'

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
    df_theme = query_job.to_dataframe()
    #print(df_worklog.info())
    #print(df_worklog.shape)

    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_theme.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_theme.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    sourceN = sourceN+1
except Exception as e:
    df_theme = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e

# COMMAND ----------

table_name ='Feature_Category_12139'

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
    df_feature_set_idea = query_job.to_dataframe()
    #print(df_worklog.info())
    #print(df_worklog.shape)

    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_feature_set_idea.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_feature_set_idea.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    sourceN = sourceN+1
except Exception as e:
    df_feature_set_idea = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e


table_name ='Key_Customers_11800'

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
    df_key_customer_idea = query_job.to_dataframe()
    #print(df_worklog.info())
    #print(df_worklog.shape)

    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_key_customer_idea.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_key_customer_idea.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    #sourceN = sourceN+1
except Exception as e:
    df_key_customer_idea = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e


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
    #sourceN = sourceN+1
except Exception as e:
    df_key_customer_idea = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e
    
table_name ='Product_Line_12367'

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
    df_productLine = query_job.to_dataframe()
    #print(df_worklog.info())
    #print(df_worklog.shape)

    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_productLine.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_productLine.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    #sourceN = sourceN+1
except Exception as e:
    df_productLine = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e
    

table_name ='Target_Market_12365'

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
    df_targetMarket = query_job.to_dataframe()
    #print(df_worklog.info())
    #print(df_worklog.shape)

    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = len(df_targetMarket.index)
    df_logtable.loc[sourceN,'tableColumns'] = len(df_targetMarket.columns)
    df_logtable.loc[sourceN,'status'] = 'ok'
    #sourceN = sourceN+1
except Exception as e:
    df_targetMarket = query_job.to_dataframe()
    df_logtable.loc[sourceN,'date'] = str_day
    df_logtable.loc[sourceN,'tableName'] = table_name
    df_logtable.loc[sourceN,'tableRows'] = 0
    df_logtable.loc[sourceN,'tableColumns'] = 0
    df_logtable.loc[sourceN,'status'] = e

print(df_logtable)

## Data Preparation
#### Worklog
df_worklog_1 = pd.merge(df_worklog,df_issues[['ISSUE_KEY','ISSUE_TYPE_NAME','Progress___11891','Story_Points_10400']],on = 'ISSUE_KEY', how= 'left')
df_worklog_1 = df_worklog_1.rename(columns={'ISSUE_TYPE_NAME':'ISSUE_TYPE_NAME_LOGGED',
                            'ISSUE_KEY':'ISSUE_KEY_LOGGED',
                            'Progress___11891':'PROGRESS_LOGGED',
                            'Story_Points_10400':'STORYPOINTS_LOGGED',
                            })


#### Versions (Select only releases)
df_versions = df_versions[df_versions['VERSION_NAME'].str.contains('^v[1-9]')].sort_values('START_DATE')
df_versions = df_versions.sort_values('START_DATE')

#### Category (Concat in one Field)
df_category_1 = df_category.pivot_table(index ='ISSUE_KEY',columns ='OKR_Category', aggfunc=any, fill_value =0)['ISSUE_ID'].reset_index()
df_category_1.loc[df_category_1['ARR']== 1,'CATEGORY_1'] = 'ARR'
df_category_1.loc[df_category_1['CYBER']== 1,'CATEGORY_2'] = 'CYBER'
df_category_1.loc[df_category_1['MIGRATE']== 1,'CATEGORY_3'] = 'MIGRATE'
df_category_1.loc[df_category_1['PARTNER']== 1,'CATEGORY_4'] = 'PARTNER'
df_category_1.loc[df_category_1['PROD']== 1,'CATEGORY_5'] = 'PROD'
df_category_1.loc[df_category_1['SOURCE']== 1,'CATEGORY_6'] = 'SOURCE'
df_category_1.loc[df_category_1['SP']== 1,'CATEGORY_7'] = 'SP'
df_category_1.loc[df_category_1['STRATEGIC']== 1,'CATEGORY_8'] = 'STRATEGIC'
df_category_1.loc[df_category_1['TECH']== 1,'CATEGORY_9'] = 'TECH'
df_category_1 = df_category_1.fillna('')
df_category_1.loc[:,'CATEGORY'] = df_category_1[['CATEGORY_1', 'CATEGORY_2','CATEGORY_3','CATEGORY_4','CATEGORY_5','CATEGORY_6','CATEGORY_7','CATEGORY_8','CATEGORY_9']].apply(":".join, axis=1)
df_category_1 = df_category_1.drop(columns=['CATEGORY_1','CATEGORY_2','CATEGORY_3','CATEGORY_4','CATEGORY_5',
                          'CATEGORY_6','CATEGORY_7','CATEGORY_8','CATEGORY_9','ARR'
                         ,'CYBER','MIGRATE','PARTNER','PROD','SOURCE'
                         ,'SP','STRATEGIC','TECH'])

df_category_1['CATEGORY'] = df_category_1['CATEGORY'].str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":")
df_category_1.loc[df_category_1['CATEGORY'].str.startswith(":"),'CATEGORY']=df_category_1['CATEGORY'].str[1:]
df_category_1.loc[df_category_1['CATEGORY'].str.endswith(":"),'CATEGORY']=df_category_1['CATEGORY'].str[:-1]

#### Customers (Concat in one Field)
df_customers['Customers_2']=df_customers['Customers'] 
df_customers_1 = df_customers.pivot_table(index ='ISSUE_KEY',columns ='Customers',values= 'Customers_2', aggfunc=sum, fill_value ='').reset_index()
list1 = []
for i in range (1,df_customers['Customers'].nunique()+1,1):
    list1.append((i))
df_customers_1.loc[:,'CUSTOMERS'] = df_customers_1.iloc[:,list1].apply(":".join, axis=1)
df_customers_1 = df_customers_1[['ISSUE_KEY','CUSTOMERS']]
df_customers_1['CUSTOMERS'] = df_customers_1['CUSTOMERS'].str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":")
df_customers_1.loc[df_customers_1['CUSTOMERS'].str.startswith(":"),'CUSTOMERS']=df_customers_1['CUSTOMERS'].str[1:]
df_customers_1.loc[df_customers_1['CUSTOMERS'].str.endswith(":"),'CUSTOMERS']=df_customers_1['CUSTOMERS'].str[:-1]

#### Business Unit (Concat in one Field)
df_businessUnit['Business_Unit_2']=df_businessUnit['Business_Unit'] 
df_businessUnit_1 = df_businessUnit.pivot_table(index ='ISSUE_KEY',columns ='Business_Unit',values= 'Business_Unit_2', aggfunc=sum, fill_value ='').reset_index()
df_businessUnit_1.loc[:,'BU'] = df_businessUnit_1.iloc[:,[1,2,3]].apply(":".join, axis=1)
df_businessUnit_1 = df_businessUnit_1[['ISSUE_KEY','BU']]
df_businessUnit_1['BU'] = df_businessUnit_1['BU'].str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":")
df_businessUnit_1.loc[df_businessUnit_1['BU'].str.startswith(":"),'BU']=df_businessUnit_1['BU'].str[1:]
df_businessUnit_1.loc[df_businessUnit_1['BU'].str.endswith(":"),'BU']=df_businessUnit_1['BU'].str[:-1]

#### Fix Version (Concat in one Field)
df_IssueFixVersions['VERSION_NAME_2']=df_IssueFixVersions['VERSION_NAME'] 
df_IssueFixVersions_1 = df_IssueFixVersions.pivot_table(index ='ISSUE_KEY',columns ='VERSION_NAME',values= 'VERSION_NAME_2', aggfunc=sum, fill_value ='').reset_index()
list1 = []
for i in range (1,df_IssueFixVersions['VERSION_NAME'].nunique()+1,1):
    list1.append((i))
df_IssueFixVersions_1.loc[:,'VERSION_NAME'] = df_IssueFixVersions_1.iloc[:,list1].apply(":".join, axis=1)
df_IssueFixVersions_1 = df_IssueFixVersions_1[['ISSUE_KEY','VERSION_NAME']]
df_IssueFixVersions_1['VERSION_NAME'] = df_IssueFixVersions_1['VERSION_NAME'].str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":")
df_IssueFixVersions_1.loc[df_IssueFixVersions_1['VERSION_NAME'].str.startswith(":"),'VERSION_NAME']=df_IssueFixVersions_1['VERSION_NAME'].str[1:]
df_IssueFixVersions_1.loc[df_IssueFixVersions_1['VERSION_NAME'].str.endswith(":"),'VERSION_NAME']=df_IssueFixVersions_1['VERSION_NAME'].str[:-1]

#### Clean Issue Sprints (delete duplicated)
df_issueSprints = df_issueSprints.sort_values('SPRINT_ID')
df_issueSprints.drop_duplicates(subset="ISSUE_KEY",keep='last', inplace=True)

#### THEME (Concat in one field)
df_theme['THEME']=df_theme['Theme'] 
df_theme_1 = df_theme.pivot_table(index ='ISSUE_KEY',columns ='Theme',values= 'THEME', aggfunc=sum, fill_value ='').reset_index()
list1 = []
for i in range (1,df_theme['Theme'].nunique()+1,1):
    list1.append((i))
df_theme_1.loc[:,'THEME'] = df_theme_1.iloc[:,list1].apply(":".join, axis=1)
df_theme_1 = df_theme_1[['ISSUE_KEY','THEME']]
df_theme_1['THEME'] = df_theme_1['THEME'].str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":")
df_theme_1.loc[df_theme_1['THEME'].str.startswith(":"),'THEME']=df_theme_1['THEME'].str[1:]
df_theme_1.loc[df_theme_1['THEME'].str.endswith(":"),'THEME']=df_theme_1['THEME'].str[:-1]
df_theme_1.loc[df_theme_1['THEME']=='Chain:Supply','THEME']='SuppplyChain'
df_theme_1.loc[df_theme_1['THEME']=='Product:Single','THEME']='SingleProduct'

#### category for IDEAS (Concat in one field)
#df_category_idea['IDEA_CATEGORY']=df_category_idea['Category'] 
#df_category_idea_1 = df_category_idea.pivot_table(index ='ISSUE_KEY',columns ='Category',values= 'IDEA_CATEGORY', aggfunc=sum, fill_value ='').reset_index()
#list1 = []
#for i in range (1,df_category_idea['Category'].nunique()+1,1):
#    list1.append((i))
#df_category_idea_1.loc[:,'IDEA_CATEGORY'] = df_category_idea_1.iloc[:,list1].apply(":".join, axis=1)
#df_category_idea_1 = df_category_idea_1[['ISSUE_KEY','IDEA_CATEGORY']]
#df_category_idea_1['IDEA_CATEGORY'] = df_category_idea_1['IDEA_CATEGORY'].str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":")
#df_category_idea_1.loc[df_category_idea_1['IDEA_CATEGORY'].str.startswith(":"),'IDEA_CATEGORY']=df_category_idea_1['IDEA_CATEGORY'].str[1:]
#df_category_idea_1.loc[df_category_idea_1['IDEA_CATEGORY'].str.endswith(":"),'IDEA_CATEGORY']=df_category_idea_1['IDEA_CATEGORY'].str[:-1]

#### FeatureSet for IDEAS (Concat in one field)
df_feature_set_idea['IDEA_FEATURE_SET']=df_feature_set_idea['Feature_Category'] 
df_feature_set_idea_1 = df_feature_set_idea.pivot_table(index ='ISSUE_KEY',columns ='Feature_Category',values= 'IDEA_FEATURE_SET', aggfunc=sum, fill_value ='').reset_index()
list1 = []
for i in range (1,df_feature_set_idea['Feature_Category'].nunique()+1,1):
    list1.append((i))
df_feature_set_idea_1.loc[:,'IDEA_FEATURE_SET'] = df_feature_set_idea_1.iloc[:,list1].apply(":".join, axis=1)
df_feature_set_idea_1 = df_feature_set_idea_1[['ISSUE_KEY','IDEA_FEATURE_SET']]
df_feature_set_idea_1['IDEA_FEATURE_SET'] = df_feature_set_idea_1['IDEA_FEATURE_SET'].str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":")
df_feature_set_idea_1.loc[df_feature_set_idea_1['IDEA_FEATURE_SET'].str.startswith(":"),'IDEA_FEATURE_SET']=df_feature_set_idea_1['IDEA_FEATURE_SET'].str[1:]
df_feature_set_idea_1.loc[df_feature_set_idea_1['IDEA_FEATURE_SET'].str.endswith(":"),'IDEA_FEATURE_SET']=df_feature_set_idea_1['IDEA_FEATURE_SET'].str[:-1]

#### Key Customer for IDEAS (Concat in one field)
df_key_customer_idea['IDEA_KEY_CUSTOMERS']=df_key_customer_idea['Key_Customers'] 
df_key_customer_idea_1 = df_key_customer_idea.pivot_table(index ='ISSUE_KEY',columns ='Key_Customers',values= 'IDEA_KEY_CUSTOMERS', aggfunc=sum, fill_value ='').reset_index()
list1 = []
for i in range (1,df_key_customer_idea['Key_Customers'].nunique()+1,1):
    list1.append((i))
df_key_customer_idea_1.loc[:,'IDEA_KEY_CUSTOMERS'] = df_key_customer_idea_1.iloc[:,list1].apply(":".join, axis=1)
df_key_customer_idea_1 = df_key_customer_idea_1[['ISSUE_KEY','IDEA_KEY_CUSTOMERS']]
df_key_customer_idea_1['IDEA_KEY_CUSTOMERS'] = df_key_customer_idea_1['IDEA_KEY_CUSTOMERS'].str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":")
df_key_customer_idea_1.loc[df_key_customer_idea_1['IDEA_KEY_CUSTOMERS'].str.startswith(":"),'IDEA_KEY_CUSTOMERS']=df_key_customer_idea_1['IDEA_KEY_CUSTOMERS'].str[1:]
df_key_customer_idea_1.loc[df_key_customer_idea_1['IDEA_KEY_CUSTOMERS'].str.endswith(":"),'IDEA_KEY_CUSTOMERS']=df_key_customer_idea_1['IDEA_KEY_CUSTOMERS'].str[:-1]

#### Target Market for IDEAS (Concat in one field)
df_targetMarket['TARGET_MARKET']=df_targetMarket['Target_Market'] 
df_targetMarket_1 = df_targetMarket.pivot_table(index ='ISSUE_KEY',columns ='Target_Market',values= 'TARGET_MARKET', aggfunc=sum, fill_value ='').reset_index()
list1 = []
for i in range (1,df_targetMarket['Target_Market'].nunique()+1,1):
    list1.append((i))
df_targetMarket_1.loc[:,'TARGET_MARKET'] = df_targetMarket_1.iloc[:,list1].apply(":".join, axis=1)
df_targetMarket_1 = df_targetMarket_1[['ISSUE_KEY','TARGET_MARKET']]
df_targetMarket_1['TARGET_MARKET'] = df_targetMarket_1['TARGET_MARKET'].str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":")
df_targetMarket_1.loc[df_targetMarket_1['TARGET_MARKET'].str.startswith(":"),'TARGET_MARKET']=df_targetMarket_1['TARGET_MARKET'].str[1:]
df_targetMarket_1.loc[df_targetMarket_1['TARGET_MARKET'].str.endswith(":"),'TARGET_MARKET']=df_targetMarket_1['TARGET_MARKET'].str[:-1]

### time In Status for Ideas
df_timeinstatus_IDEAS = df_timeinstatus[df_timeinstatus['ISSUE_KEY'].str.startswith('IDEA')]
df_timeinstatus_IDEAS = df_timeinstatus_IDEAS[['ISSUE_KEY','ISSUE_STATUS_ID','ISSUE_STATUS_NAME',
                                               'DURATION_IN_BUSINESS_DAYS_BH','FIRST_TRANSITION_TO_STATUS',
                                               'LAST_TRANSITION_TO_STATUS']]

#DELIVERED_DATE
df_timeinstatus_IDEAS_delivered = df_timeinstatus_IDEAS[df_timeinstatus_IDEAS['ISSUE_STATUS_NAME'].str.contains('Delivered')]
df_timeinstatus_IDEAS_delivered = df_timeinstatus_IDEAS_delivered[['ISSUE_KEY','LAST_TRANSITION_TO_STATUS']]
df_timeinstatus_IDEAS_delivered.rename(columns={'LAST_TRANSITION_TO_STATUS':'DELIVERED_DATE'}, inplace=True)
df_timeinstatus_IDEAS_delivered = df_timeinstatus_IDEAS_delivered.drop_duplicates('ISSUE_KEY', keep ='first')

#BACKLOG_DATE (last transition)
df_timeinstatus_IDEAS_backlog = df_timeinstatus_IDEAS[df_timeinstatus_IDEAS['ISSUE_STATUS_NAME'].str.contains('Backlog')]
df_timeinstatus_IDEAS_backlog = df_timeinstatus_IDEAS_backlog[['ISSUE_KEY','LAST_TRANSITION_TO_STATUS']]
df_timeinstatus_IDEAS_backlog.rename(columns={'LAST_TRANSITION_TO_STATUS':'BACKLOG_DATE'}, inplace=True)
df_timeinstatus_IDEAS_backlog = df_timeinstatus_IDEAS_backlog.drop_duplicates('ISSUE_KEY', keep ='first')

#PRD_DATE (first transition)
df_timeinstatus_IDEAS_PRD = df_timeinstatus_IDEAS[df_timeinstatus_IDEAS['ISSUE_STATUS_NAME'].str.contains('PRD')]
df_timeinstatus_IDEAS_PRD = df_timeinstatus_IDEAS_PRD[['ISSUE_KEY','FIRST_TRANSITION_TO_STATUS']]
df_timeinstatus_IDEAS_PRD.rename(columns={'FIRST_TRANSITION_TO_STATUS':'PRD_DATE'}, inplace=True)
df_timeinstatus_IDEAS_PRD = df_timeinstatus_IDEAS_PRD.drop_duplicates('ISSUE_KEY', keep ='first')

#FACTORY_DATE (first transition)
df_timeinstatus_IDEAS_FACTORY = df_timeinstatus_IDEAS[df_timeinstatus_IDEAS['ISSUE_STATUS_NAME'].str.contains('Factory')]
df_timeinstatus_IDEAS_FACTORY = df_timeinstatus_IDEAS_FACTORY[['ISSUE_KEY','FIRST_TRANSITION_TO_STATUS']]
df_timeinstatus_IDEAS_FACTORY.rename(columns={'FIRST_TRANSITION_TO_STATUS':'FACTORY_DATE'}, inplace=True)
df_timeinstatus_IDEAS_FACTORY = df_timeinstatus_IDEAS_FACTORY.drop_duplicates('ISSUE_KEY', keep ='first')

#ARCHITECTURE_BUSINESS_HOURS_DURATION (first DURATION_IN_BUSINESS_DAYS_BH)
df_timeinstatus_IDEAS_Architecture = df_timeinstatus_IDEAS[df_timeinstatus_IDEAS['ISSUE_STATUS_NAME'].str.contains('Architecture')]
df_timeinstatus_IDEAS_Architecture = df_timeinstatus_IDEAS_Architecture[['ISSUE_KEY','DURATION_IN_BUSINESS_DAYS_BH']]
df_timeinstatus_IDEAS_Architecture['ARCHITECTURE_BUSINESS_HOURS_DURATION']= df_timeinstatus_IDEAS_Architecture['DURATION_IN_BUSINESS_DAYS_BH']/60/60
df_timeinstatus_IDEAS_Architecture.drop(columns={'DURATION_IN_BUSINESS_DAYS_BH'}, inplace=True)
df_timeinstatus_IDEAS_Architecture = df_timeinstatus_IDEAS_Architecture.drop_duplicates('ISSUE_KEY', keep ='first')





### Join datasources and Consolidate one table complete
df_full = pd.merge(df_issues, df_issueSprints, on =['ISSUE_ID','ISSUE_KEY'],how = 'left')
df_full = pd.merge(df_full, df_IssueFixVersions_1, on =['ISSUE_KEY'],how = 'left')
df_full = pd.merge(df_full, df_businessUnit_1, on =['ISSUE_KEY'],how = 'left')
df_full = pd.merge(df_full, df_customers_1, on =['ISSUE_KEY'],how = 'left')
df_full = pd.merge(df_full, df_category_1, on =['ISSUE_KEY'],how = 'left')
df_full = pd.merge(df_full, df_theme_1, on =['ISSUE_KEY'],how = 'left')
df_full = pd.merge(df_full, df_Sprints, on =['SPRINT_ID','SPRINT_NAME'],how = 'left')
df_full = pd.merge(df_full, df_timeinstatus_IDEAS_delivered, on ='ISSUE_KEY',how = 'left')
df_full = pd.merge(df_full, df_timeinstatus_IDEAS_backlog, on ='ISSUE_KEY',how = 'left')
df_full = pd.merge(df_full, df_timeinstatus_IDEAS_PRD, on ='ISSUE_KEY',how = 'left')
df_full = pd.merge(df_full, df_timeinstatus_IDEAS_FACTORY, on ='ISSUE_KEY',how = 'left')
df_full = pd.merge(df_full, df_timeinstatus_IDEAS_Architecture, on ='ISSUE_KEY',how = 'left')
df_full = pd.merge(df_full, df_targetMarket_1[['ISSUE_KEY','TARGET_MARKET']], on ='ISSUE_KEY',how = 'left')
df_full = pd.merge(df_full, df_productLine[['ISSUE_KEY','Product_Line']] , on ='ISSUE_KEY',how = 'left')

#### Create Dataframe for IDEAS/PR
df_issues_s = df_full[['CREATED','ISSUE_ID','ISSUE_KEY','SUMMARY','ISSUE_TYPE_NAME','ISSUE_STATUS_NAME',
                         'SPRINT_ID','SPRINT_NAME','STATE','START_DATE','END_DATE','COMPLETE_DATE',
                         'VERSION_NAME',
                         'PRIORITY','RESOLUTION','CURRENT_ASSIGNEE_NAME','REPORTER_NAME','RESOLUTION_DATE','STATUS_CATEGORY_CHANGE_DATE',
                         #'TIME_SPENT','TIME_SPENT_WITH_SUBTASKS',
                       'PARENT_ISSUE_KEY','CATEGORY','BU','CUSTOMERS',
                       'THEME','DELIVERED_DATE',
                       'BACKLOG_DATE','PRD_DATE','FACTORY_DATE','ARCHITECTURE_BUSINESS_HOURS_DURATION'
                       ,'Estimate_Sprints_12213',
                       'Story_Points_10400','Progress___11891','Quarter_12166','Story_Points__Estimate__12398','Story_Points__Executed__12397','Story_Points__Guesstimate__12242',
                        'Product_Line', 'TARGET_MARKET'
                       #,'__Effort_12125','Layout_12166'
                      ]]
df_issues_s = df_issues_s.rename(columns={"Story_Points_10400":"STORYPOINTS",
                              "Progress___11891":"PROGRESS", 'Quarter_12166':'ROADMAP'
                              , 'Estimate_Sprints_12213':'ESTIMATE_SPRINTS', 'Story_Points__Guesstimate__12242':'GUESSTIMATE_STORYPOINTS'
                              ,'Story_Points__Estimate__12398':'ESTIMATE_STORYPOINTS', 'Story_Points__Executed__12397':'EXECUTED_STORYPOINTS'
                             })


#### Create Dataframe for linked issues (Epics)
df_issues_s_2 = df_issues_s[['ISSUE_ID','ISSUE_KEY','SUMMARY','ISSUE_TYPE_NAME','ISSUE_STATUS_NAME','CURRENT_ASSIGNEE_NAME'
                             ,'SPRINT_NAME','START_DATE','END_DATE','VERSION_NAME','CATEGORY','BU','CUSTOMERS',
                            'THEME',
                             'STORYPOINTS','PROGRESS']]
df_issues_s_2 = df_issues_s_2.rename(columns={"ISSUE_ID": "LINKED_1_ISSUE_ID", 
                              "ISSUE_KEY": "LINKED_1_ISSUE_KEY",
                              "SUMMARY": "LINKED_1_SUMMARY",                
                              "ISSUE_TYPE_NAME":"LINKED_1_ISSUE_TYPE_NAME",
                              "ISSUE_STATUS_NAME":"LINKED_1_ISSUE_STATUS_NAME",
                              'CURRENT_ASSIGNEE_NAME':"LINKED_1_ASSIGNEE_NAME",
                              "SPRINT_NAME":"LINKED_1_SPRINT_NAME",
                              "START_DATE":"LINKED_1_SPRINT_START_DATE",
                              "END_DATE":"LINKED_1_SPRINT_END_DATE", 
                              "VERSION_NAME":"LINKED_1_VERSION_NAME",
                              "CATEGORY":"LINKED_1_CATEGORY",
                              "BU":"LINKED_1_BU",
                              "CUSTOMERS":"LINKED_1_CUSTOMERS",
                              "THEME":"LINKED_1_THEME",                
                              "STORYPOINTS":"LINKED_1_STORYPOINTS",
                              "PROGRESS":"LINKED_1_PROGRESS"
                             }) 

#### Join ISSUES WITH issueLinks
df_issues_1 = pd.merge(df_issues_s,df_issueLinks,on =['ISSUE_ID','ISSUE_KEY'],how="left")
df_issues_1 = df_issues_1.rename(columns={"LINKED_ISSUE_ID":"LINKED_1_ISSUE_ID", 
                            "LINKED_ISSUE_KEY":"LINKED_1_ISSUE_KEY",
                            "DIRECTION":"DIRECTION_1",
                            "TYPE":"TYPE_1"
                             })

#### Enrichment ISSUES WITH Epics Dataframe
df_issues_2 = pd.merge(df_issues_1, df_issues_s_2,on=['LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_ID'],how="left")

### Product Requests (PR) - Data Preparation
try:
  print('PR transformation')
  df_PR = df_issues_2[df_issues_2['ISSUE_TYPE_NAME'].str.startswith('Product Request')][['CREATED','ISSUE_KEY','SUMMARY',
                                                                       'ISSUE_TYPE_NAME','ISSUE_STATUS_NAME',
                                                                        'CURRENT_ASSIGNEE_NAME',
        'SPRINT_ID','SPRINT_NAME','STATE','START_DATE','END_DATE','COMPLETE_DATE',
        #'VERSION_ID',
        'VERSION_NAME','PRIORITY',
        'REPORTER_NAME','RESOLUTION_DATE','STATUS_CATEGORY_CHANGE_DATE',
        #'TIME_SPENT','TIME_SPENT_WITH_SUBTASKS',
         'CATEGORY','BU','CUSTOMERS','STORYPOINTS','PROGRESS','ROADMAP','TYPE_1','DIRECTION_1',                                                              
        'LINKED_1_ISSUE_KEY','LINKED_1_SUMMARY','LINKED_1_ISSUE_TYPE_NAME','LINKED_1_ISSUE_STATUS_NAME','LINKED_1_ASSIGNEE_NAME',
        'LINKED_1_SPRINT_NAME','LINKED_1_SPRINT_START_DATE','LINKED_1_SPRINT_END_DATE','LINKED_1_VERSION_NAME',
        'LINKED_1_CATEGORY','LINKED_1_BU','LINKED_1_CUSTOMERS',
        'LINKED_1_THEME',
        'LINKED_1_STORYPOINTS','LINKED_1_PROGRESS'
         ]]
  df_PR = df_PR[(df_PR['TYPE_1']=='Depends')&(df_PR['DIRECTION_1']=='Outward')]
  df_issues_PR_1 = df_issues_2[(df_issues_2['ISSUE_TYPE_NAME']!='Product Request')
                              &(df_issues_2['PARENT_ISSUE_KEY'].notnull())][['CREATED','ISSUE_KEY','SUMMARY',
                                                                                'ISSUE_TYPE_NAME','ISSUE_STATUS_NAME',
                                                                                'STATUS_CATEGORY_CHANGE_DATE',
                                                                                'SPRINT_NAME','STATE','START_DATE',
                                                                                'END_DATE','COMPLETE_DATE',
                                                                                'VERSION_NAME','CURRENT_ASSIGNEE_NAME',
                                                                                'REPORTER_NAME',
                                                                                #'TIME_SPENT','TIME_SPENT_WITH_SUBTASKS',
                                                                                'CATEGORY','BU','CUSTOMERS',
                                                                              'THEME',
                                                                              'STORYPOINTS','PROGRESS','PARENT_ISSUE_KEY'
                                                                              ]]


  df_issues_PR_1 = df_issues_PR_1.rename(columns={"CREATED": "CHILD_CREATED", 
                                "ISSUE_KEY": "CHILD_ISSUE_KEY",
                                "SUMMARY": "CHILD_SUMMARY",                                            
                                "ISSUE_TYPE_NAME":"CHILD_ISSUE_TYPE_NAME",
                                "STATUS_CATEGORY_CHANGE_DATE":"CHILD_STATUS_CATEGORY_CHANGE_DATE",
                                "ISSUE_STATUS_NAME":"CHILD_ISSUE_STATUS_NAME",
                                "SPRINT_NAME":"CHILD_SPRINT_NAME",
                                "STATE":"CHILD_SPRINT_STATE",                  
                                "START_DATE":"CHILD_SPRINT_START_DATE",
                                "END_DATE":"CHILD_SPRINT_END_DATE",
                                "COMPLETE_DATE":"CHILD_SPRINT_COMPLETE_DATE",
                                "VERSION_NAME":"CHILD_VERSION_NAME",
                                "CURRENT_ASSIGNEE_NAME":"CHILD_ASSIGNEE_NAME",
                                'REPORTER_NAME':'CHILD_REPORTER_NAME',
                                #'TIME_SPENT':'CHILD_TIME_SPENT',
                                #'TIME_SPENT_WITH_SUBTASKS':'CHILD_TIME_SPENT_WITH_SUBTASKS',
                                'CATEGORY':'CHILD_CATEGORY',
                                'BU':'CHILD_BU',
                                'CUSTOMERS':'CHILD_CUSTOMERS',
                                'THEME':'CHILD_THEME',                  
                                'STORYPOINTS':'CHILD_STORYPOINTS',
                                'PROGRESS':'CHILD_PROGRESS',
                                'PARENT_ISSUE_KEY':'LINKED_1_ISSUE_KEY'                  
                              })

  df_PR_1 = pd.merge(df_PR, df_issues_PR_1,how='left', on='LINKED_1_ISSUE_KEY')
  df_sub_PR_1 = df_issues_2[(df_issues_2['ISSUE_TYPE_NAME']!='Product Request')
                              &(df_issues_2['PARENT_ISSUE_KEY'].notnull())][['CREATED','ISSUE_KEY','SUMMARY',
                                                                                'ISSUE_TYPE_NAME','ISSUE_STATUS_NAME',
                                                                                'STATUS_CATEGORY_CHANGE_DATE',
                                                                                'SPRINT_NAME','STATE','START_DATE',
                                                                                'END_DATE','COMPLETE_DATE',
                                                                                'VERSION_NAME','CURRENT_ASSIGNEE_NAME',
                                                                                'REPORTER_NAME',
                                                                                #'TIME_SPENT','TIME_SPENT_WITH_SUBTASKS',
                                                                                'CATEGORY','BU','CUSTOMERS',
                                                                              'THEME',
                                                                              'STORYPOINTS','PROGRESS','PARENT_ISSUE_KEY'
                                                                              ]]
  df_sub_PR_1 = df_sub_PR_1.rename(columns={"CREATED": "SUB_CREATED", 
                                "ISSUE_KEY": "SUB_ISSUE_KEY",
                                "SUMMARY": "SUB_SUMMARY", 
                                "ISSUE_TYPE_NAME":"SUB_ISSUE_TYPE_NAME",
                                "STATUS_CATEGORY_CHANGE_DATE":"SUB_STATUS_CATEGORY_CHANGE_DATE",
                                "ISSUE_STATUS_NAME":"SUB_ISSUE_STATUS_NAME",
                                "SPRINT_NAME":"SUB_SPRINT_NAME",
                                "STATE":"SUB_SPRINT_STATE",                  
                                "START_DATE":"SUB_SPRINT_START_DATE",
                                "END_DATE":"SUB_SPRINT_END_DATE",
                                "COMPLETE_DATE":"SUB_SPRINT_COMPLETE_DATE",
                                "VERSION_NAME":"SUB_VERSION_NAME",
                                "CURRENT_ASSIGNEE_NAME":"SUB_ASSIGNEE_NAME",
                                'REPORTER_NAME':'SUB_REPORTER_NAME',
                                #'TIME_SPENT':'SUB_TIME_SPENT',
                                #'TIME_SPENT_WITH_SUBTASKS':'SUB_TIME_SPENT_WITH_SUBTASKS',
                                'CATEGORY':'SUB_CATEGORY',
                                'BU':'SUB_BU',
                                'CUSTOMERS':'SUB_CUSTOMERS',
                                'THEME':'SUB_THEME',  
                                'STORYPOINTS':'SUB_STORYPOINTS',
                                'PROGRESS':'SUB_PROGRESS',
                                'PARENT_ISSUE_KEY':'CHILD_ISSUE_KEY'                  
                              })

  df_PR_f = pd.merge(df_PR_1, df_sub_PR_1,how='left', on='CHILD_ISSUE_KEY')

  df_PR_f.loc[df_PR_f['SUB_ISSUE_KEY'].notnull(),'TOTAL_STORYPOINTS']= df_PR_f['SUB_STORYPOINTS']
  df_PR_f.loc[(df_PR_f['TOTAL_STORYPOINTS'].isnull())&
              (df_PR_f['SUB_ISSUE_KEY'].isnull())&
              (df_PR_f['CHILD_ISSUE_KEY'].notnull()),'TOTAL_STORYPOINTS']= df_PR_f['CHILD_STORYPOINTS']
  df_PR_f.loc[(df_PR_f['TOTAL_STORYPOINTS'].isnull())&
              (df_PR_f['SUB_ISSUE_KEY'].isnull())&
              (df_PR_f['CHILD_ISSUE_KEY'].isnull())&
              (df_PR_f['LINKED_1_ISSUE_KEY'].notnull()),'TOTAL_STORYPOINTS']= df_PR_f['LINKED_1_STORYPOINTS']

  # Assign version from CHILD SPRINT START DATE
  df_versions['START_DATE_m'] = df_versions['RELEASE_DATE']+pd.Timedelta(days=1)
  df_versions['START_DATE_2_m'] = df_versions['RELEASE_DATE']+pd.Timedelta(seconds=86399)
  

  for i in range(0,df_versions['VERSION_ID'].count()):
      START_DATE = df_versions.iloc[i-1,5] 
      END_DATE = df_versions.iloc[i,5]  
      if i < 10:
          i_m = "0"+str(i)
      if i >=10:
          i_m = str(i)
      VERSION = i_m+" "+df_versions.iloc[i,1]
      if VERSION == '00 v8.4 Chihuahua':
          START_DATE='2021-09-27 00:00:00+00:00'
      #START_DATE = START_DATE.replace("00:00:00+00:00","23:59:59+00:00")
      #END_DATE = END_DATE
      df_PR_f.loc[(df_PR_f['CHILD_SPRINT_START_DATE']>=START_DATE)
                  &(df_PR_f['CHILD_SPRINT_START_DATE']<=END_DATE),'CHILD_SPRINT_DATE_VERSION']= VERSION
      df_PR_f.loc[(df_PR_f['CHILD_SPRINT_END_DATE']>=START_DATE)
                  &(df_PR_f['CHILD_SPRINT_END_DATE']<=END_DATE),'CHILD_SPRINT_END_DATE_VERSION']= VERSION
      #df_PR_f.loc[(df_PR_f['STATUS_DATE_VERSION'].isnull())&(df_PR_f['CHILD_STATUS_CATEGORY_CHANGE_DATE']>START_DATE)&(df_PR_f['CHILD_STATUS_CATEGORY_CHANGE_DATE']>END_DATE),'STATUS_DATE_VERSION']= VERSION
      print(START_DATE,END_DATE,VERSION)
      
  try:
    print('JIRA PRs UPLOAD to BIGQUERY')
    table = "saas-analytics-io.processed.jira_processed_PR"
    df_PR_f.to_gbq(table, if_exists='replace')
    print('JIRA PRs FULL DATA write in BQ --> done')
  except Exception as e:
    print('error: ',e)

except Exception as e:
  print('error: ',e)


### IDEA Tickets - Data Preparation
df_IDEA_w_epics = df_issues_2[(df_issues_2['ISSUE_TYPE_NAME'].str.startswith('Idea'))
                      &(df_issues_2['TYPE_1'].str.startswith('Polaris'))]
        
df_IDEA_wo_epics = df_issues_2[(df_issues_2['ISSUE_TYPE_NAME'].str.startswith('Idea'))
                      &(df_issues_2['TYPE_1'].isnull())]

df_IDEA = df_IDEA_w_epics.append(df_IDEA_wo_epics)

df_IDEA = df_IDEA[['CREATED','ISSUE_KEY','SUMMARY','ISSUE_TYPE_NAME','ISSUE_STATUS_NAME','DELIVERED_DATE','CURRENT_ASSIGNEE_NAME',
                   'SPRINT_ID','SPRINT_NAME','STATE','START_DATE','END_DATE','COMPLETE_DATE',
                    #'VERSION_ID',
                    'VERSION_NAME','PRIORITY',
                    'REPORTER_NAME','RESOLUTION_DATE','STATUS_CATEGORY_CHANGE_DATE',
                    'BACKLOG_DATE','PRD_DATE','FACTORY_DATE','ARCHITECTURE_BUSINESS_HOURS_DURATION',
                    #'TIME_SPENT','TIME_SPENT_WITH_SUBTASKS',
                    'CATEGORY','BU','CUSTOMERS','STORYPOINTS','PROGRESS','ROADMAP','ESTIMATE_SPRINTS','ESTIMATE_STORYPOINTS','GUESSTIMATE_STORYPOINTS','EXECUTED_STORYPOINTS','TYPE_1','DIRECTION_1',
                    'Product_Line', 'TARGET_MARKET',   
                    'LINKED_1_ISSUE_KEY','LINKED_1_SUMMARY','LINKED_1_ISSUE_TYPE_NAME','LINKED_1_ISSUE_STATUS_NAME','LINKED_1_ASSIGNEE_NAME',
                    'LINKED_1_SPRINT_NAME','LINKED_1_SPRINT_START_DATE','LINKED_1_SPRINT_END_DATE','LINKED_1_VERSION_NAME',
                    'LINKED_1_CATEGORY','LINKED_1_BU','LINKED_1_CUSTOMERS',
                    'LINKED_1_THEME',
                    'LINKED_1_STORYPOINTS','LINKED_1_PROGRESS',                                                                                                                             
                    #,'__Effort_12125','Layout_12166'
         ]]

#df_IDEA = pd.merge(df_IDEA, df_category_idea_1, on =['ISSUE_KEY'],how = 'left')
df_IDEA = pd.merge(df_IDEA, df_feature_set_idea_1, on =['ISSUE_KEY'],how = 'left')
df_IDEA = pd.merge(df_IDEA, df_key_customer_idea_1, on =['ISSUE_KEY'],how = 'left')

df_issues_IDEA_1 = df_issues_2[(df_issues_2['ISSUE_TYPE_NAME']!='Idea')
                             &(df_issues_2['PARENT_ISSUE_KEY'].notnull())][['CREATED','ISSUE_KEY','SUMMARY',
                                                                              'ISSUE_TYPE_NAME','ISSUE_STATUS_NAME',
                                                                              'STATUS_CATEGORY_CHANGE_DATE',
                                                                              'SPRINT_NAME','STATE','START_DATE',
                                                                              'END_DATE','COMPLETE_DATE',
                                                                              'VERSION_NAME','CURRENT_ASSIGNEE_NAME',
                                                                              'REPORTER_NAME',
                                                                              #'TIME_SPENT','TIME_SPENT_WITH_SUBTASKS',
                                                                              'BU','CUSTOMERS',
                                                                             'THEME',
                                                                              'STORYPOINTS','PROGRESS','PARENT_ISSUE_KEY'
                                                                              ,'CATEGORY'
                                                                             ]]


df_issues_IDEA_1 = df_issues_IDEA_1.rename(columns={"CREATED": "CHILD_CREATED", 
                              "ISSUE_KEY": "CHILD_ISSUE_KEY",
                              "SUMMARY": "CHILD_SUMMARY",                                            
                              "ISSUE_TYPE_NAME":"CHILD_ISSUE_TYPE_NAME",
                              "STATUS_CATEGORY_CHANGE_DATE":"CHILD_STATUS_CATEGORY_CHANGE_DATE",
                              "ISSUE_STATUS_NAME":"CHILD_ISSUE_STATUS_NAME",
                              "SPRINT_NAME":"CHILD_SPRINT_NAME",
                              "STATE":"CHILD_SPRINT_STATE",                  
                              "START_DATE":"CHILD_SPRINT_START_DATE",
                              "END_DATE":"CHILD_SPRINT_END_DATE",
                              "COMPLETE_DATE":"CHILD_SPRINT_COMPLETE_DATE",
                              "VERSION_NAME":"CHILD_VERSION_NAME",
                              "CURRENT_ASSIGNEE_NAME":"CHILD_ASSIGNEE_NAME",
                              'REPORTER_NAME':'CHILD_REPORTER_NAME',
                              'TIME_SPENT':'CHILD_TIME_SPENT',
                              'TIME_SPENT_WITH_SUBTASKS':'CHILD_TIME_SPENT_WITH_SUBTASKS',
                              'CATEGORY':'CHILD_CATEGORY',
                              'BU':'CHILD_BU',
                              'CUSTOMERS':'CHILD_CUSTOMERS',
                              'THEME':'CHILD_THEME',                  
                              'STORYPOINTS':'CHILD_STORYPOINTS',
                              'PROGRESS':'CHILD_PROGRESS',
                              'PARENT_ISSUE_KEY':'LINKED_1_ISSUE_KEY'                   
                             })

df_YTEM_1 = pd.merge(df_IDEA, df_issues_IDEA_1,how='left', on='LINKED_1_ISSUE_KEY')
df_sub_YTEM_1 = df_issues_2[(df_issues_2['ISSUE_TYPE_NAME']!='Idea')
                             &(df_issues_2['PARENT_ISSUE_KEY'].notnull())][['CREATED','ISSUE_KEY','SUMMARY',
                                                                              'ISSUE_TYPE_NAME','ISSUE_STATUS_NAME',
                                                                              'STATUS_CATEGORY_CHANGE_DATE',
                                                                              'SPRINT_NAME','STATE','START_DATE',
                                                                              'END_DATE','COMPLETE_DATE',
                                                                              'VERSION_NAME','CURRENT_ASSIGNEE_NAME',
                                                                              'REPORTER_NAME',
                                                                              #'TIME_SPENT','TIME_SPENT_WITH_SUBTASKS',
                                                                              'CATEGORY','BU','CUSTOMERS',
                                                                            #'THEME',
                                                                            'STORYPOINTS','PROGRESS','PARENT_ISSUE_KEY'
                                                                             ]]


df_sub_YTEM_1 = df_sub_YTEM_1.rename(columns={"CREATED": "SUB_CREATED", 
                              "ISSUE_KEY": "SUB_ISSUE_KEY",
                              "SUMMARY": "SUB_SUMMARY", 
                              "ISSUE_TYPE_NAME":"SUB_ISSUE_TYPE_NAME",
                              "STATUS_CATEGORY_CHANGE_DATE":"SUB_STATUS_CATEGORY_CHANGE_DATE",
                              "ISSUE_STATUS_NAME":"SUB_ISSUE_STATUS_NAME",
                              "SPRINT_NAME":"SUB_SPRINT_NAME",
                              "STATE":"SUB_SPRINT_STATE",                  
                              "START_DATE":"SUB_SPRINT_START_DATE",
                              "END_DATE":"SUB_SPRINT_END_DATE",
                              "COMPLETE_DATE":"SUB_SPRINT_COMPLETE_DATE",
                              "VERSION_NAME":"SUB_VERSION_NAME",
                              "CURRENT_ASSIGNEE_NAME":"SUB_ASSIGNEE_NAME",
                              'REPORTER_NAME':'SUB_REPORTER_NAME',
                              'TIME_SPENT':'SUB_TIME_SPENT',
                              'TIME_SPENT_WITH_SUBTASKS':'SUB_TIME_SPENT_WITH_SUBTASKS',
                              'CATEGORY':'SUB_CATEGORY',
                              'BU':'SUB_BU',
                              'CUSTOMERS':'SUB_CUSTOMERS',
                              'THEME':'SUB_THEME',  
                              'STORYPOINTS':'SUB_STORYPOINTS',
                              'PROGRESS':'SUB_PROGRESS',
                              'PARENT_ISSUE_KEY':'CHILD_ISSUE_KEY'                  
                             })

df_sub_YTEM_1 = df_sub_YTEM_1.drop_duplicates()
df_YTEM_1 = pd.merge(df_YTEM_1, df_sub_YTEM_1,how='left', on='CHILD_ISSUE_KEY')
df_YTEM_1.loc[df_YTEM_1['SUB_ISSUE_KEY'].notnull(),'TOTAL_STORYPOINTS']= df_YTEM_1['SUB_STORYPOINTS']
df_YTEM_1.loc[(df_YTEM_1['TOTAL_STORYPOINTS'].isnull())&
            (df_YTEM_1['SUB_ISSUE_KEY'].isnull())&
            (df_YTEM_1['CHILD_ISSUE_KEY'].notnull()),'TOTAL_STORYPOINTS']= df_YTEM_1['CHILD_STORYPOINTS']
df_YTEM_1.loc[(df_YTEM_1['TOTAL_STORYPOINTS'].isnull())&
            (df_YTEM_1['SUB_ISSUE_KEY'].isnull())&
            (df_YTEM_1['CHILD_ISSUE_KEY'].isnull())&
            (df_YTEM_1['LINKED_1_ISSUE_KEY'].notnull()),'TOTAL_STORYPOINTS']= df_YTEM_1['LINKED_1_STORYPOINTS']

selected_cols = ['SUB_ISSUE_KEY','SUB_SUMMARY','SUB_ISSUE_TYPE_NAME','SUB_ISSUE_STATUS_NAME','SUB_SPRINT_NAME',
                 'SUB_SPRINT_STATE','SUB_VERSION_NAME','SUB_ASSIGNEE_NAME','SUB_REPORTER_NAME','SUB_CATEGORY','SUB_BU',
                 'SUB_CUSTOMERS']
df_YTEM_1[selected_cols]=df_YTEM_1[selected_cols].fillna('')

for i in range(0,df_versions['VERSION_ID'].count()):
    START_DATE = df_versions.iloc[i-1,5] 
    END_DATE = df_versions.iloc[i,5]  
    if i < 10:
        i_m = "0"+str(i)
    if i >=10:
        i_m = str(i)
    VERSION = i_m+" "+df_versions.iloc[i,1]
    if VERSION == '00 v8.4 Chihuahua':
        START_DATE='2021-09-27 00:00:00+00:00'
    
    #START_DATE = START_DATE.replace("00:00:00+00:00","23:59:59+00:00")
    #END_DATE = END_DATE
    
    df_YTEM_1.loc[(df_YTEM_1['CHILD_SPRINT_START_DATE']>=START_DATE)
                &(df_YTEM_1['CHILD_SPRINT_START_DATE']<=END_DATE),'CHILD_SPRINT_DATE_VERSION']= VERSION
    df_YTEM_1.loc[(df_YTEM_1['CHILD_SPRINT_END_DATE']>=START_DATE)
                &(df_YTEM_1['CHILD_SPRINT_END_DATE']<=END_DATE),'CHILD_SPRINT_END_DATE_VERSION']= VERSION
    #df_PR_f.loc[(df_PR_f['STATUS_DATE_VERSION'].isnull())&(df_PR_f['CHILD_STATUS_CATEGORY_CHANGE_DATE']>START_DATE)&(df_PR_f['CHILD_STATUS_CATEGORY_CHANGE_DATE']>END_DATE),'STATUS_DATE_VERSION']= VERSION
    print(START_DATE,END_DATE,VERSION)

##TEAM ALLOCATED
url_1 = 'https://docs.google.com/spreadsheets/d/1P7E-y7eA9-pEIevrBUDztreDWjlI4D4QyyO6eo8AKDk/edit#gid=0'
url = url_1.replace('/edit#gid=', '/export?format=csv&gid=')
df_areas = pd.read_csv(url, dtype=str)

### Sprint valid filter
df_sprints_valid = df_Sprints[df_Sprints['SPRINT_NAME'].str.startswith('v')].sort_values('START_DATE')
df_sprint_by_name = df_areas.join(df_sprints_valid)

df_areas_1 = df_areas.assign(key=1)
df_sprints_valid_1 = df_sprints_valid.assign(key=1)

### Merge on the key (Cartesian product)
df_sprint_by_name = pd.merge(df_areas_1, df_sprints_valid_1[['key','SPRINT_NAME','STATE']], on='key')

### Drop the temporary key (optional)
df_sprint_by_name = df_sprint_by_name.drop('key', axis=1)
df_sprint_by_name = df_sprint_by_name.rename(columns={'SPRINT_NAME':'RESOURCE_SPRINT_NAME','STATE':'RESOURCE_STATE_SPRINT_NAME'}, )

## last Assignee Name, SPRINT
df_YTEM_1.loc[df_YTEM_1['SUB_ASSIGNEE_NAME']!='', 'LAST_ASSIGNEE_NAME'] = df_YTEM_1['SUB_ASSIGNEE_NAME']
df_YTEM_1.loc[(df_YTEM_1['CHILD_ASSIGNEE_NAME']!='')&(df_YTEM_1['SUB_ASSIGNEE_NAME']==''), 'LAST_ASSIGNEE_NAME'] = df_YTEM_1['CHILD_ASSIGNEE_NAME']
df_YTEM_1 = pd.merge(df_YTEM_1,df_sprint_by_name, how='outer' , left_on=['LAST_ASSIGNEE_NAME','CHILD_SPRINT_NAME'], right_on=['NAME','RESOURCE_SPRINT_NAME'])


df_YTEM_1.rename(columns={'NAME':'RESOURCE_NAME', 'NAME_ID':'RESOURCE_NAME_ID','AREA_ID':'RESOURCE_AREA_ID','RESOURCE_STATUS':'RESOURCE_STATUS',
                          'AREA':'RESOURCE_AREA','TRACKING_PHASE':'RESOURCE_TRACK_PHASE',
                         'DEPARTMENT':'RESOURCE_DEPARTMENT'}, inplace=True)


#### Upload Data to BigQuery table
#saas-analytics-io.processed.jira_processed_IDEAS
print('Write in BQ saas-analytics-io.processed.jira_processed_IDEAS')
try:
  table = "saas-analytics-io.processed.jira_processed_IDEAS"
  df_YTEM_1.to_gbq(table, if_exists='replace')
  print('JIRA IDEAS FULL DATA write in BQ --> done')
except Exception as e:
  print('Error: ',e)



### Customer Aggregation for PR
try:
  print('Customer Aggregation for Prs')
  df_customers_agg = pd.merge(df_customers,df_PR_f[['LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_STATUS_NAME','LINKED_1_BU','LINKED_1_VERSION_NAME',
                                                    'CHILD_SPRINT_DATE_VERSION','CHILD_SPRINT_END_DATE_VERSION',
                                                    'SUB_ISSUE_KEY','SUB_STATUS_CATEGORY_CHANGE_DATE','SUB_STORYPOINTS']],how='inner',left_on = 'ISSUE_KEY',right_on='LINKED_1_ISSUE_KEY')
  df_customers_agg = df_customers_agg.drop(['ISSUE_ID','ISSUE_KEY','Customers_2'], axis =1)
  df_customers_agg = df_customers_agg.rename( columns={'Customers' : 'CUSTOMERS'})

  df_customers_adj = df_customers.groupby('ISSUE_KEY').count()[['Customers']].reset_index()
  df_customers_adj = df_customers_adj.rename(columns={'Customers':'CUSTOMERS_COUNT','ISSUE_KEY':'LINKED_1_ISSUE_KEY'})
  df_customers_agg = pd.merge(df_customers_agg,df_customers_adj,how='left', on='LINKED_1_ISSUE_KEY')
  df_customers_agg.loc[:,'ADJ_STORYPOINTS']= df_customers_agg['SUB_STORYPOINTS']/df_customers_agg['CUSTOMERS_COUNT']
except Exception as e:
  print('Error: ',e)
### Category Aggregation for PR
try:
  print('Category Aggregation for Prs')
  df_category_agg = pd.merge(df_category[['ISSUE_KEY','OKR_Category']],df_PR_f[['LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_STATUS_NAME','LINKED_1_BU','LINKED_1_VERSION_NAME',
                                                  'CHILD_SPRINT_DATE_VERSION','CHILD_SPRINT_END_DATE_VERSION',
                                                  'SUB_ISSUE_KEY','SUB_STATUS_CATEGORY_CHANGE_DATE','SUB_STORYPOINTS']],how='inner',left_on = 'ISSUE_KEY',right_on='LINKED_1_ISSUE_KEY')
  df_category_agg = df_category_agg.drop(['ISSUE_KEY'], axis =1)
  df_category_agg = df_category_agg.rename( columns={'OKR_Category' : 'CATEGORY'})

  df_category_adj = df_category.groupby('ISSUE_KEY').count()[['OKR_Category']].reset_index()
  df_category_adj = df_category_adj.rename(columns={'OKR_Category':'CATEGORY_COUNT','ISSUE_KEY':'LINKED_1_ISSUE_KEY'})
  df_category_agg = pd.merge(df_category_agg,df_category_adj,how='left', on='LINKED_1_ISSUE_KEY')
  df_category_agg.loc[:,'ADJ_STORYPOINTS']= df_category_agg['SUB_STORYPOINTS']/df_category_agg['CATEGORY_COUNT']
except Exception as e:
  print('Error: ',e)
#### Upload Customer and Category aggregation to BigQuery
#  - saas-analytics-io.processed.jira_processed_PR_customers
#  - saas-analytics-io.processed.jira_processed_PR_category
print('Write in BQ saas-analytics-io.processed.jira_processed_PR_customers')
try:
  table = "saas-analytics-io.processed.jira_processed_PR_customers"
  df_customers_agg.to_gbq(table, if_exists='replace')
  print('PR Customers write in BQ --> done')
except Exception as e:
  print('Error:',e)
  
print('Write in BQ saas-analytics-io.processed.jira_processed_PR_category')
try:
  table = "saas-analytics-io.processed.jira_processed_PR_category"
  df_category_agg.to_gbq(table, if_exists='replace')
  print('PR Category write in BQ --> done')
except Exception as e:
  print('Error:',e)

### Customer Aggregation for IDEA
try:
  print('Customer Aggregation for IDEAs')  
  df_customers_ideas_agg = pd.merge(df_customers,df_YTEM_1[['LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_STATUS_NAME','LINKED_1_BU','LINKED_1_VERSION_NAME',
                                                    'CHILD_SPRINT_DATE_VERSION','CHILD_SPRINT_END_DATE_VERSION',
                                                    'SUB_ISSUE_KEY','SUB_STATUS_CATEGORY_CHANGE_DATE','SUB_STORYPOINTS']],how='inner',left_on = 'ISSUE_KEY',right_on='LINKED_1_ISSUE_KEY')
  df_customers_ideas_agg = df_customers_ideas_agg.drop(['ISSUE_ID','ISSUE_KEY','Customers_2'], axis =1)
  df_customers_ideas_agg = df_customers_ideas_agg.rename( columns={'Customers' : 'CUSTOMERS'})

  df_customers_ideas_adj = df_customers.groupby('ISSUE_KEY').count()[['Customers']].reset_index()
  df_customers_ideas_adj = df_customers_ideas_adj.rename(columns={'Customers':'CUSTOMERS_COUNT','ISSUE_KEY':'LINKED_1_ISSUE_KEY'})
  df_customers_ideas_agg = pd.merge(df_customers_ideas_agg,df_customers_ideas_adj,how='left', on='LINKED_1_ISSUE_KEY')
  df_customers_ideas_agg.loc[:,'ADJ_STORYPOINTS']= df_customers_ideas_agg['SUB_STORYPOINTS']/df_customers_ideas_agg['CUSTOMERS_COUNT']
except Exception as e:
  print('Error: ',e)
### Category Aggregation for IDEA
try:
  print('Category Aggregation for IDEAs') 
  df_category_ideas_agg = pd.merge(df_category[['ISSUE_KEY','OKR_Category']],df_YTEM_1[['LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_STATUS_NAME','LINKED_1_BU','LINKED_1_VERSION_NAME',
                                                    'CHILD_SPRINT_DATE_VERSION','CHILD_SPRINT_END_DATE_VERSION',
                                                    'SUB_ISSUE_KEY','SUB_STATUS_CATEGORY_CHANGE_DATE','SUB_STORYPOINTS']],how='inner',left_on = 'ISSUE_KEY',right_on='LINKED_1_ISSUE_KEY')
  df_category_ideas_agg = df_category_ideas_agg.drop(['ISSUE_KEY'], axis =1)
  df_category_ideas_agg = df_category_ideas_agg.rename( columns={'OKR_Category' : 'CATEGORY'})

  df_category_ideas_adj = df_category.groupby('ISSUE_KEY').count()[['OKR_Category']].reset_index()
  df_category_ideas_adj = df_category_ideas_adj.rename(columns={'OKR_Category':'CATEGORY_COUNT','ISSUE_KEY':'LINKED_1_ISSUE_KEY'})
  df_category_ideas_agg = pd.merge(df_category_ideas_agg,df_category_ideas_adj,how='left', on='LINKED_1_ISSUE_KEY')
  df_category_ideas_agg.loc[:,'ADJ_STORYPOINTS']= df_category_ideas_agg['SUB_STORYPOINTS']/df_category_ideas_agg['CATEGORY_COUNT']
except Exception as e:
  print('Error: ',e)
  
#### Upload Customer and Category aggregation to BigQuery
# - saas-analytics-io.processed.jira_processed_IDEAS_customers
# - saas-analytics-io.processed.jira_processed_IDEAS_category
print('Write in BQ saas-analytics-io.processed.jira_processed_IDEAS_customers')
try:
  table = "saas-analytics-io.processed.jira_processed_IDEAS_customers"
  df_customers_ideas_agg.to_gbq(table, if_exists='replace')
  print('IDEA Customers write in BQ --> done')
except Exception as e:
  print('Error:',e)

print('Write in BQ saas-analytics-io.processed.jira_processed_IDEAS_category')
try:
  table = "saas-analytics-io.processed.jira_processed_IDEAS_category"
  df_category_ideas_agg.to_gbq(table, if_exists='replace')
  print('IDEA Category write in BQ --> done')
except Exception as e:
  print('Error:',e)

# Worklogs DataPreparation and Enhancement
### Join PR AND IDEAS tickets with subtasks, childs, linked
#df_PR_worklogs_sub = df_PR_f[['ISSUE_KEY'
 #                         ,'LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_TYPE_NAME','LINKED_1_ISSUE_STATUS_NAME','LINKED_1_VERSION_NAME'
  #                        ,'CHILD_ISSUE_KEY','CHILD_ISSUE_TYPE_NAME','CHILD_ISSUE_STATUS_NAME','CHILD_SPRINT_DATE_VERSION','CHILD_SPRINT_END_DATE_VERSION','CHILD_SPRINT_NAME'
   #                       ,'SUB_ISSUE_KEY','SUB_ISSUE_TYPE_NAME','SUB_ISSUE_STATUS_NAME','SUB_STATUS_CATEGORY_CHANGE_DATE','SUB_PROGRESS','SUB_SPRINT_NAME'
    #                      ,'TOTAL_STORYPOINTS']].drop_duplicates()

#df_PR_worklogs_child = df_PR_f[(df_PR_f['CHILD_ISSUE_KEY'].notnull())&(df_PR_f['SUB_ISSUE_KEY'].isnull())][['ISSUE_KEY'
#                          ,'LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_TYPE_NAME','LINKED_1_ISSUE_STATUS_NAME'
 #                         ,'CHILD_ISSUE_KEY','CHILD_ISSUE_TYPE_NAME','CHILD_ISSUE_STATUS_NAME','CHILD_SPRINT_DATE_VERSION','CHILD_SPRINT_END_DATE_VERSION','CHILD_SPRINT_NAME'
                          #,'SUB_ISSUE_KEY','SUB_ISSUE_TYPE_NAME','SUB_ISSUE_STATUS_NAME','SUB_STATUS_CATEGORY_CHANGE_DATE'
  #                        ,'TOTAL_STORYPOINTS','SUB_PROGRESS']].drop_duplicates()
df_IDEAS_worklogs_sub = df_YTEM_1[['ISSUE_KEY','SUMMARY','ISSUE_STATUS_NAME'
                          ,'LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_TYPE_NAME','LINKED_1_ISSUE_STATUS_NAME','LINKED_1_VERSION_NAME'
                          ,'CHILD_ISSUE_KEY','CHILD_ISSUE_TYPE_NAME','CHILD_ISSUE_STATUS_NAME','CHILD_SPRINT_DATE_VERSION','CHILD_SPRINT_END_DATE_VERSION','CHILD_SPRINT_NAME'
                          ,'SUB_ISSUE_KEY','SUB_ISSUE_TYPE_NAME','SUB_ISSUE_STATUS_NAME','SUB_STATUS_CATEGORY_CHANGE_DATE','SUB_PROGRESS','SUB_SPRINT_NAME'
                          ,'TOTAL_STORYPOINTS']].drop_duplicates()

df_IDEAS_worklogs_child = df_YTEM_1[(df_YTEM_1['CHILD_ISSUE_KEY'].notnull())&(df_YTEM_1['SUB_ISSUE_KEY'].isnull())][['ISSUE_KEY','SUMMARY','ISSUE_STATUS_NAME'
                          ,'LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_TYPE_NAME','LINKED_1_ISSUE_STATUS_NAME'
                          ,'CHILD_ISSUE_KEY','CHILD_ISSUE_TYPE_NAME','CHILD_ISSUE_STATUS_NAME','CHILD_SPRINT_DATE_VERSION','CHILD_SPRINT_END_DATE_VERSION','CHILD_SPRINT_NAME'
                          #,'SUB_ISSUE_KEY','SUB_ISSUE_TYPE_NAME','SUB_ISSUE_STATUS_NAME','SUB_STATUS_CATEGORY_CHANGE_DATE'
                          ,'TOTAL_STORYPOINTS','SUB_PROGRESS']].drop_duplicates()

df_worklog_U = df_worklog_1[['ISSUE_KEY_LOGGED','ISSUE_TYPE_NAME_LOGGED','AUTHOR_NAME','UPDATE_NAME','START_DATE','LOGGED_TIME','CREATED','UPDATED','PROGRESS_LOGGED','STORYPOINTS_LOGGED']]
#df_worklog_enh = pd.merge(df_worklog_U,df_PR_worklogs_sub, left_on = 'ISSUE_KEY_LOGGED', right_on = 'SUB_ISSUE_KEY',how = 'left')
df_worklog_enh_3 = pd.merge(df_worklog_U,df_IDEAS_worklogs_sub, left_on = 'ISSUE_KEY_LOGGED', right_on = 'SUB_ISSUE_KEY',how = 'left')

## Worklog enrich with CHILD ISSUES 
## Worklog enrich with CHILD ISSUES 

#df_worklog_child = pd.DataFrame()
#df_worklog_enh_2 = df_worklog_enh[~(df_worklog_enh['ISSUE_KEY_LOGGED'].isin(df_PR_worklogs_child['CHILD_ISSUE_KEY']))]
#for index,row in df_PR_worklogs_child.iterrows():
#    df_worklog_row = df_worklog_enh[(df_worklog_enh['ISSUE_KEY_LOGGED']==row['CHILD_ISSUE_KEY'])]
#    df_worklog_row.loc[:,'ISSUE_KEY']= row['ISSUE_KEY']
#    df_worklog_row.loc[:,'LINKED_1_ISSUE_KEY']= row['LINKED_1_ISSUE_KEY']
#    df_worklog_row.loc[:,'LINKED_1_ISSUE_TYPE_NAME']= row['LINKED_1_ISSUE_TYPE_NAME']
#    df_worklog_row.loc[:,'LINKED_1_ISSUE_STATUS_NAME']= row['LINKED_1_ISSUE_STATUS_NAME'] 
#    df_worklog_row.loc[:,'CHILD_ISSUE_KEY'] = row['LINKED_1_ISSUE_KEY']
#    df_worklog_row.loc[:,'CHILD_ISSUE_TYPE_NAME'] = row['CHILD_ISSUE_TYPE_NAME']
#    df_worklog_row.loc[:,'CHILD_ISSUE_STATUS_NAME'] = row['CHILD_ISSUE_STATUS_NAME']
#    df_worklog_row.loc[:,'CHILD_SPRINT_DATE_VERSION'] = row['CHILD_SPRINT_DATE_VERSION']
#    df_worklog_row.loc[:,'CHILD_SPRINT_END_DATE_VERSION'] = row['CHILD_SPRINT_END_DATE_VERSION']
#    df_worklog_row.loc[:,'CHILD_SPRINT_NAME'] = row['CHILD_SPRINT_NAME']
#    df_worklog_row.loc[:,'TOTAL_STORYPOINTS'] = row['TOTAL_STORYPOINTS']
#    df_worklog_child = df_worklog_child.append(df_worklog_row)
#
#df_worklog_enh_2 = df_worklog_enh_2.append(df_worklog_child)


## Worklog enrich with IDEAS SUB ISSUES 

#df_worklog_sub_idea = pd.DataFrame()
#df_worklog_enh_3 = df_worklog_enh_2[~(df_worklog_enh_2['ISSUE_KEY_LOGGED'].isin(df_IDEAS_worklogs_sub['SUB_ISSUE_KEY']))]
#for index,row in df_IDEAS_worklogs_sub.iterrows():
#    df_worklog_row = df_worklog_enh_2[(df_worklog_enh_2['ISSUE_KEY_LOGGED']==row['SUB_ISSUE_KEY'])]
#    df_worklog_row.loc[:,'ISSUE_KEY']= row['ISSUE_KEY']
#    df_worklog_row.loc[:,'LINKED_1_ISSUE_KEY']= row['LINKED_1_ISSUE_KEY']
#    df_worklog_row.loc[:,'LINKED_1_ISSUE_TYPE_NAME']= row['LINKED_1_ISSUE_TYPE_NAME']
#    df_worklog_row.loc[:,'LINKED_1_ISSUE_STATUS_NAME']= row['LINKED_1_ISSUE_STATUS_NAME'] 
#    df_worklog_row.loc[:,'CHILD_ISSUE_KEY'] = row['CHILD_ISSUE_KEY']
#    df_worklog_row.loc[:,'CHILD_ISSUE_TYPE_NAME'] = row['CHILD_ISSUE_TYPE_NAME']
#    df_worklog_row.loc[:,'CHILD_ISSUE_STATUS_NAME'] = row['CHILD_ISSUE_STATUS_NAME']
#    df_worklog_row.loc[:,'CHILD_SPRINT_DATE_VERSION'] = row['CHILD_SPRINT_DATE_VERSION']
#    df_worklog_row.loc[:,'CHILD_SPRINT_END_DATE_VERSION'] = row['CHILD_SPRINT_END_DATE_VERSION']
#    df_worklog_row.loc[:,'CHILD_SPRINT_NAME'] = row['CHILD_SPRINT_NAME']
#    df_worklog_row.loc[:,'SUB_ISSUE_KEY'] = row['SUB_ISSUE_KEY']
#    df_worklog_row.loc[:,'SUB_ISSUE_TYPE_NAME'] = row['SUB_ISSUE_TYPE_NAME']
#    df_worklog_row.loc[:,'SUB_ISSUE_STATUS_NAME'] = row['SUB_ISSUE_STATUS_NAME']
#    df_worklog_row.loc[:,'SUB_STATUS_CATEGORY_CHANGE_DATE'] = row['SUB_STATUS_CATEGORY_CHANGE_DATE']
#    df_worklog_row.loc[:,'SUB_SPRINT_NAME'] = row['SUB_SPRINT_NAME']
#    
#    df_worklog_row.loc[:,'TOTAL_STORYPOINTS'] = row['TOTAL_STORYPOINTS']
#    df_worklog_sub_idea = df_worklog_sub_idea.append(df_worklog_row)
#
#df_worklog_enh_3 = df_worklog_enh_3.append(df_worklog_sub_idea)

## Worklog enrich with IDEAS SUB ISSUES 

df_worklog_child_idea = pd.DataFrame()
df_worklog_enh_4 = df_worklog_enh_3[~(df_worklog_enh_3['ISSUE_KEY_LOGGED'].isin(df_IDEAS_worklogs_child['CHILD_ISSUE_KEY']))]
for index,row in df_IDEAS_worklogs_child.iterrows():
    df_worklog_row = df_worklog_enh_3[(df_worklog_enh_3['ISSUE_KEY_LOGGED']==row['CHILD_ISSUE_KEY'])]
    df_worklog_row.loc[:,'ISSUE_KEY']= row['ISSUE_KEY']
    df_worklog_row.loc[:,'LINKED_1_ISSUE_KEY']= row['LINKED_1_ISSUE_KEY']
    df_worklog_row.loc[:,'LINKED_1_ISSUE_TYPE_NAME']= row['LINKED_1_ISSUE_TYPE_NAME']
    df_worklog_row.loc[:,'LINKED_1_ISSUE_STATUS_NAME']= row['LINKED_1_ISSUE_STATUS_NAME'] 
    df_worklog_row.loc[:,'CHILD_ISSUE_KEY'] = row['CHILD_ISSUE_KEY']
    df_worklog_row.loc[:,'CHILD_ISSUE_TYPE_NAME'] = row['CHILD_ISSUE_TYPE_NAME']
    df_worklog_row.loc[:,'CHILD_ISSUE_STATUS_NAME'] = row['CHILD_ISSUE_STATUS_NAME']
    df_worklog_row.loc[:,'CHILD_SPRINT_DATE_VERSION'] = row['CHILD_SPRINT_DATE_VERSION']
    df_worklog_row.loc[:,'CHILD_SPRINT_END_DATE_VERSION'] = row['CHILD_SPRINT_END_DATE_VERSION']
    df_worklog_row.loc[:,'CHILD_SPRINT_NAME'] = row['CHILD_SPRINT_NAME']
    #df_worklog_row.loc[:,'SUB_ISSUE_KEY'] = row['SUB_ISSUE_KEY']
    #df_worklog_row.loc[:,'SUB_ISSUE_TYPE_NAME'] = row['SUB_ISSUE_TYPE_NAME']
    #df_worklog_row.loc[:,'SUB_ISSUE_STATUS_NAME'] = row['SUB_ISSUE_STATUS_NAME']
    #df_worklog_row.loc[:,'SUB_STATUS_CATEGORY_CHANGE_DATE'] = row['SUB_STATUS_CATEGORY_CHANGE_DATE']
    df_worklog_row.loc[:,'TOTAL_STORYPOINTS'] = row['TOTAL_STORYPOINTS']
    df_worklog_child_idea = df_worklog_child_idea.append(df_worklog_row)

df_worklog_enh_4 = df_worklog_enh_4.append(df_worklog_child_idea)
## Worklog in Incidents Support TICKETS
df_ticket_support = df_worklog_enh_3[df_worklog_enh_3['ISSUE_KEY_LOGGED'].str.startswith('TICKET')]
df_worklog_enh_4 = df_worklog_enh_4.append(df_ticket_support)

#Enrichment with Resource and area
url_1 = 'https://docs.google.com/spreadsheets/d/1P7E-y7eA9-pEIevrBUDztreDWjlI4D4QyyO6eo8AKDk/edit#gid=0'
url = url_1.replace('/edit#gid=', '/export?format=csv&gid=')
df_resource_areas = pd.read_csv(url, dtype=str)

df_worklog_enh_4 = pd.merge(df_worklog_enh_4,df_resource_areas, left_on = 'UPDATE_NAME', right_on = 'NAME',how = 'left')

df_worklog_enh_4.rename(columns={'NAME_ID':'UPDATE_NAME_ID','AREA_ID':'UPDATE_NAME_AREA_ID','RESOURCE_STATUS':'UPDATE_NAME_STATUS',
                                 'AREA':'UPDATE_NAME_AREA','TRACKING_PHASE':'UPDATE_NAME_TRACK_PHASE',
                                'DEPARTMENT':'UPDATE_NAME_DEPARTMENT'}, inplace=True)
df_worklog_enh_4.drop('NAME', axis=1, inplace=True)

df_worklog_enh_4.fillna({'UPDATE_NAME':'UNK'
                         ,'UPDATE_NAME_ID':'UNK'
                         ,'UPDATE_NAME_AREA':'UNK'
                         ,'UPDATE_NAME_STATUS':'UNK'
                         ,'UPDATE_NAME_AREA_ID':'UNK'
                         ,'SUB_SPRINT_NAME':'UNK'}
                        ,inplace=True)
df_worklog_enh_4.loc[(df_worklog_enh_4['ISSUE_KEY'].isnull())&
                     (df_worklog_enh_4['ISSUE_TYPE_NAME_LOGGED'].isin(['Bug','Sub-bug'])),'WORKLOG_TYPE'] = 'Fixing'
df_worklog_enh_4.loc[(df_worklog_enh_4['ISSUE_KEY'].isnull())&
                     (~(df_worklog_enh_4['ISSUE_TYPE_NAME_LOGGED'].isin(['Bug','Sub-bug']))),'WORKLOG_TYPE'] = 'Other'
df_worklog_enh_4.loc[(df_worklog_enh_4['ISSUE_KEY'].isnull())&
                     (df_worklog_enh_4['ISSUE_KEY_LOGGED'].str.startswith('TICKET')),'WORKLOG_TYPE'] = 'Incidents'
df_worklog_enh_4.loc[(df_worklog_enh_4['ISSUE_KEY'].notnull()),'WORKLOG_TYPE'] = 'Developing'

### Allocate worklog in a Sprint from START_DATE of Worklog
df_Sprints_valid = df_Sprints[df_Sprints['SPRINT_NAME'].str.startswith('v')].sort_values('START_DATE')
for i in range(0,df_Sprints_valid['SPRINT_ID'].count()):
    START_DATE = df_Sprints_valid.iloc[i-1,6]
    END_DATE = df_Sprints_valid.iloc[i,6]
    #if i < 10:
    #    i_m = "0"+str(i)
    #if i >=10:
    #    i_m = str(i)
    #VERSION = i_m+" "+df_versions.iloc[i,1]
    VERSION = df_Sprints_valid.iloc[i,1]
    #if VERSION == '00 v8.4 Chihuahua':
    #    START_DATE='2021-09-27 00:00:00+00:00'
   
    df_worklog_enh_4.loc[(df_worklog_enh_4['START_DATE']>=START_DATE)
               &(df_worklog_enh_4['START_DATE']<END_DATE),'SPRINT_ASSIGNED']= VERSION
    df_worklog_enh_4.loc[(df_worklog_enh_4['START_DATE']>=START_DATE)
               &(df_worklog_enh_4['START_DATE']<END_DATE),'SPRINT_ASSIGNED_START_DATE']= START_DATE
    df_worklog_enh_4.loc[(df_worklog_enh_4['START_DATE']>=START_DATE)
               &(df_worklog_enh_4['START_DATE']<END_DATE),'SPRINT_ASSIGNED_END_DATE']= END_DATE
    
    ##df_PR_f.loc[(df_PR_f['STATUS_DATE_VERSION'].isnull())&(df_PR_f['CHILD_STATUS_CATEGORY_CHANGE_DATE']>START_DATE)&(df_PR_f['CHILD_STATUS_CATEGORY_CHANGE_DATE']>END_DATE),'STATUS_DATE_VERSION']= VERSION
    print(START_DATE,END_DATE,VERSION)
df_worklog_enh_4['SPRINT_ASSIGNED_START_DATE'] = pd.to_datetime(df_worklog_enh_4['SPRINT_ASSIGNED_START_DATE'])
df_worklog_enh_4['SPRINT_ASSIGNED_END_DATE'] = pd.to_datetime(df_worklog_enh_4['SPRINT_ASSIGNED_END_DATE'])


#### Upload Worklog aggregation to BigQuery
print('worklog write in saas-analytics-io.processed.jira_processed_IDEAS_worklog')
try:
  table = "saas-analytics-io.processed.jira_processed_IDEAS_worklog"
  df_worklog_enh_4.to_gbq(table, if_exists='replace')
  print('worklog write in BQ --> done')
except Exception as e:
  print('Error:',e)



