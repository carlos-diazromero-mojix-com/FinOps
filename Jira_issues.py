#!/usr/bin/env python
# coding: utf-8

# In[110]:


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
pd.set_option('display.width', 1000)
pd.set_option("max_colwidth",10000)
pd.set_option("max_rows",1000)
pd.set_option("max_columns",100)


# In[111]:


current_date = datetime.today()
dias_atras = 0
#dias_atras = int(dias_atras)
str_day = (current_date - timedelta(days = dias_atras)).strftime("%Y-%m-%d")
print(str_day)


# In[112]:


df_logtable = pd.DataFrame()
sourceN = 0


# ### Read Sources

# In[113]:


query = """
 SELECT *
 FROM `saas-analytics-io.saas_analytics_io_jira.Worklogs` 
 WHERE date(UPDATED) between '2022-01-01' and '%s'
       """%(str_day)

query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
print(query)
df_worklog = query_job.to_dataframe()
print(df_worklog.info())
print(df_worklog.shape)

df_logtable.loc[sourceN,'date'] = str_day
df_logtable.loc[sourceN,'tableName'] = 'Worklogs'
df_logtable.loc[sourceN,'tableRows'] = len(df_worklog.index)
df_logtable.loc[sourceN,'tableColumns'] = len(df_worklog.columns)
sourceN = sourceN+1


# In[114]:


query = """
 SELECT *
 FROM `saas-analytics-io.saas_analytics_io_jira.Issues` 
 WHERE date(CREATED) between '2020-01-01' and '%s' """%(str_day)

query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
print(query)
df_issues = query_job.to_dataframe()
print(df_issues.info())

df_logtable.loc[sourceN,'date'] = str_day
df_logtable.loc[sourceN,'tableName'] = 'Issues'
df_logtable.loc[sourceN,'tableRows'] = len(df_issues.index)
df_logtable.loc[sourceN,'tableColumns'] = len(df_issues.columns)
sourceN = sourceN+1
#df_issues.head()


# In[115]:


query = """
 SELECT *
 FROM `saas-analytics-io.saas_analytics_io_jira.IssueSprints` 
"""
query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
print(query)
df_issueSprints = query_job.to_dataframe()
print(df_issueSprints.info())

df_logtable.loc[sourceN,'date'] = str_day
df_logtable.loc[sourceN,'tableName'] = 'IssueSprints'
df_logtable.loc[sourceN,'tableRows'] = len(df_issueSprints.index)
df_logtable.loc[sourceN,'tableColumns'] = len(df_issueSprints.columns)
sourceN = sourceN+1


# In[116]:


query = """
 SELECT *
 FROM `saas-analytics-io.saas_analytics_io_jira.IssueLinks` 
"""
query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
print(query)
df_issueLinks = query_job.to_dataframe()
print(df_issueLinks.info())

df_logtable.loc[sourceN,'date'] = str_day
df_logtable.loc[sourceN,'tableName'] = 'IssueLinks'
df_logtable.loc[sourceN,'tableRows'] = len(df_issueLinks.index)
df_logtable.loc[sourceN,'tableColumns'] = len(df_issueLinks.columns)
sourceN = sourceN+1


# In[117]:


query = """
 SELECT *
 FROM `saas-analytics-io.saas_analytics_io_jira.IssueFixVersions` 
"""
query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
print(query)
df_IssueFixVersions = query_job.to_dataframe()
print(df_IssueFixVersions.info())

df_logtable.loc[sourceN,'date'] = str_day
df_logtable.loc[sourceN,'tableName'] = 'IssueFixVersions'
df_logtable.loc[sourceN,'tableRows'] = len(df_IssueFixVersions.index)
df_logtable.loc[sourceN,'tableColumns'] = len(df_IssueFixVersions.columns)
sourceN = sourceN+1


# In[118]:


query = """
 SELECT *
 FROM `saas-analytics-io.saas_analytics_io_jira.Versions` 
 WHERE VERSION_NAME like 'v%'"""

query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
print(query)
df_versions = query_job.to_dataframe()
print(df_versions.info())
#df_issues.head()

df_logtable.loc[sourceN,'date'] = str_day
df_logtable.loc[sourceN,'tableName'] = 'Versions'
df_logtable.loc[sourceN,'tableRows'] = len(df_versions.index)
df_logtable.loc[sourceN,'tableColumns'] = len(df_versions.columns)
sourceN = sourceN+1


# In[119]:


query = """
 SELECT *
 FROM `saas-analytics-io.saas_analytics_io_jira.Business_Unit_12031` 
"""
query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
print(query)
df_businessUnit = query_job.to_dataframe()
print(df_businessUnit.info())

df_logtable.loc[sourceN,'date'] = str_day
df_logtable.loc[sourceN,'tableName'] = 'Business_Unit_12031'
df_logtable.loc[sourceN,'tableRows'] = len(df_businessUnit.index)
df_logtable.loc[sourceN,'tableColumns'] = len(df_businessUnit.columns)
sourceN = sourceN+1


# In[120]:


query = """
 SELECT *
 FROM `saas-analytics-io.saas_analytics_io_jira.Customers_11800` 
"""
query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
print(query)
df_customers = query_job.to_dataframe()
print(df_customers.info())

df_logtable.loc[sourceN,'date'] = str_day
df_logtable.loc[sourceN,'tableName'] = 'Customers_11800'
df_logtable.loc[sourceN,'tableRows'] = len(df_customers.index)
df_logtable.loc[sourceN,'tableColumns'] = len(df_customers.columns)
sourceN = sourceN+1


# In[121]:


query = """
 SELECT *
 FROM `saas-analytics-io.saas_analytics_io_jira.OKR_Category_12032` 
"""
query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
print(query)
df_category = query_job.to_dataframe()
print(df_category.info())

df_logtable.loc[sourceN,'date'] = str_day
df_logtable.loc[sourceN,'tableName'] = 'OKR_Category_12032'
df_logtable.loc[sourceN,'tableRows'] = len(df_category.index)
df_logtable.loc[sourceN,'tableColumns'] = len(df_category.columns)
sourceN = sourceN+1


# In[122]:


query = """
 SELECT *
 FROM `saas-analytics-io.saas_analytics_io_jira.Sprints` 
"""
query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
print(query)
df_Sprints = query_job.to_dataframe()
print(df_Sprints.info())

df_logtable.loc[sourceN,'date'] = str_day
df_logtable.loc[sourceN,'tableName'] = 'Sprints'
df_logtable.loc[sourceN,'tableRows'] = len(df_Sprints.index)
df_logtable.loc[sourceN,'tableColumns'] = len(df_Sprints.columns)
sourceN = sourceN+1


# In[123]:


query = """
 SELECT *
 FROM `saas-analytics-io.saas_analytics_io_jira.Theme_12033` 
"""
query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
print(query)
df_theme = query_job.to_dataframe()
print(df_theme.info())

df_logtable.loc[sourceN,'date'] = str_day
df_logtable.loc[sourceN,'tableName'] = 'Theme_12033'
df_logtable.loc[sourceN,'tableRows'] = len(df_theme.index)
df_logtable.loc[sourceN,'tableColumns'] = len(df_theme.columns)
sourceN = sourceN+1


# In[124]:


query = """
 SELECT *
 FROM `saas-analytics-io.saas_analytics_io_jira.Category_12164` 
"""
query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
print(query)
df_category_idea = query_job.to_dataframe()
print(df_category_idea.info())

df_logtable.loc[sourceN,'date'] = str_day
df_logtable.loc[sourceN,'tableName'] = 'Category_12164'
df_logtable.loc[sourceN,'tableRows'] = len(df_category_idea.index)
df_logtable.loc[sourceN,'tableColumns'] = len(df_category_idea.columns)
sourceN = sourceN+1


# In[125]:


query = """
 SELECT *
 FROM `saas-analytics-io.saas_analytics_io_jira.Feature_Set_12139` 
"""
query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
print(query)
df_feature_set_idea = query_job.to_dataframe()
print(df_feature_set_idea.info())

df_logtable.loc[sourceN,'date'] = str_day
df_logtable.loc[sourceN,'tableName'] = 'Feature_Set_12139'
df_logtable.loc[sourceN,'tableRows'] = len(df_feature_set_idea.index)
df_logtable.loc[sourceN,'tableColumns'] = len(df_feature_set_idea.columns)
sourceN = sourceN+1


# In[126]:


query = """
 SELECT *
 FROM `saas-analytics-io.saas_analytics_io_jira.Key_customers_12132` 
"""
query_job = client.query(
  query,
    # Location must match that of the dataset(s) referenced in the query.
  location = "US",
)  # API request - starts the query
print(query)
df_key_customer_idea = query_job.to_dataframe()
print(df_key_customer_idea.info())

df_logtable.loc[sourceN,'date'] = str_day
df_logtable.loc[sourceN,'tableName'] = 'Key_customers_12132'
df_logtable.loc[sourceN,'tableRows'] = len(df_key_customer_idea.index)
df_logtable.loc[sourceN,'tableColumns'] = len(df_key_customer_idea.columns)
#sourceN = sourceN+1


# In[127]:


df_logtable


# ### Data Preparation

# In[128]:


df_worklog_1 = pd.merge(df_worklog,df_issues[['ISSUE_KEY','ISSUE_TYPE_NAME']],on = 'ISSUE_KEY', how= 'left')


# In[129]:


df_worklog_1.head()


# In[130]:


df_worklog_1.fillna(0).groupby('ISSUE_TYPE_NAME').count()


# In[ ]:





# In[131]:


df_versions = df_versions[df_versions['VERSION_NAME'].str.contains('^v[1-9]')].sort_values('START_DATE')


# In[132]:


df_versions


# In[133]:


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


# In[134]:


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


# In[135]:


df_businessUnit['Business_Unit_2']=df_businessUnit['Business_Unit'] 
df_businessUnit_1 = df_businessUnit.pivot_table(index ='ISSUE_KEY',columns ='Business_Unit',values= 'Business_Unit_2', aggfunc=sum, fill_value ='').reset_index()
df_businessUnit_1.loc[:,'BU'] = df_businessUnit_1.iloc[:,[1,2,3]].apply(":".join, axis=1)
df_businessUnit_1 = df_businessUnit_1[['ISSUE_KEY','BU']]
df_businessUnit_1['BU'] = df_businessUnit_1['BU'].str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":")
df_businessUnit_1.loc[df_businessUnit_1['BU'].str.startswith(":"),'BU']=df_businessUnit_1['BU'].str[1:]
df_businessUnit_1.loc[df_businessUnit_1['BU'].str.endswith(":"),'BU']=df_businessUnit_1['BU'].str[:-1]


# In[136]:


df_IssueFixVersions

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



# In[137]:


df_issueSprints = df_issueSprints.sort_values('SPRINT_ID')
df_issueSprints.drop_duplicates(subset="ISSUE_KEY",keep='last', inplace=True)


# In[138]:


#df_issueSprints[df_issueSprints['ISSUE_KEY']=='RPLAT-15710']


# In[139]:


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


# In[140]:


df_theme_1.head()


# In[141]:


df_category_idea['IDEA_CATEGORY']=df_category_idea['Category'] 
df_category_idea_1 = df_category_idea.pivot_table(index ='ISSUE_KEY',columns ='Category',values= 'IDEA_CATEGORY', aggfunc=sum, fill_value ='').reset_index()
list1 = []
for i in range (1,df_category_idea['Category'].nunique()+1,1):
    list1.append((i))
df_category_idea_1.loc[:,'IDEA_CATEGORY'] = df_category_idea_1.iloc[:,list1].apply(":".join, axis=1)
df_category_idea_1 = df_category_idea_1[['ISSUE_KEY','IDEA_CATEGORY']]
df_category_idea_1['IDEA_CATEGORY'] = df_category_idea_1['IDEA_CATEGORY'].str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":")
df_category_idea_1.loc[df_category_idea_1['IDEA_CATEGORY'].str.startswith(":"),'IDEA_CATEGORY']=df_category_idea_1['IDEA_CATEGORY'].str[1:]
df_category_idea_1.loc[df_category_idea_1['IDEA_CATEGORY'].str.endswith(":"),'IDEA_CATEGORY']=df_category_idea_1['IDEA_CATEGORY'].str[:-1]


# In[142]:


#df_category_idea.head()
df_category_idea_1.head()


# In[143]:


df_feature_set_idea['IDEA_FEATURE_SET']=df_feature_set_idea['Feature_Set'] 
df_feature_set_idea_1 = df_feature_set_idea.pivot_table(index ='ISSUE_KEY',columns ='Feature_Set',values= 'IDEA_FEATURE_SET', aggfunc=sum, fill_value ='').reset_index()
list1 = []
for i in range (1,df_feature_set_idea['Feature_Set'].nunique()+1,1):
    list1.append((i))
df_feature_set_idea_1.loc[:,'IDEA_FEATURE_SET'] = df_feature_set_idea_1.iloc[:,list1].apply(":".join, axis=1)
df_feature_set_idea_1 = df_feature_set_idea_1[['ISSUE_KEY','IDEA_FEATURE_SET']]
df_feature_set_idea_1['IDEA_FEATURE_SET'] = df_feature_set_idea_1['IDEA_FEATURE_SET'].str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":")
df_feature_set_idea_1.loc[df_feature_set_idea_1['IDEA_FEATURE_SET'].str.startswith(":"),'IDEA_FEATURE_SET']=df_feature_set_idea_1['IDEA_FEATURE_SET'].str[1:]
df_feature_set_idea_1.loc[df_feature_set_idea_1['IDEA_FEATURE_SET'].str.endswith(":"),'IDEA_FEATURE_SET']=df_feature_set_idea_1['IDEA_FEATURE_SET'].str[:-1]


# In[144]:


#df_feature_set_idea.head()
df_feature_set_idea_1.head()


# In[145]:


df_key_customer_idea['IDEA_KEY_CUSTOMERS']=df_key_customer_idea['Key_customers'] 
df_key_customer_idea_1 = df_key_customer_idea.pivot_table(index ='ISSUE_KEY',columns ='Key_customers',values= 'IDEA_KEY_CUSTOMERS', aggfunc=sum, fill_value ='').reset_index()
list1 = []
for i in range (1,df_key_customer_idea['Key_customers'].nunique()+1,1):
    list1.append((i))
df_key_customer_idea_1.loc[:,'IDEA_KEY_CUSTOMERS'] = df_key_customer_idea_1.iloc[:,list1].apply(":".join, axis=1)
df_key_customer_idea_1 = df_key_customer_idea_1[['ISSUE_KEY','IDEA_KEY_CUSTOMERS']]
df_key_customer_idea_1['IDEA_KEY_CUSTOMERS'] = df_key_customer_idea_1['IDEA_KEY_CUSTOMERS'].str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":").str.replace("::",":")
df_key_customer_idea_1.loc[df_key_customer_idea_1['IDEA_KEY_CUSTOMERS'].str.startswith(":"),'IDEA_KEY_CUSTOMERS']=df_key_customer_idea_1['IDEA_KEY_CUSTOMERS'].str[1:]
df_key_customer_idea_1.loc[df_key_customer_idea_1['IDEA_KEY_CUSTOMERS'].str.endswith(":"),'IDEA_KEY_CUSTOMERS']=df_key_customer_idea_1['IDEA_KEY_CUSTOMERS'].str[:-1]


# In[146]:


df_key_customer_idea_1.head()


# In[ ]:





# In[ ]:





# ### Data Preparation - Consolidate one table complete

# In[147]:


df_full = pd.merge(df_issues, df_issueSprints, on =['ISSUE_ID','ISSUE_KEY'],how = 'left')
df_full = pd.merge(df_full, df_IssueFixVersions_1, on =['ISSUE_KEY'],how = 'left')
df_full = pd.merge(df_full, df_businessUnit_1, on =['ISSUE_KEY'],how = 'left')
df_full = pd.merge(df_full, df_customers_1, on =['ISSUE_KEY'],how = 'left')
df_full = pd.merge(df_full, df_category_1, on =['ISSUE_KEY'],how = 'left')
df_full = pd.merge(df_full, df_theme_1, on =['ISSUE_KEY'],how = 'left')
df_full = pd.merge(df_full, df_Sprints, on =['SPRINT_ID','SPRINT_NAME'],how = 'left')


# In[148]:


df_full.head()


# In[ ]:





# In[149]:


df_issues_s = df_full[['CREATED','ISSUE_ID','ISSUE_KEY','SUMMARY','ISSUE_TYPE_NAME','ISSUE_STATUS_NAME',
                         'SPRINT_ID','SPRINT_NAME','STATE','START_DATE','END_DATE','COMPLETE_DATE',
                         'VERSION_NAME',
                         'PRIORITY','CURRENT_ASSIGNEE_NAME','REPORTER_NAME','RESOLUTION_DATE','STATUS_CATEGORY_CHANGE_DATE',
                         'TIME_SPENT','TIME_SPENT_WITH_SUBTASKS','PARENT_ISSUE_KEY','CATEGORY','BU','CUSTOMERS','THEME','Story_Points_10400','Progress___11891',
                       '__Effort_12125','Layout_12166'
                      ]]
df_issues_s = df_issues_s.rename(columns={"Story_Points_10400":"STORYPOINTS",
                              "Progress___11891":"PROGRESS"
                             })




# In[150]:


df_issues_s_2 = df_issues_s[['ISSUE_ID','ISSUE_KEY','SUMMARY','ISSUE_TYPE_NAME','ISSUE_STATUS_NAME','CURRENT_ASSIGNEE_NAME'
                             ,'SPRINT_NAME','START_DATE','END_DATE','VERSION_NAME','CATEGORY','BU','CUSTOMERS','THEME',
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


# In[151]:


df_issues_1 = pd.merge(df_issues_s,df_issueLinks,on =['ISSUE_ID','ISSUE_KEY'],how="left")


# In[152]:


df_issues_1 = df_issues_1.rename(columns={"LINKED_ISSUE_ID":"LINKED_1_ISSUE_ID", 
                            "LINKED_ISSUE_KEY":"LINKED_1_ISSUE_KEY",
                            "DIRECTION":"DIRECTION_1",
                            "TYPE":"TYPE_1"
                             })


# In[153]:


df_issues_2 = pd.merge(df_issues_1, df_issues_s_2,on=['LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_ID'],how="left")


# In[154]:


df_issues_2.head()


# ### Product Requests (PR) - Data Preparation

# In[155]:


df_PR = df_issues_2[df_issues_2['ISSUE_TYPE_NAME'].str.startswith('Product Request')][['CREATED','ISSUE_KEY','SUMMARY',
                                                                       'ISSUE_TYPE_NAME','ISSUE_STATUS_NAME',
                                                                        'CURRENT_ASSIGNEE_NAME',
        #'SPRINT_ID','SPRINT_NAME','STATE','START_DATE','END_DATE','COMPLETE_DATE','VERSION_ID','VERSION_NAME','PRIORITY',
        'REPORTER_NAME','RESOLUTION_DATE','STATUS_CATEGORY_CHANGE_DATE','TIME_SPENT','TIME_SPENT_WITH_SUBTASKS',
         'CATEGORY','BU','CUSTOMERS','STORYPOINTS','PROGRESS' ,'TYPE_1','DIRECTION_1',                                                              
        'LINKED_1_ISSUE_KEY','LINKED_1_SUMMARY','LINKED_1_ISSUE_TYPE_NAME','LINKED_1_ISSUE_STATUS_NAME','LINKED_1_ASSIGNEE_NAME',
        'LINKED_1_SPRINT_NAME','LINKED_1_SPRINT_START_DATE','LINKED_1_SPRINT_END_DATE','LINKED_1_VERSION_NAME',
        'LINKED_1_CATEGORY','LINKED_1_BU','LINKED_1_CUSTOMERS','LINKED_1_THEME','LINKED_1_STORYPOINTS','LINKED_1_PROGRESS'
         ]]


# In[156]:


df_PR = df_PR[(df_PR['TYPE_1']=='Depends')&(df_PR['DIRECTION_1']=='Outward')]


# In[157]:


df_issues_PR_1 = df_issues_2[(df_issues_2['ISSUE_TYPE_NAME']!='Product Request')
                             &(df_issues_2['PARENT_ISSUE_KEY'].notnull())][['CREATED','ISSUE_KEY','SUMMARY',
                                                                              'ISSUE_TYPE_NAME','ISSUE_STATUS_NAME',
                                                                              'STATUS_CATEGORY_CHANGE_DATE',
                                                                              'SPRINT_NAME','STATE','START_DATE',
                                                                              'END_DATE','COMPLETE_DATE',
                                                                              'VERSION_NAME','CURRENT_ASSIGNEE_NAME',
                                                                              'REPORTER_NAME',
                                                                              'TIME_SPENT','TIME_SPENT_WITH_SUBTASKS',
                                                                              'CATEGORY','BU','CUSTOMERS','THEME','STORYPOINTS',
                                                                              'PROGRESS','PARENT_ISSUE_KEY'
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


# In[158]:


df_PR_1 = pd.merge(df_PR, df_issues_PR_1,how='left', on='LINKED_1_ISSUE_KEY')


# In[159]:


df_sub_PR_1 = df_issues_2[(df_issues_2['ISSUE_TYPE_NAME']!='Product Request')
                             &(df_issues_2['PARENT_ISSUE_KEY'].notnull())][['CREATED','ISSUE_KEY','SUMMARY',
                                                                              'ISSUE_TYPE_NAME','ISSUE_STATUS_NAME',
                                                                              'STATUS_CATEGORY_CHANGE_DATE',
                                                                              'SPRINT_NAME','STATE','START_DATE',
                                                                              'END_DATE','COMPLETE_DATE',
                                                                              'VERSION_NAME','CURRENT_ASSIGNEE_NAME',
                                                                              'REPORTER_NAME',
                                                                              'TIME_SPENT','TIME_SPENT_WITH_SUBTASKS',
                                                                              'CATEGORY','BU','CUSTOMERS','THEME','STORYPOINTS',
                                                                              'PROGRESS','PARENT_ISSUE_KEY'
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


# In[160]:


df_PR_f = pd.merge(df_PR_1, df_sub_PR_1,how='left', on='CHILD_ISSUE_KEY')


# In[161]:


df_PR_f.loc[df_PR_f['SUB_ISSUE_KEY'].notnull(),'TOTAL_STORYPOINTS']= df_PR_f['SUB_STORYPOINTS']
df_PR_f.loc[(df_PR_f['TOTAL_STORYPOINTS'].isnull())&
            (df_PR_f['SUB_ISSUE_KEY'].isnull())&
            (df_PR_f['CHILD_ISSUE_KEY'].notnull()),'TOTAL_STORYPOINTS']= df_PR_f['CHILD_STORYPOINTS']
df_PR_f.loc[(df_PR_f['TOTAL_STORYPOINTS'].isnull())&
            (df_PR_f['SUB_ISSUE_KEY'].isnull())&
            (df_PR_f['CHILD_ISSUE_KEY'].isnull())&
            (df_PR_f['LINKED_1_ISSUE_KEY'].notnull()),'TOTAL_STORYPOINTS']= df_PR_f['LINKED_1_STORYPOINTS']


# In[162]:


df_PR_f.tail(30)[['LINKED_1_STORYPOINTS','CHILD_STORYPOINTS','SUB_STORYPOINTS','TOTAL_STORYPOINTS']]


# In[163]:


df_PR_f[df_PR_f['SUB_ISSUE_KEY']=='RPLAT-16261'][['SUB_ISSUE_KEY','SUB_ISSUE_TYPE_NAME','SUB_SPRINT_NAME','CHILD_ISSUE_KEY','CHILD_SPRINT_NAME','CHILD_ISSUE_TYPE_NAME']]


# In[164]:


df_PR_f[(df_PR_f['ISSUE_KEY'].isin(['PR-851']))
       # &(df_PR_f['CHILD_SPRINT_START_DATE'].isnull())
       ][['ISSUE_KEY','SUMMARY','LINKED_1_ISSUE_KEY','LINKED_1_SUMMARY','LINKED_1_ISSUE_TYPE_NAME',
          'LINKED_1_SPRINT_NAME','LINKED_1_SPRINT_START_DATE',
          'LINKED_1_VERSION_NAME','LINKED_1_CATEGORY','LINKED_1_BU','LINKED_1_CUSTOMERS','LINKED_1_STORYPOINTS',
         'LINKED_1_PROGRESS','CHILD_CREATED','CHILD_ISSUE_KEY','CHILD_SUMMARY','CHILD_ISSUE_TYPE_NAME',
        'CHILD_ISSUE_STATUS_NAME','CHILD_SPRINT_NAME','CHILD_SPRINT_START_DATE','CHILD_VERSION_NAME','CHILD_CATEGORY',
         'CHILD_BU','CHILD_CUSTOMERS','CHILD_STORYPOINTS','CHILD_PROGRESS','SUB_ISSUE_KEY','SUB_SUMMARY',
          'SUB_ISSUE_TYPE_NAME','SUB_ISSUE_STATUS_NAME','SUB_SPRINT_NAME','SUB_SPRINT_STATE','SUB_STORYPOINTS',
         'SUB_PROGRESS']].sort_values('ISSUE_KEY',ascending = False).head()


# In[165]:


#df_issues[df_issues['ISSUE_KEY']=='PR-732']
df_PR_f[df_PR_f['ISSUE_KEY']=='PR-732'].head()


# In[166]:


df_PR_sub_not_Null = df_PR_f[(df_PR_f['SUB_ISSUE_KEY'].notnull())]


# In[167]:


df_versions['VERSION_ID'].count()


# In[168]:


df_versions = df_versions.sort_values('START_DATE')


# In[169]:


df_versions.head()


# In[170]:


df_versions['START_DATE_m'] = df_versions['RELEASE_DATE']+pd.Timedelta(days=1)
df_versions['START_DATE_2_m'] = df_versions['RELEASE_DATE']+pd.Timedelta(seconds=86399)


# In[171]:


for i in range(0,df_versions['VERSION_ID'].count()):
    START_DATE = df_versions.iloc[i-1,10] 
    END_DATE = df_versions.iloc[i,11] 
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
    

#df_PR_f.loc[((df_PR_f['SUB_STATUS_CATEGORY_CHANGE_DATE']>df_versions['START_DATE'])(df_PR_f['SUB_STATUS_CATEGORY_CHANGE_DATE']>df_versions['START_DATE'])]


# In[172]:


df_PR_f[df_PR_f['CHILD_ISSUE_KEY']=='RPLAT-23657']


# In[173]:


df_PR_f['CHILD_SPRINT_START_DATE'].count()


# In[174]:


print(df_PR_f[df_PR_f['CHILD_SPRINT_DATE_VERSION'].isnull()].count()['CREATED'])
print(df_PR_f[df_PR_f['CHILD_SPRINT_DATE_VERSION'].notnull()].count()['CREATED'])


# In[175]:


df_PR_f.tail()
            


# In[176]:


table = "saas-analytics-io.processed.jira_processed_PR"
df_PR_f.to_gbq(table, if_exists='replace')


# ### YTEM Tickets

# In[177]:


df_issues_2[(df_issues_2['ISSUE_KEY'].str.startswith('IDEA'))&(df_issues_2['TYPE_1'].isnull())].head()


# In[178]:


df_issues_2[(df_issues_2['ISSUE_KEY'].str.startswith('IDEA'))&(df_issues_2['TYPE_1'].str.startswith('Polaris issue'))].head()


# In[179]:


#df_issues[df_issues['ISSUE_KEY']=='IDEA-16'].head()
#df_businessUnit[df_businessUnit['ISSUE_KEY']=='YTEM-16'].head().T


# In[180]:


df_issues_2[df_issues_2['ISSUE_KEY'].str.startswith('IDEA')].fillna(0).groupby(['TYPE_1','DIRECTION_1']).count()


# In[181]:


df_issues_2[(df_issues_2['ISSUE_KEY'].str.startswith('IDEA'))&
            (df_issues_2['TYPE_1'].str.startswith('Polaris issue link'))&
           (df_issues_2['ISSUE_KEY']=='IDEA-8')]


# In[182]:


df_IDEA = df_issues_2[(df_issues_2['ISSUE_TYPE_NAME'].str.startswith('Idea'))&(df_issues_2['TYPE_1'].str.startswith('Polaris issue'))][['CREATED','ISSUE_KEY','SUMMARY',
                                                                       'ISSUE_TYPE_NAME','ISSUE_STATUS_NAME',
                                                                        'CURRENT_ASSIGNEE_NAME',
        #'SPRINT_ID','SPRINT_NAME','STATE','START_DATE','END_DATE','COMPLETE_DATE','VERSION_ID','VERSION_NAME','PRIORITY',
        'REPORTER_NAME','RESOLUTION_DATE','STATUS_CATEGORY_CHANGE_DATE','TIME_SPENT','TIME_SPENT_WITH_SUBTASKS',
         'CATEGORY','BU','CUSTOMERS','STORYPOINTS','PROGRESS' ,'TYPE_1','DIRECTION_1',                                                              
        'LINKED_1_ISSUE_KEY','LINKED_1_SUMMARY','LINKED_1_ISSUE_TYPE_NAME','LINKED_1_ISSUE_STATUS_NAME','LINKED_1_ASSIGNEE_NAME',
        'LINKED_1_SPRINT_NAME','LINKED_1_SPRINT_START_DATE','LINKED_1_SPRINT_END_DATE','LINKED_1_VERSION_NAME',
        'LINKED_1_CATEGORY','LINKED_1_BU','LINKED_1_CUSTOMERS','LINKED_1_THEME','LINKED_1_STORYPOINTS','LINKED_1_PROGRESS','__Effort_12125','Layout_12166'
         ]]

df_IDEA = df_IDEA.rename(columns={'__Effort_12125':'EFFORT',
                              'Layout_12166':'LAYOUT'})


# In[183]:


df_IDEA = pd.merge(df_IDEA, df_category_idea_1, on =['ISSUE_KEY'],how = 'left')
df_IDEA = pd.merge(df_IDEA, df_feature_set_idea_1, on =['ISSUE_KEY'],how = 'left')
df_IDEA = pd.merge(df_IDEA, df_key_customer_idea_1, on =['ISSUE_KEY'],how = 'left')


# In[ ]:





# In[184]:


df_issues_IDEA_1 = df_issues_2[(df_issues_2['ISSUE_TYPE_NAME']!='Idea')
                             &(df_issues_2['PARENT_ISSUE_KEY'].notnull())][['CREATED','ISSUE_KEY','SUMMARY',
                                                                              'ISSUE_TYPE_NAME','ISSUE_STATUS_NAME',
                                                                              'STATUS_CATEGORY_CHANGE_DATE',
                                                                              'SPRINT_NAME','STATE','START_DATE',
                                                                              'END_DATE','COMPLETE_DATE',
                                                                              'VERSION_NAME','CURRENT_ASSIGNEE_NAME',
                                                                              'REPORTER_NAME',
                                                                              'TIME_SPENT','TIME_SPENT_WITH_SUBTASKS',
                                                                              'CATEGORY','BU','CUSTOMERS','THEME','STORYPOINTS',
                                                                              'PROGRESS','PARENT_ISSUE_KEY'
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


# In[185]:


df_YTEM_1 = pd.merge(df_IDEA, df_issues_IDEA_1,how='left', on='LINKED_1_ISSUE_KEY')


# In[186]:


df_sub_YTEM_1 = df_issues_2[(df_issues_2['ISSUE_TYPE_NAME']!='Idea')
                             &(df_issues_2['PARENT_ISSUE_KEY'].notnull())][['CREATED','ISSUE_KEY','SUMMARY',
                                                                              'ISSUE_TYPE_NAME','ISSUE_STATUS_NAME',
                                                                              'STATUS_CATEGORY_CHANGE_DATE',
                                                                              'SPRINT_NAME','STATE','START_DATE',
                                                                              'END_DATE','COMPLETE_DATE',
                                                                              'VERSION_NAME','CURRENT_ASSIGNEE_NAME',
                                                                              'REPORTER_NAME',
                                                                              'TIME_SPENT','TIME_SPENT_WITH_SUBTASKS',
                                                                              'CATEGORY','BU','CUSTOMERS','THEME','STORYPOINTS',
                                                                              'PROGRESS','PARENT_ISSUE_KEY'
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


# In[187]:


df_YTEM_1 = pd.merge(df_YTEM_1, df_sub_YTEM_1,how='left', on='CHILD_ISSUE_KEY')


# In[188]:


df_YTEM_1.loc[df_YTEM_1['SUB_ISSUE_KEY'].notnull(),'TOTAL_STORYPOINTS']= df_YTEM_1['SUB_STORYPOINTS']
df_YTEM_1.loc[(df_YTEM_1['TOTAL_STORYPOINTS'].isnull())&
            (df_YTEM_1['SUB_ISSUE_KEY'].isnull())&
            (df_YTEM_1['CHILD_ISSUE_KEY'].notnull()),'TOTAL_STORYPOINTS']= df_YTEM_1['CHILD_STORYPOINTS']
df_YTEM_1.loc[(df_YTEM_1['TOTAL_STORYPOINTS'].isnull())&
            (df_YTEM_1['SUB_ISSUE_KEY'].isnull())&
            (df_YTEM_1['CHILD_ISSUE_KEY'].isnull())&
            (df_YTEM_1['LINKED_1_ISSUE_KEY'].notnull()),'TOTAL_STORYPOINTS']= df_YTEM_1['LINKED_1_STORYPOINTS']


# In[189]:


for i in range(0,df_versions['VERSION_ID'].count()):
    START_DATE = df_versions.iloc[i-1,10] 
    END_DATE = df_versions.iloc[i,11] 
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


# In[190]:


table = "saas-analytics-io.processed.jira_processed_IDEAS"
df_YTEM_1.to_gbq(table, if_exists='replace')


# In[ ]:





# In[191]:


df_YTEM_1[df_YTEM_1['SUB_ISSUE_KEY']=='RPLAT-24808']


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[192]:


df_customers_agg = pd.merge(df_customers,df_PR_f[['LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_STATUS_NAME','LINKED_1_BU','LINKED_1_VERSION_NAME',
                                                  'CHILD_SPRINT_DATE_VERSION','CHILD_SPRINT_END_DATE_VERSION',
                                                  'SUB_ISSUE_KEY','SUB_STATUS_CATEGORY_CHANGE_DATE','SUB_STORYPOINTS']],how='inner',left_on = 'ISSUE_KEY',right_on='LINKED_1_ISSUE_KEY')
df_customers_agg = df_customers_agg.drop(['ISSUE_ID','ISSUE_KEY','Customers_2'], axis =1)
df_customers_agg = df_customers_agg.rename( columns={'Customers' : 'CUSTOMERS'})

df_customers_adj = df_customers.groupby('ISSUE_KEY').count()[['Customers']].reset_index()
df_customers_adj = df_customers_adj.rename(columns={'Customers':'CUSTOMERS_COUNT','ISSUE_KEY':'LINKED_1_ISSUE_KEY'})
df_customers_agg = pd.merge(df_customers_agg,df_customers_adj,how='left', on='LINKED_1_ISSUE_KEY')
df_customers_agg.loc[:,'ADJ_STORYPOINTS']= df_customers_agg['SUB_STORYPOINTS']/df_customers_agg['CUSTOMERS_COUNT']

#------------------------------------------------------------------------------------------------------------------------------------------------

df_category_agg = pd.merge(df_category[['ISSUE_KEY','OKR_Category']],df_PR_f[['LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_STATUS_NAME','LINKED_1_BU','LINKED_1_VERSION_NAME',
                                                  'CHILD_SPRINT_DATE_VERSION','CHILD_SPRINT_END_DATE_VERSION',
                                                  'SUB_ISSUE_KEY','SUB_STATUS_CATEGORY_CHANGE_DATE','SUB_STORYPOINTS']],how='inner',left_on = 'ISSUE_KEY',right_on='LINKED_1_ISSUE_KEY')
df_category_agg = df_category_agg.drop(['ISSUE_KEY'], axis =1)
df_category_agg = df_category_agg.rename( columns={'OKR_Category' : 'CATEGORY'})

df_category_adj = df_category.groupby('ISSUE_KEY').count()[['OKR_Category']].reset_index()
df_category_adj = df_category_adj.rename(columns={'OKR_Category':'CATEGORY_COUNT','ISSUE_KEY':'LINKED_1_ISSUE_KEY'})
df_category_agg = pd.merge(df_category_agg,df_category_adj,how='left', on='LINKED_1_ISSUE_KEY')
df_category_agg.loc[:,'ADJ_STORYPOINTS']= df_category_agg['SUB_STORYPOINTS']/df_category_agg['CATEGORY_COUNT']


# In[193]:


df_customers_agg.sample(10)


# In[194]:


table = "saas-analytics-io.processed.jira_processed_PR_customers"
df_customers_agg.to_gbq(table, if_exists='replace')
table = "saas-analytics-io.processed.jira_processed_PR_category"
df_category_agg.to_gbq(table, if_exists='replace')


# In[195]:


df_customers_agg.sample(15)


# In[196]:


#df_customers_agg[df_customers_agg['CHILD_SPRINT_DATE_VERSION']=='12 v8.17 Deer']
df_customers_agg.groupby('CUSTOMERS').count()


# In[197]:


#df_category_agg[df_category_agg['SUB_ISSUE_KEY'].duplicated()]


# In[198]:


df_customers[df_customers['ISSUE_KEY']=='RPLAT-21308']


# In[199]:


df_customers_ideas_agg = pd.merge(df_customers,df_YTEM_1[['LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_STATUS_NAME','LINKED_1_BU','LINKED_1_VERSION_NAME',
                                                  'CHILD_SPRINT_DATE_VERSION','CHILD_SPRINT_END_DATE_VERSION',
                                                  'SUB_ISSUE_KEY','SUB_STATUS_CATEGORY_CHANGE_DATE','SUB_STORYPOINTS']],how='inner',left_on = 'ISSUE_KEY',right_on='LINKED_1_ISSUE_KEY')
df_customers_ideas_agg = df_customers_ideas_agg.drop(['ISSUE_ID','ISSUE_KEY','Customers_2'], axis =1)
df_customers_ideas_agg = df_customers_ideas_agg.rename( columns={'Customers' : 'CUSTOMERS'})

df_customers_ideas_adj = df_customers.groupby('ISSUE_KEY').count()[['Customers']].reset_index()
df_customers_ideas_adj = df_customers_ideas_adj.rename(columns={'Customers':'CUSTOMERS_COUNT','ISSUE_KEY':'LINKED_1_ISSUE_KEY'})
df_customers_ideas_agg = pd.merge(df_customers_ideas_agg,df_customers_ideas_adj,how='left', on='LINKED_1_ISSUE_KEY')
df_customers_ideas_agg.loc[:,'ADJ_STORYPOINTS']= df_customers_ideas_agg['SUB_STORYPOINTS']/df_customers_ideas_agg['CUSTOMERS_COUNT']

#------------------------------------------------------------------------------------------------------------------------------------------------

df_category_ideas_agg = pd.merge(df_category[['ISSUE_KEY','OKR_Category']],df_YTEM_1[['LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_STATUS_NAME','LINKED_1_BU','LINKED_1_VERSION_NAME',
                                                  'CHILD_SPRINT_DATE_VERSION','CHILD_SPRINT_END_DATE_VERSION',
                                                  'SUB_ISSUE_KEY','SUB_STATUS_CATEGORY_CHANGE_DATE','SUB_STORYPOINTS']],how='inner',left_on = 'ISSUE_KEY',right_on='LINKED_1_ISSUE_KEY')
df_category_ideas_agg = df_category_ideas_agg.drop(['ISSUE_KEY'], axis =1)
df_category_ideas_agg = df_category_ideas_agg.rename( columns={'OKR_Category' : 'CATEGORY'})

df_category_ideas_adj = df_category.groupby('ISSUE_KEY').count()[['OKR_Category']].reset_index()
df_category_ideas_adj = df_category_ideas_adj.rename(columns={'OKR_Category':'CATEGORY_COUNT','ISSUE_KEY':'LINKED_1_ISSUE_KEY'})
df_category_ideas_agg = pd.merge(df_category_ideas_agg,df_category_ideas_adj,how='left', on='LINKED_1_ISSUE_KEY')
df_category_ideas_agg.loc[:,'ADJ_STORYPOINTS']= df_category_ideas_agg['SUB_STORYPOINTS']/df_category_ideas_agg['CATEGORY_COUNT']


# In[200]:


table = "saas-analytics-io.processed.jira_processed_IDEAS_customers"
df_customers_ideas_agg.to_gbq(table, if_exists='replace')
table = "saas-analytics-io.processed.jira_processed_IDEAS_category"
df_category_ideas_agg.to_gbq(table, if_exists='replace')


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# ## Worklogs enhancement

# In[201]:


df_worklog_1.head()


# In[202]:


df_PR_worklogs = df_PR_f[['ISSUE_KEY'
                          ,'LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_TYPE_NAME','LINKED_1_ISSUE_STATUS_NAME'
                          ,'CHILD_ISSUE_KEY','CHILD_ISSUE_TYPE_NAME','CHILD_ISSUE_STATUS_NAME','CHILD_SPRINT_DATE_VERSION','CHILD_SPRINT_END_DATE_VERSION'
                          ,'SUB_ISSUE_KEY','SUB_ISSUE_TYPE_NAME','SUB_ISSUE_STATUS_NAME','SUB_STATUS_CATEGORY_CHANGE_DATE','TOTAL_STORYPOINTS']]


# In[203]:


df_worklog_U = df_worklog_1[['ISSUE_KEY','ISSUE_TYPE_NAME','AUTHOR_NAME','UPDATE_NAME','START_DATE','LOGGED_TIME','CREATED','UPDATED']]


# In[204]:


df_worklog_U = df_worklog_U.rename( columns={'ISSUE_KEY' : 'SUB_ISSUE_KEY'})


# In[205]:


df_worklog_enh = pd.merge(df_worklog_U,df_PR_worklogs, on = 'SUB_ISSUE_KEY',how = 'left')


# In[206]:


df_worklog_enh


# In[207]:


table = "saas-analytics-io.processed.jira_processed_PR_worklog"
df_worklog_enh.to_gbq(table, if_exists='replace')


# In[ ]:





# In[208]:


df_YTEM_1


# In[209]:


df_IDEAS_worklogs = df_YTEM_1[['ISSUE_KEY'
                          ,'LINKED_1_ISSUE_KEY','LINKED_1_ISSUE_TYPE_NAME','LINKED_1_ISSUE_STATUS_NAME'
                          ,'CHILD_ISSUE_KEY','CHILD_ISSUE_TYPE_NAME','CHILD_ISSUE_STATUS_NAME','CHILD_SPRINT_DATE_VERSION','CHILD_SPRINT_END_DATE_VERSION'
                          ,'SUB_ISSUE_KEY','SUB_ISSUE_TYPE_NAME','SUB_ISSUE_STATUS_NAME','SUB_STATUS_CATEGORY_CHANGE_DATE','TOTAL_STORYPOINTS']]


# In[210]:


df_worklog_U = df_worklog_1[['ISSUE_KEY','ISSUE_TYPE_NAME','AUTHOR_NAME','UPDATE_NAME','START_DATE','LOGGED_TIME','CREATED','UPDATED']]


# In[211]:


df_worklog_U = df_worklog_U.rename( columns={'ISSUE_KEY' : 'SUB_ISSUE_KEY'})


# In[212]:


df_worklog_enh_ideas = pd.merge(df_worklog_U,df_IDEAS_worklogs, on = 'SUB_ISSUE_KEY',how = 'left')


# In[213]:


df_worklog_enh_ideas.head()


# In[214]:


df_worklog_U_2 = df_worklog_enh_ideas[df_worklog_enh_ideas['ISSUE_KEY'].isnull()][['SUB_ISSUE_KEY','ISSUE_TYPE_NAME','AUTHOR_NAME','UPDATE_NAME','START_DATE','LOGGED_TIME','CREATED','UPDATED']]


# In[215]:


df_worklog_enh_ideas_pr = pd.merge(df_worklog_U_2,df_PR_worklogs, on = 'SUB_ISSUE_KEY',how = 'left')


# In[216]:


df_worklog_enh_only_ideas = df_worklog_enh_ideas[df_worklog_enh_ideas['ISSUE_KEY'].notnull()]


# In[217]:


df_worklog_enh_ideas_pr = df_worklog_enh_only_ideas.append(df_worklog_enh_ideas_pr)


# In[218]:


df_worklog_enh_ideas_pr[df_worklog_enh_ideas_pr['SUB_ISSUE_KEY']=='RPLAT-24808']


# In[219]:


#table = "saas-analytics-io.processed.jira_processed_IDEAS_worklog"
#df_worklog_enh_ideas.to_gbq(table, if_exists='replace')


# In[ ]:


table = "saas-analytics-io.processed.jira_processed_IDEAS_worklog"
df_worklog_enh_ideas_pr.to_gbq(table, if_exists='replace')


# In[ ]:





# In[882]:


df_worklog_enh[df_worklog_enh['SUB_ISSUE_KEY']=='RPLAT-24304']


# In[883]:


df_PR_worklogs[df_PR_worklogs['SUB_ISSUE_KEY']=='RPLAT-23320']

