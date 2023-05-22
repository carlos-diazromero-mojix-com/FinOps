#!/usr/bin/env python
# coding: utf-8

# In[ ]:


#dbutils.widgets.text("dias_atras", defaultValue="")
#dias_atras = dbutils.widgets.get("dias_atras")


# In[1]:


#!pip install pandas-gbq


# In[24]:


import pandas_gbq
import numpy as np
import requests
import datetime
import pandas as pd
import re
import json
from datetime import date, timedelta
from datetime import datetime
import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr,first, last,when, split, col,lit, concat, date_format,from_utc_timestamp,to_utc_timestamp,to_timestamp, regexp_replace,concat_ws
from google.cloud import bigquery
client = bigquery.Client(location="us-central1")
#print("Client creating using default project: {}".format(client.project))
pd.set_option('display.width', 1000)
pd.set_option("max_colwidth",10000)
pd.set_option("max_rows",1000)
pd.set_option("max_columns",100)


# In[8]:


spark = SparkSession.builder.appName("SanboxVT1_YT1").getOrCreate()


# In[9]:


current_date = datetime.today()
dias_atras = 1
#dias_atras = int(dias_atras)
str_day = (current_date - timedelta(days = dias_atras)).strftime("%Y-%m-%d")
print(str_day)


# ## YTS1 Metadata

# In[15]:


df_yts1_metadata = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','yts1_v1data1.metadata').option("filter", """date(time) = '%s'"""%(str_day)).load()


# In[16]:


print("Total Registros metadata ", df_yt1_metadata.count())


# ## VTS1 Metadata

# In[18]:


df_vts1_metadata = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','vts1_v1data1.metadata').option("filter", """date(time) = '%s'"""%(str_day)).load()


# In[19]:


print("Total Registros metadata ", df_vt1_metadata.count())


# ### YTS1 Data Prepairing and groupping

# In[31]:


df_yts1_metadata = df_yts1_metadata.withColumn('datetime_h', date_format(df_yts1_metadata['time'], "d/M/y H"))
df_yts1_metadata = df_yts1_metadata.withColumn('datetime_h', to_utc_timestamp(to_timestamp(df_yts1_metadata['datetime_h'],'d/M/y H'), 'UTC'))
df_yts1_metadata = df_yts1_metadata.withColumn('datetime_h_LA', from_utc_timestamp(col("datetime_h"),"America/Los_Angeles"))


df_yts1_metadata_agg = df_yts1_metadata.groupBy(['datetime_h_LA','datetime_h','tenantCode','thingType']).count()
df_yts1_metadata_agg = df_yts1_metadata_agg.withColumnRenamed("count","mojix_blink_count")


# In[32]:


df_yts1_metadata_agg.show()


# ### VTS1 Data Prepairing and groupping

# In[33]:


df_vts1_metadata = df_vts1_metadata.withColumn('datetime_h', date_format(df_vts1_metadata['time'], "d/M/y H"))
df_vts1_metadata = df_vts1_metadata.withColumn('datetime_h', to_utc_timestamp(to_timestamp(df_vts1_metadata['datetime_h'],'d/M/y H'), 'UTC'))
df_vts1_metadata = df_vts1_metadata.withColumn('datetime_h_LA', from_utc_timestamp(col("datetime_h"),"America/Los_Angeles"))

df_vts1_metadata_agg = df_vts1_metadata.groupBy(['datetime_h_LA','datetime_h','tenantCode','thingType']).count()
df_vts1_metadata_agg = df_vts1_metadata_agg.withColumnRenamed("count","mojix_blink_count")


# In[34]:


df_vts1_metadata_agg.show()


# ### Write data in Bigquery

# In[ ]:


bucket = "finops-outputs"
spark.conf.set('temporaryGcsBucket', bucket)


# In[ ]:


print("Write YTS1 data in Big Query Tables")

df_yts1_metadata_agg.write.format('bigquery')     .option('table', 'saas-analytics-io.processed.yts1_event_finops')     .mode('append')     .save()


# In[ ]:


print("Write VTS1 data in Big Query Tables")

df_yts1_metadata_agg.write.format('bigquery')     .option('table', 'saas-analytics-io.processed.vts1_event_finops')     .mode('append')     .save()


# In[ ]:




