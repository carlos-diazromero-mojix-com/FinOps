#!/usr/bin/env python
# coding: utf-8


#import pandas_gbq
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
from pyspark.sql.functions import expr,first, last,when, split, col,lit, concat, date_format,to_utc_timestamp,to_timestamp, regexp_replace,concat_ws

#client = bigquery.Client(location="us-central1")
#print("Client creating using default project: {}".format(client.project))
pd.set_option('display.width', 1000)
pd.set_option("max_colwidth",10000)
pd.set_option("max_rows",1000)
pd.set_option("max_columns",100)


# In[4]:


spark = SparkSession.builder.appName("vt1EventsClassify").getOrCreate()


# In[5]:


current_date = datetime.today()
dias_atras = 2
str_day = (current_date - timedelta(days = dias_atras)).strftime("%Y-%m-%d")
#STR_YEARMONTH = (current_date - timedelta(days = dias_atras)).strftime("%Y%m")
#str_day = "2022-04-16"
#STR_YEARMONTH = "201908"
hour_ini_1 = '00:00:00'
hour_fin_1 = '23:59:59'
print(str_day,hour_ini_1,hour_fin_1)


# ### Read Events Metadata

# In[6]:


df_metadata = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','vt1_v1data1.metadata').option("filter", """date(time) = '%s' and time(time) between '%s' and '%s'"""%(str_day,hour_ini_1,hour_fin_1)).load()

#df_metadata = spark.read.format('bigquery').option('project','saas-mojixtrack-io').option('table','silver.kafka_metadata').option("filter", """date(time) = '%s' and time(time) between '%s' and '%s'"""%(str_day,hour_ini_1,hour_fin_1)).load()


# In[7]:


df_metadata.printSchema()


# ### Read Events UDF (Filtered for FinOps)

# In[ ]:


df_udf_test = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','vt1_v1data1.udf').option("filter", """date(time) = '%s' and time(time) between '%s' and '%s'"""%(str_day,hour_ini_1,hour_fin_1)).load()

#df_udf_test = spark.read.format('bigquery').option('project','saas-mojixtrack-io').option('table','silver.kafka_udf').option("filter", """date(time) = '%s' and time(time) between '%s' and '%s'"""%(str_day,hour_ini_1,hour_fin_1)).load()


# In[ ]:


print("Total Registros udf_VT1 ", df_udf_test.count())


# ### Data Prepairing

# In[ ]:


### Metadata Prepairing
df_metadata = df_metadata.withColumn('bridgeKey_2',when((df_metadata["bridgeKey"]=="/SERVICES"), split(df_metadata["bridgeKey"],"/").getItem(1))
                                     .when((df_metadata["bridgeKey"]!='/SERVICES'), split(df_metadata["bridgeKey"],"/").getItem(2)))
df_metadata = df_metadata.withColumn('id_serial_number',concat(df_metadata['id'],lit('-'), df_metadata['serialNumber']))

df_metadata = df_metadata.withColumn('datetime_h', date_format(df_metadata['time'], "d/M/y H"))
df_metadata = df_metadata.withColumn('datetime_h', to_utc_timestamp(to_timestamp(df_metadata['datetime_h'],'d/M/y H'), 'UTC'))


# In[ ]:


print("Total Registros metadata ", df_metadata.count())
print("Total Registros unicos ", df_metadata.distinct().count())


# In[ ]:


##### UDF Prepairing
df_udf_test = df_udf_test.withColumn('id_serial_number',concat(df_udf_test['metadataId'],lit('-'), df_udf_test['serialNumber']))
df_udf_test = df_udf_test.distinct()


# In[ ]:


df_udf_test.count()


# In[ ]:


df_pivot = df_udf_test.groupBy("id_serial_number").pivot("key").agg(first("value")).fillna('N/A')


# In[ ]:


df_pivot = df_udf_test.groupBy("id_serial_number").pivot("key").agg(first("value")).fillna('N/A')


# In[ ]:


#### Merge UDF With Metadata (Spark)
df_full = df_metadata.join(df_pivot,df_metadata.id_serial_number ==  df_pivot.id_serial_number,"outer")
df_full = df_full.fillna('N/A')


# In[ ]:


df_full = df_full.withColumn('KeyField',concat_ws("-",df_full['tenantCode'],df_full['thingType'],df_full['bridgeKey_2'],df_full['specName'],df_full['source']))


# In[ ]:


df_full.printSchema()


# In[ ]:


#display(df_full)


# ### Read Business Process List (FeatureSet, Process, SubProcess)

# In[17]:


#######List of Business Process
url_1 = 'https://docs.google.com/spreadsheets/d/1u2ND8VO0CifSLwIwcqrQAfYS05JpAahizeeRNYtRHCo/edit#gid=1904341781'
url = url_1.replace('/edit#gid=', '/export?format=csv&gid=')
df_list = pd.read_csv(url, dtype=str)
DF_list = spark.createDataFrame(df_list.astype(str)) 
#df_test = df_test.rename(columns={'Category Long':'CATEGORY_LONG'})


# In[ ]:





# In[25]:


DF_list.show()


# In[26]:


DF_list_f = DF_list[['KeyField_L','FeatureSet','Process','SubProcess']]


# In[27]:


df_full_c = df_full.join(DF_list_f,df_full.KeyField == DF_list_f.KeyField_L,"Left")


# In[ ]:


#Creation of three important fields --> EventSource, StoreCode, Is_InternalEvent
df_full_c = df_full_c.withColumn('EventSource',lit('N/A'))
df_full_c = df_full_c.withColumn('StoreCode',lit('N/A'))
df_full_c = df_full_c.withColumn('Is_InternalEvent',lit('N/A'))


# In[ ]:


df_full_c = df_full_c.withColumn('time', regexp_replace('time', '2022-12-07', '2022-12-08')) 
df_full_c = df_full_c.withColumn('datetime_h', date_format(df_full_c['time'], "d/M/y H"))
df_full_c = df_full_c.withColumn('datetime_h', to_utc_timestamp(to_timestamp(df_full_c['datetime_h'],'d/M/y H'), 'UTC'))


# In[ ]:


#display(df_full_c)


# In[ ]:


#display(df_full_c.groupBy(['KeyField','FeatureSet']).count())


# #### Aggregation of Events

# In[ ]:


df_agg = df_full_c.groupBy(['datetime_h','tenantCode','thingType','specName','bridgeKey_2','source','FeatureSet','Process','SubProcess','EventSource','StoreCode','Is_InternalEvent']).count()


# In[ ]:


df_agg = df_agg.withColumnRenamed("count","mojix_blink_count")


# In[ ]:


bucket = "finops-outputs"
spark.conf.set('temporaryGcsBucket', bucket)

print("Write data in Big Query Tables")

df_agg.write.format('bigquery') \
    .option('table', 'saas-analytics-io.processed.vt1_event_classify_finops') \
    .mode('append') \
    .save()

#table = "saas-analytics-io.processed.vt1_event_classify_finops"
#df_agg_pd.to_gbq(table,  if_exists='append')


# In[ ]:


#df_agg[df_agg['datetime_h'].isNull()].show()


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




