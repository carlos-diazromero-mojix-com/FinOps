#!/usr/bin/env python
# coding: utf-8

# In[177]:


from google.cloud import bigquery
import pandas as pd
import requests
import re
import json
import datetime
from datetime import datetime, timedelta
client = bigquery.Client(location="US")
print("Client creating using default project: {}".format(client.project))
def get_body(body_field):
    try:
        parsed = body_field.item().replace("\\","")
    except Exception as error:
        print(error)
    return json.loads(parsed)
pd.set_option('display.width', 1000)
pd.set_option("max_colwidth",10000)
pd.set_option('display.max_rows', 10000)
import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, expr,first, last,when, split, col,lit, concat, date_format,to_utc_timestamp,to_timestamp, regexp_replace,from_utc_timestamp
from pyspark.sql.functions import sum as _sum

from pandas.io.json import json_normalize


# In[91]:


spark = SparkSession.builder.appName("yt1EventsAllocateCost").getOrCreate()


# In[156]:


current_date = datetime.today()
#dias_atras = int(dias_atras)
dias_atras = 2
str_day = (current_date - timedelta(days = dias_atras)).strftime("%Y-%m-%d")
#STR_YEARMONTH = (current_date - timedelta(days = dias_atras)).strftime("%Y%m")
#str_day = "2022-04-16"
#STR_YEARMONTH = "201908"
hour_ini_1 = '00:00:00'
hour_fin_1 = '23:59:59'
print(str_day,hour_ini_1,hour_fin_1)


# ## YT1 COST ALLOCATION

# ### YT1 Events Classified extraction

# In[157]:


df_events_classif = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','processed.yt1_event_classify_finops').option("filter", """date(datetime_h) = '%s' and time(datetime_h)
between '%s' and '%s'"""%(str_day,hour_ini_1,hour_fin_1)).load()

#df_events_classif = spark.read.format('bigquery').option('project','saas-analytics-io')\
 #       .option('table','processed.yt1_event_classify_finops').option("filter", """date(datetime_h)
  #  between '2022-07-01' and '2023-04-12'""").load()


# In[158]:


df_events_classif_Agg_hour = df_events_classif.groupBy('datetime_h').agg(_sum('mojix_blink_count').alias('mojix_blink_count'))
                                                                         


# In[159]:


df_events_classif_Agg_hour.show()


# ### YT1 GCP cost table extraction

# In[160]:


GCP_cost_table = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','finance.gcp_billing_export_v1_01FE23_B1D2D8_10D34D').option("filter","JSON_VALUE(project.number)= '260718309650' and usage_start_time between '%s %s' and '%s %s' "%(str_day,hour_ini_1,str_day,hour_fin_1)).load()


#GCP_cost_table = spark.read.format('bigquery')\
#.option('project','saas-analytics-io')\
#.option('table','finance.gcp_billing_export_v1_01FE23_B1D2D8_10D34D')\
#.option("filter","JSON_VALUE(project.number)= '260718309650' and date(usage_start_time) between '2022-07-01' and '2023-04-12'")\
#.load()


# In[161]:


GCP_cost_table = GCP_cost_table.select(col("service.description").alias('service_description'),
                                          col("sku.description").alias('sku_description'),
                                          col("usage_start_time"),
                                          col("project.id").alias('id_project'),
                                          col("cost"),
                                          col("usage.amount").alias('usage_amount'),
                                          col("usage.unit").alias('usage_unit'),
                                          col("credits")[0].alias('credits_1'),
                                          col("credits")[1].alias('credits_2'),
                                          col("cost_type")
                                         )   

GCP_cost_table = GCP_cost_table.select(col('service_description'),
                                          col('sku_description'),
                                          col("usage_start_time"),
                                          col('id_project'),
                                          col("cost"),
                                          col('usage_amount'),
                                          col('usage_unit'),
                                          col("credits_1.name").alias('credits_1_name'),
                                          col("credits_1.amount").alias('credits_1_amount'),
                                          col("credits_1.type").alias('credits_1_type'),
                                          col("credits_2.name").alias('credits_2_name'),
                                          col("credits_2.amount").alias('credits_2_amount'),
                                          col("credits_2.type").alias('credits_2_type'),
                                          col("cost_type")
                                         ) 
#GCP_cost_table.show()


# In[162]:


GCP_cost_table = GCP_cost_table.withColumn("credits_discounts",when((GCP_cost_table['credits_1_type'].like('%DISCOUNT%'))&
                                                                    ((~(GCP_cost_table['credits_2_type'].like('%DISCOUNT%')))|
                                                                     (GCP_cost_table['credits_2_type'].isNull())),GCP_cost_table['credits_1_amount'])
                                          .when(((~(GCP_cost_table['credits_1_type'].like('%DISCOUNT%')))
                                                |(GCP_cost_table['credits_1_type'].isNull()))&
                                                (GCP_cost_table['credits_2_type'].like('%DISCOUNT%')),GCP_cost_table['credits_2_amount'])
                                          .when((GCP_cost_table['credits_1_type'].like('%DISCOUNT%'))&
                                                (GCP_cost_table['credits_2_type'].like('%DISCOUNT%')),GCP_cost_table['credits_1_amount']+GCP_cost_table['credits_2_amount']))

GCP_cost_table = GCP_cost_table.withColumn("credits_promotion",when((GCP_cost_table['credits_1_type'].like('%PROMOTION%'))&
                                                                    ((~(GCP_cost_table['credits_2_type'].like('%PROMOTION%')))|
                                                                     (GCP_cost_table['credits_2_type'].isNull())),GCP_cost_table['credits_1_amount'])
                                          .when(((~(GCP_cost_table['credits_1_type'].like('%PROMOTION%')))
                                                |(GCP_cost_table['credits_1_type'].isNull()))&
                                                (GCP_cost_table['credits_2_type'].like('%PROMOTION%')),GCP_cost_table['credits_2_amount'])
                                          .when((GCP_cost_table['credits_1_type'].like('%PROMOTION%'))&
                                                (GCP_cost_table['credits_2_type'].like('%PROMOTION%')),GCP_cost_table['credits_1_amount']+GCP_cost_table['credits_2_amount']))
GCP_cost_table = GCP_cost_table.na.fill(value=0,subset=["credits_discounts",'credits_promotion'])


# In[163]:


GCP_cost_table = GCP_cost_table.withColumn("processing_cost",when((~(GCP_cost_table['sku_description'].like('%SSD%'))),GCP_cost_table['cost']))
GCP_cost_table = GCP_cost_table.withColumn('processing_after_discount', GCP_cost_table['processing_cost'] + GCP_cost_table['credits_discounts'])


# In[164]:


GCP_cost_table=GCP_cost_table.filter((~(GCP_cost_table['sku_description'].like('%SSD%'))))
GCP_cost_table.printSchema()


# In[165]:


GCP_cost_table_agg = GCP_cost_table.groupBy(['usage_start_time'])                        .agg(_sum('processing_cost').alias('total_processing_cost'),                         _sum('processing_after_discount').alias('total_processing_after_discount'),                         _sum('credits_promotion').alias('promotion_credits'))


# In[166]:


#GCP_cost_table_agg.show()


# ### Cost per Hour assigned

# In[167]:


#df_r1 = pd.merge(df_events_classif_Agg_hour,GCP_cost_table_agg[['usage_start_time','total_processing_cost','total_processing_discount','total_processing_after_discount']] , how = 'inner',left_on ='datetime_h', right_on = 'usage_start_time')
df_r1 = df_events_classif_Agg_hour.join(GCP_cost_table_agg,df_events_classif_Agg_hour.datetime_h ==  GCP_cost_table_agg.usage_start_time,"inner")


# In[168]:


#df_r1.loc[:,'Cost_per_1000_blink_after_discount']=df_r1['total_processing_after_discount']/(df_r1['mojix_blink_count']/1000)
#df_r1.loc[:,'Cost_per_1000_blink']=df_r1['total_processing_cost']/(df_r1['mojix_blink_count']/1000)

df_r1 = df_r1.withColumn("Cost_per_1000_blink_after_discount",df_r1['total_processing_after_discount']/(df_r1['mojix_blink_count']/1000))
df_r1 = df_r1.withColumn("Cost_per_1000_blink",df_r1['total_processing_cost']/(df_r1['mojix_blink_count']/1000))


# In[169]:


#df_results_1 = pd.merge(df_events_classif,df_r1[['datetime_h','Cost_per_1000_blink_after_discount','Cost_per_1000_blink']] , how = 'inner',on ='datetime_h')

df_results_1 = df_events_classif.join(df_r1.select(col('datetime_h'),col('Cost_per_1000_blink_after_discount'),col('Cost_per_1000_blink')),'datetime_h' ,"inner")


# In[170]:


#df_results_1.loc[:,'GCP_processing_cost']=(df_results_1['mojix_blink_count']/1000)*df_results_1['Cost_per_1000_blink']
#df_results_1.loc[:,'GCP_processing_cost_after_discount']=(df_results_1['mojix_blink_count']/1000)*df_results_1['Cost_per_1000_blink_after_discount']

df_results_1 = df_results_1.withColumn("GCP_processing_cost",(df_results_1['mojix_blink_count']/1000)*df_results_1['Cost_per_1000_blink'])
df_results_1 = df_results_1.withColumn("GCP_processing_cost_after_discount",(df_results_1['mojix_blink_count']/1000)*df_results_1['Cost_per_1000_blink_after_discount'])


# In[171]:


#df_results_1.loc[:,'datetime_h_LA']= df_results_1['datetime_h'].dt.tz_convert('America/Los_Angeles').dt.tz_localize(None)
df_results_1 = df_results_1.withColumn('datetime_h_LA', from_utc_timestamp(col("datetime_h"),"America/Los_Angeles"))


# In[172]:


#df_results_1.show()


# In[173]:


#bucket = "finops-outputs"
#spark.conf.set('temporaryGcsBucket', bucket)

#print("Write data in Big Query Tables")

#df_results_1.write.format('bigquery') \
#    .option('table', 'saas-analytics-io.processed.yt1_event_classify_GCP_cost_assignment_v2') \
#    .mode('append') \
#    .save()


# In[174]:

print ('YT1 df_results_1 schema:')
df_results_1.printSchema()


# In[175]:


df_results_1 = df_results_1[['datetime_h',
 'tenantCode',
 'thingType',
 'specName',
 'bridgeKey_2',
 'source',
 'sourceModule',
 'Retail_Bizstep',
 'FeatureSet',
 'Process',
 'SubProcess',
 'EventSource',
 'StoreCode',
 'mojix_blink_count',
 'Is_InternalEvent',
 'Cost_per_1000_blink_after_discount',
 'Cost_per_1000_blink',
 'GCP_processing_cost',
 'GCP_processing_cost_after_discount',
 'datetime_h_LA']].toPandas()


# In[176]:


table = "saas-analytics-io.processed.yt1_event_classify_GCP_cost_assignment"
df_results_1.to_gbq(table, table_schema=[
                              {'name': 'datetime_h','type': 'TIMESTAMP'},
                              {'name': 'tenantCode','type': 'STRING'},
                              {'name': 'thingType','type': 'STRING'},
                              {'name': 'specName','type': 'STRING'},
                              {'name': 'bridgeKey_2','type': 'STRING'},
                              {'name': 'source','type': 'STRING'},
                              {'name': 'sourceModule','type': 'STRING'},
                              {'name': 'Retail_Bizstep','type': 'STRING'},
                              {'name': 'FeatureSet','type': 'STRING'},
                              {'name': 'Process','type': 'STRING'},
                              {'name': 'SubProcess','type': 'STRING'},
                              {'name': 'EventSource','type': 'STRING'},
                              {'name': 'StoreCode','type': 'STRING'},
                              {'name': 'mojix_blink_count','type': 'INTEGER'},
                              {'name': 'Is_InternalEvent','type': 'STRING'},
                              {'name': 'Cost_per_1000_blink_after_discount','type': 'FLOAT'},
                              {'name': 'Cost_per_1000_blink','type': 'FLOAT'},
                              {'name': 'GCP_processing_cost','type': 'FLOAT'},
                              {'name': 'GCP_processing_cost_after_discount','type': 'FLOAT'},
                              {'name': 'datetime_h_LA','type': 'DATETIME'},
                              {"name":"promotion_credits" , "type" : "FLOAT"},
                              ]
                   , if_exists='append')


# ## VT1 COST ALLOCATION

# ### VT1 Events extraction

# In[181]:


df_events_classif_vt1 = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','processed.vt1_event_classify_finops').option("filter", """date(datetime_h) = '%s' and time(datetime_h)
between '%s' and '%s'"""%(str_day,hour_ini_1,hour_fin_1)).load()

#df_events_classif_vt1 = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','processed.vt1_event_classify_finops').option("filter", """date(datetime_h)
 #   between '2023-04-02' and '2023-04-15'""").load()


# In[182]:


#df_events_classif_Agg_hour_vt1 = df_events_classif_vt1.groupby('datetime_h').sum().reset_index()

df_events_classif_Agg_hour_vt1 = df_events_classif_vt1.groupBy('datetime_h').agg(_sum('mojix_blink_count').alias('mojix_blink_count'))


# ### GCP VT1 Cost Extraction

# In[195]:


df_GCP_vt1 = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','finance.gcp_billing_export_v1_01FE23_B1D2D8_10D34D').option("filter","JSON_VALUE(project.number)= '884190693971' and usage_start_time between '%s %s' and '%s %s' "%(str_day,hour_ini_1,str_day,hour_fin_1)).load()


#df_GCP_vt1 = spark.read.format('bigquery')\
#.option('project','saas-analytics-io')\
#.option('table','finance.gcp_billing_export_v1_01FE23_B1D2D8_10D34D')\
#.option("filter","JSON_VALUE(project.number)= '884190693971' and date(usage_start_time) between '2023-04-02' and '2023-04-15'")\
#.load()


# In[196]:


df_GCP_vt1 = df_GCP_vt1.select(col("service.description").alias('service_description'),
                                          col("sku.description").alias('sku_description'),
                                          col("usage_start_time"),
                                          col("project.id").alias('id_project'),
                                          col("cost"),
                                          col("usage.amount").alias('usage_amount'),
                                          col("usage.unit").alias('usage_unit'),
                                          col("credits")[0].alias('credits_1'),
                                          col("credits")[1].alias('credits_2'),
                                          col("cost_type")
                                         )   

df_GCP_vt1 = df_GCP_vt1.select(col('service_description'),
                                          col('sku_description'),
                                          col("usage_start_time"),
                                          col('id_project'),
                                          col("cost"),
                                          col('usage_amount'),
                                          col('usage_unit'),
                                          col("credits_1.name").alias('credits_1_name'),
                                          col("credits_1.amount").alias('credits_1_amount'),
                                          col("credits_1.type").alias('credits_1_type'),
                                          col("credits_2.name").alias('credits_2_name'),
                                          col("credits_2.amount").alias('credits_2_amount'),
                                          col("credits_2.type").alias('credits_2_type'),
                                          col("cost_type")
                                         ) 
#df_GCP_vt1.show()


# In[197]:


df_GCP_vt1 = df_GCP_vt1.withColumn("credits_discounts",when((df_GCP_vt1['credits_1_type'].like('%DISCOUNT%'))&
                                                                    ((~(df_GCP_vt1['credits_2_type'].like('%DISCOUNT%')))|
                                                                     (df_GCP_vt1['credits_2_type'].isNull())),df_GCP_vt1['credits_1_amount'])
                                          .when(((~(df_GCP_vt1['credits_1_type'].like('%DISCOUNT%')))
                                                |(df_GCP_vt1['credits_1_type'].isNull()))&
                                                (df_GCP_vt1['credits_2_type'].like('%DISCOUNT%')),df_GCP_vt1['credits_2_amount'])
                                          .when((df_GCP_vt1['credits_1_type'].like('%DISCOUNT%'))&
                                                (df_GCP_vt1['credits_2_type'].like('%DISCOUNT%')),df_GCP_vt1['credits_1_amount']+df_GCP_vt1['credits_2_amount']))

df_GCP_vt1 = df_GCP_vt1.withColumn("credits_promotion",when((df_GCP_vt1['credits_1_type'].like('%PROMOTION%'))&
                                                                    ((~(df_GCP_vt1['credits_2_type'].like('%PROMOTION%')))|
                                                                     (df_GCP_vt1['credits_2_type'].isNull())),df_GCP_vt1['credits_1_amount'])
                                          .when(((~(df_GCP_vt1['credits_1_type'].like('%PROMOTION%')))
                                                |(df_GCP_vt1['credits_1_type'].isNull()))&
                                                (df_GCP_vt1['credits_2_type'].like('%PROMOTION%')),df_GCP_vt1['credits_2_amount'])
                                          .when((df_GCP_vt1['credits_1_type'].like('%PROMOTION%'))&
                                                (df_GCP_vt1['credits_2_type'].like('%PROMOTION%')),df_GCP_vt1['credits_1_amount']+df_GCP_vt1['credits_2_amount']))
df_GCP_vt1 = df_GCP_vt1.na.fill(value=0,subset=["credits_discounts",'credits_promotion'])


# In[198]:


df_GCP_vt1 = df_GCP_vt1.withColumn("processing_cost",when((~(df_GCP_vt1['sku_description'].like('%SSD%'))),df_GCP_vt1['cost']))
df_GCP_vt1 = df_GCP_vt1.withColumn('processing_after_discount', df_GCP_vt1['processing_cost'] + df_GCP_vt1['credits_discounts'])


# In[200]:


df_GCP_vt1 = df_GCP_vt1.filter((~(df_GCP_vt1['sku_description'].like('%SSD%'))))
df_GCP_vt1.printSchema()


# In[201]:


df_GCP_vt1_agg = df_GCP_vt1.groupBy(['usage_start_time'])                        .agg(_sum('processing_cost').alias('total_processing_cost'),                         _sum('processing_after_discount').alias('total_processing_after_discount'),                         _sum('credits_promotion').alias('promotion_credits'))


# In[202]:


#df_r1 = pd.merge(df_events_classif_Agg_hour,GCP_cost_table_agg[['usage_start_time','total_processing_cost','total_processing_discount','total_processing_after_discount']] , how = 'inner',left_on ='datetime_h', right_on = 'usage_start_time')
df_r1 = df_events_classif_Agg_hour_vt1.join(df_GCP_vt1_agg,df_events_classif_Agg_hour_vt1.datetime_h ==  df_GCP_vt1_agg.usage_start_time,"inner")


# In[203]:


#df_r1.loc[:,'Cost_per_1000_blink_after_discount']=df_r1['total_processing_after_discount']/(df_r1['mojix_blink_count']/1000)
#df_r1.loc[:,'Cost_per_1000_blink']=df_r1['total_processing_cost']/(df_r1['mojix_blink_count']/1000)

df_r1 = df_r1.withColumn("Cost_per_1000_blink_after_discount",df_r1['total_processing_after_discount']/(df_r1['mojix_blink_count']/1000))
df_r1 = df_r1.withColumn("Cost_per_1000_blink",df_r1['total_processing_cost']/(df_r1['mojix_blink_count']/1000))


# In[204]:


#df_results_1 = pd.merge(df_events_classif_vt1,df_r1[['datetime_h','Cost_per_1000_blink_after_discount','Cost_per_1000_blink']] , how = 'inner',on ='datetime_h')

df_results_1 = df_events_classif_vt1.join(df_r1.select(col('datetime_h'),col('Cost_per_1000_blink_after_discount'),col('Cost_per_1000_blink')),'datetime_h' ,"inner")


# In[205]:


#df_results_1.loc[:,'GCP_processing_cost']=(df_results_1['mojix_blink_count']/1000)*df_results_1['Cost_per_1000_blink']
#df_results_1.loc[:,'GCP_processing_cost_after_discount']=(df_results_1['mojix_blink_count']/1000)*df_results_1['Cost_per_1000_blink_after_discount']

df_results_1 = df_results_1.withColumn("GCP_processing_cost",(df_results_1['mojix_blink_count']/1000)*df_results_1['Cost_per_1000_blink'])
df_results_1 = df_results_1.withColumn("GCP_processing_cost_after_discount",(df_results_1['mojix_blink_count']/1000)*df_results_1['Cost_per_1000_blink_after_discount'])


# In[206]:


#df_results_1.loc[:,'datetime_h_LA']= df_results_1['datetime_h'].dt.tz_convert('America/Los_Angeles').dt.tz_localize(None)
df_results_1 = df_results_1.withColumn('datetime_h_LA', from_utc_timestamp(col("datetime_h"),"America/Los_Angeles"))


# In[207]:


#bucket = "finops-outputs"
#spark.conf.set('temporaryGcsBucket', bucket)
#
#print("Write data in Big Query Tables")
#
#df_results_1.write.format('bigquery') \
#    .option('table', 'saas-analytics-io.processed.yt1_event_classify_GCP_cost_assignment_v2') \
#    .mode('append') \
#    .save()


# In[208]:
print ('VT1 df_results_1 schema:')
df_results_1.printSchema()

df_results_1 = df_results_1[['datetime_h',
 'tenantCode',
 'thingType',
 'specName',
 'bridgeKey_2',
 'source',
 'FeatureSet',
 'Process',
 'SubProcess',
 'EventSource',
 'StoreCode',
 'mojix_blink_count',
 'Is_InternalEvent',
 'Cost_per_1000_blink_after_discount',
 'Cost_per_1000_blink',
 'GCP_processing_cost',
 'GCP_processing_cost_after_discount',
 'datetime_h_LA']].toPandas()


# In[209]:


table = "saas-analytics-io.processed.vt1_event_classify_GCP_cost_assignment"
df_results_1.to_gbq(table, table_schema=[
                               {'name': 'datetime_h','type': 'TIMESTAMP'},
                               {'name': 'tenantCode','type': 'STRING'},
                               {'name': 'thingType','type': 'STRING'},
                               {'name': 'specName','type': 'STRING'},
                               {'name': 'bridgeKey_2','type': 'STRING'},
                               {'name': 'source','type': 'STRING'},
                               {'name': 'FeatureSet','type': 'STRING'},
                               {'name': 'Process','type': 'STRING'},
                               {'name': 'SubProcess','type': 'STRING'},
                               {'name': 'EventSource','type': 'STRING'},
                               {'name': 'StoreCode','type': 'STRING'},
                               {'name': 'Is_InternalEvent','type': 'STRING'},
                                {'name': 'mojix_blink_count','type': 'INTEGER'},
                               {'name': 'Cost_per_1000_blink_after_discount','type': 'FLOAT'},
                               {'name': 'Cost_per_1000_blink','type': 'FLOAT'},
                               {'name': 'GCP_processing_cost','type': 'FLOAT'},
                               {'name': 'GCP_processing_cost_after_discount','type': 'FLOAT'},
                               {'name': 'GCP_storage_cost','type': 'FLOAT'},
                               {'name': 'GCP_storage_cost_after_discount','type': 'FLOAT'},
                               {'name': 'datetime_h_LA','type': 'DATETIME'},
                               {"name":"promotion_credits" , "type" : "FLOAT"},
                               ]
                    , if_exists='append')


# ## ALL CLOUD COST

# In[31]:


url_1 = 'https://docs.google.com/spreadsheets/d/1VQDDFfxIvSb0WAokWIxclANmWXgsy0rvTaEDuGwHo3g/edit#gid=373126690'
url = url_1.replace('/edit#gid=', '/export?format=csv&gid=')
df_test = pd.read_csv(url, dtype=str)
df_test = spark.createDataFrame(df_test.astype(str)) 


# In[32]:


#df_test.show()


# In[75]:


df_GCP_all = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','finance.gcp_billing_export_v1_01FE23_B1D2D8_10D34D').option("filter","usage_start_time between '%s %s' and '%s %s' "%(str_day,hour_ini_1,str_day,hour_fin_1)).load()

#df_GCP_all = spark.read.format('bigquery')\
#.option('project','saas-analytics-io')\
#.option('table','finance.gcp_billing_export_v1_01FE23_B1D2D8_10D34D')\
#.option("filter","date(usage_start_time) between '2022-07-01' and '2023-04-12'")\
#.load()


# In[78]:


df_GCP_all = df_GCP_all.select(col("service.description").alias('description_service'),
                                          col("sku.description").alias('description_sku'),
                                          col("usage_start_time"),
                                          col("project.id").alias('id_project'),
                                          col("cost"),
                                          col("usage.amount").alias('usage_amount'),
                                          col("usage.unit").alias('unit'),
                                          col("credits")[0].alias('credits_1'),
                                          col("credits")[1].alias('credits_2'),
                                          col("cost_type")
                                         )   

df_GCP_all = df_GCP_all.select(col('description_service'),
                                          col('description_sku'),
                                          col("usage_start_time"),
                                          col('id_project'),
                                          col("cost"),
                                          col('usage_amount'),
                                          col('unit'),
                                          col("credits_1.name").alias('credits_1_name'),
                                          col("credits_1.amount").alias('credits_1_amount'),
                                          col("credits_1.type").alias('credits_1_type'),
                                          col("credits_2.name").alias('credits_2_name'),
                                          col("credits_2.amount").alias('credits_2_amount'),
                                          col("credits_2.type").alias('credits_2_type'),
                                          col("cost_type")
                                         ) 
#df_GCP_vt1.show()


# In[79]:


#df_GCP_all.show()


# In[80]:


df_GCP_all = df_GCP_all.withColumn("credits_discounts",when((df_GCP_all['credits_1_type'].like('%DISCOUNT%'))&
                                                                    ((~(df_GCP_all['credits_2_type'].like('%DISCOUNT%')))|
                                                                     (df_GCP_all['credits_2_type'].isNull())),df_GCP_all['credits_1_amount'])
                                          .when(((~(df_GCP_all['credits_1_type'].like('%DISCOUNT%')))
                                                |(df_GCP_all['credits_1_type'].isNull()))&
                                                (df_GCP_all['credits_2_type'].like('%DISCOUNT%')),df_GCP_all['credits_2_amount'])
                                          .when((df_GCP_all['credits_1_type'].like('%DISCOUNT%'))&
                                                (df_GCP_all['credits_2_type'].like('%DISCOUNT%')),df_GCP_all['credits_1_amount']+df_GCP_all['credits_2_amount']))

df_GCP_all = df_GCP_all.withColumn("credits_promotion",when((df_GCP_all['credits_1_type'].like('%PROMOTION%'))&
                                                                    ((~(df_GCP_all['credits_2_type'].like('%PROMOTION%')))|
                                                                     (df_GCP_all['credits_2_type'].isNull())),df_GCP_all['credits_1_amount'])
                                          .when(((~(df_GCP_all['credits_1_type'].like('%PROMOTION%')))
                                                |(df_GCP_all['credits_1_type'].isNull()))&
                                                (df_GCP_all['credits_2_type'].like('%PROMOTION%')),df_GCP_all['credits_2_amount'])
                                          .when((df_GCP_all['credits_1_type'].like('%PROMOTION%'))&
                                                (df_GCP_all['credits_2_type'].like('%PROMOTION%')),df_GCP_all['credits_1_amount']+df_GCP_all['credits_2_amount']))
df_GCP_all = df_GCP_all.na.fill(value=0,subset=["credits_discounts",'credits_promotion'])


# In[67]:


#df_GCP_all.show()


# In[82]:


df_GCP_all = df_GCP_all.withColumn("processing_cost",when((~(df_GCP_all['description_sku'].like('%SSD%'))),df_GCP_all['cost']))
df_GCP_all = df_GCP_all.withColumn("storage_cost",when((df_GCP_all['description_sku'].like('%SSD%')),df_GCP_all['cost']))


df_GCP_all = df_GCP_all.withColumn("total_after_discounts",df_GCP_all['cost'] + df_GCP_all['credits_discounts'])
df_GCP_all = df_GCP_all.withColumn("amount_credits",df_GCP_all['credits_discounts'])
df_GCP_all = df_GCP_all.withColumn("processing_discount",when((~(df_GCP_all['description_sku'].like('%SSD%'))),df_GCP_all['credits_discounts']))
df_GCP_all = df_GCP_all.withColumn("storage_discount",when((df_GCP_all['description_sku'].like('%SSD%')),df_GCP_all['credits_discounts']))
df_GCP_all = df_GCP_all.withColumn("total_processing_after_discount",when((~(df_GCP_all['description_sku'].like('%SSD%'))),df_GCP_all['credits_discounts']))
df_GCP_all = df_GCP_all.withColumn("promotion_credits",df_GCP_all['credits_promotion'])


df_GCP_all = df_GCP_all.withColumn("id_project",when((df_GCP_all['description_service']=='Lacework for Google Cloud'),'saas-laceworks')
                                                     .otherwise(df_GCP_all['id_project']))


df_GCP_all_Agg = df_GCP_all.groupBy(['usage_start_time','id_project',
                               'description_service','description_sku','unit'])\
                        .agg(_sum('cost').alias('Total_cost'), \
                        _sum('usage_amount').alias('Total_amount'), \
                        _sum('total_after_discounts').alias('total_after_discounts'), \
                        _sum('amount_credits').alias('Total_amount_credits'), \
                        _sum('processing_cost').alias('total_processing_cost'), \
                        _sum('storage_cost').alias('Total_storage_cost'), \
                        _sum('promotion_credits').alias('total_promotion_credits'))


df_GCP_all_Agg = df_GCP_all_Agg.na.fill(value=0,subset=["usage_start_time",'id_project','description_service','description_sku','unit'])


df_GCP_all_Agg = df_GCP_all_Agg.join(df_test.select(col('id'),
                                                        col('provider'),
                                                        col('tier'),
                                                        col('bu'),
                                                        col('costType'),
                                                        col('category'),
                                                        col('subCategory'),
                                                        col('EntityFinance'),
                                                        col('CategoryFinance'),
                                                        col('tenantFinance'),
                                                        col('ResourceGr'),
                                                        col('unitFinance'),
                                                      col('Class')),df_GCP_all_Agg.id_project == df_test.id,"Left")

df_GCP_all_Agg = df_GCP_all_Agg.withColumn('usage_start_time_LA', from_utc_timestamp(col("usage_start_time"),"America/Los_Angeles"))


# In[83]:


df_GCP_all_Agg = df_GCP_all_Agg[['usage_start_time','id_project','description_service','description_sku','unit','Total_cost','Total_amount','total_after_discounts','Total_amount_credits'
                                 ,'total_processing_cost','Total_storage_cost','id','provider','tier','bu','costType','category','subCategory','usage_start_time_LA','total_promotion_credits'
                                 ,'EntityFinance','CategoryFinance','tenantFinance','ResourceGr','unitFinance','Class']]


# In[84]:


df_GCP_all_Agg.printSchema()


# In[85]:


df_GCP_all_Agg = df_GCP_all_Agg.toPandas()


# In[86]:


#df_GCP_all_Agg.dtypes


# In[88]:


table = "saas-analytics-io.processed.gcp_all_projects_classify_v3"


# In[89]:


df_GCP_all_Agg.to_gbq(table, table_schema= [
      {"name":"usage_start_time", "type" : "TIMESTAMP"},
      {"name":"id_project" , "type" : "STRING"},
      {"name":"description_service" , "type" : "STRING"},
      {"name":"description_sku" , "type" : "STRING"} ,
      {"name":"unit" , "type" : "STRING"},
      {"name":"Total_cost" , "type" : "FLOAT"},
      {"name":"Total_amount" , "type" : "FLOAT"},
      {"name":"total_after_discounts" , "type" : "FLOAT"},
      {"name":"Total_amount_credits", "type" : "FLOAT"},
      {"name":"total_processing_cost" , "type" : "FLOAT"},
      {"name":"Total_storage_cost" , "type" : "FLOAT"},
      {"name":"id" , "type" : "STRING"},
      {"name":"provider" , "type" : "STRING"},
      {"name":"tier" , "type" : "STRING"} ,
      {"name":"bu" , "type" : "STRING"} ,
      {"name":"costType" , "type" : "STRING"},
      {"name":"category" , "type" : "STRING"} ,
      {"name":"subCategory" , "type" : "STRING"},
      {"name":"usage_start_time_LA", "type" : "DATETIME"},
      {"name":"total_promotion_credits" , "type" : "FLOAT"},
      {"name":"EntityFinance" , "type" : "STRING"},
      {"name":"CategoryFinance" , "type" : "STRING"},
      {"name":"tenantFinance" , "type" : "STRING"},
      {"name":"ResourceGr" , "type" : "STRING"},
      {"name":"unitFinance" , "type" : "STRING"},
      {"name":"Class" , "type" : "STRING"}
],  if_exists='append')


# In[44]:


#bucket = "finops-outputs"
#spark.conf.set('temporaryGcsBucket', bucket)
#
#print("Write data in Big Query Tables")
#
#df_GCP_all_Agg.write.format('bigquery') \
#    .option('table', table) \
#    .mode('append') \
#    .save()


# In[ ]:




