#!/usr/bin/env python
# coding: utf-8

from google.cloud import bigquery
import pandas as pd
import datetime
import re
import json
from datetime import date, timedelta
from datetime import datetime
import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr,first, last,when, split, col,lit, concat, date_format,to_utc_timestamp,to_timestamp, regexp_replace
client = bigquery.Client(location="us-central1")
print("Client creating using default project: {}".format(client.project))


spark = SparkSession.builder.appName("yt1EventsClassify").getOrCreate()


current_date = datetime.today()
dias_atras = 2
#dias_atras = int(dias_atras)
str_day = (current_date - timedelta(days = dias_atras)).strftime("%Y-%m-%d")
#STR_YEARMONTH = (current_date - timedelta(days = dias_atras)).strftime("%Y%m")
#str_day = "2022-04-16"
#STR_YEARMONTH = "201908"
hour_ini_1 = '00:00:00'
hour_fin_1 = '23:59:59'
print(str_day,hour_ini_1,hour_fin_1)
output = 'All'
print(output)


# ### Read Events Metadata

#df_metadata = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','yt1_v1data1.metadata').option("filter", """date(time) = '%s' and time(time)
 #   between '%s' and '%s'"""%(str_day,hour_ini_1,hour_fin_1)).load()
df_metadata = spark.read.format('bigquery').option('project','saas-mojixretail-io').option('table','silver.kafka_metadata').option("filter", """date(time) = '%s' and time(time)
   between '%s' and '%s'"""%(str_day,hour_ini_1,hour_fin_1)).load()


df_metadata.printSchema()


# ### Read Events UDF (Filtered for FinOps)

# In[53]:


#df_udf_test = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','processed.udf_filter_finops').option("filter", """date(time) = '%s' and time(time)
 #   between '%s' and '%s'"""%(str_day,hour_ini_1,hour_fin_1)).load()
df_udf_test = spark.read.format('bigquery').option('project','saas-mojixretail-io').option('table','silver.finops_kafka_udf_filtered').option("filter", """date(time) = '%s' and time(time)
    between '%s' and '%s'"""%(str_day,hour_ini_1,hour_fin_1)).load()


# In[54]:


print("Total Registros udf_filter_finops ", df_udf_test.count())

# ### Read SOH events AGG from bigquery 

df_soh_agg = spark.read.format('bigquery').option('project','saas-mojixretail-io').option('table','silver.finops_soh_agg').option("filter", """date(datetime_h) = '%s' and time(datetime_h)
    between '%s' and '%s'"""%(str_day,hour_ini_1,hour_fin_1)).load()

print("Total Registros SOH events ", df_soh_agg.count())


# ### Read REFLIST Source

# In[55]:


#df_reflist = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','processed.yt1_rfl_data').option("filter", """date(time) = '%s' and time(time)
    #between '%s' and '%s'and key ='bizLocation'"""%(str_day,hour_ini_1,hour_fin_1)).load()
df_reflist = spark.read.format('bigquery').option('project','saas-mojixretail-io').option('table','silver.finops_rfl_data').option("filter", """date(time) = '%s' and time(time)
    between '%s' and '%s'and key ='bizLocation'"""%(str_day,hour_ini_1,hour_fin_1)).load()


# ### Read Sites Catalogue

# In[56]:


df_sites = spark.read.format('bigquery').option('project','saas-mojixretail-io').option('table','silver.locations').option("filter", """ level ='premise' """).load()


# ### Data Prepairing

# In[57]:


#### Metadata Prepairing
df_metadata = df_metadata.withColumn('bridgeKey_2',when((df_metadata["bridgeKey"]=="/SERVICES"), split(df_metadata["bridgeKey"],"/").getItem(1))
                                     .when((df_metadata["bridgeKey"]!='/SERVICES'), split(df_metadata["bridgeKey"],"/").getItem(2)))
df_metadata = df_metadata.withColumn('id_serial_number',concat(df_metadata['id'],lit('-'), df_metadata['serialNumber']))

df_metadata = df_metadata.withColumn('datetime_h', date_format(df_metadata['time'], "d/M/y H"))
df_metadata = df_metadata.withColumn('datetime_h', to_utc_timestamp(to_timestamp(df_metadata['datetime_h'],'d/M/y H'), 'UTC'))


# In[58]:


print("Total Registros metadata ", df_metadata.count())
print("Total Registros unicos ", df_metadata.distinct().count())


# In[59]:


##### UDF Prepairing
df_udf_test = df_udf_test.withColumn('id_serial_number',concat(df_udf_test['metadataId'],lit('-'), df_udf_test['serialNumber']))
df_udf_test = df_udf_test.distinct()


# In[60]:


#df_udf_test.count()


# In[61]:


df_pivot = df_udf_test.groupBy("id_serial_number").pivot("key").agg(first("value")).fillna('N/A')


# In[62]:


df_pivot.printSchema()
#display(df_pivot)


# In[63]:


#### Merge UDF With Metadata (Spark)
df_full = df_metadata.join(df_pivot,df_metadata.id_serial_number ==  df_pivot.id_serial_number,"outer")
df_full = df_full.fillna('N/A')


# In[64]:


#display(df_full[(df_full['specName']=='POE_IINTERFACE')])


# In[65]:


df_full.printSchema()


# In[66]:


fields_string =' '.join(map(str,df_full.schema.fields))
if 'transactionId' in fields_string:
    print(1)
else:
    print('no transactionId, but was added')
    df_full = df_full.withColumn('transactionId',lit('N/A'))

if 'Retail_SOHStoreNumber' in fields_string:
    print(2)
else:
    print('no Retail_SOHStoreNumber, but was added')
    df_full = df_full.withColumn('Retail_SOHStoreNumber',lit('N/A'))

if 'doorEvent' in fields_string:
    print(3)
else:
    print('no doorEvent, but was added')
    df_full = df_full.withColumn('doorEvent',lit('N/A'))

if 'Retail_StoreNumber' in fields_string:
    print(3)
else:
    print('no Retail_StoreNumber, but was added')
    df_full = df_full.withColumn('Retail_StoreNumber',lit('N/A'))


# In[67]:


df_full.printSchema()


# In[68]:


#Reflist bizLocation Preparation
df_agg_reflist = df_reflist.groupBy(['serialNumber','tenantCode','value', 'rflSiteCode']).count()
df_agg_reflist = df_agg_reflist.withColumnRenamed("value","bizLocation_rfl")
df_agg_reflist = df_agg_reflist.withColumnRenamed("serialNumber","serialNumber_rfl")
df_agg_reflist = df_agg_reflist[['serialNumber_rfl','bizLocation_rfl','rflSiteCode']]
#CONTENT JOIN with REFLIST
df_full = df_full.withColumn('referenceListSerial1',when((df_full.thingType=='CONTENT')&(df_full.transactionId=='unlink'), split(df_full['serialNumber'],"-").getItem(3))
                            .when((df_full.thingType=='CONTENT')&(df_full.transactionId!='unlink'), df_full['referenceListSerial']))
df_full = df_full.join(df_agg_reflist,df_full.referenceListSerial1 == df_agg_reflist.serialNumber_rfl,"left")
#Rename SiteCode
df_full = df_full.withColumnRenamed("rflSiteCode","premise_from_biz")


# In[69]:


#display(df_agg_reflist[df_agg_reflist['serialNumber_rfl']=='RFL7231872985653766'])
#display(df_agg_reflist)
#display(df_full[df_full['thingType']=='CONTENT'])


# In[70]:


#Get PremiseCode from BizLocation
#df_sites1 = df_sites[['tenant','code','bizLocation']]
#df_sites1 = df_sites1.dropDuplicates()
#df_sites1 = df_sites1.withColumnRenamed("code","premise_from_biz")
#df_sites1 = df_sites1.withColumnRenamed("bizLocation","bizLocation_1")
#df_full = df_full.join(df_sites1,(df_full.tenantCode == df_sites1.tenant)&(df_full.bizLocation_rfl == df_sites1.bizLocation_1),"left")
#df_full = df_full.drop('tenant','bizLocation_1')


# In[71]:


#Get PremiseCode from Fixture
#df_sites2 = df_sites[['tenant','fixture','premise']]
#df_sites2 = df_sites2.dropDuplicates()
#df_sites2 = df_sites2.withColumnRenamed("premise","premise_from_fixture")
#df_sites2 = df_sites2.withColumnRenamed("fixture","zone_1")
#df_full = df_full.join(df_sites2,(df_full.tenantCode == df_sites2.tenant)&(df_full.zone == df_sites2.zone_1),"left")
#df_full = df_full.drop('tenant','zone_1')
df_full = df_full.withColumn("premise_from_fixture", lit(""))


# In[72]:


df_full.printSchema()


# In[73]:


#display(df_full)


# #### FeatureSet Rules

# In[74]:


df_full = df_full.withColumn('FeatureSet',when((df_full.thingType=='CONTENT')&(df_full.specName=='SERVICES')&(df_full.bridgeKey_2=='SERVICES')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A')&(df_full.transactionId=='unlink'), 'SUPPLY CHAIN')
.when((df_full.thingType=='DISCREPANCY')&(df_full.specName=='')&(df_full.bridgeKey_2=='ANALYTICS')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='DEMO: SALAD Demo')&(df_full.sourceModule=='COMMISSIONING')&(df_full.Retail_Bizstep=='COMMISSIONING'), 'SOURCING')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='DEMO: SALAD Demo')&(df_full.sourceModule=='PACKING')&(df_full.Retail_Bizstep=='PACKING'), 'SUPPLY CHAIN')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='DEMO: SALAD Demo')&(df_full.sourceModule=='encodeTags')&(df_full.Retail_Bizstep=='ENCODING'), 'SOURCING')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='ENCODING')&(df_full.Retail_Bizstep=='ENCODING'), 'SOURCING')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='INSPECTING')&(df_full.Retail_Bizstep=='INSPECTING'), 'SUPPLY CHAIN')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='PACKING')&(df_full.Retail_Bizstep=='PACKING'), 'SUPPLY CHAIN')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='RECEIVING')&(df_full.Retail_Bizstep=='RECEIVING'), 'SUPPLY CHAIN')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='RETAIL_SELLING')&(df_full.Retail_Bizstep=='RETAIL_SELLING'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='SEARCHING')&(df_full.Retail_Bizstep=='SEARCHING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='SHIPPING')&(df_full.Retail_Bizstep=='SHIPPING'), 'SUPPLY CHAIN')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='CYCLE_COUNTING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='ENCODING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='MISSING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='PICKING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='RECEIVING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='REMOVING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='SHIPPING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='cycleCount')&(df_full.Retail_Bizstep=='CYCLE_COUNTING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='encodeTags')&(df_full.Retail_Bizstep=='ENCODING'), 'SOURCING')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='Mission Produce')&(df_full.sourceModule=='COMMISSIONING')&(df_full.Retail_Bizstep=='COMMISSIONING'), 'SOURCING')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='SERIALIZATION')&(df_full.sourceModule=='SERIALIZATION')&(df_full.Retail_Bizstep=='SERIALIZING'), 'SOURCING')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='UPDATE_INVENTORY')&(df_full.sourceModule=='setToMissing')&(df_full.Retail_Bizstep=='MISSING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='')&(df_full.bridgeKey_2=='FLOW')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A')&(df_full.tenantCode=='PE'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='')&(df_full.bridgeKey_2=='MOBILE')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='')&(df_full.bridgeKey_2=='MOBILE')&(df_full.source=='MOBILE')&(df_full.sourceModule=='encodeTags')&(df_full.Retail_Bizstep=='N/A'), 'SOURCING')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='DEMO: SALAD Demo')&(df_full.sourceModule=='COMMISSIONING')&(df_full.Retail_Bizstep=='COMMISSIONING'), 'SOURCING')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='DEMO: SALAD Demo')&(df_full.sourceModule=='PACKING')&(df_full.Retail_Bizstep=='PACKING'), 'SUPPLY CHAIN')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='DEMO: SALAD Demo')&(df_full.sourceModule=='encodeTags')&(df_full.Retail_Bizstep=='ENCODING'), 'SOURCING')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='ENCODING')&(df_full.Retail_Bizstep=='ENCODING'), 'SOURCING')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='INSPECTING')&(df_full.Retail_Bizstep=='INSPECTING'), 'SUPPLY CHAIN')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='PACKING')&(df_full.Retail_Bizstep=='PACKING'), 'SUPPLY CHAIN')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='RECEIVING')&(df_full.Retail_Bizstep=='RECEIVING'), 'SUPPLY CHAIN')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='RETAIL_SELLING')&(df_full.Retail_Bizstep=='RETAIL_SELLING'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='SHIPPING')&(df_full.Retail_Bizstep=='SHIPPING'), 'SUPPLY CHAIN')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='CYCLE_COUNTING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='ENCODING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='MISSING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='PICKING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='RECEIVING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='REMOVING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='cycleCount')&(df_full.Retail_Bizstep=='CYCLE_COUNTING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='encodeTags')&(df_full.Retail_Bizstep=='ENCODING'), 'SOURCING')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='Mission Produce')&(df_full.sourceModule=='COMMISSIONING')&(df_full.Retail_Bizstep=='COMMISSIONING'), 'SOURCING')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='SERIALIZATION')&(df_full.sourceModule=='SERIALIZATION')&(df_full.Retail_Bizstep=='SERIALIZING'), 'SOURCING')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='UPDATE_INVENTORY')&(df_full.sourceModule=='setToMissing')&(df_full.Retail_Bizstep=='MISSING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_519')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_524')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_540')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_549')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_558')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_570')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_607')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_519')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_524')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_540')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_549')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_555')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_558')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_570')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_607')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix')&(df_full.bridgeKey_2=='ALEB_519')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix')&(df_full.bridgeKey_2=='ALEB_555')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='LOCATION')&(df_full.specName=='')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'CONFIGURATION')
.when((df_full.thingType=='PRODUCT')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGEPRODUCTS')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='SOH')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESOH')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='SOHSTREAM')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESOH')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_519')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_524')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_540')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_544')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_549')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_555')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_558')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_570')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_607')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='CONTENT')&(df_full.specName=='SERVICES')&(df_full.bridgeKey_2=='SERVICES')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A')&(df_full.transactionId!='unlink'), 'SUPPLY CHAIN')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='PICKING')&(df_full.Retail_Bizstep=='PICKING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='ENTERING_EXITING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='SHIPPING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='ENTERING_EXITING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='PICKING')&(df_full.Retail_Bizstep=='PICKING'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_555')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'SALES AND AFTER SALES')
.when((df_full.thingType=='ITEM')&(df_full.specName=='SCHEDULED')&(df_full.bridgeKey_2=='RULESET')&(df_full.source=='REP_398')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Loc_558_nightscan')&(df_full.bridgeKey_2=='ALEB_558')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Location_Spec_588')&(df_full.bridgeKey_2=='ALEB_558')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Spec')&(df_full.bridgeKey_2=='ALEB')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Spec')&(df_full.bridgeKey_2=='ALEB_524')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Spec')&(df_full.bridgeKey_2=='ALEB_544')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Spec')&(df_full.bridgeKey_2=='ALEB_607')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Spec')&(df_full.bridgeKey_2=='ALEB_549')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Spec')&(df_full.bridgeKey_2=='ALEB_570')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF558TEMPE')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF549DOLPHIN')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF510SAWGRASS')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF524ORLANDO1')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF543ORLANDO2')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF555ROSEMONT')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF540LASVEGAS2')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF544LASVEGAS1')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF607SANMARCOS')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF566BLOCKATORANG')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF570GRANDPRAIRIE')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF519JERSEYGARDENS')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'STOCK MANAGEMENT')
 .when((df_full['thingType']=='ITEM')&(df_full['specName']=='ytem_cloud')&(df_full['bridgeKey_2']=='ALEB_540')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'STOCK MANAGEMENT')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='SERVICES')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'STOCK MANAGEMENT')
.when((df_full['thingType']=='CONTENT')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A')&(df_full['transactionId']=='unlink'), 'SUPPLY CHAIN')
.when((df_full['thingType']=='CONTENT')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A')&(df_full['transactionId']!='unlink'), 'SUPPLY CHAIN')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'), 'STOCK MANAGEMENT')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0077013000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='008195000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='AD')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='QATEST')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000509')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000226')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000172')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000486')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0896315001005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000349')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000069')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000021')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='08406621.9999.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0077013000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='008195000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='AD')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='QATEST')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000226')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000172')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000509')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000486')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0896315001005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000349')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000069')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000021')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='08406621.9999.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='YTEM-APP-CYCLECOUNT')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'STOCK MANAGEMENT')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='pick')&(df_full['Retail_Bizstep']=='PICKING'),'STOCK MANAGEMENT')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0853613005005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='7508006006598')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0711069000015')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0048321000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='YTEM-APP-CYCLECOUNT')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'STOCK MANAGEMENT')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='N/A'),'STOCK MANAGEMENT')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='pick')&(df_full['Retail_Bizstep']=='PICKING'),'STOCK MANAGEMENT')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0853613005005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='7508006006598')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0711069000015')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0048321000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
 .when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'STOCK MANAGEMENT')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'STOCK MANAGEMENT')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='STOCKING')&(df_full['Retail_Bizstep']=='STOCKING'),'STOCK MANAGEMENT')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='STOCKING')&(df_full['Retail_Bizstep']=='STOCKING'),'STOCK MANAGEMENT')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0852945001006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0852945001006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0643831.00001.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0643831.00001.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0898634001001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0898634001001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='mobileDefault')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'STOCK MANAGEMENT')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='mobileDefault')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'STOCK MANAGEMENT')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0881006000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0765321000060')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464074009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0865741000203')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070575000019')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0678183000003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0850011920008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464012001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0815860019778')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0815860010003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0089718000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010020')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='079341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0847204000005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0814587010006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0881006000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0765321000060')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464074009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0865741000203')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070575000019')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0678183000003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0850011920008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464012001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0815860019778')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0815860010003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0089718000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010020')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='079341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0847204000005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0814587010006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070424000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464011004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000028')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860009484603')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000025')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000018')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0881006000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010044')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0079341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0000000000001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0089805000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0851339002001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010709')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070424000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464011004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000028')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860009484603')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000025')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000018')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0881006000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010044')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0079341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0000000000001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0089805000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0851339002001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010709')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')                             
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='inventoryDroid')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'STOCK MANAGEMENT')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='RECEIVING')&(df_full['Retail_Bizstep']=='RECEIVING'),'SUPPLY CHAIN')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000042')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0758692000012')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010853')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0047100000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010945')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010952')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010877')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000076')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0023700103154')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000090')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000083')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000160')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464006000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010839')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000106')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000337')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0682842015932')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0889930603396')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464005003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000236')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0729062000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860006994006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0664781000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0653017000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0820402000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='inventoryDroid')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'STOCK MANAGEMENT')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='RECEIVING')&(df_full['Retail_Bizstep']=='RECEIVING'),'SUPPLY CHAIN')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000042')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0758692000012')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010853')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0047100000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010945')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010952')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010877')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000076')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0023700103154')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000090')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000083')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000160')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464006000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010839')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000106')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000337')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0682842015932')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0889930603396')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464005003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000236')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0729062000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860006994006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0664781000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0653017000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0820402000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'SOURCING')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='PRODUCT_API')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'STOCK MANAGEMENT')                             
                             .otherwise('N/A'))


# #### Process Rules

# In[75]:


df_full = df_full.withColumn('Process',when((df_full.thingType=='CONTENT')&(df_full.specName=='SERVICES')&(df_full.bridgeKey_2=='SERVICES')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A')&(df_full.transactionId=='unlink'), 'Reflist Content Update')
.when((df_full.thingType=='DISCREPANCY')&(df_full.specName=='')&(df_full.bridgeKey_2=='ANALYTICS')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='DEMO: SALAD Demo')&(df_full.sourceModule=='COMMISSIONING')&(df_full.Retail_Bizstep=='COMMISSIONING'), 'Tag Management')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='DEMO: SALAD Demo')&(df_full.sourceModule=='PACKING')&(df_full.Retail_Bizstep=='PACKING'), 'Packing')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='DEMO: SALAD Demo')&(df_full.sourceModule=='encodeTags')&(df_full.Retail_Bizstep=='ENCODING'), 'Product Association')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='ENCODING')&(df_full.Retail_Bizstep=='ENCODING'), 'Product Association')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='INSPECTING')&(df_full.Retail_Bizstep=='INSPECTING'), 'Controlling')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='PACKING')&(df_full.Retail_Bizstep=='PACKING'), 'Packing')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='RECEIVING')&(df_full.Retail_Bizstep=='RECEIVING'), 'Receiving')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='RETAIL_SELLING')&(df_full.Retail_Bizstep=='RETAIL_SELLING'), 'Sales')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='SEARCHING')&(df_full.Retail_Bizstep=='SEARCHING'), 'Localize')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='SHIPPING')&(df_full.Retail_Bizstep=='SHIPPING'), 'Shipping')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='CYCLE_COUNTING'), 'Manage')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='ENCODING'), 'Manage')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='MISSING'), 'Manage')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='PICKING'), 'Manage')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='RECEIVING'), 'Manage')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='REMOVING'), 'Manage')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='SHIPPING'), 'Manage')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='cycleCount')&(df_full.Retail_Bizstep=='CYCLE_COUNTING'), 'Inventory')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='encodeTags')&(df_full.Retail_Bizstep=='ENCODING'), 'Product Association')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='Mission Produce')&(df_full.sourceModule=='COMMISSIONING')&(df_full.Retail_Bizstep=='COMMISSIONING'), 'Tag Management')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='SERIALIZATION')&(df_full.sourceModule=='SERIALIZATION')&(df_full.Retail_Bizstep=='SERIALIZING'), 'Product Association')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='UPDATE_INVENTORY')&(df_full.sourceModule=='setToMissing')&(df_full.Retail_Bizstep=='MISSING'), 'Inventory')
.when((df_full.thingType=='ITEM')&(df_full.specName=='')&(df_full.bridgeKey_2=='FLOW')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A')&(df_full.tenantCode=='PE'), 'Manage')
.when((df_full.thingType=='ITEM')&(df_full.specName=='')&(df_full.bridgeKey_2=='MOBILE')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='N/A'), 'Manage')
.when((df_full.thingType=='ITEM')&(df_full.specName=='')&(df_full.bridgeKey_2=='MOBILE')&(df_full.source=='MOBILE')&(df_full.sourceModule=='encodeTags')&(df_full.Retail_Bizstep=='N/A'), 'Product Association')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='DEMO: SALAD Demo')&(df_full.sourceModule=='COMMISSIONING')&(df_full.Retail_Bizstep=='COMMISSIONING'), 'Tag Management')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='DEMO: SALAD Demo')&(df_full.sourceModule=='PACKING')&(df_full.Retail_Bizstep=='PACKING'), 'Packing')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='DEMO: SALAD Demo')&(df_full.sourceModule=='encodeTags')&(df_full.Retail_Bizstep=='ENCODING'), 'Product Association')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='ENCODING')&(df_full.Retail_Bizstep=='ENCODING'), 'Product Association')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='INSPECTING')&(df_full.Retail_Bizstep=='INSPECTING'), 'Controlling')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='PACKING')&(df_full.Retail_Bizstep=='PACKING'), 'Packing')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='RECEIVING')&(df_full.Retail_Bizstep=='RECEIVING'), 'Receiving')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='RETAIL_SELLING')&(df_full.Retail_Bizstep=='RETAIL_SELLING'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='SHIPPING')&(df_full.Retail_Bizstep=='SHIPPING'), 'Shipping')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='CYCLE_COUNTING'), 'Manage')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='ENCODING'), 'Manage')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='MISSING'), 'Manage')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='PICKING'), 'Manage')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='RECEIVING'), 'Manage')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='REMOVING'), 'Manage')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='cycleCount')&(df_full.Retail_Bizstep=='CYCLE_COUNTING'), 'Inventory')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='encodeTags')&(df_full.Retail_Bizstep=='ENCODING'), 'Product Association')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='Mission Produce')&(df_full.sourceModule=='COMMISSIONING')&(df_full.Retail_Bizstep=='COMMISSIONING'), 'Tag Management')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='SERIALIZATION')&(df_full.sourceModule=='SERIALIZATION')&(df_full.Retail_Bizstep=='SERIALIZING'), 'Product Association')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='UPDATE_INVENTORY')&(df_full.sourceModule=='setToMissing')&(df_full.Retail_Bizstep=='MISSING'), 'Inventory')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_519')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_524')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_540')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_549')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_558')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_570')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_607')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_519')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_524')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_540')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_549')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_555')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_558')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_570')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POS_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_607')&(df_full.source=='FLOW')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix')&(df_full.bridgeKey_2=='ALEB_519')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix')&(df_full.bridgeKey_2=='ALEB_555')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='LOCATION')&(df_full.specName=='')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Provisioning')
.when((df_full.thingType=='PRODUCT')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGEPRODUCTS')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Update Product Metadata')
.when((df_full.thingType=='SOH')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESOH')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='SOHSTREAM')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESOH')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_519')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_524')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_540')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_544')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_549')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_555')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_558')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_570')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='STORE')&(df_full.specName=='')&(df_full.bridgeKey_2=='ALEB_607')&(df_full.source=='JSRULE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='CONTENT')&(df_full.specName=='SERVICES')&(df_full.bridgeKey_2=='SERVICES')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A')&(df_full.transactionId!='unlink'), 'Reflist Content Update')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='PICKING')&(df_full.Retail_Bizstep=='PICKING'), 'Manage')
.when((df_full.thingType=='EPCISEVENT')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='ENTERING_EXITING'), 'Manage')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='SHIPPING'), 'Manage')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='changeStatus')&(df_full.Retail_Bizstep=='ENTERING_EXITING'), 'Manage')
.when((df_full.thingType=='ITEM')&(df_full.specName=='EPCIS')&(df_full.bridgeKey_2=='EPCIS')&(df_full.source=='MOBILE')&(df_full.sourceModule=='PICKING')&(df_full.Retail_Bizstep=='PICKING'), 'Manage')
.when((df_full.thingType=='ITEM')&(df_full.specName=='POE_IINTERFACE')&(df_full.bridgeKey_2=='ALEB_555')&(df_full.source=='FLOW_ALE')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Sales')
.when((df_full.thingType=='ITEM')&(df_full.specName=='SCHEDULED')&(df_full.bridgeKey_2=='RULESET')&(df_full.source=='REP_398')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Loc_558_nightscan')&(df_full.bridgeKey_2=='ALEB_558')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Location_Spec_588')&(df_full.bridgeKey_2=='ALEB_558')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Spec')&(df_full.bridgeKey_2=='ALEB')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Spec')&(df_full.bridgeKey_2=='ALEB_524')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Spec')&(df_full.bridgeKey_2=='ALEB_544')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Spec')&(df_full.bridgeKey_2=='ALEB_607')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Spec')&(df_full.bridgeKey_2=='ALEB_549')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='ITEM')&(df_full.specName=='ViZix_Spec')&(df_full.bridgeKey_2=='ALEB_570')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Inventory')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF558TEMPE')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Manage')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF549DOLPHIN')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Manage')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF510SAWGRASS')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Manage')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF524ORLANDO1')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Manage')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF543ORLANDO2')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Manage')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF555ROSEMONT')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Manage')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF540LASVEGAS2')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Manage')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF544LASVEGAS1')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Manage')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF607SANMARCOS')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Manage')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF566BLOCKATORANG')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Manage')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF570GRANDPRAIRIE')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Manage')
.when((df_full.thingType=='STOREATTRIBUTES')&(df_full.specName=='')&(df_full.bridgeKey_2=='FTPBRIDGESTOREATTDIFF519JERSEYGARDENS')&(df_full.source=='N/A')&(df_full.sourceModule=='N/A')&(df_full.Retail_Bizstep=='N/A'), 'Manage')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ytem_cloud')&(df_full['bridgeKey_2']=='ALEB_540')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Inventory')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='SERVICES')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Update Product Metadata')
.when((df_full['thingType']=='CONTENT')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A')&(df_full['transactionId']=='unlink'), 'Reflist Content Update')
.when((df_full['thingType']=='CONTENT')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A')&(df_full['transactionId']!='unlink'), 'Reflist Content Update')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'), 'Update Product Metadata')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0077013000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='008195000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='AD')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='QATEST')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000509')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000226')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000172')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000486')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0896315001005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000349')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000069')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000021')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='08406621.9999.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0077013000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='008195000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='AD')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='QATEST')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000226')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000172')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000509')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000486')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0896315001005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000349')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000069')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000021')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='08406621.9999.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='YTEM-APP-CYCLECOUNT')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Inventory')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='pick')&(df_full['Retail_Bizstep']=='PICKING'),'Manage')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0853613005005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='7508006006598')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0711069000015')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0048321000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='YTEM-APP-CYCLECOUNT')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Inventory')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='N/A'),'Manage')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='pick')&(df_full['Retail_Bizstep']=='PICKING'),'Manage')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0853613005005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='7508006006598')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0711069000015')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0048321000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')                             
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Inventory')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Inventory')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='STOCKING')&(df_full['Retail_Bizstep']=='STOCKING'),'Inventory')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='STOCKING')&(df_full['Retail_Bizstep']=='STOCKING'),'Inventory')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0852945001006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0852945001006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0643831.00001.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0643831.00001.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0898634001001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0898634001001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='mobileDefault')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Inventory')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='mobileDefault')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Inventory')                             
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0881006000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0765321000060')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464074009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0865741000203')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070575000019')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0678183000003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0850011920008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464012001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0815860019778')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0815860010003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0089718000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010020')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='079341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0847204000005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0814587010006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0881006000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0765321000060')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464074009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0865741000203')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070575000019')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0678183000003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0850011920008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464012001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0815860019778')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0815860010003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0089718000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010020')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='079341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0847204000005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0814587010006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070424000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464011004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000028')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860009484603')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000025')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000018')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0881006000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010044')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0079341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0000000000001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0089805000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0851339002001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010709')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070424000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464011004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000028')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860009484603')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000025')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000018')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0881006000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010044')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0079341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0000000000001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0089805000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0851339002001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010709')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')                             
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='inventoryDroid')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Inventory')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='RECEIVING')&(df_full['Retail_Bizstep']=='RECEIVING'),'Receiving')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000042')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0758692000012')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010853')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0047100000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010945')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010952')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010877')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000076')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0023700103154')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000090')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000083')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000160')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464006000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010839')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000106')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000337')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0682842015932')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0889930603396')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464005003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000236')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0729062000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860006994006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0664781000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0653017000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0820402000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='inventoryDroid')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Inventory')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='RECEIVING')&(df_full['Retail_Bizstep']=='RECEIVING'),'Receiving')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000042')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0758692000012')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010853')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0047100000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010945')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010952')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010877')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000076')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0023700103154')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000090')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000083')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000160')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464006000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010839')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000106')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000337')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0682842015932')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0889930603396')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464005003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000236')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0729062000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860006994006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0664781000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0653017000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0820402000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Tag Management')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='PRODUCT_API')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Update Product Metadata')
                             .otherwise('N/A'))


# #### Sub Process Rules

# In[76]:


df_full = df_full.withColumn('SubProcess',when((df_full['thingType']=='CONTENT')&(df_full['specName']=='SERVICES')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A')&(df_full['transactionId']=='unlink'),'Dissociation')
.when((df_full['thingType']=='DISCREPANCY')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ANALYTICS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Discrepancy')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='DEMO: SALAD Demo')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commisioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='DEMO: SALAD Demo')&(df_full['sourceModule']=='PACKING')&(df_full['Retail_Bizstep']=='PACKING'),'Packing')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='DEMO: SALAD Demo')&(df_full['sourceModule']=='encodeTags')&(df_full['Retail_Bizstep']=='ENCODING'),'Encoding')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='ENCODING')&(df_full['Retail_Bizstep']=='ENCODING'),'Encoding')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='INSPECTING')&(df_full['Retail_Bizstep']=='INSPECTING'),'Quality Control')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='PACKING')&(df_full['Retail_Bizstep']=='PACKING'),'Packing')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='RECEIVING')&(df_full['Retail_Bizstep']=='RECEIVING'),'Receiving')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='RETAIL_SELLING')&(df_full['Retail_Bizstep']=='RETAIL_SELLING'),'Selling')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='SEARCHING')&(df_full['Retail_Bizstep']=='SEARCHING'),'Product Searching')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='SHIPPING')&(df_full['Retail_Bizstep']=='SHIPPING'),'Shipping')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Edit Items')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='ENCODING'),'Edit Items')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='MISSING'),'Edit Items')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='PICKING'),'Edit Items')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='RECEIVING'),'Edit Items')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='REMOVING'),'Edit Items')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='SHIPPING'),'Edit Items')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Stock Taking')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='encodeTags')&(df_full['Retail_Bizstep']=='ENCODING'),'Encoding')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='Mission Produce')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='SERIALIZATION')&(df_full['sourceModule']=='SERIALIZATION')&(df_full['Retail_Bizstep']=='SERIALIZING'),'Serialization')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='UPDATE_INVENTORY')&(df_full['sourceModule']=='setToMissing')&(df_full['Retail_Bizstep']=='MISSING'),'Update Inventory')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FLOW')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A')&(df_full['tenantCode']=='PE'),'Replenishment')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='MOBILE')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='N/A'),'Edit Items')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='MOBILE')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='encodeTags')&(df_full['Retail_Bizstep']=='N/A'),'Encoding')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='DEMO: SALAD Demo')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commisioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='DEMO: SALAD Demo')&(df_full['sourceModule']=='PACKING')&(df_full['Retail_Bizstep']=='PACKING'),'Packing')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='DEMO: SALAD Demo')&(df_full['sourceModule']=='encodeTags')&(df_full['Retail_Bizstep']=='ENCODING'),'Encoding')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='ENCODING')&(df_full['Retail_Bizstep']=='ENCODING'),'Encoding')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='INSPECTING')&(df_full['Retail_Bizstep']=='INSPECTING'),'Quality Control')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='PACKING')&(df_full['Retail_Bizstep']=='PACKING'),'Packing')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='RECEIVING')&(df_full['Retail_Bizstep']=='RECEIVING'),'Receiving')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='RETAIL_SELLING')&(df_full['Retail_Bizstep']=='RETAIL_SELLING'),'Selling')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='SHIPPING')&(df_full['Retail_Bizstep']=='SHIPPING'),'Shipping')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Edit Items')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='ENCODING'),'Edit Items')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='MISSING'),'Edit Items')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='PICKING'),'Edit Items')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='RECEIVING'),'Edit Items')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='REMOVING'),'Edit Items')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Stock Taking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='encodeTags')&(df_full['Retail_Bizstep']=='ENCODING'),'Encoding')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='Mission Produce')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='SERIALIZATION')&(df_full['sourceModule']=='SERIALIZATION')&(df_full['Retail_Bizstep']=='SERIALIZING'),'Serialization')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='UPDATE_INVENTORY')&(df_full['sourceModule']=='setToMissing')&(df_full['Retail_Bizstep']=='MISSING'),'Update Inventory')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Antitheft')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_519')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Antitheft')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_524')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Antitheft')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_540')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Antitheft')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_549')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Antitheft')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_558')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Antitheft')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_570')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Antitheft')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_607')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Antitheft')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Selling')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_519')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Selling')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_524')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Selling')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_540')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Selling')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_549')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Selling')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_555')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Selling')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_558')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Selling')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_570')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Selling')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_607')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Selling')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix')&(df_full['bridgeKey_2']=='ALEB_519')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Automated Stock Taking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix')&(df_full['bridgeKey_2']=='ALEB_555')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Automated Stock Taking')
.when((df_full['thingType']=='LOCATION')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Site Configuration')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGEPRODUCTS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Update Product Metadata')
.when((df_full['thingType']=='SOH')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESOH')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'SOH')
.when((df_full['thingType']=='SOHSTREAM')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESOH')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'SOH')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Freshness')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_519')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Freshness')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_524')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Freshness')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_540')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Freshness')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_544')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Freshness')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_549')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Freshness')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_555')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Freshness')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_558')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Freshness')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_570')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Freshness')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_607')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Freshness')
.when((df_full['thingType']=='CONTENT')&(df_full['specName']=='SERVICES')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A')&(df_full['transactionId']!='unlink'),'Update Reflist')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='PICKING')&(df_full['Retail_Bizstep']=='PICKING'),'Picking')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='ENTERING_EXITING'),'Edit Items')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='SHIPPING'),'Edit Items')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='ENTERING_EXITING'),'Edit Items')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='PICKING')&(df_full['Retail_Bizstep']=='PICKING'),'Picking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_555')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Antitheft')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='SCHEDULED')&(df_full['bridgeKey_2']=='RULESET')&(df_full['source']=='REP_398')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Update Inventory')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Loc_558_nightscan')&(df_full['bridgeKey_2']=='ALEB_558')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Automated Stock Taking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Location_Spec_588')&(df_full['bridgeKey_2']=='ALEB_558')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Automated Stock Taking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Spec')&(df_full['bridgeKey_2']=='ALEB')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Automated Stock Taking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Spec')&(df_full['bridgeKey_2']=='ALEB_524')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Automated Stock Taking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Spec')&(df_full['bridgeKey_2']=='ALEB_544')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Automated Stock Taking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Spec')&(df_full['bridgeKey_2']=='ALEB_607')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Automated Stock Taking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Spec')&(df_full['bridgeKey_2']=='ALEB_549')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Automated Stock Taking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Spec')&(df_full['bridgeKey_2']=='ALEB_570')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Automated Stock Taking')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF558TEMPE')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Replenishment')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF549DOLPHIN')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Replenishment')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF510SAWGRASS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Replenishment')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF524ORLANDO1')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Replenishment')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF543ORLANDO2')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Replenishment')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF555ROSEMONT')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Replenishment')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF540LASVEGAS2')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Replenishment')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF544LASVEGAS1')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Replenishment')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF607SANMARCOS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Replenishment')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF566BLOCKATORANG')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Replenishment')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF570GRANDPRAIRIE')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Replenishment')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF519JERSEYGARDENS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Replenishment')
 .when((df_full['thingType']=='ITEM')&(df_full['specName']=='ytem_cloud')&(df_full['bridgeKey_2']=='ALEB_540')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Automated Stock Taking')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='SERVICES')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Update Product Metadata')
.when((df_full['thingType']=='CONTENT')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A')&(df_full['transactionId']=='unlink'), 'Dissociation')
.when((df_full['thingType']=='CONTENT')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A')&(df_full['transactionId']!='unlink'), 'Update Reflist')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'), 'Update Product Metadata')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0077013000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='008195000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='AD')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='QATEST')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000509')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000226')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000172')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000486')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0896315001005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000349')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000069')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000021')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='08406621.9999.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0077013000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='008195000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='AD')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='QATEST')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000226')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000172')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000509')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000486')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0896315001005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000349')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000069')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000021')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='08406621.9999.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), 'Commissioning')                            
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='YTEM-APP-CYCLECOUNT')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Stock Taking')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='pick')&(df_full['Retail_Bizstep']=='PICKING'),'Picking')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0853613005005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='7508006006598')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0711069000015')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0048321000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='YTEM-APP-CYCLECOUNT')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Stock Taking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='N/A'),'Edit Items')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='pick')&(df_full['Retail_Bizstep']=='PICKING'),'Picking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0853613005005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='7508006006598')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0711069000015')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0048321000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Stock Taking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Stock Taking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='STOCKING')&(df_full['Retail_Bizstep']=='STOCKING'),'Automated Inventory')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='STOCKING')&(df_full['Retail_Bizstep']=='STOCKING'),'Automated Inventory')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0852945001006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0852945001006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0643831.00001.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0643831.00001.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0898634001001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0898634001001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='mobileDefault')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Stock Taking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='mobileDefault')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Stock Taking')                             
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0881006000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0765321000060')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464074009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0865741000203')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070575000019')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0678183000003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0850011920008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464012001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0815860019778')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0815860010003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0089718000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010020')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='079341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0847204000005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0814587010006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0881006000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0765321000060')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464074009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0865741000203')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070575000019')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0678183000003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0850011920008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464012001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0815860019778')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0815860010003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0089718000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010020')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='079341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0847204000005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0814587010006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070424000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464011004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000028')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860009484603')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000025')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000018')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0881006000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010044')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0079341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0000000000001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0089805000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0851339002001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010709')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070424000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464011004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000028')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860009484603')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000025')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000018')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0881006000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010044')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0079341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0000000000001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0089805000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0851339002001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010709')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='inventoryDroid')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Stock Taking')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='RECEIVING')&(df_full['Retail_Bizstep']=='RECEIVING'),'Receiving')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000042')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0758692000012')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010853')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0047100000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010945')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010952')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010877')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000076')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0023700103154')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000090')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000083')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000160')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464006000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010839')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000106')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000337')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0682842015932')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0889930603396')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464005003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000236')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0729062000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860006994006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0664781000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0653017000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0820402000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='inventoryDroid')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'Stock Taking')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='RECEIVING')&(df_full['Retail_Bizstep']=='RECEIVING'),'Receiving')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000042')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0758692000012')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010853')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0047100000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010945')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010952')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010877')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000076')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0023700103154')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000090')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000083')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000160')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464006000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010839')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000106')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000337')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0682842015932')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0889930603396')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464005003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000236')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0729062000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860006994006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0664781000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0653017000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0820402000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'Commissioning')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='PRODUCT_API')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'Update Product Metadata') 
                            .otherwise('N/A'))


# #### EventSource Rules

# In[77]:


df_full = df_full.withColumn('EventSource',when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source'].isin(['MOBILE','Mission Produce','SERIALIZATION','UPDATE_INVENTORY','DEMO: SALAD Demo','0077013000006','008195000002','AD','QATEST','0732966000007','0070740000509','0070740000226','0070740000172','0070740000486','0896315001005','0070740000349','0761010000069','0732966000021','08406621.9999.0','0853613005005','7508006006598','0711069000015','0048321000002','YTEM-APP-CYCLECOUNT','ytemApp','0852945001006','0643831.00001.0','0898634001001','0013941305110','0881006000009','0030223000006','0030223000037','0765321000060','0073464074009','0865741000203','0070575000019','0678183000003','0850011920008','0073464012001','0815860019778','0815860010003','0089718000007','0073731010020','079341000004','0847204000005','0073731','0814587010006','0070424000009','0073464011004','0073420000028','0860009484603','0885460000025','0013941305110','0885460000018','0881006000009','0030223000037','0073731010044','0079341000004','0000000000001','0089805000002','0851339002001','0073731010709','0023700103154','0047100000004','0071117000160','0071117000337','0073420000042','0073464005003','0073464006000','0073731010839','0073731010853','0073731010877','0073731010945','0073731010952','0653017000008','0664781000002','0682842015932','0729062000000','0758692000012','0761010000076','0761010000083','0761010000090','0761010000106','0761010000236','0820402000008','0860006994006','0889930603396','inventoryDroid','yTem_automated_droid','yTem_automated_droid'])),df_full['source'])
.when((df_full['thingType']=='ITEM')&(df_full['specName'].isin(['','YTEM']))&(df_full['bridgeKey_2'].isin(['MOBILE','FLOW','SERVICES']))&(df_full['source'].isin(['MOBILE','FLOW'])),df_full['source'])
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source'].isin(['MOBILE','Mission Produce','SERIALIZATION','UPDATE_INVENTORY','DEMO: SALAD Demo','REP_398','0077013000006','008195000002','AD','QATEST','0732966000007','0070740000509','0070740000226','0070740000172','0070740000486','0896315001005','0070740000349','0761010000069','0732966000021','08406621.9999.0','0853613005005','7508006006598','0711069000015','0048321000002','YTEM-APP-CYCLECOUNT','ytemApp','0852945001006','0643831.00001.0','0898634001001','0013941305110','0881006000009','0030223000006','0030223000037','0765321000060','0073464074009','0865741000203','0070575000019','0678183000003','0850011920008','0073464012001','0815860019778','0815860010003','0089718000007','0073731010020','079341000004','0847204000005','0073731','0814587010006','0070424000009','0073464011004','0073420000028','0860009484603','0885460000025','0013941305110','0885460000018','0881006000009','0030223000037','0073731010044','0079341000004','0000000000001','0089805000002','0851339002001','0073731010709''0023700103154','0047100000004','0071117000160','0071117000337','0073420000042','0073464005003','0073464006000','0073731010839','0073731010853','0073731010877','0073731010945','0073731010952','0653017000008','0664781000002','0682842015932','0729062000000','0758692000012','0761010000076','0761010000083','0761010000090','0761010000106','0761010000236','0820402000008','0860006994006','0889930603396','inventoryDroid','yTem_automated_droid','yTem_automated_droid'])),df_full['source'])
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),df_full['source'])
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),df_full['source'])
.when((df_full['thingType']=='ITEM')&(df_full['specName'].isin(['ViZix_Loc_558_nightscan','ViZix_Location_Spec_588','ViZix_Location_Spec','ViZix','ViZix_Spec','ytem_cloud']))&(df_full['bridgeKey_2'].like('ALEB%'))&(df_full['source']=='N/A'),  split(df_full['bridgeKey_2'],"_").getItem(0))
.when((df_full['thingType']=='ITEM')&(df_full['specName']==' SCHEDULED')&(df_full['bridgeKey_2'].isin(['RULESET']))&(df_full['source'].isin(['REP_398'])),df_full['source'])
.when((df_full['thingType']=='SOH'),df_full['bridgeKey_2'])
.when((df_full['thingType']=='SOHSTREAM'),df_full['bridgeKey_2'])
.when((df_full['thingType']=='DISCREPANCY')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ANALYTICS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),df_full['bridgeKey_2'])
.when((df_full['thingType']=='LOCATION')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),df_full['bridgeKey_2'])
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGEPRODUCTS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),df_full['bridgeKey_2'])
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2'].like('ALEB%')),split(df_full['bridgeKey_2'],"_").getItem(0))     
.when((df_full['thingType']=='CONTENT'),df_full['bridgeKey_2'])
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['bridgeKey_2'].like('FTPBRIDGE%')),'FTPBRIDGE')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName'].isin(['SERVICES','YTEM','PRODUCT_API']))&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),df_full['bridgeKey_2'])
                             .otherwise('N/A'))


# In[78]:


#df_full.groupBy(["thingType",'specName',"bridgeKey_2",'EventSource']).count().show(80, False)


# #### StoreCode Rules

# In[79]:


df_full = df_full.withColumn('StoreCode',when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source'].isin(['MOBILE','Mission Produce','SERIALIZATION','UPDATE_INVENTORY','DEMO: SALAD Demo','0077013000006','008195000002','AD','QATEST','0732966000007','0070740000509','0070740000226','0070740000172','0070740000486','0896315001005','0070740000349','0761010000069','0732966000021','08406621.9999.0','0853613005005','7508006006598','0711069000015','0048321000002','YTEM-APP-CYCLECOUNT','ytemApp','0852945001006','0643831.00001.0','0898634001001','0013941305110','0881006000009','0030223000006','0030223000037','0765321000060','0073464074009','0865741000203','0070575000019','0678183000003','0850011920008','0073464012001','0815860019778','0815860010003','0089718000007','0073731010020','079341000004','0847204000005','0073731','0814587010006','0070424000009','0073464011004','0073420000028','0860009484603','0885460000025','0013941305110','0885460000018','0881006000009','0030223000037','0073731010044','0079341000004','0000000000001','0089805000002','0851339002001','0073731010709''0023700103154','0047100000004','0071117000160','0071117000337','0073420000042','0073464005003','0073464006000','0073731010839','0073731010853','0073731010877','0073731010945','0073731010952','0653017000008','0664781000002','0682842015932','0729062000000','0758692000012','0761010000076','0761010000083','0761010000090','0761010000106','0761010000236','0820402000008','0860006994006','0889930603396','inventoryDroid','yTem_automated_droid','yTem_automated_droid'])),regexp_replace('Retail_Premise', r'^[0]*', ''))
.when((df_full['thingType']=='ITEM')&(df_full['specName'].isin(['','YTEM']))&(df_full['bridgeKey_2'].isin(['MOBILE','FLOW','SERVICES']))&(df_full['source'].isin(['MOBILE','FLOW'])),regexp_replace('premise_from_fixture', r'^[0]*', ''))
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source'].isin(['MOBILE','Mission Produce','SERIALIZATION','UPDATE_INVENTORY','DEMO: SALAD Demo','0077013000006','008195000002','AD','QATEST','0732966000007','0070740000509','0070740000226','0070740000172','0070740000486','0896315001005','0070740000349','0761010000069','0732966000021','08406621.9999.0','0853613005005','7508006006598','0711069000015','0048321000002','YTEM-APP-CYCLECOUNT','ytemApp','0852945001006','0643831.00001.0','0898634001001','0013941305110','0881006000009','0030223000006','0030223000037','0765321000060','0073464074009','0865741000203','0070575000019','0678183000003','0850011920008','0073464012001','0815860019778','0815860010003','0089718000007','0073731010020','079341000004','0847204000005','0073731','0814587010006','0070424000009','0073464011004','0073420000028','0860009484603','0885460000025','0013941305110','0885460000018','0881006000009','0030223000037','0073731010044','0079341000004','0000000000001','0089805000002','0851339002001','0073731010709''0023700103154','0047100000004','0071117000160','0071117000337','0073420000042','0073464005003','0073464006000','0073731010839','0073731010853','0073731010877','0073731010945','0073731010952','0653017000008','0664781000002','0682842015932','0729062000000','0758692000012','0761010000076','0761010000083','0761010000090','0761010000106','0761010000236','0820402000008','0860006994006','0889930603396','inventoryDroid','yTem_automated_droid','yTem_automated_droid'])),regexp_replace('Retail_Premise', r'^[0]*', ''))
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'), split(df_full['doorEvent'],"/").getItem(2)) 
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),split(df_full['zone'],"_").getItem(0))
.when((df_full['thingType']=='ITEM')&(df_full['specName'].isin(['ViZix_Loc_558_nightscan','ViZix_Location_Spec_588','ViZix_Location_Spec','ViZix','ViZix_Spec','ytem_cloud']))&(df_full['bridgeKey_2'].like('ALEB%'))&(df_full['source']=='N/A'),split(df_full['bridgeKey_2'],"_").getItem(1))
.when((df_full['thingType']=='ITEM')&(df_full['specName']==' SCHEDULED')&(df_full['bridgeKey_2'].isin(['RULESET']))&(df_full['source'].isin(['REP_398'])),'No Store')
.when((df_full['thingType']=='SOH'),regexp_replace('Retail_SOHStoreNumber', r'^[0]*', ''))
.when((df_full['thingType']=='SOHSTREAM'),regexp_replace('Retail_SOHStoreNumber', r'^[0]*', ''))
.when((df_full['thingType']=='DISCREPANCY')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ANALYTICS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),regexp_replace('Retail_StoreNumber', r'^[0]*', ''))
.when((df_full['thingType']=='LOCATION')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'No Store')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGEPRODUCTS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'No Store')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2'].like('ALEB%')),split(df_full['bridgeKey_2'],"_").getItem(1))
.when((df_full['thingType']=='CONTENT'),regexp_replace('premise_from_biz', r'^[0]*', ''))
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['bridgeKey_2'].like('FTPBRIDGE%')),df_full['Retail_StoreNumber'])
.when((df_full['thingType']=='PRODUCT')&(df_full['specName'].isin(['SERVICES','YTEM']))&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'No Store')
                             .otherwise('N/A'))


# In[80]:


#df_full.groupBy(["thingType",'StoreCode','Retail_Premise']).count().show(80, False)


# #### Is_InternalEvent Rules

# In[81]:


##### Is_InternalEvent Rules
df_full = df_full.withColumn('Is_InternalEvent',when((df_full['thingType']=='CONTENT')&(df_full['specName']=='SERVICES')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A')&(df_full['transactionId']=='unlink'),'0')
.when((df_full['thingType']=='DISCREPANCY')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ANALYTICS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'1')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='DEMO: SALAD Demo')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='DEMO: SALAD Demo')&(df_full['sourceModule']=='PACKING')&(df_full['Retail_Bizstep']=='PACKING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='DEMO: SALAD Demo')&(df_full['sourceModule']=='encodeTags')&(df_full['Retail_Bizstep']=='ENCODING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='ENCODING')&(df_full['Retail_Bizstep']=='ENCODING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='INSPECTING')&(df_full['Retail_Bizstep']=='INSPECTING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='PACKING')&(df_full['Retail_Bizstep']=='PACKING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='RECEIVING')&(df_full['Retail_Bizstep']=='RECEIVING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='RETAIL_SELLING')&(df_full['Retail_Bizstep']=='RETAIL_SELLING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='SEARCHING')&(df_full['Retail_Bizstep']=='SEARCHING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='SHIPPING')&(df_full['Retail_Bizstep']=='SHIPPING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='ENCODING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='MISSING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='PICKING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='RECEIVING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='REMOVING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='SHIPPING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='encodeTags')&(df_full['Retail_Bizstep']=='ENCODING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='Mission Produce')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='SERIALIZATION')&(df_full['sourceModule']=='SERIALIZATION')&(df_full['Retail_Bizstep']=='SERIALIZING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='UPDATE_INVENTORY')&(df_full['sourceModule']=='setToMissing')&(df_full['Retail_Bizstep']=='MISSING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FLOW')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A')&(df_full['tenantCode']=='PE'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='MOBILE')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='MOBILE')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='encodeTags')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='DEMO: SALAD Demo')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='DEMO: SALAD Demo')&(df_full['sourceModule']=='PACKING')&(df_full['Retail_Bizstep']=='PACKING'),'')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='DEMO: SALAD Demo')&(df_full['sourceModule']=='encodeTags')&(df_full['Retail_Bizstep']=='ENCODING'),'')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='ENCODING')&(df_full['Retail_Bizstep']=='ENCODING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='INSPECTING')&(df_full['Retail_Bizstep']=='INSPECTING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='PACKING')&(df_full['Retail_Bizstep']=='PACKING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='RECEIVING')&(df_full['Retail_Bizstep']=='RECEIVING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='RETAIL_SELLING')&(df_full['Retail_Bizstep']=='RETAIL_SELLING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='SHIPPING')&(df_full['Retail_Bizstep']=='SHIPPING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='ENCODING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='MISSING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='PICKING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='RECEIVING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='REMOVING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='encodeTags')&(df_full['Retail_Bizstep']=='ENCODING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='Mission Produce')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='SERIALIZATION')&(df_full['sourceModule']=='SERIALIZATION')&(df_full['Retail_Bizstep']=='SERIALIZING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='UPDATE_INVENTORY')&(df_full['sourceModule']=='setToMissing')&(df_full['Retail_Bizstep']=='MISSING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_519')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_524')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_540')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_549')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_558')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_570')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_607')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_519')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_524')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_540')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_549')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_555')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_558')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_570')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POS_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_607')&(df_full['source']=='FLOW')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix')&(df_full['bridgeKey_2']=='ALEB_519')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix')&(df_full['bridgeKey_2']=='ALEB_555')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='LOCATION')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGEPRODUCTS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='SOH')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESOH')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'1')
.when((df_full['thingType']=='SOHSTREAM')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESOH')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'1')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_519')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'1')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_524')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'1')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_540')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'1')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_544')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'1')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_549')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'1')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_555')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'1')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_558')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'1')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_570')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'1')
.when((df_full['thingType']=='STORE')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='ALEB_607')&(df_full['source']=='JSRULE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'1')
.when((df_full['thingType']=='CONTENT')&(df_full['specName']=='SERVICES')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A')&(df_full['transactionId']!='unlink'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='PICKING')&(df_full['Retail_Bizstep']=='PICKING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='ENTERING_EXITING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='SHIPPING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='ENTERING_EXITING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='PICKING')&(df_full['Retail_Bizstep']=='PICKING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='POE_IINTERFACE')&(df_full['bridgeKey_2']=='ALEB_555')&(df_full['source']=='FLOW_ALE')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='SCHEDULED')&(df_full['bridgeKey_2']=='RULESET')&(df_full['source']=='REP_398')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Loc_558_nightscan')&(df_full['bridgeKey_2']=='ALEB_558')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Location_Spec_588')&(df_full['bridgeKey_2']=='ALEB_558')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Spec')&(df_full['bridgeKey_2']=='ALEB')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Spec')&(df_full['bridgeKey_2']=='ALEB_524')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Spec')&(df_full['bridgeKey_2']=='ALEB_544')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Spec')&(df_full['bridgeKey_2']=='ALEB_607')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Spec')&(df_full['bridgeKey_2']=='ALEB_549')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ViZix_Spec')&(df_full['bridgeKey_2']=='ALEB_570')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF558TEMPE')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF549DOLPHIN')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF510SAWGRASS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF524ORLANDO1')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF543ORLANDO2')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF555ROSEMONT')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF540LASVEGAS2')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF544LASVEGAS1')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF607SANMARCOS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF566BLOCKATORANG')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF570GRANDPRAIRIE')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='STOREATTRIBUTES')&(df_full['specName']=='')&(df_full['bridgeKey_2']=='FTPBRIDGESTOREATTDIFF519JERSEYGARDENS')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='ytem_cloud')&(df_full['bridgeKey_2']=='ALEB_540')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='SERVICES')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'1')
.when((df_full['thingType']=='CONTENT')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A')&(df_full['transactionId']=='unlink'), '0')
.when((df_full['thingType']=='CONTENT')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A')&(df_full['transactionId']!='unlink'), '0')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'), '1')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0077013000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='008195000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='AD')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='QATEST')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000509')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000226')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000172')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000486')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0896315001005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000349')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000069')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000021')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='08406621.9999.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0077013000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='008195000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='AD')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='QATEST')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000226')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000172')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000509')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000486')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0896315001005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070740000349')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000069')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0732966000021')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='08406621.9999.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'), '0')  
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='YTEM-APP-CYCLECOUNT')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='pick')&(df_full['Retail_Bizstep']=='PICKING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0853613005005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='7508006006598')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0711069000015')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0048321000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='YTEM-APP-CYCLECOUNT')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='YTEM')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='changeStatus')&(df_full['Retail_Bizstep']=='N/A'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='pick')&(df_full['Retail_Bizstep']=='PICKING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0853613005005')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='7508006006598')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0711069000015')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0048321000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='STOCKING')&(df_full['Retail_Bizstep']=='STOCKING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='MOBILE')&(df_full['sourceModule']=='STOCKING')&(df_full['Retail_Bizstep']=='STOCKING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0852945001006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0852945001006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0643831.00001.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0643831.00001.0')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0898634001001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0898634001001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='mobileDefault')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='ytemApp')&(df_full['sourceModule']=='mobileDefault')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'0')                            
 .when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0881006000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='30223000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='30223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='765321000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='73464074009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='865741000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='70575000019')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='678183000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='850012000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='73464012001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='815860000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='815860000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='89718000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='73731010020')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='79341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='847204000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='73731')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='814587000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='881006000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='765321000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='73464074009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='865741000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='70575000019')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='678183000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='850012000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='73464012001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='815860000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='815860000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='89718000007')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='73731010020')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='79341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='847204000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='814587000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='73731')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070424000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464011004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000028')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860009484603')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000025')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000018')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0881006000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010044')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0079341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0000000000001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0089805000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0851339002001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010709')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0070424000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464011004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000028')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860009484603')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000025')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0013941305110')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0885460000018')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0881006000009')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0030223000037')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010044')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0079341000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0000000000001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0089805000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0851339002001')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010709')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'1')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='inventoryDroid')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'1')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='RECEIVING')&(df_full['Retail_Bizstep']=='RECEIVING'),'1')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000042')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0758692000012')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010853')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0047100000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010945')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010952')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010877')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000076')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0023700103154')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000090')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000083')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000160')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464006000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010839')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000106')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000337')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0682842015932')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0889930603396')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464005003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000236')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0729062000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860006994006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0664781000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0653017000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='EPCISEVENT')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0820402000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'1')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='inventoryDroid')&(df_full['sourceModule']=='cycleCount')&(df_full['Retail_Bizstep']=='CYCLE_COUNTING'),'1')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='yTem_automated_droid')&(df_full['sourceModule']=='RECEIVING')&(df_full['Retail_Bizstep']=='RECEIVING'),'1')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073420000042')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0758692000012')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010853')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0047100000004')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010945')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010952')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010877')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000076')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0023700103154')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000090')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000083')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000160')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464006000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073731010839')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000106')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0071117000337')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0682842015932')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0889930603396')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0073464005003')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0761010000236')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0729062000000')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0860006994006')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0664781000002')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0653017000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='ITEM')&(df_full['specName']=='EPCIS')&(df_full['bridgeKey_2']=='EPCIS')&(df_full['source']=='0820402000008')&(df_full['sourceModule']=='COMMISSIONING')&(df_full['Retail_Bizstep']=='COMMISSIONING'),'0')
.when((df_full['thingType']=='PRODUCT')&(df_full['specName']=='PRODUCT_API')&(df_full['bridgeKey_2']=='SERVICES')&(df_full['source']=='N/A')&(df_full['sourceModule']=='N/A')&(df_full['Retail_Bizstep']=='N/A'),'0')                             
                             .otherwise('N/A'))





#df_full_2 = df_full[(df_full['storeCode'].isin(['N/A','','No Store']))|(df_full['storeCode'].isNull())][['id','serialNumber','tenantCode','time','group','thingType','bridgeKey','specName','priority','requestId','action','bridgeKey_2','datetime_h','Event_Activity','Event_GroupCode','Event_Source','Event_SubCategory','Retail_Bizlocation','Retail_Bizstep','Retail_Disposition','Retail_Extension','Retail_Premise','Retail_SOHStoreNumber','Retail_StoreNumber','bizStep','logicalReader','source','sourceModule','status','transactionId','zone','doorEvent','FeatureSet','Process','SubProcess','EventSource','StoreCode','Is_InternalEvent']]


# In[83]:


#df_full_2 = df_full_2.toPandas()
#table = "saas-analytics-io.processed.yt1_no_store_analysis"
#df_full_2.to_gbq(table,  if_exists='append')


# In[84]:


#df_full['FeatureSet'] = df_full['FeatureSet'].fillna('N/A')
#df_full['Process'] = df_full['Process'].fillna('N/A')
#df_full['SubProcess'] = df_full['SubProcess'].fillna('N/A')
#df_full['EventSource'] = df_full['EventSource'].fillna('N/A')
#df_full['StoreCode'] = df_full['StoreCode'].fillna('N/A')
#df_full['Is_InternalEvent'] = df_full['Is_InternalEvent'].fillna('N/A')


# In[85]:


#display(df_full.filter((df_full['sourceModule']=='changeStatus')&(df_full['retail_Bizstep']=='N/A')))


# In[86]:


#display(df_full.filter((df_full['thingType']=='CONTENT')))


# #### Aggregation of Events

# In[87]:


#df_full = df_full.withColumn('time', regexp_replace('time', '2022-12-07', '2022-12-08')) 


# In[88]:


#df_full = df_full.withColumn('datetime_h', date_format(df_full['time'], "d/M/y H"))
#df_full = df_full.withColumn('datetime_h', to_utc_timestamp(to_timestamp(df_full['datetime_h'],'d/M/y H'), 'UTC'))


# In[89]:


df_full.printSchema()


# In[90]:


df_agg_1 = df_full.groupBy(['datetime_h','tenantCode','thingType','specName','bridgeKey_2','source','sourceModule','Retail_Bizstep','FeatureSet','Process','SubProcess','EventSource','StoreCode','Is_InternalEvent']).count()

df_agg_1  = df_soh_agg.union(df_agg_1)

# In[91]:


df_agg_1.printSchema()


# In[92]:


df_agg_2 = df_full.groupBy(['datetime_h','serialNumber','tenantCode','thingType','FeatureSet','Process','SubProcess','EventSource','StoreCode','Is_InternalEvent']).count()


# In[93]:


df_agg_2.printSchema()


# In[94]:


df_agg_1 = df_agg_1.withColumnRenamed("count","mojix_blink_count")
df_agg_2 = df_agg_2.withColumnRenamed("count","mojix_blink_count")


# In[97]:


#df_agg_1.show()


# In[98]:


#spark.conf.set("spark.sql.debug.maxToStringFields", 1000)


# In[104]:


#df_agg_1.write.format('bigquery') \
 # .option('table', 'saas-analytics-io.processed.yt1_event_classify_finops_test_cluster') \
  #.mode('overwrite') \
  #.save()



#df_agg_1.write.format(parquet).save('gs://finops-outputs/events-classify_%s.parquet')%str_day
#df_agg_1.write.mode('append').parquet('gs://finops-outputs/yt1_events-classify')


# In[ ]:

bucket = "finops-outputs"
spark.conf.set('temporaryGcsBucket', bucket)

print("Write data in Big Query Tables")

if (output == 'events_classification'):
    print(output)
    df_agg_1.write.format('bigquery') \
    .option('table', 'saas-analytics-io.processed.yt1_event_classify_finops') \
    .mode('append') \
    .save()
if (output == 'serialNumber_classification'):
    print(output)
    df_agg_2.write.format('bigquery') \
    .option('table', 'saas-analytics-io.processed.yt1_serial_number_event_classify_finops') \
    .mode('append') \
    .save()
if (output == 'All'):
    print(output)
   
    df_agg_1.write.format('bigquery') \
    .option('table', 'saas-analytics-io.processed.yt1_event_classify_finops') \
    .mode('append') \
    .save()
    
    df_agg_2.write.format('bigquery') \
    .option('table', 'saas-analytics-io.processed.yt1_serial_number_event_classify_finops') \
    .mode('append') \
    .save()






