#!/usr/bin/env python
# coding: utf-8



from google.cloud import bigquery
import pandas as pd
import pandas_gbq
import requests
import datetime
import re
import json
from datetime import date, timedelta
#client = bigquery.Client(location="us-central1")
client = bigquery.Client(location="US")
#print("Client creating using default project: {}".format(client.project))
pd.set_option('display.width', 1000)
import pyspark as ps
from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, expr,first, last,when, split, col,lit, concat, date_format,to_utc_timestamp,from_utc_timestamp,to_timestamp, regexp_replace
from pyspark.sql.functions import sum as _sum



spark = SparkSession.builder.appName("StorageAllocation").getOrCreate()


current_date = datetime.datetime.today()

dias_atras = 3
#dias_atras = int(dias_atras)

str_day_process = (current_date - timedelta(days = dias_atras)).strftime("%Y-%m-%d")
str_day_process_dt = pd.to_datetime(str_day_process)
str_lastday = (str_day_process_dt - timedelta(days = 1)).strftime("%Y-%m-%d")
str_today = (str_day_process_dt + timedelta(days = 1)).strftime("%Y-%m-%d")

#STR_YEARMONTH = (current_date - timedelta(days = dias_atras)).strftime("%Y%m")
#str_day_process = "2022-07-25"
#STR_YEARMONTH = "201908"
print("Dia Analizado --> ",str_day_process," ; ","extraido de :",str_lastday," - ",str_today)


# ## YT1 
print("Start YT1")
environment = 'YT1'


# ### Read YT1 Mongo DB Collection

df_mongo_yt1 = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','raw.yt1_mongodb_collection').option("filter","date between '%s 00:00:00' and '%s 23:59:59' "%(str_day_process,str_day_process)).load()

df_mongo_yt1 = df_mongo_yt1.withColumn("environment",lit(environment))
df_mongo_yt1 = df_mongo_yt1.withColumn('date_normalized', to_date(col("date")))
df_mongo_agg = df_mongo_yt1.groupBy(['date_normalized','tenant','real_name','environment']).agg(_sum('size_db').alias('size_db'),_sum('size_storage').alias('size_storage'),_sum('size_index').alias('size_index'))


# ### YT1 Storage Cost Extraction - GCP

environment = 'YT1'
project_number = '260718309650'

GCP_cost_table = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','finance.gcp_billing_export_v1_01FE23_B1D2D8_10D34D').option("filter","JSON_VALUE(project.number) = '%s' and usage_start_time between '%s 00:00:00' and '%s 23:59:59' "%(project_number,str_lastday, str_today)).load()

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


GCP_cost_table = GCP_cost_table.withColumn("storage_cost",when((GCP_cost_table['sku_description'].like('%SSD%')),GCP_cost_table['cost']))
GCP_cost_table = GCP_cost_table.withColumn("storage_discount",when((GCP_cost_table['sku_description'].like('%SSD%')),GCP_cost_table['credits_discounts']))
GCP_cost_table = GCP_cost_table.withColumn('total_storage_after_discount', GCP_cost_table['storage_cost'] + GCP_cost_table['credits_discounts'])


GCP_cost_table = GCP_cost_table.filter((GCP_cost_table['sku_description'].like('%SSD%')))



GCP_cost_table_agg = GCP_cost_table.groupBy(['usage_start_time']).agg(_sum('storage_cost').alias('daily_storage_cost_sum'),_sum('total_storage_after_discount').alias('daily_storage_after_discount_sum'),_sum('credits_promotion').alias('daily_promotion_credits'))

df_cost_storage_gcp = GCP_cost_table.groupBy(['usage_start_time','sku_description']).agg(_sum('storage_cost').alias('daily_storage_cost_sum'),_sum('storage_discount').alias('daily_storage_discount_sum'),_sum('total_storage_after_discount').alias('daily_storage_after_discount_sum'))

df_cost_storage_gcp = df_cost_storage_gcp.withColumn('date_normalized', to_date(from_utc_timestamp(col("usage_start_time"),"America/Los_Angeles")))

df_cost_storage_gcp_agg = df_cost_storage_gcp.filter((df_cost_storage_gcp['date_normalized']==str_day_process)).groupBy(['date_normalized']).agg(_sum('daily_storage_cost_sum').alias('daily_storage_cost_sum'), _sum('daily_storage_discount_sum').alias('daily_storage_discount_sum'),_sum('daily_storage_after_discount_sum').alias('daily_storage_after_discount_sum'))


# ### YT1 JOIN Storage Usage with GCP Costs

df_results = df_mongo_agg.join(df_cost_storage_gcp_agg,['date_normalized'],"left")
df_results = df_results.join(df_mongo_agg.groupBy('date_normalized').agg(_sum('size_db').alias('size_db_tot_day'),_sum('size_storage').alias('size_storage_tot_day'),_sum('size_index').alias('size_index_tot_day')),['date_normalized'],"left")


df_results = df_results.withColumn("storage_cost_assigned",((df_results['daily_storage_cost_sum']/df_results['size_storage_tot_day'])*df_results['size_storage']))
df_results = df_results.withColumn("storage_cost_assigned_after_discounts",((df_results['daily_storage_after_discount_sum']/df_results['size_storage_tot_day'])*df_results['size_storage']))
df_results = df_results.withColumn("size_storage_percentage",((df_results['size_storage']/df_results['size_storage_tot_day'])*100))


df_results_upload = df_results[['environment','date_normalized','tenant','real_name',
                                'size_storage','size_storage_percentage',
                                'storage_cost_assigned','storage_cost_assigned_after_discounts']]


print ("Upload data to bigQuery")

bucket = "finops-outputs"
spark.conf.set('temporaryGcsBucket', bucket)
print("Write YT1 data in Big Query Tables")

df_results_upload.write.format('bigquery')\
.option('table', 'saas-analytics-io.processed.storage_cost_assign_v2')\
.mode('append')\
.save()


# ## VT1 

print("Start VT1")
environment = 'VT1'


# ### Read VT1 Mongo DB Collection


df_mongo_vt1 = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','raw.vt1_mongodb_collection').option("filter","date between '%s 00:00:00' and '%s 23:59:59' "%(str_day_process,str_day_process)).load()

df_mongo_vt1 = df_mongo_vt1.withColumn("environment",lit(environment))
df_mongo_vt1 = df_mongo_vt1.withColumn('date_normalized', to_date(col("date")))
df_mongo_agg = df_mongo_vt1.groupBy(['date_normalized','tenant','real_name','environment'])                        .agg(_sum('size_db').alias('size_db'),                         _sum('size_storage').alias('size_storage'),                         _sum('size_index').alias('size_index'))


# ### VT1 Storage Cost Extraction - GCP

environment = 'VT1'
project_number = '884190693971'

GCP_cost_table = spark.read.format('bigquery').option('project','saas-analytics-io').option('table','finance.gcp_billing_export_v1_01FE23_B1D2D8_10D34D').option("filter","JSON_VALUE(project.number) = '%s' and usage_start_time between '%s 00:00:00' and '%s 23:59:59' "%(project_number,str_lastday, str_today)).load()

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

GCP_cost_table = GCP_cost_table.withColumn("storage_cost",when((GCP_cost_table['sku_description'].like('%SSD%')),GCP_cost_table['cost']))
GCP_cost_table = GCP_cost_table.withColumn("storage_discount",when((GCP_cost_table['sku_description'].like('%SSD%')),GCP_cost_table['credits_discounts']))
GCP_cost_table = GCP_cost_table.withColumn('total_storage_after_discount', GCP_cost_table['storage_cost'] + GCP_cost_table['credits_discounts'])


GCP_cost_table = GCP_cost_table.filter((GCP_cost_table['sku_description'].like('%SSD%')))



GCP_cost_table_agg = GCP_cost_table.groupBy(['usage_start_time']).agg(_sum('storage_cost').alias('daily_storage_cost_sum'),_sum('total_storage_after_discount').alias('daily_storage_after_discount_sum'),_sum('credits_promotion').alias('daily_promotion_credits'))

df_cost_storage_gcp = GCP_cost_table.groupBy(['usage_start_time','sku_description']).agg(_sum('storage_cost').alias('daily_storage_cost_sum'),_sum('storage_discount').alias('daily_storage_discount_sum'),_sum('total_storage_after_discount').alias('daily_storage_after_discount_sum'))

df_cost_storage_gcp = df_cost_storage_gcp.withColumn('date_normalized', to_date(from_utc_timestamp(col("usage_start_time"),"America/Los_Angeles")))

df_cost_storage_gcp_agg = df_cost_storage_gcp.filter((df_cost_storage_gcp['date_normalized']==str_day_process)).groupBy(['date_normalized']).agg(_sum('daily_storage_cost_sum').alias('daily_storage_cost_sum'),_sum('daily_storage_discount_sum').alias('daily_storage_discount_sum'),_sum('daily_storage_after_discount_sum').alias('daily_storage_after_discount_sum'))


# ### VT1 JOIN Storage Usage with GCP Costs

df_results = df_mongo_agg.join(df_cost_storage_gcp_agg,['date_normalized'],"left")
df_results = df_results.join(df_mongo_agg.groupBy('date_normalized').agg(_sum('size_db').alias('size_db_tot_day'),_sum('size_storage').alias('size_storage_tot_day'),_sum('size_index').alias('size_index_tot_day')),['date_normalized'],"left")


df_results = df_results.withColumn("storage_cost_assigned",((df_results['daily_storage_cost_sum']/df_results['size_storage_tot_day'])*df_results['size_storage']))
df_results = df_results.withColumn("storage_cost_assigned_after_discounts",((df_results['daily_storage_after_discount_sum']/df_results['size_storage_tot_day'])*df_results['size_storage']))
df_results = df_results.withColumn("size_storage_percentage",((df_results['size_storage']/df_results['size_storage_tot_day'])*100))


df_results_upload = df_results[['environment','date_normalized','tenant','real_name',
                                'size_storage','size_storage_percentage',
                                'storage_cost_assigned','storage_cost_assigned_after_discounts']]


print("Upload data to bigQuery")

bucket = "finops-outputs"
spark.conf.set('temporaryGcsBucket', bucket)
print("Write VT1 data in Big Query Tables")

df_results_upload.write.format('bigquery') \
    .option('table', 'saas-analytics-io.processed.storage_cost_assign_v2') \
    .mode('append') \
    .save()

print("JOB FINISHED SUCCESSFUL!!")
df_results_upload.show()
