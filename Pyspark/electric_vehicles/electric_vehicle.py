#!/usr/bin/env python
# coding: utf-8

# In[1]:


#Start spark
import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext()


# In[2]:


#Start spark Import libraries
from pyspark import SparkConf, SparkContext,SparkFiles
from pyspark.sql.functions import col, isnan,to_date,explode,concat_ws,expr,split,trim, length
from pyspark.sql import SparkSession, functions as F
from urllib.request import urlopen
import os


# In[3]:


#Configuration of spark session, take in account that this is running on a Windows machine.
#In mac os x and linus most likely this configuration must be changed
os.environ["HADOOP_HOME"] = "file:///C:/hadoop/bin/"
spark = SparkSession.builder \
    .appName("YourAppName") \
    .config("spark.driver.memory", "6g") \
    .config("spark.executor.memory", "6g") \
    .getOrCreate()
#Data is in this github repository
vehicle_json="https://raw.githubusercontent.com/ctasaycos/Python/main/Data/electric_vehicle_population_data.json"
jsonData = urlopen(vehicle_json).read().decode('utf-8')
rdd = spark.sparkContext.parallelize([jsonData])
df = spark.read.json(rdd)


# In[4]:


#The nested json file has metadata and data separately
df_transformed = df.select(
    col("data"),
    col("meta.view.approvals").alias("approvals"),
    col("meta.view.assetType").alias("assetType"),
    col("meta.view.attribution").alias("attribution"),
    col("meta.view.averageRating").alias(""),
    col("meta.view.category").alias("category"),
    col("meta.view.clientContext").alias("clientContext"),
    col("meta.view.columns").alias("columns"),
    col("meta.view.createdAt").alias("createdAt"),
    col("meta.view.description").alias("description"),
    col("meta.view.displayType").alias("displayType"),
    col("meta.view.downloadCount").alias("downloadCount"),
    col("meta.view.flags").alias("viewFlags"),
    col("meta.view.grants").alias("grants"),
    col("meta.view.hideFromCatalog").alias("hideFromCatalog"),
    col("meta.view.hideFromDataJson").alias("hideFromDataJson"),
    col("meta.view.id").alias("viewId"),
    col("meta.view.license").alias("license"),
    col("meta.view.licenseId").alias("licenseId"),
    col("meta.view.metadata").alias("metadata"),
    col("meta.view.name").alias("viewName"),
    col("meta.view.newBackend").alias("newBackend"),
    col("meta.view.numberOfComments").alias("numberOfComments"),
    col("meta.view.oid").alias("oid"),
    col("meta.view.owner").alias("owner"),
    col("meta.view.provenance").alias("provenance"),
    col("meta.view.publicationAppendEnabled").alias("publicationAppendEnabled"),
    col("meta.view.publicationDate").alias("publicationDate"),
    col("meta.view.publicationGroup").alias("publicationGroup"),
    col("meta.view.publicationStage").alias("publicationStage"),
    col("meta.view.rights").alias("rights"),
    col("meta.view.rowsUpdatedAt").alias("rowsUpdatedAt"),
    col("meta.view.rowsUpdatedBy").alias("rowsUpdatedBy"),
    col("meta.view.tableAuthor").alias("tableAuthor"),
    col("meta.view.tableId").alias("tableId"),
    col("meta.view.tags").alias("tags"),
    col("meta.view.totalTimesRated").alias("totalTimesRated"),
    col("meta.view.viewCount").alias("viewCount"),
    col("meta.view.viewLastModified").alias("viewLastModified"),
    col("meta.view.viewType").alias("viewType")
)


# In[5]:


#In this case we only need the data and columns (this field contains the headers)
df_transformed=df_transformed.select("data","columns")


# In[6]:


#We get the headers and stored them in a list, also cleaned the data
df_headers=df_transformed.select("columns", explode("columns").alias("exploded_column")) \
    .select("exploded_column.id", "exploded_column.name", "exploded_column.description") \
    .filter((col("description").isNotNull()) & (length(trim(col("description"))) > 0))
list_headers = df_headers.select("name").rdd.flatMap(lambda x: x).collect()


# In[7]:


#We only need relevant data we will not pull all.
df_data = (
    df_transformed
    .select(explode("data").alias("flattened_data"))
    .withColumn(
        "processed_data",
        expr("CONCAT_WS(', ', transform(flattened_data, x -> IFNULL(x, '')))")
    )
    .select(
        split(col("processed_data"), ", ")[8].alias("col9"),
        split(col("processed_data"), ", ")[9].alias("col10"),
        split(col("processed_data"), ", ")[10].alias("col11"),
        split(col("processed_data"), ", ")[11].alias("col12"),
        split(col("processed_data"), ", ")[12].alias("col13"),
        split(col("processed_data"), ", ")[13].alias("col14"),
        split(col("processed_data"), ", ")[14].alias("col15"),
        split(col("processed_data"), ", ")[15].alias("col16"),
        split(col("processed_data"), ", ")[16].alias("col17"),
        split(col("processed_data"), ", ")[17].alias("col18"),
        split(col("processed_data"), ", ")[18].alias("col19"),
        split(col("processed_data"), ", ")[19].alias("col20"),
        split(col("processed_data"), ", ")[20].alias("col21"),
        split(col("processed_data"), ", ")[21].alias("col22"),
        split(col("processed_data"), ", ")[22].alias("col23"),
        split(col("processed_data"), ", ")[23].alias("col24"),
        split(col("processed_data"), ", ")[24].alias("col25")
    )
)


# In[8]:


#Use the real headers
df_export = df_data.toDF(*list_headers)


# In[12]:


#In this project we push the data in parquet
local_parquet_path = "file:///C:/Users/ctasa/Pyspark/electric_vehicle_population_data/parquet"
df_export.write.parquet(local_parquet_path, mode="overwrite")


# In[16]:


#To display to the stakeholders we show a sample of the data in csv
df_sample = df_export.sample(fraction=0.01, seed=42)
csv_path = "file:///C:/Users/ctasa/Pyspark/electric_vehicle_population_data/csv"
df_sample.write.csv(csv_path, header=True, mode="overwrite")

