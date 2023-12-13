#!/usr/bin/env python
# coding: utf-8

# In[1]:


import findspark
findspark.init()
import pyspark
sc = pyspark.SparkContext()


# In[2]:


from pyspark import SparkConf,SparkContext,SparkFiles
from pyspark.sql.functions import col, isnan,to_date
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql import SparkSession, functions as F
from urllib.request import urlopen
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, ArrayType
import os


# In[3]:


os.environ["HADOOP_HOME"] = "file:///C:/hadoop/bin/"
github_csv_url = "https://raw.githubusercontent.com/ctasaycos/Python/main/Data/owid-covid-data.csv"
spark = SparkSession.builder\
.master("local")\
.appName("GitHubCSV")\
.config("spark.sql.warehouse.dir", "file:///C:/Users/ctasa/Pyspark/")\
.getOrCreate()
spark.sparkContext.addFile(github_csv_url)
sparkDF=spark.read.csv(SparkFiles.get("owid-covid-data.csv"), header=True)


# In[4]:


df_filtered = sparkDF.filter(~isnan(col("total_deaths")))


# In[5]:


#The stakeholder just one some fields
df_filtered=df_filtered.select(col("index"),\
                   col("iso_code"),\
                   col("location"),\
                   col("total_deaths"),\
                   col("new_deaths"),\
                   col("median_age"),\
                   col("aged_70_older"),\
                   to_date(col("date"),"yyyy-MM-dd").alias("date_formatted"))
df_filtered=df_filtered\
    .where((col("date_formatted")>="2020-01-01") & (col("total_deaths")>5) )


# In[6]:


csv_path = "file:///C:/Users/ctasa/Pyspark/owid_covid/csv"
df_filtered.write.csv(csv_path, header=True, mode="overwrite")

