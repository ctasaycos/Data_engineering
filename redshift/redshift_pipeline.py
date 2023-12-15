'''
Pulling data weekely from a redshift source and push it into oracle database
Technology: Databricks, pyspark
'''
# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.dropdown("run_env", "DEV" ,["DEV","QA","PRD","UAT"])
run_env = dbutils.widgets.get("run_env").strip().upper()
print("run_env: ", run_env)

# COMMAND ----------

# COMMAND ----------

obfusc_url,obfusc_conn = jdbc_conn_config(obfusc_host, obfusc_port, obfusc_db,obfusc_username,obfusc_password)
cx_conn = cx_Oracle.connect(obfusc_username+'/'+obfusc_password+'@'+obfusc_host+'/'+obfusc_db,encoding = 'UTF-8', nencoding='UTF-8')

# COMMAND ----------
from pyspark.sql.functions import col,row_number,current_timestamp,lit
from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql import Window, Row
from pyspark.sql.functions import lpad
from pyspark.sql.functions import col, substring
from decimal import Decimal
import pandas as pd
from pyspark.sql.functions import lpad
import datetime
import time
import cx_Oracle




# COMMAND ----------

def TargetDeltaLoad(deltaQuery):
  try:
    cursor = cx_conn.cursor()
    print(deltaQuery)
    cursor.execute(deltaQuery)
    cx_conn.commit()
    return True
  except Exception as e:
    print(e)
    return False

# COMMAND ----------

query="""select a.companyid as companyid2,drug_count,companyname,companyaddress1,companyaddress2,companycity,companycountry,companystate,companytelephone,companypostalcode
from ph_intel.b_obfusc_o_company_companyname a
left join (select companyid, count(distinct drugid) as drug_count
from ph_intel.b_obfusc_drugprogram_companyid
group by companyid ) drug_companyid on a.companyid=drug_companyid.companyid
left join ph_intel.b_obfusc_o_company_companyaddress1 b on a.companyid=b.companyid
left join ph_intel.b_obfusc_o_company_companyaddress2 c on a.companyid=c.companyid
left join ph_intel.b_obfusc_o_company_companycity d on a.companyid=d.companyid
left join ph_intel.b_obfusc_o_company_companycountry e on a.companyid=e.companyid
left join ph_intel.b_obfusc_o_company_companystate f on a.companyid=f.companyid
left join ph_intel.b_obfusc_o_company_companytelephone g on a.companyid=g.companyid
left join ph_intel.b_obfusc_o_company_companypostalcode h on a.companyid=h.companyid
"""
df_ph_intel = spark.read \
     .format("jdbc") \
     .option("url", jdbc_url) \
     .option("query", query) \
     .load()

# COMMAND ----------

# DBTITLE 1,Geo Country
query="""select distinct b.COUNTRY_LU,LABEL from geo_country a
inner join geo_country_lu b on a.COUNTRY_ID=b.COUNTRY_ID"""
df_geo_country = pd.read_sql(query, con=cx_conn)
df_geo_country = spark.createDataFrame(df_geo_country)

# COMMAND ----------

from pyspark.sql.functions import *
df_ph_intel.createOrReplaceTempView("vw_ph_intel")
df_geo_country.createOrReplaceTempView("vw_geo_country")
df_ph_intel2 = spark.sql("""
select
ph_intel.*,
case when geo_country.COUNTRY_LU is not null then LABEL else companycountry end companycountry_2
  From 
  vw_ph_intel  ph_intel
  LEFT JOIN vw_geo_country geo_country ON upper(ph_intel.companycountry)=geo_country.COUNTRY_LU
 """)

# COMMAND ----------
df_ph_intel2.createOrReplaceTempView("df_informa_account")
df_ACCOUNTS_STAGING_RAW = spark.sql("""select DISTINCT 
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.companyid2)), CHAR(13), ''), CHAR(10), ''),1,48)    AS BU_ACCOUNT_KEY,
                                            '1021'                                                                                           AS ERP_ID,
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.companyname)), CHAR(13), ''), CHAR(10), ''),1,90)        AS ACCOUNT_NAME,
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.companyaddress1)), CHAR(13), ''), CHAR(10), ''),1,90)        AS STREET,
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.companyaddress2)), CHAR(13), ''), CHAR(10), ''),1,90)    AS   STR_SUPPL1,
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.companycity)), CHAR(13), ''), CHAR(10), ''),1,90)        AS CITY,
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.companystate)), CHAR(13), ''), CHAR(10), ''),1,90)            AS STATE,
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.companycountry_2)), CHAR(13), ''), CHAR(10), ''),1,40)         AS COUNTRY,
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.companypostalcode)), CHAR(13), ''), CHAR(10), ''),1,10)     AS ZIP_CODE, 
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.companytelephone)), CHAR(13), ''), CHAR(10), ''),1,30) AS TEL_NUMBER,                                                 
                                            current_date()                              AS ACCOUNT_CREATED_DT,
                                            current_date()                                       AS ACCOUNT_UPDATE_DT,
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.companyid2)), CHAR(13), ''), CHAR(10), ''),1,100)         AS BU_ACCOUNT_NBR,
                                            'INFORMA'                                                                                     AS SOURCE,
                                            'INFORMA'                                                                                     AS SRC_SYS_ID,
                                            accnt.drug_count                                                                                     AS CY_AMOUNT
                                            --accnt.PY_AMOUNT                                                                                     AS PY_AMOUNT,
                                            --accnt.P2PY_AMOUNT                                                                                   AS P2PY_AMOUNT,                                                 accnt.P3PY_AMOUNT                                                                                   AS P3PY_AMOUNT
                                      From df_informa_account  accnt
                                   """)

# COMMAND ----------

df_ACCOUNTS_STAGING_RAW = df_ACCOUNTS_STAGING_RAW\
  .withColumn("BU_ACCOUNT_KEY", col("BU_ACCOUNT_KEY")[0:48])\
  .withColumn("ERP_ID", lit('1021').cast(IntegerType()))\
  .withColumn("BU_SECONDARY_KEY", lit('-1'))\
  .withColumn("ACCOUNT_NAME", col("ACCOUNT_NAME")[0:90])\
  .withColumn("STREET", col("STREET")[0:90])\
  .withColumn("STR_SUPPL1", col("STR_SUPPL1")[0:90])\
  .withColumn("CITY", col("CITY")[0:40])\
  .withColumn("STATE", col("STATE")[0:60])\
  .withColumn("COUNTRY", col("COUNTRY")[0:40])\
  .withColumn("ZIP_CODE", col("ZIP_CODE")[0:10])\
  .withColumn("TEL_NUMBER", col("TEL_NUMBER")[0:30])\
  .withColumn("BU_ACCOUNT_NBR", col("BU_ACCOUNT_NBR")[0:100])\
  .withColumn("SOURCE", col("SOURCE")[0:50])\
  .withColumn("SRC_SYS_ID", col("SRC_SYS_ID")[0:50])\
  .withColumn("CY_AMOUNT", col("CY_AMOUNT"))

df_ACCOUNTS_STAGING_RAW = df_ACCOUNTS_STAGING_RAW\
  .withColumn("rno", row_number().over(Window.partitionBy("BU_ACCOUNT_KEY","ERP_ID","BU_SECONDARY_KEY").orderBy("BU_ACCOUNT_KEY","ERP_ID","BU_SECONDARY_KEY")))\
  .where(col("rno")==1) \
  .drop("rno")

# COMMAND ----------

stageTable='ACCOUNTS_STAGING_RAW'

# COMMAND ----------

#display(df_ACCOUNTS_STAGING_RAW)
customer_count = df_ACCOUNTS_STAGING_RAW.count()
customer_count

# COMMAND ----------


# COMMAND ----------

df_ACCOUNTS_STAGING_RAW.write.jdbc(url=obfusc_url,table=stageTable,mode="append",properties=obfusc_conn)
customer_count = df_ACCOUNTS_STAGING_RAW.count()

# COMMAND ----------

server = smtplib.SMTP('smtprelay1.tt.com')
server.ehlo()
server.starttls()
sender = "ofbsuca_databrick@tt.com"
recipient =  ['1@tt.com', '12@tt.com']
msg = MIMEMultipart()
msg['Subject'] = 'obfusca'
msg['From'] = sender
msg['To'] = ", ".join(recipient)
msg.attach(MIMEText("Customer count: {}".format(customer_count)
))
server.sendmail(sender, recipient, msg.as_string())
server.close()

# COMMAND ----------

