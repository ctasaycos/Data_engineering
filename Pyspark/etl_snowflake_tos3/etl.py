'''
ETL Pipeline from snowflake to S3
Data source: Snowflake, S3 Buckets AWS
Dinamically pull product, invoices, orders, customer data from some views in Snowflake, for each fiscal month and push it to Oracle
'''
# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.dropdown("run_env", "PRD" ,["DEV","QA","PRD","UAT"])

# COMMAND ----------

run_env = dbutils.widgets.get("run_env").strip().upper()
print("run_env: ", run_env)

# COMMAND ----------

# MAGIC %run  ./Connection_CRG_MOSAIC $env = run_env

# COMMAND ----------

# MAGIC %sql
# MAGIC set spark.sql.sources.commitProtocolClass=org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol

# COMMAND ----------

# DBTITLE 1,Setting configurations
spark.conf.set("spark.databricks.io.cache.enabled", True)
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "LEGACY")
spark.conf.set("spark.sql.legacy.typeCoercion.datetimeToString.enabled", True)
spark.conf.set('spark.sql.sources.commitProtocolClass','org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol')
spark.conf.set('spark.sql.sources.commitProtocolClass','org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol')
spark.conf.set('mapreduce.fileoutputcommitter.marksuccessfuljobs',False)
spark.conf.set('mapreduce.fileoutputcommitter.marksuccessfuljobs',False)
spark.conf.set('spark.sql.sources.partitionOverwriteMode','dynamic')
spark.conf.set("parquet.enable.summary-metadata", False)
spark.conf.set("spark.sql.shuffle.partitions",50)
spark.conf.set("spark.sql.adaptive.enabled", True)
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

# COMMAND ----------

# DBTITLE 1,Import Library
import datetime
import time
import cx_Oracle
from pyspark.sql.types import StructType, IntegerType, StringType,DecimalType
from pyspark.sql.functions import lit, substring, to_date, to_timestamp, current_date, trim, regexp_replace
from datetime import date
from pyspark.sql.functions import col,row_number,current_timestamp,lit
from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql import Window, Row
from pyspark.sql.functions import lpad
from pyspark.sql.functions import col, substring
from decimal import Decimal

# COMMAND ----------

# DBTITLE 1,Define Functions
def oracle_conn_config(host,port,db,username,password):
  jdbcUrl = "jdbc:oracle:thin:@//{0}:{1}/{2}".format(host, port, db)
  connectionProperties = {
    "user" : username,
    "password" : password,
    "driver" : "oracle.jdbc.driver.OracleDriver",
    "oracle.jdbc.timezoneAsRegion" : "false",
    "fetchsize":"100000"}
  return (jdbcUrl,connectionProperties)

def snowflake_conn_config(host,username,password,schema,database,account,sf_role):  
      options = {
        "sfUrl":host,
          "sfUser":username,
          "sfPassword":password,
          "sfWarehouse":warehouse,
          "sfDatabase":database,
          "sfSchema":schema,
          "sfRole" : sf_role,
          "sfAccount":"pp.us-east-1"
      }
      return options

# COMMAND ----------

# DBTITLE 1,cc Database Instance Connection
cc_url, cc_conn = oracle_conn_config(cc_host, cc_port, cc_db, cc_dw_user_scratch_username, cc_dw_user_scratch_password)

# COMMAND ----------

# DBTITLE 1,Snowflake Connection
prop = snowflake_conn_config(source_host,source_username,source_password,schema_name,source_db,source_host[:13],sfRole)
print(prop)

# COMMAND ----------

# DBTITLE 1,Load the Customers
sf_account_query = "select * from "+  schema_name+"."+ db_table_name
df_customer = spark.read.format("snowflake").options(**prop).option("query",sf_account_query).load()
df_customer.createOrReplaceTempView("crg_snflk_sf_accounts")

display(df_customer)

# COMMAND ----------

df_scratch_bu_account = spark.sql("""select DISTINCT 
                                           '968'                                                                                           AS ERP_ID,
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.PARTY_NUMBER)), CHAR(13), ''), CHAR(10), ''),1,48)    AS BU_ACCOUNT_KEY,
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.PARTY_NAME)), CHAR(13), ''), CHAR(10), ''),1,90)      AS BU_ACCOUNT,
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.ADDRESS1)), CHAR(13), ''), CHAR(10), ''),1,90)        AS ADDRESS1,
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.ADDRESS2)), CHAR(13), ''), CHAR(10), ''),1,90)        AS ADDRESS2,
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.ADDRESS3)), CHAR(13), ''), CHAR(10), ''),1,90)        AS ADDRESS3,
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.ADDRESS4)), CHAR(13), ''), CHAR(10), ''),1,90)        AS ADDRESS4,
                                            SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.CITY)), CHAR(13), ''), CHAR(10), ''),1,90)            AS CITY_TOWN,
                                            case when state is not null 
                                                 then SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(STATE)), CHAR(13), ''), CHAR(10), ''),1,32)  
                                                 else SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(PROVINCE)), CHAR(13), ''), CHAR(10), ''),1,32)   
                                              end                                                                                           AS STATE_PROVINCE,
                                           case when COUNTRY = 'US' and length(POSTAL_CODE)<5 
                                                then lpad(POSTAL_CODE,5,0) 
                                                else POSTAL_CODE 
                                             end                                                                                            AS POSTAL_CODE,
                                           REPLACE(REPLACE(RTRIM(LTRIM(accnt.COUNTRY)), CHAR(13), ''), CHAR(10), '')                        AS COUNTRY,
                                           current_date()                              AS RECORD_CREATION_TIMESTAMP,
                                           'P'                                         AS SENT_TO_STAGING_STATUS,
                                           SUBSTR(REPLACE(REPLACE(RTRIM(LTRIM(accnt.TAX_REGISTRATION_NUMBER)), CHAR(13), ''), CHAR(10), ''),1,60)  AS TAX_ID,
                                           REPLACE(REPLACE(RTRIM(LTRIM(accnt.PARTY_NUMBER)), CHAR(13), ''), CHAR(10), '')                   AS BU_ACCOUNT_NBR,
                                           'CRG_MOSAIC'                                                                                     AS SRC_SYS_ID
                                      From crg_snflk_sf_accounts  accnt
                                   """)

df_scratch_bu_account = df_scratch_bu_account\
  .withColumn("rno", row_number().over(Window.partitionBy("ERP_ID","BU_ACCOUNT_KEY").orderBy("ERP_ID","BU_ACCOUNT_KEY")))\
  .where(col("rno")==1) \
  .drop("rno")

# COMMAND ----------

# DBTITLE 1,Load the customers into cc Scratch table
stageTable='SCRATCH_NEW_BU_ACCOUNT'
df_scratch_bu_account.write.jdbc(url=cc_url,table=stageTable,mode="append",properties=cc_conn)
customer_count = df_scratch_bu_account.count()

# COMMAND ----------
#Dinamic Fiscal month
query = """(
  select
    to_number (           
                (
                  select min(date_id) fiscal_month_start
                  from dim_date
                  where fiscal_month_id
                        = (
                            select fiscal_month_id
                            from dim_date
                            where fiscal_month = to_char(date_of_year, 'Month')
                            and date_of_year <= add_months(sysdate+3, -1)
                            and date_of_year
                                = (
                                    select max(date_of_year)
                                    from dim_date
                                    where fiscal_month = to_char(date_of_year, 'Month')
                                    and date_of_year <= add_months(sysdate+3, -1)) )
                                    and fiscal_year
                                        = (
                                            select max(fiscal_year)
                                            from dim_date
                                            where fiscal_month = to_char(date_of_year, 'Month')
                                            and date_of_year <= add_months(sysdate+3, -1)
                                           )
                                   )
                                                         
                           ) fiscal_start,
    to_number (    
                (
                  select max(date_id) fiscal_month_end
                  from dim_date
                  where fiscal_month_id
                        = (
                            select fiscal_month_id
                            from dim_date
                            where fiscal_month = to_char(date_of_year, 'Month')
                            and date_of_year <= add_months(sysdate+3, -1)
                            and date_of_year
                                = (
                                    select max(date_of_year)
                                    from dim_date
                                    where fiscal_month = to_char(date_of_year, 'Month')
                                    and date_of_year <= add_months(sysdate+3, -1)) )
                                    and fiscal_year
                                        = (
                                            select max(fiscal_year)
                                            from dim_date
                                            where fiscal_month = to_char(date_of_year, 'Month')
                                            and date_of_year <= add_months(sysdate+3, -1)
                                           )
                                   )
                           ) fiscal_end
  from dual
)
 """

df2 = spark.read.jdbc(url=cc_url, table=query, properties=cc_conn)
df2 = df2.select(col("fiscal_start").cast(IntegerType()).alias("fiscal_start"),
                 col("fiscal_end").cast(IntegerType()).alias("fiscal_end")
                )
startDate2 = df2.select('FISCAL_START').first()[0]
endDate2 = df2.select('FISCAL_END').first()[0]

# COMMAND ----------

print(startDate2)
print(endDate2)

# COMMAND ----------

# DBTITLE 1,Invoice Load
from pyspark.sql.functions import col,row_number,current_timestamp,lit
from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql import Window, Row
vw_tf_billing_query_request = "select * from "+  schema_name+"."+ db_billing_name
df_invoices= spark.read.format("snowflake").options(**prop).option("query",vw_tf_billing_query_request).load()
df_invoices = df_invoices.select(col("*")).where( ( upper(trim(col("INVOICE_STATUS_CODE"))) == 'ACCEPTED') & ( upper(trim(col("LEVEL_TYPE"))) == 'BILLING' )  )
df_invoices = df_invoices.withColumn('invoice_date_1',date_format("AR_DATE","MM/dd/yyyy"))
df_invoices = df_invoices.withColumn('invoice_date_id', date_format(col("AR_DATE"), 'yyyyMMdd').cast(IntegerType() ))
df_invoices = df_invoices.filter( ((df_invoices.invoice_date_id >= startDate2) & (df_invoices.invoice_date_id <= endDate2)))
df_invoices = df_invoices.withColumn('ERP_RECORD_CREATION_DATE',to_date(col("DRAFT_DATE"), 'yyyyMMdd'))
df_invoices = df_invoices.withColumn("INVOICE_NUMBER", concat_ws('|',col("CONTRACT_LINE"),col("AR_INVOICE_CREDIT_NUMBER"), col("BUSINESS_UNIT")))
df_invoices = df_invoices.withColumn("PRODUCT_KEY",when( trim(col("CONTRACT_LINE")).rlike("Investigator|Passthrough"), concat_ws('|',lit("INDIR"),col("CONTRACT_LINE"),substring(col("LOB"),1,12)) ).otherwise(concat_ws('|',lit("DIR"), lit("Direct"),substring(col("LOB"),1,12))))
df_invoices = df_invoices.withColumn("PRODUCT", when(trim(col("CONTRACT_LINE")).rlike("Investigator|Passthrough"), concat_ws('|',lit("INDIR"),col("CONTRACT_LINE"),substring(col("LOB"),1,12)) ).otherwise(concat_ws('|',lit("DIR"), lit("Direct"),col("LOB"))))
df_invoices = df_invoices.withColumn("Legal_Entity",regexp_replace(col("LEGAL_ENTITY_NAME"), '[^0-9a-zA-Z_\-]+', '') )
df_invoices = df_invoices.withColumn("ILI_AMOUNT", regexp_replace(col("TOTAL_INVOICE_CREDIT_AMOUNT_NET") , ',', ''))

# COMMAND ----------

df_crg_invoices = df_invoices.select(col("INVOICE_NUMBER").alias("INVOICE_NUMBER"),
                                 col("invoice_date_1").alias("INVOICE_DATE"),
                                 col("CUSTOMER_NUMBER").alias("BU_ACCOUNT_KEY"),
                                 col("CUSTOMER_NUMBER").alias("SOLDTO_KEY"),
                                 col("CUSTOMER_NUMBER").alias("BILLTO_KEY"),
                                 col("CUSTOMER_NUMBER").alias("SHIPTO_KEY"),
                                 col("INVOICE_CURRENCY").alias("CURRENCY_KEY"),
                                 col("PRODUCT_KEY").alias("PRODUCT_KEY"),
                                 col("ERP_RECORD_CREATION_DATE").alias("ERP_RECORD_CREATION_DATE"),
                                 col("LOB").alias("business_unit"),
                                 col("Legal_Entity").alias("Legal_Entity"),
                                 col("ILI_AMOUNT").alias("ILI_AMOUNT")                                 
                                )
df_crg_invoices.createOrReplaceTempView("crg_invoices")

# COMMAND ----------

df_business_unit = spark.read \
      .format("jdbc") \
      .option("driver", "oracle.jdbc.OracleDriver")\
      .option("url", cc_url) \
      .option("user", cc_dw_user_scratch_username) \
      .option("password", cc_dw_user_scratch_password)\
      .option("query", "select * FROM dd.business_unit where erp_id = 333") \
      .option("oracle.jdbc.timezoneAsRegion", False) \
      .option('fetchsize','1000')\
      .load()

df_business_unit = df_business_unit.select(col("bu_id").alias("bu_id"), col("BUSINESS_UNIT").alias("BUSINESS_UNIT"))

df_business_unit = df_business_unit\
  .withColumn("rno", row_number().over(Window.partitionBy("BUSINESS_UNIT").orderBy("BUSINESS_UNIT")))\
  .where(col("rno")==1) \
  .drop("rno")


df_business_unit.createOrReplaceTempView("business_unit")

# COMMAND ----------

df_hfm_entity = spark.read.format("csv").option("header",True).option('delimiter',',').option('endian', 'little').option('encoding','UTF-8').option("charset", "UTF-8").option("quote", "\"").option("escape", "\"").option("multiLine", "true").load(s3_hfm_entity)
df_hfm_entity = df_hfm_entity.select(col("Legal"), col("HFM Entity").alias("HFM_Entity")).distinct()
df_hfm_entity = df_hfm_entity.withColumn("Legal_Entity", regexp_replace(regexp_replace(col("Legal"),'[^0-9a-zA-Z_\-]+', ''),'IndiaPrivateLtd','IndiaPrivateLimited'))
df_hfm_entity = df_hfm_entity\
  .withColumn("rno", row_number().over(Window.partitionBy("Legal_Entity").orderBy("Legal_Entity")))\
  .where(col("rno")==1) \
  .drop("rno")
df_hfm_entity.createOrReplaceTempView("hfm_entity")


# COMMAND ----------

df_SCRATCH_INVOICE_LINE_ITEM = spark.sql("""select  '968'                           AS ERP_ID,
                                                    case when bu.BU_ID is not null 
                                                         then bu.BU_ID 
                                                         else '999'                   
                                                     end                            AS BU_ID,
                                                    inv.INVOICE_NUMBER           AS INVOICE_NUMBER,
                                                    inv.INVOICE_DATE             AS INVOICE_DATE,
                                                    inv.BU_ACCOUNT_KEY           AS BU_ACCOUNT_KEY,
                                                    inv.SOLDTO_KEY               AS SOLDTO_KEY,
                                                    inv.BILLTO_KEY               AS BILLTO_KEY,
                                                    inv.SHIPTO_KEY               AS SHIPTO_KEY,
                                                    inv.CURRENCY_KEY             AS CURRENCY_KEY,                                                    
                                                    inv.PRODUCT_KEY              AS PRODUCT_KEY,
                                                    inv.ERP_RECORD_CREATION_DATE AS ERP_RECORD_CREATION_DATE,
                                                    current_date()               AS RECORD_CREATION_TIMESTAMP,
                                                    current_date()               AS LAST_UPDATED_TIMESTAMP, 
                                                    hfm.HFM_Entity               AS ENTITY_KEY,
                                                    inv.ILI_AMOUNT               AS ILI_AMOUNT
                                              From crg_invoices  inv
                                                          LEFT OUTER JOIN   
                                                   hfm_entity  hfm                                                   
                                                            on inv.Legal_Entity = hfm.Legal_Entity
                                                          LEFT OUTER JOIN 
                                                   business_unit bu 
                                                            on inv.business_unit= bu.business_unit
                                           """
                                        )


# COMMAND ----------

df_SCRATCH_INVOICE_LINE_ITEM_agg = df_SCRATCH_INVOICE_LINE_ITEM.select("ERP_ID","BU_ID","INVOICE_NUMBER", "INVOICE_DATE","BU_ACCOUNT_KEY","SOLDTO_KEY","BILLTO_KEY","SHIPTO_KEY","CURRENCY_KEY","PRODUCT_KEY", "ERP_RECORD_CREATION_DATE","RECORD_CREATION_TIMESTAMP","LAST_UPDATED_TIMESTAMP","ILI_AMOUNT" )\
.groupBy("ERP_ID","BU_ID","INVOICE_NUMBER", "INVOICE_DATE","BU_ACCOUNT_KEY","SOLDTO_KEY","BILLTO_KEY","SHIPTO_KEY","CURRENCY_KEY","PRODUCT_KEY", "ERP_RECORD_CREATION_DATE","RECORD_CREATION_TIMESTAMP","LAST_UPDATED_TIMESTAMP") \
    .agg(sum("ILI_AMOUNT").alias("ILI_AMOUNT") \
     )

# COMMAND ----------

stageTable='scratch_invoice_line_item'
df_SCRATCH_INVOICE_LINE_ITEM_agg.write.jdbc(url=cc_url,table=stageTable,mode="append",properties=cc_conn)

invoice_count = df_SCRATCH_INVOICE_LINE_ITEM_agg.count()
df_invoice_agg = df_SCRATCH_INVOICE_LINE_ITEM_agg.select(round(sum("ILI_AMOUNT"), scale=2).alias("total_invoice_amount"))
invoice_amount = df_invoice_agg.collect()[0][0]
amount = invoice_amount

# COMMAND ----------

df_crg_products = df_invoices.select(col("PRODUCT").alias("PRODUCT"), col("PRODUCT_KEY").alias("PRODUCT_KEY")).distinct()
df_crg_products = df_crg_products\
  .withColumn("rno", row_number().over(Window.partitionBy("PRODUCT_KEY").orderBy("PRODUCT_KEY")))\
  .where(col("rno")==1) \
  .drop("rno")

# COMMAND ----------

df_scratch_bu_product = df_crg_products.select(col("PRODUCT_KEY").alias("BU_PRODUCT_KEY"), col("PRODUCT").alias("PRODUCT_NAME"), col("PRODUCT").alias("PRODUCT_DESCRIPTION"))
df_scratch_bu_product = df_scratch_bu_product.withColumn("ERP_ID",lit("968"))

# COMMAND ----------

stageTable='scratch_bu_product'
df_scratch_bu_product.write.jdbc(url=cc_url,table=stageTable,mode="append",properties=cc_conn)
product_count = df_scratch_bu_product.count()

# COMMAND ----------

# DBTITLE 1,Snowflake Connection for Authorizations
crg_auth_prop = snowflake_conn_config(crg_source_host,crg_source_username,crg_source_password,crg_schema_name,crg_source_db,crg_source_host[:13],crg_sfRole)
print(crg_auth_prop)

# COMMAND ----------

sf_order_query = "select * from "+  crg_schema_name+"."+ crg_db_table_name
print(sf_order_query)
df_crg_snflk_sf_orders = spark.read.format("snowflake").options(**crg_auth_prop).option("query",sf_order_query).load()

display(df_crg_snflk_sf_orders)

# COMMAND ----------

df_crg_snflk_sf_orders = df_crg_snflk_sf_orders.withColumn('period_end_dt_id', date_format(col("period_end_dt"), 'yyyyMMdd').cast(IntegerType() ))
df_crg_snflk_sf_orders = df_crg_snflk_sf_orders.withColumn('Period_End_Date', date_format(col("period_end_dt"), 'yyyyMM').cast(IntegerType() ))
df_crg_snflk_sf_orders = df_crg_snflk_sf_orders.filter( ((df_crg_snflk_sf_orders.period_end_dt_id >= startDate2) & (df_crg_snflk_sf_orders.period_end_dt_id <= endDate2))   )
df_crg_snflk_sf_orders = df_crg_snflk_sf_orders.filter( df_crg_snflk_sf_orders.NET_AUTH_AMT_605 !=0)

df_crg_snflk_sf_orders = df_crg_snflk_sf_orders.select(col("BUS_UNIT_CLIENT_NM"),
                                                       col("NET_AUTH_AMT_605"),
                                                       col("BC_NUMBER"),
                                                       col("CONSOLIDTD_BU"),
                                                       col("PERIOD_END_DT"),
                                                       col("Period_End_Date"),
                                                       col("INDIRECT_INIT_AUTH_AMT_AWARDED"),
                                                       col("INDIRECT_REV_PROPSL_AMT_AWARDED"),
                                                       col("INDIRECT_CANCELN_REV_PROPSL_AMT_AWARDED"),
                                                       col("INDIRECT_CON_MOD_AMT_AWARDED"),
                                                       col("INDIRECT_ADJ_AMT_AWARDED"),
                                                       col("INDIRECT_CANCELN_AMT_AWARDED"),
                                                       col("INDIRECT_FX_AMT_AWARDED")
                                                     )
df_crg_snflk_sf_orders = df_crg_snflk_sf_orders.withColumn("Total_indirect_amt",
                                                          coalesce(col("INDIRECT_INIT_AUTH_AMT_AWARDED"),lit(0) ) +
                                                          coalesce(col("INDIRECT_REV_PROPSL_AMT_AWARDED"),lit(0) ) + 
                                                          coalesce(col("INDIRECT_CANCELN_REV_PROPSL_AMT_AWARDED") ,lit(0) ) +
                                                          coalesce(col("INDIRECT_CON_MOD_AMT_AWARDED") + col("INDIRECT_ADJ_AMT_AWARDED") ,lit(0) ) +
                                                          coalesce(col("INDIRECT_CANCELN_AMT_AWARDED") ,lit(0) ) +
                                                          coalesce(col("INDIRECT_FX_AMT_AWARDED"),lit(0) ) 
                                                           )
df_crg_snflk_sf_orders = df_crg_snflk_sf_orders.withColumn("bu_account", substring(upper(trim(col("BUS_UNIT_CLIENT_NM")) ),1,90) ) 
df_crg_snflk_sf_orders = df_crg_snflk_sf_orders.withColumn("order_number", substring(coalesce(col("BC_NUMBER"), col("Period_End_Date")),1,20))


# COMMAND ----------

# DBTITLE 1,Unique List of the Customers
df_crg_orders_customers = df_crg_snflk_sf_orders.select(upper(trim(col("bu_account")) ).alias("bu_account")).distinct()

# COMMAND ----------

# DBTITLE 1,Reading the Existing CRG Accounts from cc
df_crg_bu_account = spark.read \
      .format("jdbc") \
      .option("driver", "oracle.jdbc.OracleDriver")\
      .option("url", cc_url) \
      .option("user", cc_dw_user_scratch_username) \
      .option("password", cc_dw_user_scratch_password)\
      .option("query", "select distinct bu_account_key, erp_id, upper(bu_account) AS bu_account FROM dwsa.bu_account where erp_id = 1009") \
      .option("oracle.jdbc.timezoneAsRegion", False) \
      .option('fetchsize','1000')\
      .load()
df_crg_bu_account = df_crg_bu_account.withColumn('id_prefix', split(df_crg_bu_account['bu_account_key'], '_').getItem(0)) \
             .withColumn('id_index', split(df_crg_bu_account['bu_account_key'], '_').getItem(1).cast("int"))
account_max_id= df_crg_bu_account.select(max('id_index')).first()[0]
print(account_max_id)
df_crg_bu_account_1 = df_crg_bu_account.select(col("BU_ACCOUNT_KEY"), col("ERP_ID"),col("BU_ACCOUNT") )
df_crg_old_account = df_crg_bu_account.select( col("bu_account")).distinct()
df_crg_old_account = df_crg_old_account.withColumn("bu_account", upper(col("bu_account")))

# COMMAND ----------

# DBTITLE 1,In case no CRG accounts in bu_account table
if account_max_id is None:
  account_max_id =1
print(account_max_id)

# COMMAND ----------

# DBTITLE 1,Delta between new Accounts and old accounts
df_crg_accounts_delta = df_crg_orders_customers.exceptAll(df_crg_old_account)

# COMMAND ----------

# DBTITLE 1,Generate the account key for the new accounts

if df_crg_accounts_delta.count()>0:  
  df_crg_accounts_delta = df_crg_accounts_delta.withColumn('index', monotonically_increasing_id() + 1 + account_max_id)
  df_crg_accounts_delta = df_crg_accounts_delta.withColumn('index', lpad(df_crg_accounts_delta['index'], 6, '0')) 
  df_crg_accounts_delta = df_crg_accounts_delta.withColumn('prefix_label',lit("CRG"))
  df_crg_accounts_delta = df_crg_accounts_delta.withColumn("bu_account_key", 
                                                            concat_ws('_',df_crg_accounts_delta.prefix_label,df_crg_accounts_delta.index)
                                                          )
  df_crg_accounts_delta = df_crg_accounts_delta.withColumn("erp_id", lit(1009))
  df_crg_accounts_delta = df_crg_accounts_delta.drop('prefix_label')
  df_crg_accounts_delta = df_crg_accounts_delta.drop('index')
  df_crg_accounts_delta = df_crg_accounts_delta.select("bu_account_key", "erp_id", "bu_account")
  df_crg_all_accounts = df_crg_bu_account_1.unionAll(df_crg_accounts_delta)
elif df_crg_accounts_delta.count() == 0:
  df_crg_all_accounts = df_crg_bu_account_1

# COMMAND ----------

# DBTITLE 1,Writing the result back to S3 bucket
df_crg_all_accounts.write.format("parquet").mode("overwrite").parquet(crg_accounts_s3_path)

# COMMAND ----------

# DBTITLE 1,Reading the result from S3 bucket 
df_customer = spark.read.parquet(crg_accounts_s3_path + "*.parquet")

# COMMAND ----------

# DBTITLE 1,Get the CRG BU'S
df_crg_bu_list = spark.read \
      .format("jdbc") \
      .option("driver", "oracle.jdbc.OracleDriver")\
      .option("url", cc_url) \
      .option("user", cc_dw_user_scratch_username) \
      .option("password", cc_dw_user_scratch_password)\
      .option("query", "select * FROM dw_user_scratch.crg_bu_list ") \
      .option("oracle.jdbc.timezoneAsRegion", False) \
      .option('fetchsize','1000')\
      .load()

df_crg_bu_list = df_crg_bu_list.select(col("BU_ID").alias("BU_ID"), col("CONSOLIDATE_BU").alias("CONSOLIDATE_BU"))
df_crg_bu_list = df_crg_bu_list\
  .withColumn("rno", row_number().over(Window.partitionBy("CONSOLIDATE_BU").orderBy("CONSOLIDATE_BU")))\
  .where(col("rno")==1) \
  .drop("rno")

# COMMAND ----------

df_crg_bu_list.createOrReplaceTempView("crg_bu_list")
df_customer.createOrReplaceTempView("crg_accounts")
df_crg_snflk_sf_orders.createOrReplaceTempView("crg_orders")
df_crg_orders = spark.sql("""select ord.*,
                                    accnt.bu_account_key,
                                    crg_bu.bu_id
                               From crg_orders  ord
                                        inner join
                                    crg_accounts accnt
                                           on accnt.bu_account = ord.bu_account
                                        left outer join 
                                    crg_bu_list crg_bu
                                          on crg_bu.CONSOLIDATE_BU = ord.CONSOLIDTD_BU
                          """
                        )

# COMMAND ----------

df_direct_orders = df_crg_orders.select(col("*")).filter(col("NET_AUTH_AMT_605") !=0 )
df_direct_orders = df_crg_orders.select(col("bu_account_key").alias("bu_account_key"),
                                        col("order_number").alias("order_number"),
                                        col("bu_id").alias("bu_id"),  
                                        col("NET_AUTH_AMT_605").alias("oli_amount"), 
                                        col("CONSOLIDTD_BU"),
                                        col("PERIOD_END_DT").alias("order_date")
                                       )
df_direct_orders = df_direct_orders.withColumn("line_number", lit("-1"))
df_direct_orders = df_direct_orders.withColumn("erp_id", lit("1009"))
df_direct_orders = df_direct_orders.withColumn("currency_key", lit("USD"))
df_direct_orders = df_direct_orders.withColumn("product_key", substring(concat_ws('|',lit("DIR"),trim(df_direct_orders.CONSOLIDTD_BU) ),1,32) )
df_direct_orders = df_direct_orders.select("bu_account_key",\
                                           "product_key",\
                                           "order_date",\
                                           "order_number",\
                                           "line_number",\
                                           "erp_id", \
                                           "bu_id",\
                                           "currency_key",\
                                           "oli_amount" \
                                           )\
                                  .groupBy("bu_account_key",\
                                           "product_key",\
                                           "order_date",\
                                           "order_number",\
                                           "line_number",\
                                           "erp_id", \
                                           "bu_id",\
                                           "currency_key") \
                                  .agg(sum("oli_amount").alias("oli_amount") \
                                          )
                                  
direct_auth_count = df_direct_orders.count()
df_direct_auth_agg = df_direct_orders.select(round(sum("oli_amount"), scale=2).alias("total_oli_amount"))
direct_auth_amount = df_direct_auth_agg.collect()[0][0]
direct_auth_amnt = direct_auth_amount                                  


# COMMAND ----------

df_indirect_orders = df_crg_orders.select(col("*")).filter(col("Total_indirect_amt") !=0 )
df_indirect_orders = df_indirect_orders.select(col("bu_account_key").alias("bu_account_key"),
                                        col("order_number").alias("order_number"),
                                        col("bu_id").alias("bu_id"),  
                                        col("Total_indirect_amt").alias("oli_amount"), 
                                        col("CONSOLIDTD_BU"),
                                        col("PERIOD_END_DT").alias("order_date")
                                       )
df_indirect_orders = df_indirect_orders.withColumn("line_number", lit("-1"))
df_indirect_orders = df_indirect_orders.withColumn("erp_id", lit("1009"))
df_indirect_orders = df_indirect_orders.withColumn("currency_key", lit("USD"))
df_indirect_orders = df_indirect_orders.withColumn("product_key", substring(concat_ws('|',lit("INDIR"),trim(df_indirect_orders.CONSOLIDTD_BU)),1,32) )
df_indirect_orders = df_indirect_orders.select("bu_account_key",\
                                           "product_key",\
                                           "order_date",\
                                           "order_number",\
                                           "line_number",\
                                           "erp_id", \
                                           "bu_id",\
                                           "currency_key",\
                                           "oli_amount" \
                                           )\
                                  .groupBy("bu_account_key",\
                                           "product_key",\
                                           "order_date",\
                                           "order_number",\
                                           "line_number",\
                                           "erp_id", \
                                           "bu_id",\
                                           "currency_key") \
                                  .agg(sum("oli_amount").alias("oli_amount") \
                                          )
                                  
indirect_auth_count = df_indirect_orders.count()
df_indirect_auth_agg = df_indirect_orders.select(round(sum("oli_amount"), scale=2).alias("total_oli_amount"))
indirect_auth_amount = df_indirect_auth_agg.collect()[0][0]
indirect_auth_amnt = indirect_auth_amount



# COMMAND ----------

display(df_indirect_orders)

# COMMAND ----------

df_orders_all = df_direct_orders.unionAll(df_indirect_orders)

# COMMAND ----------

stageTable='SCRATCH_NEW_BU_ACCOUNT'
df_customer.write.jdbc(url=cc_url,table=stageTable,mode="append",properties=cc_conn)
auth_customer_count = df_customer.count()

# COMMAND ----------

stageTable='scratch_order_line_item'
df_orders_all.write.jdbc(url=cc_url,table=stageTable,mode="append",properties=cc_conn)

# COMMAND ----------

df_crg_product = df_crg_snflk_sf_orders.select(col("CONSOLIDTD_BU")).distinct()

# COMMAND ----------

# DBTITLE 1,Indirect Products
df_crg_indirect_product = df_crg_product.select(col("CONSOLIDTD_BU"))
df_crg_indirect_product = df_crg_indirect_product.withColumn("BU_PRODUCT_KEY", substring(concat_ws('|',lit("INDIR"),trim(df_crg_indirect_product.CONSOLIDTD_BU)),1,32) )
df_crg_indirect_product = df_crg_indirect_product.withColumn("product_name", substring(concat_ws('|',lit("INDIR"),trim(df_crg_indirect_product.CONSOLIDTD_BU)),1,96) )
df_crg_indirect_product = df_crg_indirect_product.withColumn("erp_id", lit("1009"))
df_crg_indirect_product = df_crg_indirect_product.drop(col("CONSOLIDTD_BU"))

# COMMAND ----------

# DBTITLE 1,Direct Products
df_crg_direct_product = df_crg_product.select(col("CONSOLIDTD_BU"))
df_crg_direct_product = df_crg_direct_product.withColumn("BU_PRODUCT_KEY", substring(concat_ws('|',lit("DIR"),trim(df_crg_direct_product.CONSOLIDTD_BU)),1,32) )
df_crg_direct_product = df_crg_direct_product.withColumn("product_name", substring(concat_ws('|',lit("DIR"),trim(df_crg_direct_product.CONSOLIDTD_BU)),1,96) )
df_crg_direct_product = df_crg_direct_product.withColumn("erp_id", lit("1009"))
df_crg_direct_product = df_crg_direct_product.drop(col("CONSOLIDTD_BU"))

# COMMAND ----------

# DBTITLE 1,Combine Products
df_crg_all_products = df_crg_indirect_product.unionAll(df_crg_direct_product)

# COMMAND ----------

stageTable='scratch_bu_product'
df_crg_all_products.write.jdbc(url=cc_url,table=stageTable,mode="append",properties=cc_conn)
auth_prd_count = df_crg_all_products.count()

# COMMAND ----------

import smtplib
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
server = smtplib.SMTP('smtprelay1.tt.com')
server.ehlo()
server.starttls()
sender = "tff.databricks@tt.com"
recipient = []
msg = MIMEMultipart()
msg['Subject'] = 'CRG MOSAIC TO cc Monthly Load'
msg['From'] = sender
msg['To'] = ', '.join(recipient)
msg.attach(MIMEText("Product count: {}".format(product_count) + "\r\n"
                    "Customer count: {}".format(customer_count) + "\r\n"
                    "Invoice count: {}".format(invoice_count) + "\r\n"
                    "Invoice amount: {}".format(amount) + "\r\n"
                    "Autorization Customer Count :{}".format(auth_customer_count) + "\r\n"
                    "Direct Autorization count: {}".format(direct_auth_count) + "\r\n"
                    "Direct Autorization amount: {}".format(direct_auth_amnt) + "\r\n"
                    "Indirect Autorization count: {}".format(indirect_auth_count) + "\r\n"
                    "Indirect Autorization amount: {}".format(indirect_auth_amnt) + "\r\n"                    
                    "Autorization Product count: {}".format(auth_prd_count) + "\r\n"
                   )
           )
server.sendmail(sender, (recipient), msg.as_string())
server.close()
