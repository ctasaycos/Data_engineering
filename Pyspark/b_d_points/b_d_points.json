'''
Project to generate breadth and depth points dataset for corporate companies and non-corporate companies, those poings has to be above of the threshold (90% percentile), 12 roll months USD amounts, for each period from 2020-01-01 to today.
Periods:
2020-01-01 (from 2019-02-01 to 2020-01-01 )
...
2023-09-01 (from 2022-10-01 to 2023-09-01 )
AWS cloud
sparks dataframe
4 export datasets for new accounts and old accounts in ca and non ca accounts
Data source: ORACLE DATABASE and S3 buckets
obfuscated private daata of the company and its divisions

Why run this?
There is a lot of data aproximately 80 millions rows with over 200 fields and different data sources, to make a light power bi report is mandatory to run the following pyspark script every fiscal month, we are only storing the breadth and depth points (binary), ca_number,
and period.
'''
# COMMAND ----------

from pyspark.sql.functions import lpad
import datetime
import time
import cx_Oracle
from pyspark.sql.types import StructType, IntegerType, StringType,DecimalType
from pyspark.sql.functions import lit, substring, to_date, to_timestamp, current_date, trim, regexp_replace
import pandas as pd
from pyspark.sql.functions import * 
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql import Window, Row
from pyspark.sql.functions import lpad
from pyspark.sql.functions import col, substring
from decimal import Decimal


# COMMAND ----------

df_threshold = spark.read.csv(s3_threshold, header=True, sep=',', encoding="UTF-8", escape = '"',multiLine=True)
df_s3_universe = spark.read.parquet(s3_universe, header=True, sep=',', encoding="UTF-8", escape = '"',multiLine=True)
df_s3_universe=df_s3_universe.withColumn("dn_numb",lpad("dn_numb",9,"0"))
df_s3_universe=df_s3_universe.withColumn("revenue", col("revenue").cast('Double'))
df_s3_universe=df_s3_universe.withColumn("Breadth", col("Breadth").cast('Int'))
df_s3_universe=df_s3_universe.withColumn("Depth", col("Depth").cast('Int'))

# COMMAND ----------

df_s3_universe.createOrReplaceTempView("vw_s3_universe")
df_universe = spark.sql(f'''select 
                    distinct dn_numb
                      From vw_s3_universe  vw_s3_universe
                    ''')

# COMMAND ----------

sql_grouping="""
select distinct dn_numb, Grouping from 
(select a.dn_numb,PARENT_DUNS_NAME,CA_FLAG,CA_SEGMENT,ACCOUNT_RANK,SEGMENT,
                case
                    when (a.CA_SEGMENT = 'Pharma and Applied' and SEGMENT in ('Medical Devices','Medical Instr')) OR (ACCOUNT_RANK LIKE '%TOP100%' and SEGMENT in ('Medical Devices','Medical Instr'))  then 'Group10'
                    when (a.CA_SEGMENT = 'Pharma and Applied' and SEGMENT in ('FoodAgri')) OR (ACCOUNT_RANK LIKE '%TOP100%' and SEGMENT in ('FoodAgri'))  then 'Group9'
                    when (a.CA_SEGMENT = 'Pharma and Applied' and SEGMENT in ('Chemicals')) OR (ACCOUNT_RANK LIKE '%TOP100%' and SEGMENT in ('Chemicals'))  then 'Group8'
                    when (a.CA_SEGMENT = 'Pharma and Applied' and SEGMENT in ('Diagnostics')) OR (ACCOUNT_RANK LIKE '%TOP100%' and SEGMENT in ('Diagnostics')) or a.dn_numb='404248536'  then 'Group7'
                    when (a.CA_SEGMENT = 'Pharma and Applied' and SEGMENT in ('Testing Lab')) OR (ACCOUNT_RANK LIKE '%TOP100%' and SEGMENT in ('Testing Lab'))  then 'Group6'
                    when ACCOUNT_RANK = 'TOP100' and SEGMENT = 'Academic'  or a.dn_numb='148648665' then 'Group5'
                    when b.New_Account_Rank ='Biotech Larger'  then 'Group3'
                    when CA_Segment ='Global Biotech'  then 'Group4'
                    when CA_SEGMENT = 'BioPharma' then 'Group1'
                    when CA_SEGMENT = 'Pharma and Applied' and SEGMENT IN ('BioPharma','BioPharma Animal Health','CDMO','CRO','Pharma APAC') then 'Group2'
                else 'Not_group'
                END Grouping
                from dwsa.cc_data a
                left join (select distinct dn_numb,
                           case 
                           when CA_Segment ='Global Biotech' and New_Account_Rank ='Not_Biotech' and ACCOUNT_RANK LIKE 'TOP1000' then 'Biotech Larger' 
                           when CA_Segment ='Global Biotech' and New_Account_Rank ='Not_Biotech' then 'Biotech Smaller' else New_Account_Rank end New_Account_Rank
                            from 
                            (  select New_Account_Rank,CA_Segment,ACCOUNT_RANK,dn_numb
                              from
                                  (select
                                  a.*,b.USD_ILI_AMOUNT,
                                  case 
                                  when (CA_Segment='Global Biotech' and b.USD_ILI_AMOUNT>2500000) or ( CA_Segment='Pharma and Applied' and SEGMENT like '%BD Biotech%') or (CA_Segment='Global Biotech' and ACCOUNT_RANK LIKE '%TOP100%')  then 'Biotech Larger'
                                  when CA_Segment='Global Biotech' and b.USD_ILI_AMOUNT<2500000 then 'Biotech Smaller'
                                  else 'Not_Biotech'
                                  end New_Account_Rank
                                  from  dwsa.cc_data a
                                  left join 
                                      (select dn_numb, sum(USD_ILI_AMOUNT) USD_ILI_AMOUNT from  dwsa.cust_duns_invoice_data
                                      WHERE FISCAL_YEAR=2022 and DIVISION_GROUP not in ('Clinical Research Group','Pharma Services Group','BioProduction Group')
                                      GROUP BY dn_numb) b
                                  on a.dn_numb=b.dn_numb) f
                               where New_Account_Rank<>'Not_Biotech' or ACCOUNT_RANK in ('Biotech Larger','Biotech Smaller') AND CA_FLAG='Y'
                              group by New_Account_Rank,CA_Segment,ACCOUNT_RANK,dn_numb)  a) b on a.dn_numb=b.dn_numb
                ) a
where CA_FLAG='Y' or  a.dn_numb='148648665' AND grouping <> 'Not_group'
group by dn_numb,PARENT_DUNS_NAME,CA_FLAG,CA_SEGMENT,ACCOUNT_RANK,SEGMENT,Grouping"""
df_grouping_ca = pd.read_sql(sql_grouping, con=cx_conn)
df_grouping_ca = spark.createDataFrame(df_grouping_ca)

# COMMAND ----------

display(df_grouping_ca)

# COMMAND ----------

non_ca="""
select distinct dn_numb, Grouping
                            from 
                            (  select CA_FLAG,PARENT_DUNS_NUMBER as dn_numb,PARENT_DUNS_NAME,Segment,ACCOUNT_RANK,Grouping
                              from
                                  (select
                                  a.*,b.USD_ILI_AMOUNT,
                                  case
                                  when CA_FLAG<>'Y' and ((Segment like '%Pharma%' and  Segment <> 'Pharma Services') or Segment in ('Biopharma Animal Health','Animal Diagnostics','Generics','Consumer Health Nutrition') OR (Segment in ('CDMO','CRO') and  ACCOUNT_RANK like ('%TOP%')) )
                                  then 'Group2'  
                                  when CA_FLAG<>'Y' and (Segment  like '%BD Biotech%' or Segment like '%Biosimilars%' or Segment like '%CDMO%'or Segment like '%CMO%' or Segment like '%CRO%' or Segment like '%Contract Testing Lab%' or Segment like '%Molecular Diagnostics Services%' or Segment like '%Genomics DTC%' or Segment like '%Molecular Diagnostics Mfg%') 
                                  and b.USD_ILI_AMOUNT>2500000  then 'Group3'
                                  when CA_FLAG<>'Y' and (Segment  like '%BD Biotech%' or Segment like '%Biosimilars%' or Segment like '%CDMO%'or Segment like '%CMO%' or Segment like '%CRO%' or Segment like '%Contract Testing Lab%' or Segment like '%Molecular Diagnostics Services%' or Segment like '%Genomics DTC%' or Segment like '%Molecular Diagnostics Mfg%') 
                                  and b.USD_ILI_AMOUNT<2500000  then 'Group4'  
                                  when CA_FLAG<>'Y' and (Segment in ('Diagnostics'))
                                  then 'Group7'  
                                  end Grouping
                                  from  dwsa.cc_data a
                                  left join 
                                      (select PARENT_DUNS_NUMBER, sum(USD_ILI_AMOUNT) USD_ILI_AMOUNT from  dwsa.cust_duns_invoice_data
                                      WHERE FISCAL_YEAR=2022 and DIVISION_GROUP not in ('Clinical Research Group','Pharma Services Group','BioProduction Group')
                                      GROUP BY PARENT_DUNS_NUMBER) b
                                  on a.PARENT_DUNS_NUMBER=b.PARENT_DUNS_NUMBER) f
                               where Grouping is not null)
"""
df_grouping_nonca = pd.read_sql(non_ca, con=cx_conn)
df_grouping_nonca = spark.createDataFrame(df_grouping_nonca)

# COMMAND ----------

df_grouping=df_grouping_ca.union(df_grouping_nonca)

# COMMAND ----------

sql_invoices= """
select PARENT_DUNS_NUMBER AS dn_numb, derived_division, FISCAL_MONTH_AS_DATE, TRUNC (SYSDATE, 'MONTH') SYSDATE_MONTH,sum(usd_ili_amount) usd_ili_amount
from
    (select cust_duns.PARENT_DUNS_NUMBER,
    case 
    when cust_duns.bt_sub_division is not null and BT_SUB_DIVISION='RSD_LSG' then 'BID'
    when cust_duns.bt_sub_division is not null and BT_SUB_DIVISION='HMD_LSG' then 'BID'
    when cust_duns.bt_sub_division is not null then REGEXP_SUBSTR(cust_duns.bt_sub_division, '[^_]+$') 
    else cust_duns.bt_division  
    end AS derived_division,
    FISCAL_MONTH_AS_DATE,SYSDATE,
    sum(usd_ili_amount) usd_ili_amount
    FROM 
    dwsa.cust_duns_invoice_data  cust_duns
    inner join (select distinct PARENT_DUNS_NUMBER from cc_data where CA_FLAG<>'Y') recognition on recognition.PARENT_DUNS_NUMBER=cust_duns.PARENT_DUNS_NUMBER
    where FISCAL_YEAR >= 2021 
    group by cust_duns.PARENT_DUNS_NUMBER,cust_duns.bt_division,cust_duns.bt_sub_division,FISCAL_MONTH_AS_DATE) a
where derived_division  in ('ASD',
'a','b','c'...,'z')
group by PARENT_DUNS_NUMBER, derived_division, FISCAL_MONTH_AS_DATE
"""
df_invoices_nonca = pd.read_sql(sql_invoices, con=cx_conn)
df_invoices_nonca = spark.createDataFrame(df_invoices_nonca)



sql_invoices= """
select dn_numb, derived_division, FISCAL_MONTH_AS_DATE, TRUNC (SYSDATE, 'MONTH') SYSDATE_MONTH,sum(usd_ili_amount) usd_ili_amount
from
    (select cust_duns.dn_numb,
    case 
    when cust_duns.bt_sub_division is not null and BT_SUB_DIVISION='RSD_LSG' then 'BID'
    when cust_duns.bt_sub_division is not null and BT_SUB_DIVISION='HMD_LSG' then 'BID'
    when cust_duns.bt_sub_division is not null then REGEXP_SUBSTR(cust_duns.bt_sub_division, '[^_]+$') 
    else cust_duns.bt_division  
    end AS derived_division,
    FISCAL_MONTH_AS_DATE,SYSDATE,
    sum(usd_ili_amount) usd_ili_amount
    FROM 
    dwsa.cust_duns_invoice_data  cust_duns
    inner join (select distinct dn_numb from cc_data where CA_FLAG='Y' or dn_numb='148648665' ) recognition on recognition.dn_numb=cust_duns.dn_numb
    where FISCAL_YEAR >= 2021 
    group by cust_duns.dn_numb,cust_duns.bt_division,cust_duns.bt_sub_division,FISCAL_MONTH_AS_DATE) a
where derived_division  in ('ASD',
'a','b','c'...,'z')
group by dn_numb, derived_division, FISCAL_MONTH_AS_DATE
"""
df_invoices_ca = pd.read_sql(sql_invoices, con=cx_conn)
df_invoices_ca = spark.createDataFrame(df_invoices_ca)

# COMMAND ----------

# DBTITLE 1,New Accounts
try: 
  df_grouping.createOrReplaceTempView("vw_grouping")
  df_universe.createOrReplaceTempView("vw_universe")
  data_new_accounts = spark.sql(f'''
                      select distinct a.dn_numb,grouping from vw_grouping a
                      left join vw_universe b on a.dn_numb=b.dn_numb
                      where b.dn_numb is null and a.dn_numb is not null and grouping<>'Not_group'
                ''')
except:
  print('No new data')

# COMMAND ----------

# DBTITLE 1,New Accounts
try: 
  emp_RDD = spark.sparkContext.emptyRDD()
  columns = StructType([StructField("dn_numb", StringType(), True),
                        StructField("grouping", StringType(), True),
                      StructField("derived_division", StringType(), True)
                      ]
                      )
  myDataFrame = spark.createDataFrame(data = emp_RDD,
                              schema = columns)
  list_derived_divisions=[
'a','b','c'...,'z'
  ]
  data_new_accounts.createOrReplaceTempView("VW_data_new_accounts")
  for j in range(0,26):
    df_test = spark.sql(f''' 
              select  dn_numb,grouping,
              case when dn_numb is not null then '{list_derived_divisions[j]}' end derived_division
              from VW_data_new_accounts
                        ''')
    myDataFrame = myDataFrame.union(df_test)
  data_new=myDataFrame

  df_threshold.createOrReplaceTempView("VW_threshold")
  data_new.createOrReplaceTempView("VW_data_new")
  data_new = spark.sql(f''' 
            select  dn_numb,a.GROUPING, a.derived_division, Breadth, Depth
            from VW_data_new a
            left join VW_threshold b on a.GROUPING=b.GROUPING and a.derived_division=b.derived_division
                      ''')
except:
  print('No new data')

# COMMAND ----------

# DBTITLE 1,Historic new non corporate account
try:
  import datetime
  import time
  from datetime import date
  num_months = (date.today().year - datetime.datetime(2021, 1, 1).year) * 12 + (date.today().month - datetime.datetime(2021, 1, 1).month)    
  emp_RDD_1 = spark.sparkContext.emptyRDD()
  columns_1 = StructType([
                      StructField("dn_numb", StringType(), True),
                      StructField("derived_division", StringType(), True),
                      StructField("PERIOD", DateType(), True),
                      StructField("usd_ili_amount", IntegerType(), True)
                      ]
                      )
  myDataFrame = spark.createDataFrame(data = emp_RDD_1, schema = columns_1)
  total=data_new.count()
  for i in range(1,num_months-11):
    date1=i+11-num_months
    date3=i+12-num_months
    date2=i-num_months
    df_invoices_nonca.createOrReplaceTempView("vw_invoices")
    data_new_accounts.createOrReplaceTempView("vw_data_new_accounts")
    df_test = spark.sql(f'''
          --select FISCAL_MONTH_AS_DATE,PARENT_DUNS_NUMBER,derived_division,add_months(date(SYSDATE_MONTH),-1) period,  usd_ili_amount from
          --(
          SELECT dn_numb as dn_numb,derived_division,add_months(date(SYSDATE_MONTH),{date1}-1) period, sum(a.usd_ili_amount) usd_ili_amount
          FROM (
              select a.dn_numb,fiscal_month_as_date,derived_division,SYSDATE_MONTH,    
              case when FISCAL_MONTH_AS_DATE>= add_months(date(SYSDATE_MONTH),{date2}-1) and FISCAL_MONTH_AS_DATE<= add_months(date(SYSDATE_MONTH),{date3}-2) then 1 else 0 end P1,
              SUM(USD_ILI_AMOUNT) USD_ILI_AMOUNT
              FROM vw_invoices a
              inner join vw_data_new_accounts  b on a.dn_numb=b.dn_numb
              GROUP BY a.dn_numb,fiscal_month_as_date,derived_division,SYSDATE_MONTH) a
          where a.P1=1
          group by dn_numb,derived_division,period--) b
          --group by FISCAL_MONTH_AS_DATE,PARENT_DUNS_NUMBER,derived_division,usd_ili_amount
        ''')
    myDataFrame = myDataFrame.union(df_test)
  data_month=myDataFrame
  data_month.createOrReplaceTempView("vw_myDataFrame")
  data_new.createOrReplaceTempView("vw_test2")
  df_historic_newnonca = spark.sql(f''' 
                select dn_numb,derived_division,2022 as reference_year,max(PERIOD) PERIOD,round(USD_ILI_AMOUNT,2) as Revenue,
                CASE WHEN usd_ili_amount>cast(Breadth as INT) then 1 else 0 end Breadth,
                CASE WHEN usd_ili_amount>cast(Depth as INT) then 1 else 0 end Depth
                from
                (select a.dn_numb as dn_numb,GROUPING,a.derived_division,Breadth,Depth,sum(usd_ili_amount) usd_ili_amount,PERIOD
                from (
                  select a.dn_numb,GROUPING,a.derived_division,usd_ili_amount,PERIOD,
                  case when b.dn_numb is null then 10000 else Breadth end Breadth,
                  case when b.dn_numb is null then 20000 else Depth end Depth
                  from vw_test2 a
                  left join
                              vw_myDataFrame b on a.dn_numb=b.dn_numb and a.derived_division=b.derived_division
                              ) a
                group by a.dn_numb,GROUPING,DERIVED_DIVISION,Breadth,Depth,PERIOD) H
                where PERIOD is not null
                group by dn_numb,DERIVED_DIVISION,Breadth,Depth,USD_ILI_AMOUNT
                ''')
except:
  columns = StructType([StructField("dn_numb", StringType(), True),
                      StructField("derived_division", StringType(), True),
                      StructField("reference_year", IntegerType(), True),
                      StructField("PERIOD", DateType(), True),
                      StructField("Revenue", DoubleType(), True),
                      StructField("Breadth", IntegerType(), True),
                      StructField("Depth", IntegerType(), True),
                      ]
                      )
  df_historic_newnonca = spark.createDataFrame(data = emp_RDD,
                              schema = columns)
  print('No new data')

# COMMAND ----------

# DBTITLE 1,Historic new  corporate account
try:
  import datetime
  import time
  from datetime import date
  num_months = (date.today().year - datetime.datetime(2021, 1, 1).year) * 12 + (date.today().month - datetime.datetime(2021, 1, 1).month)    
  emp_RDD_1 = spark.sparkContext.emptyRDD()
  columns_1 = StructType([
                      StructField("dn_numb", StringType(), True),
                      StructField("derived_division", StringType(), True),
                      StructField("PERIOD", DateType(), True),
                      StructField("usd_ili_amount", IntegerType(), True)
                      ]
                      )
  myDataFrame = spark.createDataFrame(data = emp_RDD_1, schema = columns_1)
  total=data_new.count()
  for i in range(1,num_months-11):
    date1=i+11-num_months
    date3=i+12-num_months
    date2=i-num_months
    df_invoices_ca.createOrReplaceTempView("vw_invoices")
    data_new_accounts.createOrReplaceTempView("vw_data_new_accounts")
    df_test = spark.sql(f'''
          --select FISCAL_MONTH_AS_DATE,PARENT_DUNS_NUMBER,derived_division,add_months(date(SYSDATE_MONTH),-1) period,  usd_ili_amount from
          --(
          SELECT dn_numb as dn_numb,derived_division,add_months(date(SYSDATE_MONTH),{date1}-1) period, sum(a.usd_ili_amount) usd_ili_amount
          FROM (
              select a.dn_numb,fiscal_month_as_date,derived_division,SYSDATE_MONTH,    
              case when FISCAL_MONTH_AS_DATE>= add_months(date(SYSDATE_MONTH),{date2}-1) and FISCAL_MONTH_AS_DATE<= add_months(date(SYSDATE_MONTH),{date3}-2) then 1 else 0 end P1,
              SUM(USD_ILI_AMOUNT) USD_ILI_AMOUNT
              FROM vw_invoices a
              inner join vw_data_new_accounts  b on a.dn_numb=b.dn_numb
              GROUP BY a.dn_numb,fiscal_month_as_date,derived_division,SYSDATE_MONTH) a
          where a.P1=1
          group by dn_numb,derived_division,period--) b
          --group by FISCAL_MONTH_AS_DATE,PARENT_DUNS_NUMBER,derived_division,usd_ili_amount
        ''')
    myDataFrame = myDataFrame.union(df_test)
  data_month=myDataFrame
  data_month.createOrReplaceTempView("vw_myDataFrame")
  data_new.createOrReplaceTempView("vw_test2")
  df_historic_newca = spark.sql(f''' 
                select dn_numb,derived_division,2022 as reference_year,max(PERIOD) PERIOD,round(USD_ILI_AMOUNT,2) as Revenue,
                CASE WHEN usd_ili_amount>cast(Breadth as INT) then 1 else 0 end Breadth,
                CASE WHEN usd_ili_amount>cast(Depth as INT) then 1 else 0 end Depth
                from
                (select a.dn_numb as dn_numb,GROUPING,a.derived_division,Breadth,Depth,sum(usd_ili_amount) usd_ili_amount,PERIOD
                from (
                  select a.dn_numb,GROUPING,a.derived_division,usd_ili_amount,PERIOD,
                  case when b.dn_numb is null then 10000 else Breadth end Breadth,
                  case when b.dn_numb is null then 20000 else Depth end Depth
                  from vw_test2 a
                  left join
                              vw_myDataFrame b on a.dn_numb=b.dn_numb and a.derived_division=b.derived_division
                              ) a
                group by a.dn_numb,GROUPING,DERIVED_DIVISION,Breadth,Depth,PERIOD) H
                where PERIOD is not null
                group by dn_numb,DERIVED_DIVISION,Breadth,Depth,USD_ILI_AMOUNT
                ''')
except:
  columns = StructType([StructField("dn_numb", StringType(), True),
                      StructField("derived_division", StringType(), True),
                      StructField("reference_year", IntegerType(), True),
                      StructField("PERIOD", DateType(), True),
                      StructField("Revenue", DoubleType(), True),
                      StructField("Breadth", IntegerType(), True),
                      StructField("Depth", IntegerType(), True),
                      ]
                      )
  df_historic_newca = spark.createDataFrame(data = emp_RDD,
                              schema = columns)
  print('No new data')

# COMMAND ----------

# DBTITLE 1,All customers
try: 
  emp_RDD = spark.sparkContext.emptyRDD()
  columns = StructType([StructField("dn_numb", StringType(), True),
                        StructField("grouping", StringType(), True),
                      StructField("derived_division", StringType(), True)
                      ]
                      )
  myDataFrame = spark.createDataFrame(data = emp_RDD,
                              schema = columns)
  list_derived_divisions=[
'a','b','c'...,'z'
  ]
  df_grouping.createOrReplaceTempView("VW_grouping")
  for j in range(0,26):
    df_test = spark.sql(f''' 
              select  dn_numb,grouping,
              case when dn_numb is not null then '{list_derived_divisions[j]}' end derived_division
              from VW_grouping
                        ''')
    myDataFrame = myDataFrame.union(df_test)
  data_all=myDataFrame

  df_threshold.createOrReplaceTempView("VW_threshold")
  data_all.createOrReplaceTempView("VW_data_all")
  data_all = spark.sql(f''' 
            select  dn_numb,a.GROUPING, a.derived_division, Breadth, Depth
            from VW_data_all a
            left join VW_threshold b on a.GROUPING=b.GROUPING and a.derived_division=b.derived_division
            WHERE a.dn_numb is not null and grouping<>'Not_group'
                      ''')
except:
  print('No new data')

# COMMAND ----------

# DBTITLE 1,Current month nonCA
try:
  df_invoices_nonca.createOrReplaceTempView("vw_invoices")
  df_grouping.createOrReplaceTempView("vw_grouping")
  df_test = spark.sql(f'''
        select  dn_numb,derived_division,PERIOD, sum(b.usd_ili_amount) usd_ili_amount  from
        (
        SELECT dn_numb,derived_division,add_months(date(SYSDATE_MONTH),-1) period, sum(a.usd_ili_amount) usd_ili_amount
        FROM (
            select dn_numb,fiscal_month_as_date,derived_division,SYSDATE_MONTH,    
            case when FISCAL_MONTH_AS_DATE>= add_months(date(SYSDATE_MONTH),-12) and FISCAL_MONTH_AS_DATE<= add_months(date(SYSDATE_MONTH),-1) then 1 else 0 end P1,
            SUM(USD_ILI_AMOUNT) USD_ILI_AMOUNT
            FROM vw_invoices
            inner join vw_grouping  b on a.dn_numb=b.dn_numb
            GROUP BY dn_numb,fiscal_month_as_date,derived_division,SYSDATE_MONTH) a
        where a.P1=1
        group by fiscal_month_as_date,dn_numb,derived_division,SYSDATE_MONTH) b
        group by dn_numb,derived_division,PERIOD
      ''')
  data_month=df_test

  data_month.createOrReplaceTempView("vw_myDataFrame")
  data_all.createOrReplaceTempView("vw_test2")
  df_ALL_newnonca = spark.sql(f''' 
                select dn_numb,derived_division,2022 as reference_year,max(PERIOD) PERIOD,round(USD_ILI_AMOUNT,2) as Revenue,
                CASE WHEN usd_ili_amount>cast(Breadth as INT) then 1 else 0 end Breadth,
                CASE WHEN usd_ili_amount>cast(Depth as INT) then 1 else 0 end Depth
                from
                (select a.dn_numb as dn_numb,GROUPING,a.derived_division,Breadth,Depth,sum(usd_ili_amount) usd_ili_amount,PERIOD
                from (
                  select a.dn_numb,GROUPING,a.derived_division,usd_ili_amount,PERIOD,
                  case when b.dn_numb is null then 10000 else Breadth end Breadth,
                  case when b.dn_numb is null then 20000 else Depth end Depth
                  from vw_test2 a
                  left join
                              vw_myDataFrame b on a.dn_numb=b.dn_numb and a.derived_division=b.derived_division
                              ) a
                group by a.dn_numb,GROUPING,DERIVED_DIVISION,Breadth,Depth,PERIOD) H
                where PERIOD is not null
                group by dn_numb,DERIVED_DIVISION,Breadth,Depth,USD_ILI_AMOUNT
                ''')


except:
  columns = StructType([StructField("dn_numb", StringType(), True),
                      StructField("derived_division", StringType(), True),
                      StructField("reference_year", IntegerType(), True),
                      StructField("PERIOD", DateType(), True),
                      StructField("Revenue", DoubleType(), True),
                      StructField("Breadth", IntegerType(), True),
                      StructField("Depth", IntegerType(), True),
                      ]
                      )
  df_historic_newnonca = spark.createDataFrame(data = emp_RDD,
                              schema = columns)
  print('No new data')

# COMMAND ----------

# DBTITLE 1,Current month CA
try:
  df_invoices_ca.createOrReplaceTempView("vw_invoices")
  df_grouping.createOrReplaceTempView("vw_grouping")
  df_test = spark.sql(f'''
        select  dn_numb,derived_division,PERIOD, sum(b.usd_ili_amount) usd_ili_amount  from
        (
        SELECT dn_numb,derived_division,add_months(date(SYSDATE_MONTH),-1) period, sum(a.usd_ili_amount) usd_ili_amount
        FROM (
            select dn_numb,fiscal_month_as_date,derived_division,SYSDATE_MONTH,    
            case when FISCAL_MONTH_AS_DATE>= add_months(date(SYSDATE_MONTH),-12) and FISCAL_MONTH_AS_DATE<= add_months(date(SYSDATE_MONTH),-1) then 1 else 0 end P1,
            SUM(USD_ILI_AMOUNT) USD_ILI_AMOUNT
            FROM vw_invoices
            inner join vw_grouping  b on a.dn_numb=b.dn_numb
            GROUP BY dn_numb,fiscal_month_as_date,derived_division,SYSDATE_MONTH) a
        where a.P1=1
        group by fiscal_month_as_date,dn_numb,derived_division,SYSDATE_MONTH) b
        group by dn_numb,derived_division,PERIOD
      ''')
  data_month=df_test

  data_month.createOrReplaceTempView("vw_myDataFrame")
  data_all.createOrReplaceTempView("vw_test2")
  df_ALL_newca = spark.sql(f''' 
                select dn_numb,derived_division,2022 as reference_year,max(PERIOD) PERIOD,round(USD_ILI_AMOUNT,2) as Revenue,
                CASE WHEN usd_ili_amount>cast(Breadth as INT) then 1 else 0 end Breadth,
                CASE WHEN usd_ili_amount>cast(Depth as INT) then 1 else 0 end Depth
                from
                (select a.dn_numb as dn_numb,GROUPING,a.derived_division,Breadth,Depth,sum(usd_ili_amount) usd_ili_amount,PERIOD
                from (
                  select a.dn_numb,GROUPING,a.derived_division,usd_ili_amount,PERIOD,
                  case when b.dn_numb is null then 10000 else Breadth end Breadth,
                  case when b.dn_numb is null then 20000 else Depth end Depth
                  from vw_test2 a
                  left join
                              vw_myDataFrame b on a.dn_numb=b.dn_numb and a.derived_division=b.derived_division
                              ) a
                group by a.dn_numb,GROUPING,DERIVED_DIVISION,Breadth,Depth,PERIOD) H
                where PERIOD is not null
                group by dn_numb,DERIVED_DIVISION,Breadth,Depth,USD_ILI_AMOUNT
                ''')
except:
  columns = StructType([StructField("dn_numb", StringType(), True),
                      StructField("derived_division", StringType(), True),
                      StructField("reference_year", IntegerType(), True),
                      StructField("PERIOD", DateType(), True),
                      StructField("Revenue", DoubleType(), True),
                      StructField("Breadth", IntegerType(), True),
                      StructField("Depth", IntegerType(), True),
                      ]
                      )
  df_historic_newnonca = spark.createDataFrame(data = emp_RDD,
                              schema = columns)
  print('No new data')

# COMMAND ----------

df_s3_universe.coalesce(1).write.format("PARQUET").mode("overwrite").option("header", "true").save("s3://path/Depth_breadth/Universe/")


