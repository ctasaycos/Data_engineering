# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.dropdown("run_env", "DEV" ,["DEV","QA","PRD","UAT"])
run_env = dbutils.widgets.get("run_env").strip().upper()
print("run_env: ", run_env)

# COMMAND ----------

import pandas as pd

# COMMAND ----------

# MAGIC %run  ./Connection_DNB $env = run_env

# COMMAND ----------

query="""
select 
CASE WHEN translated_bu_account is null then bu_account else translated_bu_account end name,
COUNTRY_SHORT as countryISOAlpha2Code,
case when CITY_TRANSLATED is null then GEOCODED_CITY else CITY_TRANSLATED end addressLocality,
CASE WHEN ADDRESS_1_TRANSLATED is null then STREET_ADDRESS else ADDRESS_1_TRANSLATED end streetAddressLine1,
zip as postalCode,
STATE as addressRegion
from dwsa.GEOCODE_TRANSLATE_ACCOUNTS_BKP
where COUNTRY_SHORT  IN ('US') and ROWNUM <= 6
union 
select 
CASE WHEN translated_bu_account is null then bu_account else translated_bu_account end name,
COUNTRY_SHORT as countryISOAlpha2Code,
case when CITY_TRANSLATED is null then GEOCODED_CITY else CITY_TRANSLATED end addressLocality,
CASE WHEN ADDRESS_1_TRANSLATED is null then STREET_ADDRESS else ADDRESS_1_TRANSLATED end streetAddressLine1,
zip as postalCode,
STATE as addressRegion
from dwsa.GEOCODE_TRANSLATE_ACCOUNTS_BKP
where COUNTRY_SHORT  IN ('FR') and ROWNUM <= 2
union 
select 
CASE WHEN translated_bu_account is null then bu_account else translated_bu_account end name,
COUNTRY_SHORT as countryISOAlpha2Code,
case when CITY_TRANSLATED is null then GEOCODED_CITY else CITY_TRANSLATED end addressLocality,
CASE WHEN ADDRESS_1_TRANSLATED is null then STREET_ADDRESS else ADDRESS_1_TRANSLATED end streetAddressLine1,
zip as postalCode,
STATE as addressRegion
from dwsa.GEOCODE_TRANSLATE_ACCOUNTS_BKP
where COUNTRY_SHORT  IN ('CN') and ROWNUM <= 5
"""
df_GEOCODE_TRANSLATE_ACCOUNTS_BKP = pd.read_sql(query, con=cx_base_conn)

# COMMAND ----------

df_dnb

# COMMAND ----------

# DBTITLE 1,Conditions
import re
#Match pattern for new match engine>=5
#A**AA*******
E1c1=re.compile('^A[A-Za-z][A-Za-z]AA.*$', re.IGNORECASE)
#B*AAA*****
E1c2=re.compile('^[A-Ba-b][A-Za-z]AAA.*$', re.IGNORECASE)
#F**AAA*****
E1c3=re.compile('^[A-Fa-f][A-Za-z][A-Za-z]AAA.*$', re.IGNORECASE)
#A***A**A***
E1c4=re.compile('^A[A-Za-z][A-Za-z][A-Za-z]A[A-Za-z][A-Za-z]A.*$', re.IGNORECASE)
#B*A*A***A****
E1c5=re.compile('^[A-Ba-b][A-Za-z]A[A-Za-z]A[A-Za-z][A-Za-z]A.*$', re.IGNORECASE)
#F***AA*A***
E1c6=re.compile('^[A-Fa-f][A-Za-z][A-Za-z][A-Za-z]AA[A-Za-z]A.*$', re.IGNORECASE)
#Match patter for legacy ending>=7
#BA*A***
E2c1=re.compile('^[A-Ba-b]A[A-Za-z]A.*$', re.IGNORECASE)
#F**A*A*
E2c2=re.compile('^[A-Fa-f][A-Za-z][A-Za-z]A[A-Za-z]A.*$', re.IGNORECASE)
#B**A*A*
E2c3=re.compile('^[A-Ba-b][A-Za-z][A-Za-z]A[A-Za-z]A.*$', re.IGNORECASE)

word_active='Active'
word_oob='Out of business'
df_dnb_final=pd.DataFrame()
df_dnb_final=pd.DataFrame(columns=['ID','Score_thermo','Priorization','Name', 'streetAddressLine1', 'countryISOAlpha2Code', 'addressRegion', 'addressLocality','postalCode','displaySequence','duns','dnb.confidenceCode','dnb.matchGrade','dnb.tradeStyleNames','dnb.nameMatchScore','dnb.name','dnb.address','operatingStatus','dnb.addressLocality','dnb.postal_code','Comment'])
df_dnb=pd.DataFrame()
n_rows_j=len(df_GEOCODE_TRANSLATE_ACCOUNTS_BKP)
for j in range(0,n_rows_j):
  df_dnb_ds2=pd.DataFrame()
  df_dnb_ds1=pd.DataFrame()
  try: 
    
    conn = http.client.HTTPSConnection("plus.dnb.com")
    headers = {
    'accept': "application/json;charset=utf-8",
    'authorization': authorization_key,
    }
    url=f"""{url_company_resolution}={df_GEOCODE_TRANSLATE_ACCOUNTS_BKP.iloc[j,0]}&streetAddressLine1={df_GEOCODE_TRANSLATE_ACCOUNTS_BKP.iloc[j,3]}&countryISOAlpha2Code={df_GEOCODE_TRANSLATE_ACCOUNTS_BKP.iloc[j,1]}&addressLocality={df_GEOCODE_TRANSLATE_ACCOUNTS_BKP.iloc[j,2]}&addressRegion={df_GEOCODE_TRANSLATE_ACCOUNTS_BKP.iloc[j,5]}&postalCode{df_GEOCODE_TRANSLATE_ACCOUNTS_BKP.iloc[j,4]}"""
    url = url.replace(" ", "%20")
    url = url.replace("None", "")
    conn.request("GET", url, headers=headers)
    res = conn.getresponse()
    data = res.read()
    json_obj =json.loads(data)
    
    df_dnb=pd.json_normalize(json_obj['matchCandidates'])
    df_dnb_ds2=df_dnb[['organization.primaryName','organization.primaryAddress.streetAddress.line1','displaySequence','organization.duns','matchQualityInformation.matchGrade','matchQualityInformation.confidenceCode','organization.dunsControlStatus.operatingStatus.description','organization.tradeStyleNames','matchQualityInformation.nameMatchScore','organization.primaryAddress.addressLocality.name','organization.primaryAddress.postalCode']].rename(columns = {'displaySequence':'displaySequence','organization.duns':'duns','organization.primaryName':'dnb.name','organization.primaryAddress.streetAddress.line1':'dnb.address','matchQualityInformation.matchGrade':'dnb.matchGrade','matchQualityInformation.confidenceCode':'dnb.confidenceCode','organization.dunsControlStatus.operatingStatus.description':'operatingStatus','organization.tradeStyleNames':'dnb.tradeStyleNames','matchQualityInformation.nameMatchScore':'dnb.nameMatchScore','organization.primaryAddress.addressLocality.name':'dnb.addressLocality','organization.primaryAddress.postalCode':'dnb.postal_code'})
    n_rows=len(df_dnb_ds2)
    for i in range(0,n_rows):
      df_a=pd.DataFrame(df_dnb_ds2.iloc[i,])
      df_b=df_a.transpose()
      df_b['Name']=df_GEOCODE_TRANSLATE_ACCOUNTS_BKP.iloc[j,0]
      df_b['streetAddressLine1']=df_GEOCODE_TRANSLATE_ACCOUNTS_BKP.iloc[j,3]
      df_b['countryISOAlpha2Code']=df_GEOCODE_TRANSLATE_ACCOUNTS_BKP.iloc[j,1]
      df_b['addressRegion']=df_GEOCODE_TRANSLATE_ACCOUNTS_BKP.iloc[j,2]
      df_b['postalCode']=df_GEOCODE_TRANSLATE_ACCOUNTS_BKP.iloc[j,4]
      df_b['addressLocality']=df_GEOCODE_TRANSLATE_ACCOUNTS_BKP.iloc[j,5]
      df_b['ID']=f"""{j}"""
      #New Engine
      if df_b['dnb.matchGrade'].str.len().item()>=8:
        if df_b['dnb.confidenceCode'].item()>=7 and df_b['operatingStatus'].item() in word_active:
          df_b['Comment']=f"""Best match, New Engine"""
          df_b['Priorization']=10
          df_dnb_final = df_dnb_final.append(df_b)
          #break
        elif df_b['dnb.confidenceCode'].item()>=9 and df_b['operatingStatus'].item() in word_oob:
          df_b['Comment']=f"""Good match, New Engine"""
          df_b['Priorization']=5
          df_dnb_final = df_dnb_final.append(df_b)
          #break       
        elif df_b['dnb.confidenceCode'].item()>=5 and df_b['operatingStatus'].item() in word_active:
          if E1c1.match(df_b['dnb.matchGrade'].item()) or E1c2.match(df_b['dnb.matchGrade'].item()) or E1c3.match(df_b['dnb.matchGrade'].item()) or E1c4.match(df_b['dnb.matchGrade'].item()) or E1c5.match(df_b  ['dnb.matchGrade'].item()) or E1c6.match(df_b['dnb.matchGrade'].item()):
            df_b['Comment']=f"""Accomplished the conditions, New Engine"""
            df_b['Priorization']=1
            df_dnb_final = df_dnb_final.append(df_b)
            #break
          else:
            df_b['Comment']=f"""Not accomplished the conditions, New Engine"""
            df_b['Priorization']=999
            df_dnb_final = df_dnb_final.append(df_b)
            #break
        else:
          df_b['Comment']=f"""Not enougth quality, New Engine"""
          df_b['Priorization']=999
          df_dnb_final = df_dnb_final.append(df_b)
          #break
      #Legacy Engine 
      else:
          if df_b['dnb.confidenceCode'].item()>=8 and df_b['operatingStatus'].item() in word_active:
            df_b['Comment']=f"""Best match, Legacy Engine"""
            df_b['Priorization']=10
            df_dnb_final = df_dnb_final.append(df_b)
           # break
          elif df_b['dnb.confidenceCode'].item()>=9 and df_b['operatingStatus'].item() in word_oob:
            df_b['Comment']=f"""Good match, Legacy Engine"""
            df_b['Priorization']=5
            df_dnb_final = df_dnb_final.append(df_b)           
          elif df_b['dnb.confidenceCode'].item()>=7 and df_b['operatingStatus'].item() in word_active:
            if E2c1.match(df_b['dnb.matchGrade'].item()) or E2c2.match(df_b['dnb.matchGrade'].item()) or E2c3.match(df_b['dnb.matchGrade'].item()):
              df_b['Comment']=f"""Accomplished the conditions, Legacy Engine"""
              df_b['Priorization']=1
              df_dnb_final = df_dnb_final.append(df_b)
            #  break
            else:
              df_b['Comment']=f"""Not accomplished the conditions, Legacy Engine"""
              df_b['Priorization']=999
              df_dnb_final = df_dnb_final.append(df_b)
             # break
          else:
            df_b['Comment']=f"""Not enougth quality, Legacy Engine"""
            df_b['Priorization']=999
            df_dnb_final = df_dnb_final.append(df_b)
           # break      
  except:
    break

# COMMAND ----------

# DBTITLE 1,Filtering creating dataframes
from pyspark.sql.types import StructType, StructField, LongType, StringType
from pyspark.sql import functions as sf
df_dnb_final['Score_thermo']=df_dnb_final['Priorization']+df_dnb_final['dnb.confidenceCode']
df_dnb_final['ID'] = df_dnb_final['ID'].astype(int)
df_dnb_final['Score_thermo'] = df_dnb_final['Score_thermo'].astype(int)
df_dnb_final['Priorization'] = df_dnb_final['Priorization'].astype(int)
df_dnb_final['dnb.confidenceCode'] = df_dnb_final['dnb.confidenceCode'].astype(int)
df_dnb_final['dnb.nameMatchScore'] = df_dnb_final['dnb.nameMatchScore'].astype(int)
df_dnb_final['postalCode'] = df_dnb_final['postalCode'].astype(str)
df_dnb_final['dnb.postal_code'] = df_dnb_final['dnb.postal_code'].astype(str)
df_dnb_final['displaySequence'] = df_dnb_final['displaySequence'].astype(int)
df_dnb_final['duns'] = df_dnb_final['duns'].astype(str)
schema = StructType([
  StructField("ID", LongType()),
  StructField("Score_thermo", LongType(),True),
  StructField("Priorization", LongType()),
  StructField("Name", StringType()),
  StructField("streetAddressLine1", StringType()),
  StructField("countryISOAlpha2Code", StringType()),
  StructField("addressRegion", StringType()),
  StructField("addressLocality", StringType()),
  StructField("postalCode", StringType()),
  StructField("displaySequence", LongType()),
  StructField("duns", StringType()),
  StructField("dnb.confidenceCode", LongType()),
  StructField("dnb.matchGrade", StringType()),
  StructField("dnb.tradeStyleNames", StringType()),
  StructField("dnb.nameMatchScore", LongType()),
  StructField("dnb.name", StringType()),
  StructField("dnb.address", StringType()),
  StructField("operatingStatus", StringType()),
  StructField("dnb.addressLocality", StringType()),
  StructField("dnb.postal_code", StringType()),
  StructField("Comment", StringType()),
])
spark_df = spark.createDataFrame(df_dnb_final, schema = schema)
spark_df_2 = spark_df.filter("Score_thermo <= 999")
spark_df_2=spark_df_2.groupBy('ID').agg(functions.max('Score_thermo'))
spark_df_2=spark_df_2.withColumnRenamed("ID","ID_2")
spark_df_2=spark_df_2.withColumnRenamed("max(Score_thermo)","Score_thermo2")
spark_df_3=spark_df.join(spark_df_2,sf.concat(spark_df.Score_thermo,spark_df.ID)== sf.concat(spark_df_2.Score_thermo2,spark_df_2.ID_2),'left').select(spark_df["*"],spark_df_2["ID_2"])
spark_df_4 = spark_df_3.filter("ID_2 >=0")

# COMMAND ----------

display(spark_df_3)

# COMMAND ----------

      df_dnb_ds1=pd.DataFrame()
      conn = http.client.HTTPSConnection("plus.dnb.com")
      headers = {
      'accept': "application/json;charset=utf-8",
      'authorization': authorization_key,
      }      
      url=f"""{url_company_resolution}={df_DNB_DATA_RAW.iloc[0,1]}&streetAddressLine1={df_DNB_DATA_RAW.iloc[0,2]}&countryISOAlpha2Code={df_DNB_DATA_RAW.iloc[0,0]}&addressLocality={df_DNB_DATA_RAW.iloc[0,4]}&addressRegion={df_DNB_DATA_RAW.iloc[0,5]}&confidenceLowerLevelThresholdValue=5&exclusionCriteria=ExcludeOutofBusiness%2CExcludeNonHeadQuarters"""
      url = url.replace(" ", "%20")
      conn.request("GET", url, headers=headers)
      res = conn.getresponse()
      data = res.read()
      json_obj =json.loads(data)
      df_dnb=pd.json_normalize(json_obj['matchCandidates'])
      df_dnb_ds1=df_dnb.loc[df_dnb['displaySequence'] == 1]
      df_dnb_ds1

# COMMAND ----------

df_dnb_ds2=pd.DataFrame()
if df_dnb_ds1['matchQualityInformation.confidenceCode'].item()>=7:
  df_dnb_final=df_dnb_ds1[['organization.duns','matchQualityInformation.matchGrade','matchQualityInformation.confidenceCode']]
  print(f"""confidenceCode: {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()} matchGrade: {df_dnb_ds1['matchQualityInformation.matchGrade'].item()} """)
elif df_dnb_ds1['matchQualityInformation.confidenceCode'].item()>=5:
  if c1.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c2.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c3.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c4.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c5.match(df_dnb_ds1  ['matchQualityInformation.matchGrade'].item()) or c6.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()):
    df_dnb_final=df_dnb_ds1[['displaySequence','matchQualityInformation.confidenceCode','matchQualityInformation.matchGrade','organization.duns','organization.primaryName','organization.primaryAddress.addressCountry.isoAlpha2Code','organization.primaryAddress.addressLocality.name','organization.primaryAddress.streetAddress.line1']]
    print (f"""confidenceCode {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()}""")
  else:
    print (f"""confidenceCode {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()} No accomplished the conditions""")
else:
  print (f"""confidenceCode {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()} No enough quality""")

# COMMAND ----------

import re
c1=re.compile('^A[A-Za-z][A-Za-z]AA.*$', re.IGNORECASE)
c2=re.compile('^[A-Ba-b][A-Za-z]AAA.*$', re.IGNORECASE)
c3=re.compile('^[A-Fa-f][A-Za-z][A-Za-z]AAA.*$', re.IGNORECASE)
c4=re.compile('^A[A-Za-z][A-Za-z][A-Za-z]A[A-Za-z][A-Za-z]A.*$', re.IGNORECASE)
c5=re.compile('^[A-Ba-b][A-Za-z]A[A-Za-z]A[A-Za-z][A-Za-z]A.*$', re.IGNORECASE)
c6=re.compile('^[A-Fa-f][A-Za-z][A-Za-z][A-Za-z]AA[A-Za-z]A.*$', re.IGNORECASE)
for j in range(336681,336681):
  try: 
    df_dnb_ds1=pd.DataFrame()
    df_dnb_ds2=pd.DataFrame()
    conn = http.client.HTTPSConnection("plus.dnb.com")
    headers = {
    'accept': "application/json;charset=utf-8",
    'authorization': authorization_key,
    }
    url=f"""{url_company_resolution}={df_DNB_DATA_RAW.iloc[j,1]}&streetAddressLine1={streetAddressLindf_DNB_DATA_RAW.iloc[j,2]}&countryISOAlpha2Code={df_DNB_DATA_RAW.iloc[j,0]}&addressLocality={df_DNB_DATA_RAW.iloc[j,4]}&addressRegion={df_DNB_DATA_RAW.iloc[j,5]}&confidenceLowerLevelThresholdValue=5&exclusionCriteria=ExcludeOutofBusiness%2CExcludeNonHeadQuarters"""
    url = url.replace(" ", "%20")
    conn.request("GET", url, headers=headers)
    res = conn.getresponse()
    data = res.read()
    json_obj =json.loads(data)
    #df_dnb=pd.json_normalize(json_obj['matchCandidates'])
    #df_dnb_ds1=df_dnb.loc[df_dnb['displaySequence'] == 1]
    df_dnb=pd.json_normalize(json_obj['matchCandidates'])
    df_dnb_ds1=df_dnb.loc[df_dnb['displaySequence'] == 1]
    if df_dnb_ds1['matchQualityInformation.confidenceCode'].item()>=7:
      df_dnb_final=df_dnb_ds1[['displaySequence','matchQualityInformation.confidenceCode','matchQualityInformation.matchGrade','organization.duns','organization.primaryName','organization.primaryAddress.addressCountry.isoAlpha2Code','organization.primaryAddress.addressLocality.name','organization.primaryAddress.streetAddress.line1']]
      print(f"""confidenceCode: {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()} matchGrade: {df_dnb_ds1['matchQualityInformation.matchGrade'].item()} """)
    elif df_dnb_ds1['matchQualityInformation.confidenceCode'].item()>=5:
      if c1.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c2.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c3.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c4.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c5.match(df_dnb_ds1  ['matchQualityInformation.matchGrade'].item()) or c6.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()):
        df_dnb_final=df_dnb_ds1[['displaySequence','matchQualityInformation.confidenceCode','matchQualityInformation.matchGrade','organization.duns','organization.primaryName','organization.primaryAddress.addressCountry.isoAlpha2Code','organization.primaryAddress.addressLocality.name','organization.primaryAddress.streetAddress.line1']]
        print (f"""confidenceCode {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()}""")
      else:
        print (f"""confidenceCode {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()} No accomplished the conditions""")
    else:
      print (f"""confidenceCode {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()} No enought quality""")
  except:
    print("No response from DNB")


# COMMAND ----------



# COMMAND ----------

df_dnb_ds2=df_dnb_ds1[['organization.duns','matchQualityInformation.matchGrade','matchQualityInformation.confidenceCode']]
df_dnb_ds2['Comment']=f"""confidenceCode {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()} No accomplished the conditions"""

# COMMAND ----------

df_dnb_ds2

# COMMAND ----------

import re
c1=re.compile('^A[A-Za-z][A-Za-z]AA.*$', re.IGNORECASE)
c2=re.compile('^[A-Ba-b][A-Za-z]AAA.*$', re.IGNORECASE)
c3=re.compile('^[A-Fa-f][A-Za-z][A-Za-z]AAA.*$', re.IGNORECASE)
c4=re.compile('^A[A-Za-z][A-Za-z][A-Za-z]A[A-Za-z][A-Za-z]A.*$', re.IGNORECASE)
c5=re.compile('^[A-Ba-b][A-Za-z]A[A-Za-z]A[A-Za-z][A-Za-z]A.*$', re.IGNORECASE)
c6=re.compile('^[A-Fa-f][A-Za-z][A-Za-z][A-Za-z]AA[A-Za-z]A.*$', re.IGNORECASE)

conn = http.client.HTTPSConnection("plus.dnb.com")
headers = {
'accept': "application/json;charset=utf-8",
'authorization': authorization_key,
}
url=f"""{url_company_resolution}={name}&streetAddressLine1={streetAddressLine1}&countryISOAlpha2Code={countryISOAlpha2Code}&addressLocality={addressLocality}&addressRegion={addressRegion}&confidenceLowerLevelThresholdValue=5&exclusionCriteria=ExcludeOutofBusiness%2CExcludeNonHeadQuarters"""
url = url.replace(" ", "%20")
conn.request("GET", url, headers=headers)
res = conn.getresponse()
data = res.read()
json_obj =json.loads(data)
#df_dnb=pd.json_normalize(json_obj['matchCandidates'])
#df_dnb_ds1=df_dnb.loc[df_dnb['displaySequence'] == 1]
df_api_responses = pd.DataFrame()
try: 
  df_dnb=pd.json_normalize(json_obj['matchCandidates'])
  df_dnb_ds1=df_dnb.loc[df_dnb['displaySequence'] == 1]
  if df_dnb_ds1['matchQualityInformation.confidenceCode'].item()>=7:
    df_dnb_final=df_dnb_ds1[['displaySequence','matchQualityInformation.confidenceCode','matchQualityInformation.matchGrade','organization.duns','organization.primaryName','organization.primaryAddress.addressCountry.isoAlpha2Code','organization.primaryAddress.addressLocality.name','organization.primaryAddress.streetAddress.line1']]
    print(f"""confidenceCode: {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()} matchGrade: {df_dnb_ds1['matchQualityInformation.matchGrade'].item()} """)
  elif df_dnb_ds1['matchQualityInformation.confidenceCode'].item()>=5:
    if c1.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c2.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c3.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c4.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c5.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c6.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()):
      df_dnb_final=df_dnb_ds1[['displaySequence','matchQualityInformation.confidenceCode','matchQualityInformation.matchGrade','organization.duns','organization.primaryName','organization.primaryAddress.addressCountry.isoAlpha2Code','organization.primaryAddress.addressLocality.name','organization.primaryAddress.streetAddress.line1']]
      print (f"""confidenceCode {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()}""")
    else:
      print (f"""confidenceCode {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()} No accomplished the conditions""")
  else:
    print (f"""confidenceCode {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()} No enought quality""")
except:
  print("No response from DNB")

# COMMAND ----------

df_dnb

# COMMAND ----------

try: 
  df_dnb_ds1=df_dnb.loc[df_dnb['displaySequence'] == 1]
  if df_dnb_ds1['matchQualityInformation.confidenceCode'].item()>=7:
    df_dnb_final=df_dnb_ds1[['displaySequence','matchQualityInformation.confidenceCode','matchQualityInformation.matchGrade','organization.duns','organization.primaryName','organization.primaryAddress.addressCountry.isoAlpha2Code','organization.primaryAddress.addressLocality.name','organization.primaryAddress.streetAddress.line1']]
    print(f"""confidenceCode: {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()} matchGrade: {df_dnb_ds1['matchQualityInformation.matchGrade'].item()} """)
  elif df_dnb_ds1['matchQualityInformation.confidenceCode'].item()>=5:
    if c1.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c2.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c3.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c4.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c5.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c6.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()):
      df_dnb_final=df_dnb_ds1[['displaySequence','matchQualityInformation.confidenceCode','matchQualityInformation.matchGrade','organization.duns','organization.primaryName','organization.primaryAddress.addressCountry.isoAlpha2Code','organization.primaryAddress.addressLocality.name','organization.primaryAddress.streetAddress.line1']]
      print (f"""confidenceCode {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()}""")
    else:
      print (f"""confidenceCode {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()} No accomplished the conditions""")
  else:
    print (f"""confidenceCode {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()} No enought quality""")
except:
  print("No response from DNB")

# COMMAND ----------

['matchQualityInformation.confidenceCode'].item()

# COMMAND ----------

try: 
  if df_dnb_ds1['matchQualityInformation.confidenceCode'].item()>=7:
    df_dnb_final=df_dnb_ds1[['displaySequence','matchQualityInformation.confidenceCode','matchQualityInformation.matchGrade','organization.duns','organization.primaryName','organization.primaryAddress.addressCountry.isoAlpha2Code','organization.primaryAddress.addressLocality.name','organization.primaryAddress.streetAddress.line1']]
    print(f"""confidenceCode: {df_dnb_ds1['matchQualityInformation.confidenceCode'].item()} matchGrade: {df_dnb_ds1['matchQualityInformation.matchGrade'].item()} """)
  elif df_dnb_ds1['matchQualityInformation.confidenceCode'].item()>=5:
    if c1.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c2.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c3.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c4.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c5.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c6.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()):
      df_dnb_final=df_dnb_ds1[['displaySequence','matchQualityInformation.confidenceCode','matchQualityInformation.matchGrade','organization.duns','organization.primaryName','organization.primaryAddress.addressCountry.isoAlpha2Code','organization.primaryAddress.addressLocality.name','organization.primaryAddress.streetAddress.line1']]
      print (f"""Match {df_dnb_ds1['matchQualityInformation.matchGrade'].item()}""")
  else:
    print ("No enough quality")
except:
  print("No response from DNB")

# COMMAND ----------

df_dnb_final

# COMMAND ----------



# COMMAND ----------

import re
c1=re.compile('^A[A-Za-z][A-Za-z]AA.*$', re.IGNORECASE)
c2=re.compile('^[A-Ba-b][A-Za-z]AAA.*$', re.IGNORECASE)
c3=re.compile('^[A-Fa-f][A-Za-z][A-Za-z]AAA.*$', re.IGNORECASE)
c4=re.compile('^A[A-Za-z][A-Za-z][A-Za-z]A[A-Za-z][A-Za-z]A.*$', re.IGNORECASE)
c5=re.compile('^[A-Ba-b][A-Za-z]A[A-Za-z]A[A-Za-z][A-Za-z]A.*$', re.IGNORECASE)
c6=re.compile('^[A-Fa-f][A-Za-z][A-Za-z][A-Za-z]AA[A-Za-z]A.*$', re.IGNORECASE)
if c1.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) or c2.match(df_dnb_ds1['matchQualityInformation.matchGrade'].item()) :
  print ("Match")
else:
  print ("No match")

# COMMAND ----------

import re
x=re.compile('^A.*AA.*$', re.IGNORECASE)
stri='BFAAAZZBFZZ'
if x.match(stri):
  print ("Match")
else:
  print ("No match")

# COMMAND ----------

import re
x=re.compile('^.*AAA.*$', re.IGNORECASE)
#stri='BFAAAZZBFZZ'
stri=data_general_filter['matchQualityInformation.matchGrade'].item()
if x.match(stri):
  print ("Match")
else:
  print ("No match")
#data_general_filter['matchQualityInformation.matchGrade'].astype("string")

# COMMAND ----------

data_general_filter['matchQualityInformation.matchGrade'].item()

# COMMAND ----------

data_general_filter['matchQualityInformation.matchGrade'].str[2:3].startswith('A')

# COMMAND ----------

display(data_general[['displaySequence','matchQualityInformation.confidenceCode','matchQualityInformation.matchGrade','organization.duns','organization.primaryName','organization.primaryAddress.addressCountry.isoAlpha2Code','organization.primaryAddress.addressLocality.name','organization.primaryAddress.streetAddress.line1']])

# COMMAND ----------

from pyspark.sql.types import *
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "false")
data_general=data_general.astype(str)
spark_df = spark.createDataFrame(data_general)

# COMMAND ----------

display(matchQualityInformation)

# COMMAND ----------

matchCandidates=pd.json_normalize(json_obj, record_path=['matchCandidates', 'matchCandidates'])
display(matchCandidates)

# COMMAND ----------

matchCandidates=pd.json_normalize(json_obj, record_path=['organization', 'businessTradingNorms'],meta=        [['organization','duns']])matchCandidates=pd.json_normalize(json_obj, record_path=['organization', 'businessTradingNorms'],meta=        [['organization','duns']])

# COMMAND ----------

