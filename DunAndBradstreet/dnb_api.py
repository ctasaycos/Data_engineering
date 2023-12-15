'''
Dun & Bradstreet API
The following script request data from the API of Dun & Bradstreet, to do this you need general information of the company such as:
Address
Country
Name
City
and so on
The data we are pulling is DUNS Number
further information here: https://directplus.documentation.dnb.com/openAPI.html?apiID=authenticationV3
'''
# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.dropdown("run_env", "DEV" ,["DEV","QA","PRD","UAT"])
run_env = dbutils.widgets.get("run_env").strip().upper()
print("run_env: ", run_env)

# COMMAND ----------

import pandas as pd
import re

# COMMAND ----------

# MAGIC %run  ./Connection_DNB $env = run_env

# COMMAND ----------
# General information of the company is stored in a Oracle database
query="""
select 
CASE WHEN translated_bu_account is null then bu_account else translated_bu_account end name,
COUNTRY_SHORT as countryISOAlpha2Code,
case when CITY_TRANSLATED is null then GEOCODED_CITY else CITY_TRANSLATED end addressLocality,
CASE WHEN ADDRESS_1_TRANSLATED is null then STREET_ADDRESS else ADDRESS_1_TRANSLATED end streetAddressLine1,
zip as postalCode,
STATE as addressRegion
from  ddd_table
where COUNTRY_SHORT  IN ('US') and ROWNUM <= 6
union 
select 
CASE WHEN translated_bu_account is null then bu_account else translated_bu_account end name,
COUNTRY_SHORT as countryISOAlpha2Code,
case when CITY_TRANSLATED is null then GEOCODED_CITY else CITY_TRANSLATED end addressLocality,
CASE WHEN ADDRESS_1_TRANSLATED is null then STREET_ADDRESS else ADDRESS_1_TRANSLATED end streetAddressLine1,
zip as postalCode,
STATE as addressRegion
from ddd_table
where COUNTRY_SHORT  IN ('FR') and ROWNUM <= 2
union 
select 
CASE WHEN translated_bu_account is null then bu_account else translated_bu_account end name,
COUNTRY_SHORT as countryISOAlpha2Code,
case when CITY_TRANSLATED is null then GEOCODED_CITY else CITY_TRANSLATED end addressLocality,
CASE WHEN ADDRESS_1_TRANSLATED is null then STREET_ADDRESS else ADDRESS_1_TRANSLATED end streetAddressLine1,
zip as postalCode,
STATE as addressRegion
from  ddd_table
where COUNTRY_SHORT  IN ('CN') and ROWNUM <= 5
"""
df_GEOCODE_TRANSLATE_ACCOUNTS_BKP = pd.read_sql(query, con=ss)

# COMMAND ----------

#We need to generate a regex logic because the information has to be personalized (Business decision)
# DBTITLE 1,Conditions
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
df_dnb_final=pd.DataFrame(columns=['ID','Score_tt','Priorization','Name', 'streetAddressLine1', 'countryISOAlpha2Code', 'addressRegion', 'addressLocality','postalCode','displaySequence','duns','dnb.confidenceCode','dnb.matchGrade','dnb.tradeStyleNames','dnb.nameMatchScore','dnb.name','dnb.address','operatingStatus','dnb.addressLocality','dnb.postal_code','Comment'])
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
