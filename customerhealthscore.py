#Install libraries
#python -m pip install google-api-cient
#pip install google-api-python-client
#python -m pip install google-auth


#Collect data from google sheet
from googleapiclient.discovery import build
from google.oauth2 import service_account
import pandas as pd

SERVICE_ACCOUNT_FILE ='/Users/carlostasaycosilva/Library/Mobile Documents/com~apple~CloudDocs/Telematica/Python/customer_health_score.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

creds = None
creds = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

SAMPLE_SPREADSHEET_ID = '17V4G6kYYoTzafOjxAhLdPwag8zdQ_I15KGeOEag_txM'

service = build('sheets','v4',credentials=creds)

sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
range="Datos!A1:L500").execute()

data_google=result
data_google2=data_google['values']
df = pd.DataFrame(data_google2)
new_header = df.iloc[0]
df = df[1:]
df.columns = new_header
df= df.rename(columns=new_header)

#Collect data from salesforce
from simple_salesforce import Salesforce
sf = Salesforce(instance_url='https://telematica.lightning.force.com/', session_id='ctasayco@telematica.com.pe')
sf = Salesforce(username='ctasayco@telematica.com.pe', password='26KPaMsbaK71VyJHgAc5', security_token='ekcf2TEmvphmtDpwvhjCuEqxe')
opportunity_output = sf.bulk.Opportunity.query("Select Id,N_Oportunidad__c,AccountId, StageName,CloseDate FROM Opportunity where StageName='Orden/Contrato Recepcionado'")
df_opportunities_bought = pd.DataFrame(opportunity_output,columns=['Id','N_Oportunidad__c','AccountId','StageName','CloseDate'])
OpportunityLineItem_output = sf.bulk.OpportunityLineItem.query("Select OpportunityId,PricebookEntryId FROM OpportunityLineItem")
df_OpportunityLineItem_output = pd.DataFrame(OpportunityLineItem_output,columns=['OpportunityId','PricebookEntryId'])
PricebookEntry_output = sf.bulk.PricebookEntry.query("Select Id, ProductCode FROM PricebookEntry")
df_PricebookEntry_output = pd.DataFrame(PricebookEntry_output,columns=['Id','ProductCode'])
Product2_output = sf.bulk.Product2.query("Select ProductCode,Marca__c,Familia__c,Subfamilia__c,Name FROM Product2")
df_Product2_output = pd.DataFrame(Product2_output,columns=['ProductCode','Marca__c','Familia__c','Subfamilia__c','Name'])
Account_output = sf.bulk.Account.query("Select Id,Numero_de_Cliente__c FROM Account")
df_Account_output = pd.DataFrame(Account_output,columns=['Id','Numero_de_Cliente__c'])

#Table with opportunities, account, product
df_opportunity_bought_product1=df_opportunities_bought.merge(df_OpportunityLineItem_output.rename({'OpportunityId': 'OpportunityId_r'}, axis=1),left_on='Id', right_on='OpportunityId_r', how='left')
df_opportunity_bought_product2=df_opportunity_bought_product1.merge(df_PricebookEntry_output.rename({'Id': 'Id2'}, axis=1),left_on='PricebookEntryId', right_on='Id2', how='left')
df_opportunity_bought_product3=df_opportunity_bought_product2.merge(df_Product2_output.rename({'ProductCode': 'ProductCode2'}, axis=1),left_on='ProductCode', right_on='ProductCode2', how='left')
#df_opportunity_bought_product3.to_excel(r'D:/CS/count.xlsx', index = False)

#Number of licenses from ESRI
#df_number_licenses_0=df_opportunity_bought_product3.loc[df_opportunity_bought_product3['Marca__c'] == 'ESRI'].groupby(['AccountId']).count()
df_number_licenses_0=df_opportunity_bought_product3.loc[df_opportunity_bought_product3['Marca__c'] == 'ESRI'].groupby(["AccountId"], as_index=False).count()
df_number_licenses = pd.DataFrame(df_number_licenses_0,columns=['AccountId','Id'])
df_number_licenses = df_number_licenses.rename({'AccountId': 'AccountId', 'Id': 'flag_licenses'}, axis=1)

#Number of SSPP
df_number_sspp_0=df_opportunity_bought_product3.loc[df_opportunity_bought_product3['Familia__c'] == 'SERVICIOS'].groupby(['AccountId'],as_index=False).count()
df_number_sspp = pd.DataFrame(df_number_sspp_0,columns=['AccountId','Id'])
df_number_sspp = df_number_sspp.rename({'AccountId': 'AccountId', 'Id': 'flag_SSPP'}, axis=1)

#Number of Courses
df_number_cursos_0=df_opportunity_bought_product3.loc[((df_opportunity_bought_product3['Familia__c'] == 'Cursos') | (df_opportunity_bought_product3['Familia__c'] == 'Paquetes')) & (df_opportunity_bought_product3['Subfamilia__c'] != 'Curso 708')].groupby(['AccountId'],as_index=False).count()
df_number_cursos = pd.DataFrame(df_number_cursos_0,columns=['AccountId','Id'])
df_number_cursos = df_number_cursos.rename({'AccountId': 'AccountId', 'Id': 'flag_cursos'}, axis=1)

#Variable Y
import pandasql as ps
df_opportunities_bought0 = df_opportunities_bought
import numpy as np
q_scorey = """SELECT AccountId,max(Closedate) Closedate, current_date , count(distinct Id) as nro_oport  FROM df_opportunities_bought0 group by AccountId"""
df_scorey_v1 = ps.sqldf(q_scorey, locals())
df_scorey_v1['Closedate'] = pd.to_datetime(df_scorey_v1['Closedate'])
df_scorey_v1['current_date'] = pd.to_datetime(df_scorey_v1['current_date'])
df_scorey_v1.reset_index(drop=True)
df_scorey_v1['dif'] = (df_scorey_v1['current_date']-df_scorey_v1['Closedate']).dt.days
q_scorey2 = """SELECT AccountId,Closedate, current_date ,  nro_oport,  dif,
case when  nro_oport > 1 and dif < 365 then 1 else 0 end 'Y'
FROM df_scorey_v1 
"""
df_scorey_v2 = ps.sqldf(q_scorey2, locals())
df_scorey_v2.head()
df_number_licenses
df=df.loc[(df['Numero de Cliente'] != 'Nuevo') & (df['Numero de Cliente'] != 'NUEVO')]
dataset2=df.merge(df_Account_output.rename({'Numero_de_Cliente__c': 'Numero_de_Cliente__c'}, axis=1),left_on='Numero de Cliente', right_on='Numero_de_Cliente__c', how='left')
dataset3=dataset2.merge(df_number_licenses.rename({'AccountId': 'Id2'}, axis=1),left_on='Id', right_on='Id2', how='left')
dataset4=dataset3.merge(df_number_sspp.rename({'AccountId': 'Id2'}, axis=1),left_on='Id', right_on='Id2', how='left')
dataset5=dataset4.merge(df_number_cursos.rename({'AccountId': 'Id2'}, axis=1),left_on='Id', right_on='Id2', how='left')
dataset6=dataset5.merge(df_scorey_v2.rename({'AccountId': 'Id3'}, axis=1),left_on='Id', right_on='Id3', how='left')
dataset6.to_csv('/Users/carlostasaycosilva/Library/Mobile Documents/com~apple~CloudDocs/Telematica/Python/compare1.csv',index=False)

#Modeling
data=dataset6[['Id','Flag_Esri_Training','Flag_ArcGIS_Pro','Flag_estado','perc_creditos','perc_usu_activos','soporte','perc_usopro','flag_licenses','flag_SSPP','flag_cursos','Y']]
data=data.fillna(0)
data[['perc_creditos']]=data[['perc_creditos']].stack().str.replace(',','.').unstack()
data[['perc_usu_activos']]=data[['perc_usu_activos']].stack().str.replace(',','.').unstack()
data[['perc_usopro']]=data[['perc_usopro']].stack().str.replace(',','.').unstack()
data["perc_creditos"] = data.perc_creditos.astype(float)
data["perc_usu_activos"] = data.perc_usu_activos.astype(float)
data["soporte"] = data.soporte.astype(float)
data["perc_usopro"] = data.perc_usopro.astype(float)
data["Flag_Esri_Training"] = data.Flag_Esri_Training.astype(float)
data["Flag_estado"] = data.Flag_estado.astype(float)
data["Flag_ArcGIS_Pro"] = data.Flag_ArcGIS_Pro.astype(float)
data.dtypes
data=data.set_index("Id",inplace = False)
data_x=data[['Flag_Esri_Training','Flag_ArcGIS_Pro','Flag_estado','perc_creditos','perc_usu_activos','soporte','perc_usopro','flag_licenses','flag_SSPP','flag_cursos']]
data_y=data[['Y']]

import matplotlib.pyplot as plt
from sklearn.model_selection import train_test_split
x_train, x_test, y_train, y_test = train_test_split(data_x, data_y, test_size=0.25, random_state=0)
from sklearn.linear_model import LogisticRegression
logisticRegr = LogisticRegression()
logisticRegr.fit(x_train, y_train)


score = logisticRegr.score(x_test, y_test)
print(score)
predictions = logisticRegr.predict(x_test)

data.dtypes
data['soporte']
import numpy as np; np.random.seed(0)
import seaborn as sns; sns.set_theme()
correlation_mat = data.corr()
sns.heatmap(correlation_mat)
plt.show()

y_pred_proba = logisticRegr.predict_proba(x_test)[::,1]


https://engoo.com/app/daily-news