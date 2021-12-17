#Login
from simple_salesforce import Salesforce
sf = Salesforce(instance_url='https://telematica.lightning.force.com/', session_id='ctasayco@telematica.com.pe')
sf = Salesforce(username='ctasayco@telematica.com.pe', password='26KPaMsbaK71VyJHgAc5', security_token='ekcf2TEmvphmtDpwvhjCuEqxe')
import json
import numpy as np
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt
pd.set_option('display.max_columns',10)
account_output = sf.bulk.Account.query("Select Id,Name,Sector_1__c, Industria_1__c,Numero_de_Cliente__c FROM Account")
df_account= pd.DataFrame(account_output,columns=['Id','Name','Sector_1__c','Industria_1__c','Numero_de_Cliente__c'])
#list(df_account.columns.values) 
df_account["llave"] = df_account["Numero_de_Cliente__c"] + df_account["Name"]
import pandasql as ps
#Clasificacion de cuentas
dataset = pd.read_csv('D:/Clasificacion de clientes/clasificacion3.csv',sep=',',encoding='latin-1')
df_account2=df_account.merge(dataset.rename({'ï»¿Id': 'Id2'}, axis=1),
              left_on='Id', right_on='Id2', how='left')
#df_account2=df_account2.dropna()
#df_account2.isna().sum()
#df_account2
#q1 = """SELECT Id, count(*) FROM df_account2 group by Id ORDER BY COUNT(*) DESC """
#print(ps.sqldf(q1, locals()))
#list(df_account2.columns.values) 
opportunity_output = sf.bulk.Opportunity.query("Select Id,N_Oportunidad__c,AccountId,CloseDate,StageName FROM Opportunity")
df_opportunity= pd.DataFrame(opportunity_output,columns=['Id','N_Oportunidad__c','AccountId','CloseDate','StageName'])
df_opportunity['CloseDate'] = pd.to_datetime(df_opportunity['CloseDate'],format='%Y-%m-%d')
df_opportunity = sf.bulk.Opportunity.query(
"Select Id,Name,AccountId,N_Oportunidad__c, StageName, CloseDate FROM Opportunity where CloseDate >= 2017-01-01 and StageName = 'Orden/Contrato Recepcionado'")
df_opportunity = pd.DataFrame(df_opportunity,columns=['Id','N_Oportunidad__c','AccountId','CloseDate','StageName'])
#df_opportunity
product_output = sf.bulk.OpportunityLineItem.query(
    "Select Id,Name,OpportunityId,Quantity,ProductCode,Precio_Total_Sin_IGV__c FROM OpportunityLineItem")
df_OpportunityLineItem = pd.DataFrame(product_output,columns=['Id','Name','OpportunityId','Quantity','ProductCode','Precio_Total_Sin_IGV__c'])
#df_OpportunityLineItem
q2 = """SELECT OpportunityId, sum(Quantity) as total_productos, sum(Precio_Total_Sin_IGV__c) as venta_total FROM df_OpportunityLineItem group by OpportunityId """
#print(ps.sqldf(q2, locals()))
df_OpportunityLineItem=ps.sqldf(q2, locals())
#df_OpportunityLineItem
df_opportunity=df_opportunity.merge(df_OpportunityLineItem.rename({'OpportunityId': 'Id2'}, axis=1),
              left_on='Id', right_on='Id2', how='left') 
df_opportunity0=df_opportunity.merge(df_OpportunityLineItem.rename({'OpportunityId': 'Id2'}, axis=1),
              left_on='Id', right_on='Id2', how='left') 
q000 = """SELECT AccountId,sum(venta_total_x) as venta  FROM df_opportunity0 group by AccountId """
df_opportunity0 = ps.sqldf(q000, locals())
#df_opportunity0
df_opportunity['CloseDate'] = pd.to_datetime(df_opportunity['CloseDate'],format='%Y-%m-%d')
df_opportunity['year']=df_opportunity['CloseDate'].dt.year
#df_opportunity
#list(df_opportunity.columns.values) 
q6 = """SELECT AccountId,year, sum(total_productos) as total_productos, sum(venta_total) as venta  FROM df_opportunity group by AccountId,year """
df_opportunity = ps.sqldf(q6, locals())
#print(ps.sqldf(q6, locals()))
#df_opportunity
#df_opportunity
q7 = """SELECT AccountId, sum(total_productos) as total_productos,
  max(case when year = 2018 then venta end) anho_2018,
  max(case when year = 2019  then venta end) anho_2019,
  max(case when year = 2020  then venta end) anho_2020,
  max(case when year = 2021  then venta end) anho_2021
from df_opportunity group by AccountId"""
df_opportunity =ps.sqldf(q7, locals())
q8 = """SELECT AccountId,sum(total_productos) as total_productos,
case when anho_2018>0 then 1 else 0 end anho_2018v2,
case when anho_2019>0 then 1 else 0 end anho_2019v2,
case when anho_2020>0 then 1 else 0 end anho_2020v2,
case when anho_2021>0 then 1 else 0 end anho_2021v2
from df_opportunity group by AccountId"""
df_opportunity =ps.sqldf(q8, locals())
q9 = """SELECT AccountId, sum(total_productos) as total_productos,
case 
when anho_2021v2 >0 then 'Compro_ult_anho'
when anho_2018v2 + anho_2019v2 + anho_2020v2 + anho_2021v2=1 then 'Compro_una_vez'
when anho_2018v2 + anho_2019v2 + anho_2020v2 + anho_2021v2=2 then 'Compro_dos_vez'
when anho_2018v2 + anho_2019v2 + anho_2020v2 + anho_2021v2>=3 then 'Compro_tres_vez'
else 0 end frec_compra
from df_opportunity group by AccountId"""
#print(ps.sqldf(q9, locals()))
df_opportunity =ps.sqldf(q9, locals())
activos_output = sf.bulk.Asset.query("Select Id,AccountId,UsageEndDate FROM Asset")
df_activos = pd.DataFrame(activos_output,columns=['Id','AccountId','UsageEndDate'])
df_activos['UsageEndDate'] = pd.to_datetime(df_activos['UsageEndDate'],format='%Y-%m-%d')
df_activos = sf.bulk.Asset.query("Select Id,AccountId,UsageEndDate FROM Asset where UsageEndDate >= 2017-01-01")                                             
df_activos = pd.DataFrame(activos_output,columns=['Id','AccountId','UsageEndDate'])
q3 = """SELECT Id,AccountId,UsageEndDate,case when UsageEndDate <= CURRENT_DATE then 1 else 0 end 'Estado' FROM df_activos  """
df_activos = ps.sqldf(q3, locals())
q4 = """SELECT AccountId, count(Id) as nro_activos, sum(Estado) as Estado FROM df_activos group by AccountId """
df_activos = ps.sqldf(q4, locals())
q5 = """SELECT AccountId,  nro_activos, Estado, CAST(Estado as float)/nro_activos as prop_activos FROM df_activos group by AccountId """
df_activos = ps.sqldf(q5, locals())
q0 = """SELECT AccountId, nro_activos,prop_activos,
case when prop_activos > 0.7 then 1 ELSE 0 end 'Churn2'
FROM df_activos group by AccountId,nro_activos """
df_activos = ps.sqldf(q0, locals())
df_account3=df_account2.merge(df_activos.rename({'AccountId': 'AccountId2'}, axis=1),
              left_on='Id', right_on='AccountId2', how='left')
df_account4=df_account3.merge(df_opportunity.rename({'AccountId': 'AccountId3'}, axis=1),
              left_on='Id', right_on='AccountId3', how='inner')
tareas_output = sf.bulk.Task.query("Select Id,AccountId FROM Task")
df_tareas = pd.DataFrame(tareas_output,columns=['Id','AccountId'])
q11 = """SELECT AccountId, count(distinct Id) as total_tareas  FROM df_tareas group by AccountId"""
df_tareas= (ps.sqldf(q11, locals()))
df_account5=df_account4.merge(df_tareas.rename({'AccountId': 'AccountId4'}, axis=1),
              left_on='Id', right_on='AccountId4', how='left')
q13 = """SELECT Id, Sector_1__c, Industria_1__c,Numero_de_Cliente__c,"Clasificacion de Cuentas",
total_productos,frec_compra,total_tareas,nro_activos,
case when prop_activos=0 then 0 when prop_activos>0 then 1 else null end 'Churn'
FROM df_account5 """
df_account6 =ps.sqldf(q13, locals())
na_cols = df_account6.loc[:, df_account6.notnull().any(axis = 0)]
na_cols = na_cols[na_cols == True].reset_index()
na_cols = na_cols["index"].tolist()
for col in df_account6["total_tareas"]:
     if col in na_cols:
        if df_account6["total_tareas"].dtype != 'object':
             df_account6["total_tareas"] =  df_account6["total_tareas"].fillna(df_account6["total_tareas"].mean()).round(0)
#df_account6=df_account6.dropna()
contact_output = sf.bulk.Contact.query("Select Id,AccountId FROM Contact")
df_contact = pd.DataFrame(contact_output,columns=['Id','AccountId'])
q12 = """SELECT AccountId, count(distinct Id) as total_contactos  FROM df_contact group by AccountId"""
df_contact = (ps.sqldf(q12, locals()))
df_account7=df_account6.merge(df_contact.rename({'AccountId': 'AccountId5'}, axis=1),
              left_on='Id', right_on='AccountId5', how='left')
productline_output = sf.bulk.OpportunityLineItem.query(
    "Select Id,ProductCode,OpportunityId FROM OpportunityLineItem")
df_productline = pd.DataFrame(productline_output,columns=['Id','ProductCode','OpportunityId'])
#df_OpportunityLineItem
opportunity_output2 = sf.bulk.Opportunity.query("Select Id,AccountId,CloseDate,StageName FROM Opportunity")
df_opportunity2= pd.DataFrame(opportunity_output2,columns=['Id','AccountId','CloseDate','StageName'])
df_opportunity2['CloseDate'] = pd.to_datetime(df_opportunity2['CloseDate'],format='%Y-%m-%d')
df_opportunity2 = sf.bulk.Opportunity.query(
"Select Id,AccountId,CloseDate,StageName FROM Opportunity where CloseDate >= 2017-01-01 and StageName = 'Orden/Contrato Recepcionado'")
df_opportunity2 = pd.DataFrame(df_opportunity2,columns=['Id','AccountId','CloseDate','StageName'])
df_opportunity3=df_opportunity2.merge(df_productline.rename({'OpportunityId': 'OpportunityId2'}, axis=1),
              left_on='Id', right_on='OpportunityId2', how='left')
q17 = """SELECT AccountId, count(distinct ProductCode) as dif_product  FROM df_opportunity3 group by AccountId """
#print(ps.sqldf(q13, locals()))
df_opportunity3 = ps.sqldf(q17, locals())
df_account8=df_account7.merge(df_opportunity3.rename({'AccountId': 'AccountId6'}, axis=1),
              left_on='Id', right_on='AccountId6', how='left')
df_account9=df_account8.merge(df_activos.rename({'AccountId': 'AccountId4'}, axis=1),
              left_on='Id', right_on='AccountId4', how='left')
df_account10=df_account9.merge(df_opportunity0.rename({'AccountId': 'AccountId7'}, axis=1),
             left_on='Id', right_on='AccountId7', how='left')
q14 = """SELECT Id, Sector_1__c,Industria_1__c,"Clasificacion de Cuentas",frec_compra,total_tareas,total_contactos,nro_activos_y as nro_activos,dif_product,prop_activos,Churn2,
case when venta>=200000 then 'Alto Valor' when venta>=100000 then 'Medio Valor'  else 'Bajo Valor' end 'Tipo_mto'
FROM df_account10 """
df_account10=ps.sqldf(q14, locals())
na_cols = df_account10.isna().any()
na_cols = na_cols[na_cols == True].reset_index()
na_cols = na_cols["index"].tolist()
for col in df_account10.columns[1:]:
     if col in na_cols:
        if df_account10[col].dtype != 'object':
             df_account10[col] =  df_account10[col].fillna(df_account10[col].mean()).round(0)
df_account10['Churn2'] = df_account10['Churn2'].fillna(1)
q15 = """SELECT Id, Sector_1__c,Industria_1__c,"Clasificacion de Cuentas",nro_activos,Tipo_mto,
case when "Clasificacion de Cuentas" is null then 'Transaccionales' else "Clasificacion de Cuentas" end cc_cuentas,
frec_compra,total_tareas,total_contactos,dif_product,prop_activos,Churn2
FROM df_account10 where Sector_1__c IS NOT NULL and Churn2>=0"""
df_account10 =ps.sqldf(q15, locals())
na_cols = df_account10.loc[:, df_account10.notnull().any(axis = 0)]
na_cols = na_cols[na_cols == True].reset_index()
na_cols = na_cols["index"].tolist()
for col in df_account10["total_contactos"]:
     if col in na_cols:
        if df_account10["total_contactos"].dtype != 'object':
             df_account10["total_contactos"] =  df_account10["total_contactos"].fillna(df_account10["total_contactos"].mean()).round(0)
#df_account10.to_excel(r'D:/CS/Churn2.xlsx', index = False)

#model
data_account=df_account10[['Id','Industria_1__c','dif_product','nro_activos','Churn2','cc_cuentas','frec_compra','Tipo_mto']]
activos_output = sf.bulk.Asset.query("Select Id,AccountId,UsageEndDate FROM Asset")
df_activos = pd.DataFrame(activos_output,columns=['Id','AccountId','UsageEndDate'])
df_activos['UsageEndDate'] = pd.to_datetime(df_activos['UsageEndDate'],format='%Y-%m-%d')
df_activos = sf.bulk.Asset.query("Select Id,AccountId,UsageEndDate FROM Asset where UsageEndDate >= 2017-01-01")                                             
df_activos = pd.DataFrame(activos_output,columns=['Id','AccountId','UsageEndDate'])
q3 = """SELECT Id,AccountId,UsageEndDate,case when UsageEndDate <= CURRENT_DATE then 1 else 0 end 'Estado' FROM df_activos  """
df_activos = ps.sqldf(q3, locals())
q4 = """SELECT AccountId, count(Id) as nro_activos, sum(Estado) as Estado FROM df_activos group by AccountId """
df_activos = ps.sqldf(q4, locals())
q5 = """SELECT AccountId,  nro_activos, Estado, CAST(Estado as float)/nro_activos as prop_activos FROM df_activos group by AccountId """
df_activos = ps.sqldf(q5, locals())
data_account2=data_account.merge(df_activos.rename({'AccountId': 'AccountId5'}, axis=1),
              left_on='Id', right_on='AccountId5', how='left')
data_account2['mediana']=data_account2['prop_activos'].mean()
q5 = """SELECT Id, Industria_1__c, dif_product, nro_activos_x,cc_cuentas,frec_compra,Tipo_mto,mediana,
prop_activos, case when prop_activos>=0.5 or prop_activos is null then 1 else 0 end 'churn1'
FROM data_account2  """
data_account3=ps.sqldf(q5, locals())
opportunity_output = sf.bulk.Opportunity.query("Select Id,N_Oportunidad__c,AccountId,CloseDate,StageName FROM Opportunity")
df_opportunity= pd.DataFrame(opportunity_output,columns=['Id','N_Oportunidad__c','AccountId','CloseDate','StageName'])
df_opportunity['CloseDate'] = pd.to_datetime(df_opportunity['CloseDate'],format='%Y-%m-%d')
df_opportunity = sf.bulk.Opportunity.query(
"Select Id,Name,AccountId,N_Oportunidad__c, StageName, CloseDate FROM Opportunity where CloseDate >= 2017-01-01 ")
df_opportunity = pd.DataFrame(df_opportunity,columns=['Id','N_Oportunidad__c','AccountId','CloseDate','StageName'])
q11 = """SELECT AccountId,StageName,max(CloseDate) 
FROM df_opportunity  group by AccountId  """
df_opportunity2=ps.sqldf(q11, locals())
data_account4=data_account3.merge(df_opportunity2.rename({'AccountId': 'AccountId5'}, axis=1),
              left_on='Id', right_on='AccountId5', how='left')
q12 = """SELECT Id,Industria_1__c,dif_product,nro_activos_x,Tipo_mto,cc_cuentas,frec_compra,churn1,prop_activos
,StageName,
case when StageName in ('Perdido') then 1 else 0 end 'Churn0'
FROM data_account4  """
df_opportunity4=ps.sqldf(q12, locals())
q13 = """SELECT Id,Industria_1__c,dif_product,nro_activos_x,Tipo_mto,cc_cuentas,frec_compra,StageName,churn1,prop_activos
,StageName, Churn0,
case when churn1 + Churn0>1 then 1 else 0 end 'Churn2'
FROM df_opportunity4  """
df_opportunity5=ps.sqldf(q13, locals())
sf = Salesforce(instance_url='https://telematica.lightning.force.com/', session_id='ctasayco@telematica.com.pe')
sf = Salesforce(username='ctasayco@telematica.com.pe', password='26KPaMsbaK71VyJHgAc5', security_token='ekcf2TEmvphmtDpwvhjCuEqxe')
tareas_output = sf.bulk.Task.query("Select Id,AccountId,CreatedDate,ActivityDate FROM Task")
df_tareas = pd.DataFrame(tareas_output,columns=['Id','AccountId','CreatedDate','ActivityDate'])
q25 = """SELECT AccountId,max(ActivityDate) ult_activitydate, current_date
 FROM df_tareas group by AccountId"""
df_tareas2 = ps.sqldf(q25, locals())
df_tareas2['ult_activitydate'] = pd.to_datetime(df_tareas2['ult_activitydate'],format='%Y-%m-%d')
df_tareas2['current_date'] = pd.to_datetime(df_tareas2['current_date'],format='%Y-%m-%d')
df_tareas2['diff_days_task'] = df_tareas2['current_date'] - df_tareas2['ult_activitydate']
df_tareas2['diff_days_task']=df_tareas2['diff_days_task']/np.timedelta64(1,'D')
df_opportunity6=df_opportunity5.merge(df_tareas2.rename({'AccountId': 'AccountId5'}, axis=1),
              left_on='Id', right_on='AccountId5', how='left')
df_opportunity6['diff_days_task'] = df_opportunity6['diff_days_task'].fillna(1000)

df_opportunity = sf.bulk.Opportunity.query(
"Select Id,Name,AccountId,N_Oportunidad__c, StageName, CreatedDate FROM Opportunity where CloseDate >= 2017-01-01 ")
df_opportunity = pd.DataFrame(df_opportunity,columns=['Id','N_Oportunidad__c','AccountId','CreatedDate','StageName'])
q11 = """SELECT AccountId,StageName,max(CreatedDate) as cdate
FROM df_opportunity   where  StageName  in ('Orden/Contrato Recepcionado','Perdido') group by AccountId  """
df_opportunity20=ps.sqldf(q11, locals())
from datetime import datetime
df_opportunity20['cdate2'] = pd.to_datetime(df_opportunity20['cdate']*1000000)
df_opportunity20['cdate2'] = pd.to_datetime(df_opportunity20['cdate2'],format='%Y-%m-%d').dt.date
q12 = """SELECT AccountId,StageName, current_date,cdate2,current_date - cdate2 dif,
case when current_date - cdate2 > 270 then 1 else 0 end 'filtro3'
FROM df_opportunity20     """
df_opportunity21=ps.sqldf(q12, locals())
df_opportunity21['current_date'] = pd.to_datetime(df_opportunity21['current_date'],format='%Y-%m-%d').dt.date
df_opportunity21['cdate2'] = pd.to_datetime(df_opportunity21['cdate2'],format='%Y-%m-%d').dt.date
df_opportunity21['dif_days']=(df_opportunity21['current_date']-df_opportunity21['cdate2'])
df_opportunity21['dif_days']=(df_opportunity21['dif_days']/ np.timedelta64(1, 'D')).astype(int)
q13 = """SELECT AccountId,current_date,cdate2,dif_days,
case when dif_days>270 then 1 else 0 end 'filtro3'
FROM df_opportunity21    group by AccountId """
df_opportunity22=ps.sqldf(q13, locals())
df_opportunity7=df_opportunity6.merge(df_opportunity22.rename({'AccountId': 'AccountId4'}, axis=1),
              left_on='Id', right_on='AccountId4', how='left')
df_opportunity7['filtro3'] = df_opportunity7['filtro3'].fillna(0)
q14 = """SELECT Id,Industria_1__c,dif_product,Tipo_mto,cc_cuentas,frec_compra,Churn0,Churn1,Churn2,dif_days,diff_days_task,nro_activos_x,
case when filtro3+Churn0>0 then 1 else 0 end 'Churn_00'
FROM df_opportunity7    """
df_opportunity8=ps.sqldf(q14, locals())
q15 = """SELECT Id,Industria_1__c,dif_product,Tipo_mto,cc_cuentas,Churn0,Churn_00,frec_compra,Churn1,dif_days,diff_days_task,nro_activos_x,
case when Churn1 + Churn_00>1 then 1 else 0 end 'Churn_2_f'
FROM df_opportunity8    """
df_opportunity8=ps.sqldf(q15, locals())
q115 = """SELECT Id,Industria_1__c,dif_product,Tipo_mto,cc_cuentas,Churn0,frec_compra,Churn_00,Churn1,dif_days,diff_days_task,nro_activos_x,
Churn_2_f as Churn2
FROM df_opportunity8    """
df_opportunity9=ps.sqldf(q115, locals())

#########################
dataset=df_opportunity9[['Id','Industria_1__c','nro_activos_x','Tipo_mto','cc_cuentas','diff_days_task',
                         'frec_compra','Churn2']]
dataset3=dataset
identity = dataset["Id"]
dataset = dataset.drop(columns="Id")
dataset= pd.get_dummies(dataset)
dataset_test= pd.get_dummies(dataset)
dataset = pd.concat([dataset, identity], axis = 1)
import numpy as np
import matplotlib.pyplot as plt 
import pandas as pd 
from scipy.stats import norm,skew
from scipy import stats
import statsmodels.api as sm 
# sklearn modules for data preprocessing:
from sklearn.impute import SimpleImputer
from sklearn.preprocessing import LabelEncoder, OneHotEncoder
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
#sklearn modules for Model Selection:
from sklearn import svm, tree, linear_model, neighbors
from sklearn import naive_bayes, ensemble, discriminant_analysis, gaussian_process
from sklearn.neighbors import KNeighborsClassifier
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
from xgboost import XGBClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.svm import SVC
from sklearn.neighbors import KNeighborsClassifier
from sklearn.naive_bayes import GaussianNB
from sklearn.tree import DecisionTreeClassifier
from sklearn.ensemble import RandomForestClassifier
#sklearn modules for Model Evaluation & Improvement:
    
from sklearn.metrics import confusion_matrix, accuracy_score 
from sklearn.metrics import f1_score, precision_score, recall_score, fbeta_score
from statsmodels.stats.outliers_influence import variance_inflation_factor
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import ShuffleSplit
from sklearn.model_selection import KFold
from sklearn import feature_selection
from sklearn import model_selection
from sklearn import metrics
from sklearn.metrics import classification_report, precision_recall_curve
from sklearn.metrics import auc, roc_auc_score, roc_curve
from sklearn.metrics import make_scorer, recall_score, log_loss
from sklearn.metrics import average_precision_score
#Standard libraries for data visualization:
import seaborn as sn
from matplotlib import pyplot
import matplotlib.pyplot as plt
import matplotlib.pylab as pylab
import matplotlib 
#%matplotlib inline
#color = sn.color_palette()
import matplotlib.ticker as mtick
from IPython.display import display
pd.options.display.max_columns = None
from pandas.plotting import scatter_matrix
from sklearn.metrics import roc_curve
#Miscellaneous Utilitiy Libraries:
    
import random
import os
import re
import sys
import timeit
import string
import time
from datetime import datetime
from time import time
from dateutil.parser import parse
import joblib
import numpy as np
import pandas as pd
import networkx as nx
import matplotlib.pyplot as plt

response = dataset["Churn2"]
dataset = dataset.drop(columns="Churn2")

#Generate training and test datasets
X_train, X_test, y_train, y_test = train_test_split(dataset, response,stratify=response, test_size = 0.2)
print("Number transactions X_train dataset: ", X_train.shape)
print("Number transactions y_train dataset: ", y_train.shape)
print("Number transactions X_test dataset: ", X_test.shape)
print("Number transactions y_test dataset: ", y_test.shape)
train_identity = X_train['Id']
X_train = X_train.drop(columns = ['Id'])
test_identity = X_test['Id']
X_test = X_test.drop(columns = ['Id'])
sc_X = StandardScaler()
X_train2 = pd.DataFrame(sc_X.fit_transform(X_train))
X_train2.columns = X_train.columns.values
X_train2.index = X_train.index.values
X_train = X_train2
X_test2 = pd.DataFrame(sc_X.transform(X_test))
X_test2.columns = X_test.columns.values
X_test2.index = X_test.index.values
X_test = X_test2
from numpy import mean
from sklearn.datasets import make_classification
from sklearn.model_selection import cross_val_score
from sklearn.model_selection import RepeatedStratifiedKFold
from sklearn.ensemble import RandomForestClassifier
from imblearn.over_sampling import SMOTE
model = RandomForestClassifier()
# evaluate pipeline
cv = RepeatedStratifiedKFold(n_splits=5, n_repeats=3, random_state=1)
scores = cross_val_score(model, X_train, y_train, scoring='roc_auc', cv=cv, n_jobs=-1)
print('Mean ROC AUC: %.3f' % mean(scores))
oversample = SMOTE()
X_train, y_train = oversample.fit_resample(X_train, y_train)
cv = RepeatedStratifiedKFold(n_splits=5, n_repeats=3, random_state=1)
scores = cross_val_score(model, X_train, y_train, scoring='roc_auc', cv=cv, n_jobs=-1)
print('Mean ROC AUC: %.3f' % mean(scores))
dataset2 = df_account10[['Id','Industria_1__c','dif_product','nro_activos','Churn2','cc_cuentas','frec_compra','Tipo_mto']]
dataset2_y = dataset2['Churn2']
model.fit(X_train, y_train)
model.fit(X_train, y_train)
y_pred = model.predict(X_test)
print("Test Data Accuracy: %0.4f" % accuracy_score(y_test, y_pred))
dataset3_y = dataset3['Churn2']
y_pred = model.predict(X_train)
identity = dataset3["Id"]
dataset3 = dataset3.drop(columns="Id")
dataset3= pd.get_dummies(dataset3)
dataset3 = pd.concat([dataset3, identity], axis = 1)

response3 = dataset3["Churn2"]
dataset3 = dataset3.drop(columns="Churn2")
train_identity3 = dataset3['Id']
dataset3 = dataset3.drop(columns = ['Id'])
sc_X = StandardScaler()
X_dataset4 = pd.DataFrame(sc_X.fit_transform(dataset3))
X_dataset4.columns = dataset3.columns.values
X_dataset4.index = dataset3.index.values
X_dataset3 = X_dataset4
y_pred = model.predict(X_dataset3)
final_results = pd.concat([train_identity3, dataset3_y], axis = 1).dropna()
final_results['predictions'] = y_pred
y_pred_probs = model.predict_proba(X_dataset3)
y_pred_probs  = y_pred_probs [:, 1]
final_results["propensity_to_churn(%)"] = y_pred_probs
final_results["propensity_to_churn(%)"] = final_results["propensity_to_churn(%)"]*100
final_results["propensity_to_churn(%)"]=final_results["propensity_to_churn(%)"].round(2)
final_results = final_results[['Id', 'Churn2', 'predictions', 'propensity_to_churn(%)']]
final_results ['Ranking'] = pd.qcut(final_results['propensity_to_churn(%)'].rank(method = 'first'),10,labels=range(10,0,-1))
final_results['propensity_to_churn(%)'] = final_results['propensity_to_churn(%)'].astype(np.int64)
final_results.to_excel(r'D:/CS/churn7.xlsx', index = False)