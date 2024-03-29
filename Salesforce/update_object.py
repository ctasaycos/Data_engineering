'''
The company needs to update every record in the object (Suscription), obfuscated privaate information of the company
'''
import pandas as pd
from simple_salesforce import Salesforce

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# Salesforce connection
sf = Salesforce(username='use, password='password', security_token='token')

# Querying data from Salesforce
tm_output = sf.bulk.tm.query("SELECT Id, Cuenta__c, Codigo_licencia__c, Version_de_compra__c, OC__c, Fecha_final_de_uso__c FROM tm")
tm_bought = pd.DataFrame(tm_output, columns=['Id', 'Cuenta__c', 'Codigo_licencia__c', 'Version_de_compra__c', 'OC__c', 'Fecha_final_de_uso__c'])

# Filtering data using pandasql
import pandasql as ps
df_TM_Suscripcion__c = ps.sqldf("""
    SELECT * 
    FROM tm_bought 
    WHERE Cuenta__c = '2222222' 
        AND Fecha_final_de_uso__c = '2023-08-04' 
        AND Version_de_compra__c IS NULL
""", locals())

# Extracting Ids from the filtered data
Id = df_TM_Suscripcion__c["Id"].tolist()

# Deleting and updating records in Salesforce
for j in range(len(Id)):
    try:
        sf.tm.delete(Id[j])
        print(f"{bcolors.WARNING}{Id[j]} {bcolors.ENDC}")
    except:
        print(f"{bcolors.FAIL}Error with the account {Id[j]} in the record {Id[j]}{bcolors.ENDC}")

# Example of updating a specific record
sf.tm.update(Id[20], {'Codigo_licencia__c': 'ESU735314146', 'Version_de_compra__c': '10.8'})
