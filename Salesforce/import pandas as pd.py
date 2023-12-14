import pandas as pd
from simple_salesforce import Salesforce
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
import email.mime.application

# Salesforce connection
sf = Salesforce(
    username=config('SALESFORCE_USERNAME'),
    password=config('SALESFORCE_PASSWORD'),
    security_token=config('SALESFORCE_SECURITY_TOKEN'),
    domain='test'
)

# Querying Account data from Salesforce
ob_Account_output = sf.bulk.Account.query("SELECT Id, Name, Sector_1__c, Industria_1__c, Clasificacion_de_la_Cuenta__c, OwnerId, Check_cc_manual__c FROM Account")
ob_Account_bought = pd.DataFrame(ob_Account_output, columns=['Id', 'Name', 'Sector_1__c', 'Industria_1__c', 'Clasificacion_de_la_Cuenta__c', 'OwnerId', 'Check_cc_manual__c'])

# Querying User data from Salesforce
ob_User_output = sf.bulk.User.query("SELECT Id, Name FROM User")
ob_User_bought = pd.DataFrame(ob_User_output, columns=['Id', 'Name'])

# Querying Case data from Salesforce
ob_Case_output = sf.bulk.Case.query("SELECT Id, CreatedDate, AccountId, Type, Status, Origin, Campana_CxE__c, Tags_de_Interes__c FROM Case")
ob_Case_bought = pd.DataFrame(ob_Case_output, columns=['Id', 'CreatedDate', 'AccountId', 'Type', 'Status', 'Origin', 'Campana_CxE__c', 'Tags_de_Interes__c'])

# Joining Account and User data
ob_Account_bought_0 = pd.merge(ob_Account_bought, ob_User_bought, left_on='OwnerId', right_on='Id', how='left')
ob_Account_bought_0.rename(columns={'Name_y': 'Propietario_cuenta'}, inplace=True)
ob_Account_bought_0.drop('Id_y', axis=1, inplace=True)

# Calculating interactions and supports
df_case_account = ob_Case_bought.query("AccountId != 'None' and Campana_CxE__c == 'Customer Success'").groupby('AccountId').size().reset_index(name='q_interacciones_cs')
df_soporte_account = ob_Case_bought.query("AccountId != 'None' and Tags_de_Interes__c == 'Registro en DDX-ESRI'").groupby('AccountId').size().reset_index(name='q_soportes_cs')

# Querying Opportunity data from Salesforce
ob_Opportunity_output = sf.bulk.Opportunity.query("SELECT Id, AccountId, StageName, CloseDate, Importe_Sin_IGV__c FROM Opportunity WHERE StageName = 'Orden/Contrato Recepcionado'")
ob_Opportunity_bought = pd.DataFrame(ob_Opportunity_output, columns=['Id', 'AccountId', 'StageName', 'CloseDate', 'Importe_Sin_IGV__c'])

# Calculating opportunities data
df_oportunidad_Id = ob_Opportunity_bought.query("StageName == 'Orden/Contrato Recepcionado'").groupby('AccountId').agg(Id=('Id', 'first'), last_closedate=('CloseDate', 'max')).reset_index()
df_oportunidad_Id_last365 = df_oportunidad_Id.query("JULIANDAY(DATE('now')) - JULIANDAY(last_closedate) <= 365")

df_oportunidad_Id_last365_amount = pd.merge(df_oportunidad_Id_last365, ob_Opportunity_bought, on='Id', how='left')

# Selecting accounts with closed opportunities in the last 2 years
df_oportunidad_accounts_last3years = ob_Opportunity_bought.query("StageName == 'Orden/Contrato Recepcionado' and JULIANDAY(DATE('now')) - JULIANDAY(CloseDate) <= 730")

df_oportunidad_accounts_last3years_amount = df_oportunidad_accounts_last3years.groupby('AccountId')['Importe_Sin_IGV__c'].sum().reset_index(name='Importe_Sin_IGV__c')

# Querying Subscription data from Salesforce
ob_Suscripcion__c_output = sf.bulk.ob_Suscripcion__c.query("SELECT Id, Cuenta__c, Codigo_licencia__c, Version_de_compra__c, OC__c, Fecha_final_de_uso__c FROM ob_Suscripcion__c")
ob_Suscripcion__c_bought = pd.DataFrame(ob_Suscripcion__c_output, columns=['Id', 'Cuenta__c', 'Codigo_licencia__c', 'Version_de_compra__c', 'OC__c', 'Fecha_final_de_uso__c'])

# Counting distinct subscriptions per account
df_ob_Suscripcion__c = ob_Suscripcion__c_bought.query("Cuenta__c != 'None'").groupby('Cuenta__c').agg(q_suscriptions=('Id', 'nunique')).reset_index()

# Getting unique account Ids
df_account = ob_Account_bought[['Id']].drop_duplicates()

# Merging dataframes for analysis
df_cc = pd.merge(ob_Account_bought_0, df_case_account, left_on='Id', right_on='AccountId', how='left')
df_cc = pd.merge(df_cc, df_soporte_account, left_on='Id', right_on='AccountId', how='left')
df_cc = pd.merge(df_cc, df_oportunidad_accounts_last3years_amount, left_on='Id', right_on='AccountId', how='left')
df_cc = pd.merge(df_cc, df_ob_Suscripcion__c, left_on='Id', right_on='Cuenta__c', how='left')

# Analyzing accounts with positive Importe_Sin_IGV__c
df_cc_analytics = df_cc.query("Importe_Sin_IGV__c > 0")

# Identifying high-value accounts
df_cc_high_amount = df_cc.query("Importe_Sin_IGV__c >= (SELECT AVG(Importe_Sin_IGV__c) FROM df_cc WHERE Importe_Sin_IGV__c > 0)")
df_cc_high_suscriptions = df_cc.query("q_suscriptions >= (SELECT AVG(q_suscriptions) FROM df_cc)")
df_cc_high_soporte = df_cc.query("q_soportes_cs >= (SELECT AVG(q_soportes_cs) FROM df_cc)")
df_cc_high_interaccciones = df_cc.query("q_interacciones_cs >= (SELECT AVG(q_interacciones_cs) FROM df_cc)")

# Creating final dataframes
df_cc2 = pd.merge(df_cc, df_cc_high_amount[['Id', 'Importe_Sin_IGV__c']], on='Id', how='left')
df_cc2 = pd.merge(df_cc2, df_cc_high_suscriptions[['Id', 'q_suscriptions']], on='Id', how='left')
df_cc2 = pd.merge(df_cc2, df_cc_high_soporte[['Id', 'q_soportes_cs']], on='Id', how='left')
df_cc2 = pd.merge(df_cc2, df_cc_high_interaccciones[['Id', 'q_interacciones_cs']], on='Id', how='left')

# Creating classification based on conditions
df_cc3 = df_cc2.copy()
df_cc3['High_interacciones'] = df_cc3['q_interacciones_cs'].apply(lambda x: 1 if pd.notnull(x) else None)
df_cc3['High_soportes'] = df_cc3['q_soportes_cs'].apply(lambda x: 1 if pd.notnull(x) else None)
df_cc3['High_suscriptions'] = df_cc3['q_suscriptions'].apply(lambda x: 1 if pd.notnull(x) else None)
df_cc3['High_Amount'] = df_cc3['Importe_Sin_IGV__c'].apply(lambda x: 1 if pd.notnull(x) else None)

# Classifying accounts
df_cc4 = df_cc3.copy()
df_cc4['Clasificacion'] = df_cc4.apply(lambda row: 
    'ELA' if row['Id'] in ["0014600000VkbrNAAR", "0014600000WPcGuAAL", "0014600000VkbrOAAR", "0014600000VkbrZAAR"] 
    else 'Cuenta_Clave' if ((row['High_interacciones'] and row['High_soportes'] and row['High_suscriptions'] and row['High_Amount']) 
    or (row['total_importe_sin_igv'] > 1000000) or (row['total_importe_sin_igv'] > 100000 and row['total_interacciones'] > 2)) 
    else 'Cuenta_Crecer' if (((row['High_Amount'] or row['High_suscriptions']) and (row['High_soportes'] is None and row['total_interacciones'] is None)) 
    or (row['total_importe_sin_igv'] > 80000 and row['total_interacciones'] < 3)) and (row['total_importe_sin_igv'] > 0) 
    else 'Cuenta_Defender_y_Mantener' if ((row['High_soportes'] or row['High_interacciones']) and (row['High_Amount'] is None)) 
    else 'Cuenta_Transaccional', axis=1)

# Counting accounts by classification
df_cc5 = df_cc4.groupby('Clasificacion').size().reset_index(name='count')

# Extracting relevant columns for analysis
export_dataframe = df_cc4.query("Clasificacion <> Clasificacion_de_la_Cuenta__c")[['Name', 'Clasificacion', 'Clasificacion_de_la_Cuenta__c', 'Propietario_cuenta']]

#Sending email
sender_email = config('GMAIL_USERNAME')
receiver_email = 'cc@gmail.com'
password = config('GMAIL_PASSWORD')
receiver_email2 = ['cc@gmail.com', 'ff@tt.com.pe']

# Plain text version
text = "This is the plain text version."

# HTML body
html = """<html><p>This is some HTML</p></html>"""

# Iterating over recipients
for j in range(len(receiver_email2)):
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = "Deliverability Report"
    msg['From'] = sender_email
    msg['To'] = receiver_email2[j]

    # Record the MIME types of both parts - text/plain and text/html
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    # Create PDF attachment
    filename = "C:\\Users\\ctasa\\Downloads\\ctadsfasdfasd.pdf"
    fp = open(filename, 'rb')
    att = email.mime.application.MIMEApplication(fp.read(), _subtype="pdf")
    fp.close()
    att.add_header('Content-Disposition', 'attachment', filename=filename)

    # Attach parts into message container.
    msg.attach(att)
    msg.attach(part1)
    msg.attach(part2)

    # Send the message via local SMTP server.
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=context) as server:
        server.login(sender_email, password)
        server.sendmail(
            sender_email, receiver_email2[j], msg.as_string()
        )
