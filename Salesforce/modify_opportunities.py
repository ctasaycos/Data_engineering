import pandas as pd
from simple_salesforce import Salesforce
sf = Salesforce(username='user', password='password', security_token='token',domain='test')

TM_Opportunity_output = sf.bulk.Opportunity.query("Select Id,AccountId,StageName,CloseDate, gross_amount from Opportunity where StageName = 'order_inprocess' and CloseDate >= 2020-01-01 ")
TM_Opportunity_bought = pd.DataFrame(TM_Opportunity_output,columns=['Id','AccountId','StageName','CloseDate','gross_amount'])

TM_OpportunityLineItem_output = sf.bulk.OpportunityLineItem.query("Select Id, ProductCode , OpportunityId, Gross_price from OpportunityLineItem where ProductCode = 'ES82O' ")
TM_OpportunityLineItem_bought = pd.DataFrame(TM_OpportunityLineItem_output,columns=['Id','ProductCode', 'OpportunityId', 'Gross_price'])

import pandasql as ps
df_Opportunitylineitem = ps.sqldf("""
select OpportunityId, sum(Gross_price) as total_amount from  TM_OpportunityLineItem_bought 
group by OpportunityId
""", locals())


df_Opportunity = ps.sqldf("""
select b.AccountId, sum(total_amount) as total_amount_account from  df_Opportunitylineitem a 
left join TM_Opportunity_bought b on a.OpportunityId=b.Id
group by b.AccountId
""", locals())


Id = df_Opportunity["AccountId"].tolist()
total_amount = df_Opportunity["total_amount_account"].tolist()

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

for j in range(len(Id)):
    try:
        sf.Account.update(Id[j],{'Online_sales': total_amount[j]})
        print(f"{bcolors.WARNING} {Id[j]} has been updated with  {total_amount[j]} {bcolors.ENDC}")
    except:
        print(f"{bcolors.FAIL}Error{Id[j]}  {bcolors.ENDC}")