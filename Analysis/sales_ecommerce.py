'''
Ecommerce company, using inhouse data analytics are looking to provide a dashboards where you can see the sales and orders by classification
Because is a start up located in Brooklyn they dont have a database or datalake to use proper sql sintax, I was pushed to develop it in python, company use django.
'''
import pandas as pd
import pandasql as ps
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

# Load data
classifications = pd.read_csv(r'C:\Users\ctasa\Downloads\classifications (2).csv')
orders = pd.read_csv(r'C:\Users\ctasa\Downloads\orders (1).csv')
events = pd.read_csv(r'C:\Users\ctasa\Downloads\events_2 (1).csv')

###########1) Basics#############################

# a) Most common primary classification
q1 = ps.sqldf("""
    SELECT primary_classification, COUNT(event_id) AS q_events 
    FROM classifications
    GROUP BY primary_classification
    ORDER BY q_events DESC
    LIMIT 1
""", locals())

# b) Change considering RSVP events vs. greeting cards
q2 = ps.sqldf("""
    SELECT a.primary_classification, COUNT(*) 
    FROM classifications a
    INNER JOIN events b ON a.event_id=b.event_id
    WHERE b.type IN ("RsvpEvent", "GreetingCard")
    GROUP BY a.primary_classification
    LIMIT 1
""", locals())

# c) Average score for each primary classification
q3 = ps.sqldf("""
    SELECT primary_classification, AVG(primary_score) AS average_score
    FROM classifications 
    GROUP BY primary_classification
""", locals())

###########2) Event Distribution#############################

# a) Average days for an event to be sent after being created
events_1 = events.loc[(events.sent_at != "<null>")]
events_1['created_at'] = pd.to_datetime(events_1['created_at'])
events_1['sent_at'] = pd.to_datetime(events_1['sent_at'])
events_1['difference'] = (events_1['sent_at'] - events_1['created_at']).dt.days
average_days = events_1.difference.mean()

# b) Event trends in the summer months (June - August)
q4 = ps.sqldf("""
    SELECT strftime('%m', created_at) AS month, COUNT(*)
    FROM events
    GROUP BY month
""", locals())

# c) User event distribution comparison
q5 = ps.sqldf("""
    SELECT account_id, COUNT(*) AS q_events
    FROM events
    WHERE account_id <> "<null>"
    GROUP BY account_id
    ORDER BY q_events DESC
""", locals())
sns.displot(q5, x="q_events", bins=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10])

###########3) Monetization#############################

# a) Monthly revenue
orders = ps.sqldf("""
    SELECT *
    FROM orders
    WHERE invoiced_at <> "<null>"
""", locals())
orders['created_at'] = pd.to_datetime(orders['created_at'])
orders['invoiced_at'] = pd.to_datetime(orders['invoiced_at'])

q6 = ps.sqldf("""
    SELECT strftime('%m', created_at) AS month, (SUM(cents))/100 total_dollars
    FROM orders
    GROUP BY month
""", locals())

# b) Peaks or valleys in user’s purchase behavior
q7 = ps.sqldf("""
    SELECT strftime('%m', invoiced_at) AS invoiced_created, COUNT(DISTINCT account_id) q_users
    FROM orders
    GROUP BY invoiced_created
""", locals())
sns.set_theme(style="whitegrid")
ax = sns.barplot(x="invoiced_created", y="q_users", data=q7)

plt.show()
