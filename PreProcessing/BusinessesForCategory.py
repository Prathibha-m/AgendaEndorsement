#PYTHON 5


'''
Pickinig all businesses that have the top umbrella terms in the decription for the business (in original data)
'Restaurant', 'Shopping', 'Home Finishings', 'Health & Wellness', 'Restaurant-Bakery', 'Restaurant-Pub', 'Restaurants-Pubs', 'Restaurant-Dessert', 
'Restaurants', 'Restaurant-Coffee', "Shopping-Women's", 'Restaurants-Bagels', 'Restaurant Pub', 'Restaurant-Burgers', 'Shopping-Sports', 'Restaurant Desserts'
'Restaurant-Bubble Tea', 'Restaurant-Pubs', 'Restaurant-Desserts', 'Restaurants-Hotels', 'Restaurants-Beverages',
'Restaurants-French', 'Restaurant-Beverages', 'Restaurant-Beverage'

'''

import pandas as pd
import operator
import json

df = pd.read_csv("/Users/Prathibha//Documents/CategoryExtraction/Annotated_LasVegas_Biz.csv")
print(df.head(5))


newA=[ 'Restaurant', 'Shopping', 'Home Finishings', 'Health & Wellness', 'Restaurant-Bakery', 'Restaurant-Pub', 'Restaurants-Pubs', 'Restaurant-Dessert', 'Restaurants', 'Restaurant-Coffee', "Shopping-Women's", 'Restaurants-Bagels', 'Restaurant Pub', 'Restaurant-Burgers', 'Shopping-Sports', 'Restaurant Desserts'
 'Restaurant-Bubble Tea', 'Restaurant-Pubs', 'Restaurant-Desserts', 'Restaurants-Hotels', 'Restaurants-Beverages',
       'Restaurants-French', 'Restaurant-Beverages', 'Restaurant-Beverage']
filtered_set = list()

for index,row in df.iterrows():

    umbrellaTerm = row['UmbrellaTermCategory']

    if not pd.isnull(umbrellaTerm):
        for categ in umbrellaTerm.split(','):
            if categ in newA:
                filtered_set.append(row)
                break

filteredDF = pd.DataFrame(filtered_set,columns=list(df)).to_csv('filteredBusinesses.csv',index=False)

