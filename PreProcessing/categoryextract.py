import pandas as pd
import math
import re
import csv
import json
df = pd.read_csv("/media/sharath/EADEDFA2DEDF657B/DM Project/Category Extraction/Converted_Business.csv")

categoryItemSet = set()
# Grouping by city and retaining only biz ids : aggregate to get the count/city
#Columns : City, Count
df.groupby('city')['business_id'].count().reset_index(name='count').sort_values(['count'],ascending=False).to_csv('City_Counts.csv',index=False)

undefCategList = []
# any city with null values, fill it with 'abc'
df['city'].fillna('abc',inplace=True)
#make the city names lowercase and update the df
df['city']=map(lambda x:x.lower(), df['city'])
#df['city'].str.lower()
#Filter the df only on Las Vegas
dfLasVegas = df[df['city'].str.contains('las vegas')]

dfLasVegas.to_csv('LasVegas_Biz.csv',index=False)
print "No of businesses in Las Vegas : ",len(dfLasVegas)

for index,row in dfLasVegas.iterrows():
    # if pd.isnull(row['city']) or 'las vegas' not in row['city'].lower():
    #     continue
    # get the categories column for a single biz row
    categListVal = row['categories']
    # if the Category List is null , putting it into a list of biz with undefined Categories list
    if pd.isnull(categListVal):
        undefCategList.append([row['business_id'],row['name']])
        continue
    # Get each category in category list and add it to a set
    for categ in categListVal.split(','):
        categoryItemSet.add(categ.strip())
# sort the category set
categoryItemSet = sorted(categoryItemSet)
with open('categoryItemSet.json', 'w') as outfile:
    json.dump(categoryItemSet, outfile)
print "No of Business with no categories list column :",len(undefCategList)
print "No of unique category items : ",len(categoryItemSet)
print "Length of entire biz dataset :",len(df)
# casting biz categories into a dataframe
categListDF = pd.DataFrame(list(categoryItemSet),columns=['category'])

categListDF.to_csv('Category_List.csv',index=False)

# df of biz with undefined category list
dfUndefinedCategory = pd.DataFrame(undefCategList,columns=['business_id','name'])
