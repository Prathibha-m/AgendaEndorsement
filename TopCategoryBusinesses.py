import pandas as pd
import operator

import json

df = pd.read_csv("/Users/Prathibha//Documents/CategoryExtraction/Annotated_LasVegas_Biz.csv")
print(df.head(5))

UmbrellaTermCount= dict()

for index,row in df.iterrows():

    umbrellaTerm = row['UmbrellaTermCategory']

    if not pd.isnull(umbrellaTerm):
        for categ in umbrellaTerm.split(','):
            if categ not in UmbrellaTermCount:
                UmbrellaTermCount[categ]=1
            else:
                UmbrellaTermCount[categ]+=1


newA = dict(sorted(UmbrellaTermCount.items(), key=operator.itemgetter(1), reverse=True)[:5])
for key in UmbrellaTermCount.keys():
    for val in newA:
       #print(val+" "+key)
        if val != key and val in key:
           # print("in if")
            newA[key]=UmbrellaTermCount[key]
            #//print(newA[va])
            break

print(newA)

filtered_set = list()

for index,row in df.iterrows():

    umbrellaTerm = row['UmbrellaTermCategory']

    if not pd.isnull(umbrellaTerm):
        for categ in umbrellaTerm.split(','):
            if categ in newA:
                filtered_set.append(row)
            break

filteredDF = pd.DataFrame(filtered_set,columns=list(df)).to_csv('filteredBusinesses.csv',index=False)

