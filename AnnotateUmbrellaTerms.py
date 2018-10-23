import pandas as pd
import json
import math
import re
import csv

df_dataLasVegas = pd.read_csv('/media/sharath/EADEDFA2DEDF657B/DM Project/Category Extraction/LasVegas_Biz.csv')
df_umbrellaTerms = pd.read_csv('/media/sharath/EADEDFA2DEDF657B/DM Project/Category Extraction/umbrellaTerms.csv')

#umbrella map for reference
#just to seee the umbrella terms 
umbrellaMap = {}
for index,row in df_umbrellaTerms.iterrows():
    if not row["Type"] in umbrellaMap:
        umbrellaMap[row["Type"]] = []
    else :
        if not row["SubType"] in umbrellaMap[row["Type"]] and not pd.isnull(row["SubType"]):
            umbrellaMap[row["Type"]].append(row["SubType"])
#print(umbrellaMap)
with open('umbrellaMap.json', 'w') as outfile:
    json.dump(umbrellaMap, outfile)



df_dataLasVegas["UmbrellaTermCategory"] = ""
i =0
#annnotates the main businesses file with aaumbrella terms 
# Output : Annotated_LasVegas_Biz.csv
for index,row in df_dataLasVegas.iterrows():
    #print row["categories"]
    if index % 1000==0:
        print index
    if pd.isnull(row["categories"]):
        continue;
    categList = str(row["categories"]).split(',')
    #print categList
    umbrellaSet = set()
    for categ in categList:
        #print "Categ-", categ
        filteredDf = df_umbrellaTerms[df_umbrellaTerms["category"]==categ.strip()]
        #print filteredDf
        if pd.isnull(filteredDf["Type"].iloc[0]):
            continue
        umbrellaSetVar = filteredDf["Type"].iloc[0].strip()
        if(not pd.isnull(filteredDf["SubType"].iloc[0])):
            umbrellaSetVar +="-"+filteredDf["SubType"].iloc[0].strip()

        #print "Umbrella-",umbrellaSetVar

        umbrellaSet.add(umbrellaSetVar)
        #print "Umbrella Set ", umbrellaSet
    df_dataLasVegas.at[index,"UmbrellaTermCategory"] = ','.join(umbrellaSet)
    #print row["List(Categ-subCateg)"]

print df_dataLasVegas.head(10)
df_dataLasVegas.to_csv('Annotated_LasVegas_Biz.csv',index=False)
