import pandas as pd
import math
import re
import csv

# file created to rectify the blunder in duplication
#Sorry HHE, #HHEYelpFire
df_annotated = pd.read_csv('/media/sharath/EADEDFA2DEDF657B/DM Project/Category Extraction/annotatedList.csv')
df_categSet = pd.read_csv('/media/sharath/EADEDFA2DEDF657B/DM Project/Category Extraction/Category_List.csv')
df_categSet["Type"]=""
df_categSet["SubType"]=""
print list(df_annotated)
#for each row in the non duplicated categories
for index,row in df_categSet.iterrows():
    #filter the annotated file in progress
    filtered_df = df_annotated[df_annotated["category"].str.contains(row["category"])]
    #print (len(filtered_df))
    #annotate the categories in the new file with the file which was being annotated
    if(len(filtered_df)== 0):
        continue
    for i,r in filtered_df.iterrows():
        if (pd.isnull(r["Type"])):
            continue
        else :
            row["Type"] = r["Type"]
            if( not pd.isnull(r["SubType"])):
                row["SubType"] = r["SubType"]
            break
df_categSet.to_csv('reformed.csv',index=False)
