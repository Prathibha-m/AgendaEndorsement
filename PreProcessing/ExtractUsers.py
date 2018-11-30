import pandas as pd

columns = ["user_id", "business_id", "stars"]
# lvBiz_df = pd.read_csv('Annotated_LasVegas_Biz.csv')
lvBiz_df = pd.read_csv('filteredBusinesses_new.csv')
reviews_df = pd.read_csv('Converted_Reviews.csv', usecols=columns, encoding='latin-1')
usersForBiz = {'user_id': [], 'business_id': [], 'stars': []}


uniqueUsers = set()
usersFile = open("usersFile.txt", 'w')
for index, row in lvBiz_df.iterrows():
    currentBiz = row["business_id"].strip()
    print(currentBiz)
    currentUsers_df = reviews_df[reviews_df["business_id"] == currentBiz]
    currentUsers = currentUsers_df["user_id"]

    for index, row in currentUsers_df.iterrows():
        if row['user_id'] not in uniqueUsers:
            uniqueUsers.add(row['user_id'].strip())
        else:
            continue

        usersForBiz['user_id'].append(row['user_id'])
        usersForBiz['business_id'].append(row['business_id'])
        usersForBiz['stars'].append(row['stars'])


print(len(uniqueUsers))
usersForBiz_df = pd.DataFrame(usersForBiz, columns=columns)
usersFile.write(str(uniqueUsers))
usersForBiz_df.to_csv("usersForBiz_v3.csv", index=False)

