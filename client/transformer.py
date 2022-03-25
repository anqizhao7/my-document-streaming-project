import numpy as np
from numpy import add
import pandas as pd


df = pd.read_csv ('./output/22.21.03_US_videos.csv') 

# remove some columns we don't need for now
df = df.drop(["tags","thumbnail_link","comments_disabled","ratings_disabled","description"], axis=1)
df.rename(columns = {'publishedAt':'publish_time','channelId':'channel_title', 'categoryId':'category_id','view_count':'views'}, inplace = True)# df = df.drop(["tags","thumbnail_link","comments_disabled","ratings_disabled","video_error_or_removed","description"], axis=1)
#print(df)

# add a json column to the dataframe
# splitlines will split the json into multiple rows not a single one
df['json'] = df.to_json(orient='records', lines=True).splitlines()
#print(df)

# just take the json column of the dataframe
dfjson = df['json']
#print(dfjson)

# print out the dataframe to a file
# Note that the timestamp forward slash will be escaped to stay true to JSON schema
np.savetxt(r'./output.txt', dfjson.values, fmt='%s')
