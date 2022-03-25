import streamlit as st
from pandas import DataFrame
import pymongo


myclient = pymongo.MongoClient("mongodb://localhost:27017/",username='root',password='example')
mydb = myclient["docstreaming"]
mycol = mydb["videos"] 


# Below the first chart add a input field for the video_id
video_id = st.sidebar.text_input("Video ID:")

# if enter has been used on the input field 
if video_id:

    myquery = {"video_id": video_id}
    # only includes or excludes
    mydoc = mycol.find( myquery , { "category_id": 0, "_id": 0})

    # create dataframe from resulting documents to use drop_duplicates
    df = DataFrame(mydoc)
    
    # drop duplicates, but keep the first one
    df.drop_duplicates(subset ="channel_title", keep = 'first', inplace = True)
    
    # Add the table with a headline
    st.header("Output Videos by ID")
    table2 = st.dataframe(data=df) 
    

# Below the fist chart add a input field for the invoice number
channel_title = st.sidebar.text_input("Channel Title:")
#st.text(channel_title)  # Use this to print out the content of the input field

# if enter has been used on the input field 
if channel_title:
    
    myquery = {"channel_title": channel_title}
    mydoc = mycol.find( myquery, { "_id": 0, "trending_date": 0, "views": 0, "likes": 0, "dislikes": 0, "comment_count": 0 })

    # create the dataframe
    df = DataFrame(mydoc)

    # drop duplicates, but keep the first one
    df.drop_duplicates(subset ="video_id", keep = 'first', inplace = True)
    
    # Add the table with a headline
    st.header("Output by Channel Title")
    table2 = st.dataframe(data=df) 


