from googleapiclient.discovery import build
import pymongo
import pandas as pd
import  psycopg2
import streamlit as st
#------------------------------------------------------------------------------------------
from googleapiclient.discovery import build

def api_connect():
    api_key = "AIzaSyD13nRwTFTYKy5y_ILPXBhU50rYKSbwF7Y"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = build(api_service_name, api_version, developerKey=api_key)
    return youtube

youtube = api_connect()
#-------------------------------------------------------------------------

#get cahnnel information
def get_channel1_info(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    
    response = request.execute()
    for i in response["items"]:
        data = dict(
            channel_name=i["snippet"]["title"],
            channel_id=i["id"],
            subscribers=i['statistics']['subscriberCount'],
            views=i['statistics']['viewCount'],
            videos=i['statistics']['videoCount'],
            channel_description=i['snippet']['description'],
            playlist_id=i['contentDetails']['relatedPlaylists']['uploads']
        )
        return data
#-------------------------------------------------------------------------
#get vedio ids
def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id, part="contentDetails").execute()
    
    playlist_id = response["items"][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    
    while True:
        response1 = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token
        ).execute()
        
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
            
        next_page_token = response1.get('nextPageToken')
        
        if next_page_token is None:
            break

    return video_ids
#------------------------------------------------------------------------------------
# get vedio details
def get_vedio_info(video_ids):
    video_data = []
    for video_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response = request.execute()
        for item in response["items"]:
            data = dict(
                channel_name=item['snippet']['channelTitle'],
                channel_id=item['snippet']['channelId'],
                video_id=item['id'],
                title=item['snippet']['title'],
                tags=item['snippet'].get('tags'),
                thumbnail=item['snippet']['thumbnails']['default']['url'],
                description=item['snippet'].get('description'),
                publish_date=item['snippet']['publishedAt'],
                duration=item['contentDetails']['duration'],
                views=item['statistics'].get('viewCount'),
                likes=item['statistics'].get("likeCount"),  # Corrected this line
                comments=item['statistics'].get('commentCount'),
                favorite_count=item['statistics']['favoriteCount'],
                definition=item['contentDetails']['definition'],
                caption=item['contentDetails']['caption']
            )
            video_data.append(data)
    return video_data
#------------------------------------------------------------------------
# # get comment detail
def get_comment_info(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id ,  # Ensure video_id is enclosed in quotes
                maxResults=50
            )
            response = request.execute()

            for item in response["items"]:
                data = dict(
                    comment_id=item["snippet"]["topLevelComment"]["id"],
                    video_id=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                    comment_text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    comment_author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    comment_published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                )
                comment_data.append(data)
    except :
        pass
        
    return comment_data
#---------------------------------------------------------------------------------------------
# # get comment detail
def get_comment_info(video_ids):
    comment_data = []
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id ,  # Ensure video_id is enclosed in quotes
                maxResults=50
            )
            response = request.execute()

            for item in response["items"]:
                data = dict(
                    comment_id=item["snippet"]["topLevelComment"]["id"],
                    video_id=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                    comment_text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    comment_author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                    comment_published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                )
                comment_data.append(data)
    except :
        pass
        
    return comment_data
#--------------------------------------------------------------------------------------------
# get playlists detail
def get_playlists_details(channel_id):
    next_page_token=None
    all_data=[]
    while True:
        request = youtube.playlists().list(
            part="snippet,contentDetails",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token
        )
        response = request.execute()
        for item in response["items"]:
            data = dict(playlist_id=item['id'],
                        title=item['snippet']['title'],
                        channel_id=item['snippet']['channelId'],
                        channel_name=item['snippet']['channelTitle'],
                        published_at=item['snippet']['publishedAt'],
                        video_count=item['contentDetails']['itemCount'])
            all_data.append(data)

        next_page_token = response.get('nextPageToken')
        if next_page_token is None:
            break

    return all_data

#-----------------------------------------------------------------------------------------
#connection to  mongoDb
client = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
db =client["youtube_data"]
#----------------------------------------------------------------------------------
def channel_details(channel_id):
    ch_details = get_channel1_info(channel_id)
    pl_details = get_playlists_details(channel_id)
    vi_ids = get_video_ids(channel_id)
    vi_details = get_vedio_info(vi_ids) 
    com_details = get_comment_info(vi_ids)
    coll1 = db["channel_details"]
    coll1.insert_one({
        "channel_information": ch_details,
        "playlist_information": pl_details,
        "video_information": vi_details,
        "comment_information": com_details
    })
    return "upload completed successfully"
#-----------------------------------------------------------------------------------

def channel_table():
    # Connect to PostgreSQL
    mydb = psycopg2.connect(host='localhost', user='postgres', password='admin', database="youtube_data", port="5432")
    cursor = mydb.cursor()
    
 
    drop_query = ''' 
    DROP TABLE IF EXISTS channels;
    '''
    cursor.execute(drop_query)
    mydb.commit()

    
    try:
        # Create the table if it doesn't exist
        create_query = '''
        CREATE TABLE IF NOT EXISTS channels(
            channel_name VARCHAR(100) PRIMARY KEY,
            channel_id VARCHAR(80),
            subscribers BIGINT,
            views BIGINT,
            videos INT,
            channel_description TEXT,
            playlist_id VARCHAR(80)
        );
        '''
        cursor.execute(create_query)
        mydb.commit()
    except:
        print("Channel table already created")
    

    
    # Connect to MongoDB
    client = pymongo.MongoClient('mongodb://127.0.0.1:27017/')
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    
    ch_list = []
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    
    df = pd.DataFrame(ch_list)
    
    
    # Insert data into the table
    for index, row in df.iterrows():
        insert_query = '''
        INSERT INTO channels(channel_name,
                            channel_id,
                            subscribers,
                            views,
                            videos,
                            channel_description,
                            playlist_id)
        
                        VALUES (%s, %s, %s, %s, %s, %s, %s);
        '''
        values = (
            row['channel_name'],
            row['channel_id'],
            row['subscribers'],
            row['views'],
            row['videos'],
            row['channel_description'],
            row['playlist_id'])
        try:
            cursor.execute(insert_query, values)
            mydb.commit()
        except:
            print("Channels are already inserted")
#------------------------------------------------------------------------------
def playlist_table():
    mydb = psycopg2.connect(host='localhost', user='postgres', password='admin', database="youtube_data", port="5432")
    cursor = mydb.cursor()
    
    # Drop the table if it exists
    drop_query = ''' 
    DROP TABLE IF EXISTS playlists;
    '''
    cursor.execute(drop_query)
    mydb.commit()
    
    # Create the table if it doesn't exist
    create_query = '''
    CREATE TABLE IF NOT EXISTS playlists(
        playlist_id VARCHAR(100) PRIMARY KEY,
        title VARCHAR(100),
        channel_id VARCHAR(100),  -- Corrected datatype here
        channel_name VARCHAR(100),
        published_at TIMESTAMP,    -- Corrected datatype here
        video_count INT
    );
    '''
    cursor.execute(create_query)
    mydb.commit()
    
    pl_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
        df1 = pd.DataFrame(pl_list)

    for index, row in df1.iterrows():
        insert_query = '''
        INSERT INTO playlists(playlist_id,
                              title,
                              channel_id,
                              channel_name,
                              published_at,
                              video_count)
        VALUES (%s, %s, %s, %s, %s, %s);
        '''
        values = (
            row['playlist_id'],
            row['title'],
            row['channel_id'],
            row['channel_name'],
            row['published_at'],
            row['video_count'])
        
        cursor.execute(insert_query, values)
        mydb.commit()


#-----------------------------------------------------------------------------------------import psycopg2
import pandas as pd
from pymongo import MongoClient

def videos_table():
    # Connect to MongoDB
    client = MongoClient('mongodb://127.0.0.1:27017/')
    db = client["youtube_data"]  # Corrected the database name
    coll1 = db["channel_details"]

    vi_list = []
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
        df2 = pd.DataFrame(vi_list)

    # Connect to PostgreSQL
    mydb = psycopg2.connect(
        host='localhost',
        user='postgres',
        password='admin',
        database="youtube_data",
        port="5432"
    )
    cursor = mydb.cursor()

    # Drop the table if it exists
    drop_query = 'DROP TABLE IF EXISTS videos'
    cursor.execute(drop_query)
    mydb.commit()

    # Create the table if it doesn't exist
    create_query = '''
    CREATE TABLE IF NOT EXISTS videos (
        channel_name VARCHAR(100),
        channel_id VARCHAR(100),
        video_id VARCHAR(30) PRIMARY KEY,
        title VARCHAR(150),
        tags TEXT,
        thumbnail VARCHAR(200),
        description TEXT,
        publish_date TIMESTAMP,
        duration INTERVAL,
        views BIGINT,  
        likes BIGINT,  
        comments BIGINT,  
        favorite_count BIGINT,  
        definition VARCHAR(10),
        caption VARCHAR(50)
    )
    '''

    cursor.execute(create_query)
    mydb.commit()

    # Iterate through DataFrame and insert data into PostgreSQL
    for index, row in df2.iterrows():
        insert_query = '''
        INSERT INTO videos (
            channel_name,
            channel_id,
            video_id,
            title,
            tags,
            thumbnail,
            description,
            publish_date,
            duration,
            views,
            likes,
            comments,
            favorite_count,
            definition,
            caption
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''

        values = (
            row['channel_name'],
            row['channel_id'],
            row['video_id'],
            row['title'],
            row['tags'],
            row['thumbnail'],
            row['description'],
            row['publish_date'],
            row['duration'],
            row['views'],
            row['likes'],
            row['comments'],
            row['favorite_count'],
            row['definition'],
            row['caption']
        )

        cursor.execute(insert_query, values)
        mydb.commit()

    # Close the database connections
    cursor.close()
    mydb.close()

#----------------------------------------------------------------------------
# get comment details
def comments_table():
    mydb = psycopg2.connect(host='localhost', user='postgres', password='admin', database="youtube_data", port="5432")
    cursor = mydb.cursor()
    
    # Drop the table if it exists
    drop_query = ''' 
    DROP TABLE IF EXISTS comments;
    '''
    cursor.execute(drop_query)
    mydb.commit()
    
    # Create the table if it doesn't exist
    create_query = '''
    CREATE TABLE IF NOT EXISTS comments( comment_id varchar(100),
                    video_id varchar(50),
                    comment_text text ,
                    comment_author varchar(150),
                    comment_published timestamp)'''
    cursor.execute(create_query)
    mydb.commit()

    
    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data[ "comment_information"])):
            com_list.append(com_data["comment_information"][i])
        df3 = pd.DataFrame(com_list)
    
    for index, row in df3.iterrows():
        insert_query = '''
        INSERT INTO comments(
            comment_id,
            video_id,
            comment_text,
            comment_author,
            comment_published
        )
        VALUES (%s, %s, %s, %s, %s)
        '''
        values = (
            row['comment_id'],
            row['video_id'],
            row['comment_text'],
            row['comment_author'],
            row['comment_published']
        )
    
        cursor.execute(insert_query, values)
        mydb.commit()
#------------------------------------------------------------------------
def tables():
    channel_table()
    playlist_table()
    videos_table()
    comments_table()

    return  "tables created successfully"
#---------------------------------------------------------------------------
def show_channel_table():
    ch_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    
    ch_list = []
    for ch_data in coll1.find({}, {"_id": 0, "channel_information": 1}):
        ch_list.append(ch_data["channel_information"])
    
    df = st.dataframe(ch_list)
    return df
def show_playlists_table():
    pl_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    
    for pl_data in coll1.find({}, {"_id": 0, "playlist_information": 1}):
        for i in range(len(pl_data["playlist_information"])):
            pl_list.append(pl_data["playlist_information"][i])
        df1 = st.dataframe(pl_list)
        return df1
def show_video_tables():
    vi_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]

    vi_list = []
    for vi_data in coll1.find({}, {"_id": 0, "video_information": 1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])

    df2 = st.dataframe(vi_list)
    return df2

def show_comments_tables():
    com_list = []
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    
    for com_data in coll1.find({}, {"_id": 0, "comment_information": 1}):
        for i in range(len(com_data[ "comment_information"])):
            com_list.append(com_data["comment_information"][i])
        df3 = st.dataframe(com_list)
        return df3
#-----------------------------------------------------------------------------------------------
#streamlit part
with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header(":blue[skill take away]")
    st.caption(":blue[python scripting]")
    st.caption(":blue[data collection]")
    st.caption(":blue[mongo db]")
    st.caption(":blue[api integration]")
    st.caption(":blue[data management using mongodb and sql]")

channel_id = st.text_input(":red[ENTER THE CHANNEL ID   :)  ]")

if st.button (":blue[COLLECT AND STORE DATA]"):
    ch_ids=[]
    db = client["youtube_data"]
    coll1 = db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data['channel_information']['channel_id'])
    if channel_id in ch_ids:
            st.sucess("channel details of the given channel id already exists")
    else:
            insert = channel_details(channel_id)
            st.sucess(insert)

if st.button(":blue[MIGRATE TO SQL]"):
    Table = tables()
    st.sucess(Table)

show_table = st.radio(":red[SELECT THE TABLE FOR VIEW]" ,(":blue[CHANNELS]" ,":blue[PLAYLISTS]" ,":blue[VIDEOS]",":blue[COMMENTS]"))


if show_table == ":blue[CHANNELS]":
    show_channel_table()
elif show_table == ":blue[PLAYLISTS]":
    show_playlists_table()
elif show_table == ":blue[VIDEOS]":  
    show_video_tables()
elif show_table == ":blue[COMMENTS]":
    show_comments_tables()

# sql connection 
mydb = psycopg2.connect(
    host='localhost',
    user='postgres',
    password='admin',
    database="youtube_data",
    port="5432"
)
cursor = mydb.cursor()
question = st.selectbox(":red[SELECT THE QUESTION]",("1. all the videos and the channel name",
                                                "2.channels with most number of videos",
                                                "3.10 most viewed videos",
                                                "4.comments in each videos",
                                                "5.videos with heigest likes",
                                                "6.likes of all videos",
                                                "7.views of each channel",
                                                "8.videos published in the year of 2022",
                                                "9.average duration of all videos in each channel",
                                                "10.videos with heighest number of comments"))
 
if question =="1. all the videos and the channel name":
    query1 ='''select title as videos,channel_name as channelname from videos'''
    cursor.execute(query1)
    mydb.commit()
    t1 = cursor.fetchall()
    df=pd.DataFrame(t1,columns=['video title' , "channel name"])
    st.write(df)
elif question =="2.channels with most number of videos":
    query2 ='''select channel_name as channelname,videos as no_of_videos from channels order by videos desc'''
    cursor.execute(query2)
    mydb.commit()
    t2 = cursor.fetchall()
    df1=pd.DataFrame(t2,columns=['channel name' , "total no videos"])
    st.write(df1)
elif question =="3.10 most viewed videos":
    query3 ='''select views as views , channel_name as channelname ,title as videotitile from videos where views is not null order by views desc limit 10'''
    cursor.execute(query3)
    mydb.commit()
    t3= cursor.fetchall()
    df2=pd.DataFrame(t3,columns=['views' , "channel name", "video title"])
    st.write(df2)
elif question =="4.comments in each videos":
    query4 ='''select comments as nocomments , title as videotitle from videos where comments is not null  '''
    cursor.execute(query4)
    mydb.commit()
    t4= cursor.fetchall()
    df3=pd.DataFrame(t4,columns=['no of comments'  , "video title"])
    st.write(df3)
elif question =="5.videos with heigest likes":
    query5 ='''select title as videotitle,channel_name as channelname,likes as likescount from videos where likes is not null order by likes desc '''
    cursor.execute(query5)
    mydb.commit()
    t5= cursor.fetchall()
    df4=pd.DataFrame(t5,columns=['no of comments' , "channel name", "video title"])
    st.write(df4)   
elif question ==  "6.likes of all videos":
    query6 ='''select likes as likescount, title as videostitle from videos'''
    cursor.execute(query6)
    mydb.commit()
    t6= cursor.fetchall()
    df5=pd.DataFrame(t6,columns=["likecount" , "videotitle"])
    st.write(df5)   
elif question =="7.views of each channel":
    query7 ='''select channel_name as channelname , views as totalviews from channels'''
    cursor.execute(query7)
    mydb.commit()
    t7= cursor.fetchall()
    df6=pd.DataFrame(t7,columns=["channelname" , "totalviews"])
    st.write(df6)
elif question =="8.videos published in the year of 2022":
    query8 ='''select title as videos_title , publish_date as videorelease , channel_name as channelname  from videos where extract (year from publish_date) = 2022'''
    cursor.execute(query8)
    mydb.commit()
    t8= cursor.fetchall()
    df7=pd.DataFrame(t8,columns=["videotitle" , "published_date", "channelname"])
    st.write(df7)
elif question == "9.average duration of all videos in each channel":
    query9 = '''select channel_name as channelname, AVG(duration) as averageduration from videos group by channel_name'''
    cursor.execute(query9)
    mydb.commit()
    t9 = cursor.fetchall()
    df8 = pd.DataFrame(t9, columns=["channelname", "averageduration"])

    t9 = []
    for index, row in df8.iterrows():
        channel_title = row["channelname"]
        avg_duration = row["averageduration"]
        avg_dura_str = str(avg_duration)
        t9.append(dict(channeltitle=channel_title, avgduration=avg_dura_str))

    df1 = pd.DataFrame(t9)
    df1
elif question ==     "10.videos with heighest number of comments":
    query10 ='''select title as videotitle , channel_name as channel , comments as comments  from videos where comments is not null order by comments desc'''
    cursor.execute(query10)
    mydb.commit()
    t10= cursor.fetchall()
    df9=pd.DataFrame(t10,columns=["video title" , "channel name" ,"comments"])
    st.write(df9)