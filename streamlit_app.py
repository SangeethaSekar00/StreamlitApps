import streamlit as st
from streamlit_option_menu import option_menu
import plotly.express as px
from pymongo import MongoClient
import mysql.connector as sql
from googleapiclient.discovery import build
import pandas as pd
from PIL import Image
import plotly.express as px
import plotly.graph_objects as go
# SETTING THE TITLE ELEMENT FOR THE PAGE
icon = Image.open('download.png')
st.set_page_config(page_title="Youtube Data Harvesting and Warehousing",
                   page_icon=icon,
                   layout="wide",
                   initial_sidebar_state="expanded",
                   )

# CREATING OPTION MENU
with st.sidebar:
    selected = option_menu(None, ["Home", "Extract and Transform", "View Analytics"],
                           icons=["house-door-fill", "tools", "card-text"],
                           default_index=0,
                           orientation="vertical",
                           styles={"nav-link": {"font-size": "20px", "text-align": "centre", "margin-top": "20px",
                                                "--hover-color": "#266c81"},
                                   "icon": {"font-size": "20px"},
                                   "container": {"max-width": "6000px"},
                                   "nav-link-selected": {"background-color": "#266c81"}})

# CONNECTING TO THE MONGODB ATLAS DATA BASE AND CREATING A DATABASE

url = st.secret["mongodb+srv://Maxwell:naveenmaxvel@cluster0.83j93c5.mongodb.net/?retryWrites=true&w=majority"]
client = MongoClient(url)

db = client['yt_project']

# MAKING CONNECTION TO MYSQL

mydb = sql.connect(host='localhost',
                    user='root',
                    password='Mysql@12345',
                    database='youtube_project'
                    )

cursor = mydb.cursor(buffered=True)

# GOOGLE API_KEY FOR FETCHING THE DATA
api_key = st.secret["AIzaSyBpDipJPtTcQqIqnBiJrtfrn291uvpXMXY"]

# BUILDING CONNECTION WITH GOOGLE YOUTUBE
youtube = build('youtube', 'v3', developerKey=api_key)

# GETTING CHANNEL DETAILS FROM YOUTUBE CONNECTION.


def get_channel_details(channel_id):
    ch_data = []
    request = youtube.channels().list(
        part='snippet,contentDetails,statistics',
        id=channel_id
    )
    response = request.execute()

    for i in range(len(response['items'])):
        data = dict(
            Channel_id=response['items'][i]['id'],
            Channel_name=response['items'][i]['snippet']['title'],
            Upload_id=response['items'][i]['contentDetails']['relatedPlaylists']['uploads'],
            Subscribers=response['items'][i]['statistics']['subscriberCount'],
            Views=response['items'][i]['statistics']['viewCount'],
            Total_videos=response['items'][i]['statistics']['videoCount'],
            Description=response['items'][i]['snippet']['description'][:30],
            Country=response['items'][i]['snippet'].get('country'),
            Thumbnail=response['items'][i]['snippet']["thumbnails"]["default"]["url"]
        )

        ch_data.append(data)

    return ch_data


# GETTING VIDEOS IDS FOR THE RESPECTIVE CHANNELS


def get_channel_videos_details(channel_id):
    video_ids = []

    request = youtube.channels().list(
        part='contentDetails',
        id=channel_id
    )

    response = request.execute()
    Upload_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None
    more_pages = True

    request = youtube.playlistItems().list(playlistId=Upload_id,
                                           part='snippet',
                                           maxResults=50,
                                           pageToken=next_page_token)
    response = request.execute()

    for i in range(len(response['items'])):
        video_ids.append(
            response['items'][i]['snippet']['resourceId']['videoId']
        )

    next_page_token = response.get('nextPageToken')

    if next_page_token is None:
        more_pages = False

    else:

        while more_pages:

            request = youtube.playlistItems().list(playlistId=Upload_id,
                                                   part='snippet',
                                                   maxResults=50,
                                                   pageToken=next_page_token)
            response = request.execute()

            for i in range(len(response['items'])):
                video_ids.append(response['items'][i]
                                 ['snippet']['resourceId']['videoId'])
            if response.get('nextPageToken') is None:
                break
            else:
                next_page_token = response.get('nextPageToken')

    return video_ids

# GETTING VIDEOS DETAILS FOR THE RESPECTIVE CHANNELS


def get_video_details(vd_ids):
    video_stats = []

    for i in range(0, len(vd_ids), 50):
        request = youtube.videos().list(
            part='snippet,contentDetails,statistics',
            id=",".join(vd_ids[i:i+50])
        )

        response = request.execute()

        def time_duration(t):
            a = pd.Timedelta(t)
            b = str(a).split()[-1]
            return b

        for video in response['items']:
            video = dict(Channel_name=video['snippet']['channelTitle'],
                         Channel_id=video['snippet']['channelId'],
                         Video_id=video['id'],
                         Title=video['snippet']['title'],
                         Published_date=video['snippet']['publishedAt'],
                         Duration=time_duration(
                             video['contentDetails']['duration']),
                         Views=video['statistics'].get('viewCount'),
                         Likes=video['statistics'].get('likeCount'),
                         Comments=video['statistics'].get('commentCount')
                         )
            video_stats.append(video)
    return video_stats

# GETTING COMMENTS FOR THE RESPECTIVE VIDEO_IDS


def get_comments_details(v_id):
    comment_data = []
    try:
        next_page_token = None
        more_comments = True
        while more_comments:
            response = youtube.commentThreads().list(part="snippet,replies",
                                                     videoId=v_id,
                                                     maxResults=30,
                                                     pageToken=next_page_token).execute()
            for cmt in response['items']:
                data = dict(Comment_id=cmt['id'],
                            Video_id=cmt['snippet']['videoId'],
                            Comment_text=cmt['snippet']['topLevelComment']['snippet']['textDisplay'],
                            Comment_author=cmt['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                            Comment_posted_date=cmt['snippet']['topLevelComment']['snippet']['publishedAt'],
                            Like_count=cmt['snippet']['topLevelComment']['snippet']['likeCount'],
                            Reply_count=cmt['snippet']['totalReplyCount']
                            )
                comment_data.append(data)
            next_page_token = None
            if next_page_token is None:
                more_comments = False
                break
    except:
        pass
    return comment_data

# FUNCTION FOR GETTING CHANNEL LIST

def channel_list():
    channel_list = []
    for i in db.channel_details.find():
        channel_list.append(i['Channel_name'])

    if channel_list != []:
        return channel_list
    else:
        channel_list = ["NO COLLECTION TO DISPLAY PLEASE EXTRACT !!!"]
        return channel_list
    
    # FUNCTION TO COLLECT ALL THE COMMENTS FOR RESPECTIVE CHANNEL AND THEIR VIDEO_IDS


def get_comments(v_ids):
    com_d = []
    for i in v_ids:
        com_d = com_d + get_comments_details(v_id=i)
    return com_d


# CREAING A TABLE SCHEMA FOR MYSQL TABLES
def create_mysql_tables():
    cursor.execute("""CREATE TABLE IF NOT EXISTS Channel_table (
    Channel_id VARCHAR(40) PRIMARY KEY,
    Channel_name VARCHAR(40),
    Upload_id VARCHAR(40),
    Subscribers BIGINT,
    Views BIGINT,
    Total_videoes INT,
    Description VARCHAR(100),
    Country VARCHAR(10))""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS Videos_table (
    Channel_name VARCHAR(200),
    Channel_id VARCHAR(100),
    Video_id VARCHAR(20) PRIMARY KEY,
    Title VARCHAR(250),
    Published_date VARCHAR(30),
    Duration VARCHAR(30),
    Views BIGINT,
    Likes BIGINT,
    Comments BIGINT   
    )""")

    cursor.execute("""CREATE TABLE IF NOT EXISTS Comments_table (
    Comment_id VARCHAR(30) PRIMARY KEY,
    Video_id VARCHAR(30),
    Comment_text MEDIUMTEXT,
    Comment_author MEDIUMTEXT,
    Comment_posted_date VARCHAR(30),
    Like_count INT,
    Reply_count INT
    )""")

# CALLING THIS FUNCTION CREATES AN MYSQL TABLES
create_mysql_tables()

# HOME PAGE
if selected == "Home":
    # Title Image

    st.markdown("##  :red[OBJECTIVE OF THE PROJECT :]")
    st.markdown("#### :green[This project aims to develop a user-friendly Streamlit application that utilizes the Google API to extract information on a YouTube channel, stores it in a MongoDB database, migrates it to a SQL data warehouse, and enables users to search for channel details and get some Insights and Bussiness value in the Streamlit app]")
    st.markdown("### :red[Technologies Used in this Application :]")
    st.markdown(
        "##### :green[API Intergration Fetching data from the server (google api client)]")
    st.markdown(
        "##### :green[Making connection to MongoDB Atlas (using Python,pymongo MongoClient)]")
    st.markdown(
        "##### :green[Creating a database and storing the data for Migration(Mongodb Atlas)]")
    st.markdown(
        "##### :green[Migrating Big data as Normalized data with multiple Tables in local server (MYSQL)]")
    st.markdown(
        "##### :green[Using pandas powerfull data frames for data manipulation and multiple operations(Pandas)]")
    st.markdown(
        "##### :green[Visualization using beautiful plotly charts(Plotly)]")
    st.markdown(
        "#####  :green[Using streamlit rapidly and customized building web Application(Streamlit)]")
    st.markdown("### :red[Application flow:]")
    st.markdown("##### :green[ 1.Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count,video ID, likes,comments of each video) using Google API.]")
    st.markdown(
        "##### :green[ 2.Option to store the data in a MongoDB database as a data lake.]")
    st.markdown(
        "##### :green[3.Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.]")
    st.markdown(
        "##### :green[4.Option to select a channel name and migrate its data from the data lake to a SQL database as tables.]")
    st.markdown(
        "##### :green[5.Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details]")
    st.markdown(
        "##### :green[6.Visualizing Some Imporant Parameters for Each Channel using Plotly]")
    st.markdown("##### :green[7.Getting some Bussiness Value from the data]")

# EXTRACT and TRANSFORM PAGE
if selected == "Extract and Transform":
    tab1, tab2 = st.tabs(
        ["$\huge EXTRACT $",      "$\huge TRANSFORM $"])

    # EXTRACT TAB
    with tab1:
        st.markdown("#    ")
        st.write("### Enter YouTube Channel_ID below :")
        ch_id = st.text_input(
            "Hint : Goto channel's home page > Right click > View page source > Find channel_id").split(',')
        if ch_id and st.button("Extract Data"):
            with st.spinner('Please Wait for it...'):
                ch_details = get_channel_details(ch_id)
                st.image(f'{ch_details[0]["Thumbnail"]}',
                         # Manually Adjust the width of the image as per requirement
                         width=150, caption=f'{ch_details[0]["Channel_name"]}'
                         )
                st.write(
                    f'##### Extracted data from :green["{ch_details[0]["Channel_name"]}"] channel')
                st.table(ch_details)
                st.success("Data Successfully extracted !!")

        if st.button("Extract to MongoDB"):
            with st.spinner('Please Wait...'):
                flag = 0
                ch_details = get_channel_details(ch_id)
                Channel_name = ch_details[0]["Channel_name"]
                Channel_id = ch_details[0]["Channel_id"]
                for i in db.channel_details.find():
                    if i["Channel_name"] == Channel_name:
                        flag = 1
                        st.warning('Channel Already Extracted', icon="⚠️")
                        break
                if flag == 0:
                    v_ids = get_channel_videos_details(ch_id)
                    vid_details = get_video_details(v_ids)
                    comm_details = get_comments(v_ids)
                    st.table(ch_details)
                    st.write("Sample Videos Data")
                    st.write(vid_details[:5])
                    st.write("Sample comments Data")
                    st.write(comm_details[:5])
                    coll1 = db.channel_details
                    coll1.insert_many(ch_details)
                    coll3 = db.video_details
                    coll3.insert_many(vid_details)
                    coll2 = db.comments_details
                    coll2.insert_many(comm_details)
                    st.success("Upload to MogoDB successful !!")
    with tab2:
        st.markdown("#")

        st.markdown("### Select a channel to begin Transformation to SQL")

        ch_names = channel_list()

        user_inp = st.selectbox("Select channel", options=ch_names)

        st.markdown("#")

        def table_for_added_channel_to_sql():
            query = ('select * from Channel_table')
            cursor.execute(query)
            tabel = cursor.fetchall()

            i = [i for i in range(1, len(tabel)+1)]
            tabel = pd.DataFrame(tabel, columns=cursor.column_names, index=i)
            tabel = tabel[["Channel_name", "Subscribers", "Views"]]
            st.markdown("### channels migrated to mysql")
            st.dataframe(tabel)

        table_for_added_channel_to_sql()
 # FUNCTION FOR MIGRATING OF CHANNEL DETAILS FROM MONGODB TO MYSQL TABLE
    def insert_into_channels(user):
            coll1 = db.channel_details
            query = """INSERT INTO Channel_table VALUES(%s,%s,%s,%s,%s,%s,%s,%s)"""
            for i in coll1.find({"Channel_name" :user},{'_id':0,'Thumbnail':0}):
                a=i["Description"]
                i["Description"]=a[:30]
                t=tuple(i.values())
                cursor.execute(query,t)
    mydb.commit()    
   

# FUNCTION FOR MIGRATION OF VIDEOS FROM MONGODB TO MYSQL TABLE
    def insert_into_videos(user):
        coll3= db.video_details
        query1 = """INSERT INTO Videos_table VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)"""
        for i in coll3.find({"Channel_name" :user},{"_id":0}):
            t=tuple(i.values())
            cursor.execute(query1,t)
    mydb.commit()     

# FUNCTION FOR MIGRATION OF COMMENTS FROM MONGODB TO MYSQL TABLE
    def insert_into_comments(user):
        coll3=db.video_details
        coll2=db.comments_details
        query="""INSERT INTO comments_table VALUES(%s,%s,%s,%s,%s,%s,%s)"""
        for vid in coll3.find({'Channel_name':user},{'_id':0}):
            for i in coll2.find({'Video_id':vid["Video_id"]},{'_id':0}):
                cursor.execute(query,tuple(i.values()))
    mydb.commit()  
    
if selected == "View Analytics":
        with st.container():
            st.write("## :orange[Select Here To View Some Pre-Defined Queries]")
        questions = st.selectbox('Questions',
                                 ['Click the question that you would like to query',
                                  '1. What are the names of all the videos and their corresponding channels?',
                                  '2. Which channels have the most number of videos, and how many videos do they have?',
                                  '3. What are the top 10 most viewed videos and their respective channels?',
                                  '4. How many comments were made on each video, and what are their corresponding video names?',
                                  '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                  '6. What is the total number of likes for each video, and what are their corresponding video names?',
                                  '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                  '8. What are the names of all the channels that have published videos in the year 2022?',
                                  '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                  '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

        if questions == '1. What are the names of all the videos and their corresponding channels?':
            cursor.execute(
                """SELECT Channel_name,Title AS TITLE,Comments,Likes FROM videos_table ORDER BY Channel_name;""")
            data = cursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            data = pd.DataFrame(data, columns=cursor.column_names, index=i)
            st.write(data)

        elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
            cursor.execute("""SELECT Channel_name,COUNT(Video_id)  AS Total_videos FROM Videos_table GROUP BY Channel_name ORDER BY Total_videos DESC;
""")
            data = cursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            table = pd.DataFrame(data, columns=cursor.column_names, index=i)
            st.write(table)
            

        elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
            cursor.execute(
                """SELECT Channel_name,Title AS TITLE,Views AS VIEWS FROM Videos_table ORDER BY VIEWS DESC LIMIT 10;""")
            data = cursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            table = pd.DataFrame(data, columns=cursor.column_names, index=i)
            st.write(table)

        elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
            cursor.execute(
                """SELECT Channel_table.Channel_name,Videos_table.Title,Videos_table.Comments
FROM Channel_table INNER JOIN Videos_table ON Videos_table.Channel_name=Channel_table.Channel_name ORDER BY Channel_name ASC,Comments DESC;;""")
            data = cursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            table = pd.DataFrame(data, columns=cursor.column_names, index=i)
            st.write(table)

        elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
            cursor.execute(
                """SELECT Channel_name,Title,Likes FROM videos_table ORDER BY Likes DESC LIMIT 10;""")
            data = cursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            table = pd.DataFrame(data, columns=cursor.column_names, index=i)
            st.write(table)

        elif questions == '6. What is the total number of likes for each video, and what are their corresponding video names?':
            cursor.execute(
                """SELECT Channel_name,Title, Likes, Published_date FROM videos_table ORDER BY Channel_name, Likes DESC;""")
            data = cursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            table = pd.DataFrame(data, columns=cursor.column_names, index=i)
            st.write(table)

        elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
            cursor.execute(
                """SELECT Channel_table.Channel_name,SUM(Videos_table.Views) AS VIEWS
FROM Channel_table INNER JOIN Videos_table ON Videos_table.Channel_name=Channel_table.Channel_name 
GROUP BY Channel_name;;""")
            data = cursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            table = pd.DataFrame(data, columns=cursor.column_names, index=i)
            st.table(table)

        elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
            cursor.execute(
                """SELECT Channel_table.Channel_name,COUNT(Videos_table.Video_id) AS TOTAL_UPLOADS
FROM Channel_table INNER JOIN Videos_table ON Videos_table.Channel_name=Channel_table.Channel_name 
WHERE Videos_table.Published_date LIKE "2022%" GROUP BY Channel_name;;""")
            data = cursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            table = pd.DataFrame(data, columns=cursor.column_names, index=i)
            st.table(table)

        elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
            def get_duration():
                cursor.execute(
                    """SELECT Channel_name,Video_id,Duration FROM Videos_table;""")
                data = cursor.fetchall()
                i = [i for i in range(1, len(data)+1)]
                table = pd.DataFrame(
                    data, columns=cursor.column_names, index=i)
                return table
            table = get_duration()
            table["Duration"] = pd.to_datetime(
                table["Duration"], format="%H:%M:%S")
            uploads = table.groupby("Channel_name")["Video_id"].size()
            uploads = pd.DataFrame(uploads)
            uploads = uploads.reset_index()
            table["hour"] = table["Duration"].apply(lambda x: x.hour)
            table["minute"] = table["Duration"].apply(lambda x: x.minute)
            table["second"] = table["Duration"].apply(lambda x: x.second)
            table.drop(columns=["Video_id", "Duration"], inplace=True)
            hour = table.groupby("Channel_name")["hour"].sum().reset_index()
            minute = table.groupby("Channel_name")[
                "minute"].sum().reset_index()
            second = table.groupby("Channel_name")[
                "second"].sum().reset_index()
            hour["hour"] = hour["hour"].apply(lambda x: x*60*60)
            hour.rename(columns={"hour": "hour_sec"}, inplace=True)
            minute["minute"] = minute["minute"].apply(lambda x: x*60)
            minute.rename(columns={"minute": "minute_sec"}, inplace=True)
            df = hour.merge(minute, how="inner")
            df = second.merge(df, how="inner")

            def total_time(df):
                time_total = []
                for i in range(len(df)):
                    data = df.iloc[i]["second"] + \
                        df.iloc[i]["hour_sec"]+df.iloc[i]["minute_sec"]
                    time_total.append(data)
                return time_total

            time = total_time(df)
            time = pd.DataFrame(time)
            df1 = pd.concat([uploads, time], axis=1)

            def avg_dur(df1):
                avg_time = []
                for i in range(len(df1)):
                    data = (df1.iloc[i][0]/df1.iloc[i]["Video_id"])
                    avg_time.append(data)
                return avg_time
            avg = avg_dur(df1)
            av = pd.DataFrame(avg, columns=["time"])
            av.rename(columns={"time": "Average_Duration"}, inplace=True)
            av['Average_Duration'] = pd.to_datetime(
                av['Average_Duration'], unit='s')
            av['Average_Duration'] = av['Average_Duration'].dt.strftime(
                '%H:%M:%S')
            final = pd.concat([df1, av], axis=1, join="inner")
            final = final[["Channel_name", "Average_Duration"]]
            i = [i for i in range(1, len(final)+1)]
            final_df = pd.DataFrame(
                final.values.tolist(), columns=["Channel_name", "Average_Duration"], index=i)

            st.table(final_df)

        elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
            cursor.execute(
                """SELECT Channel_name, Title, Comments FROM videos_table ORDER BY Comments DESC;""")
            data = cursor.fetchall()
            i = [i for i in range(1, len(data)+1)]
            data = pd.DataFrame(data, columns=cursor.column_names, index=i)

            st.write(data)
    
