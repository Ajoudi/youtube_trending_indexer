
import requests 
import pandas as pd
import time
from tqdm import tqdm
import base64
import pickle
import re
import string
from collections import Counter
import json
import html_to_json
from datetime import datetime, timedelta
import os

API_KEY = "" # your youtube API key 

def convert_YouTube_duration_to_seconds(duration):
    day_time = duration.split('T')
    day_duration = day_time[0].replace('P', '')
    day_list = day_duration.split('D')
    if len(day_list) == 2:
        day = int(day_list[0]) * 60 * 60 * 24
        day_list = day_list[1]
    else:
        day = 0
        day_list = day_list[0]
    hour_list = day_time[1].split('H')
    if len(hour_list) == 2:
        hour = int(hour_list[0]) * 60 * 60
        hour_list = hour_list[1]
    else:
        hour = 0
        hour_list = hour_list[0]
    minute_list = hour_list.split('M')
    if len(minute_list) == 2:
        minute = int(minute_list[0]) * 60
        minute_list = minute_list[1]
    else:
        minute = 0
        minute_list = minute_list[0]
    second_list = minute_list.split('S')
    if len(second_list) == 2:
        second = int(second_list[0])
    else:
        second = 0
    return day + hour + minute + second


def concat_str_list(x):
    x = " ".join(x)
    return x

# Get the current date
today = datetime.today()

# Calculate the date 10 days ago
ten_days_ago = today - timedelta(days=10)

# Format the date as a string
date_str = ten_days_ago.strftime('%Y-%m-%d')
date_today = today.strftime('%Y-%m-%d')

newpath = 'youtube_indexer_folder/{}'.format(date_today)

if not os.path.exists(newpath):
    os.makedirs(newpath)
    
region_code = 'US'


category_df = pd.DataFrame(requests.get("https://www.googleapis.com/youtube/v3/videoCategories?part=snippet&regionCode={}&key={}".format(region_code, API_KEY)).json()['items'])
list_of_categories = list(category_df['id'])


list_of_trending_dfs = []
list_of_channel_stats_dfs = []
list_of_comment_stats_dfs = []

for i in tqdm(range(len(category_df))):
    
    try:
        category_id = category_df['id'].iloc[i]
        category_title = category_df['snippet'].iloc[i]['title']

        list_of_trending_videos = requests.get("https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics, contentDetails&chart=mostPopular&maxResults=20&regionCode={}&key={}&publishedAfter={}T00:00:00Z&videoCategoryId={}".format(region_code, API_KEY, date_str, category_id)).json()['items']


        trending_df_list = []

        for j in range(len(list_of_trending_videos)):
            try:
                v_id = list_of_trending_videos[j]['id']
                v_link = 'https://www.youtube.com/watch?v=' + list_of_trending_videos[j]['id']
                v_thumbnail =  list_of_trending_videos[j]['snippet']['thumbnails']['standard']['url']
                
                c_id = list_of_trending_videos[j]['snippet']['channelId']
                c_title = list_of_trending_videos[j]['snippet']['channelTitle']
                

                desc = list_of_trending_videos[j]['snippet']['description']
                v_date = list_of_trending_videos[j]['snippet']['publishedAt']
                v_title = list_of_trending_videos[j]['snippet']['title']

                try:
                    v_likes = list_of_trending_videos[j]['statistics']['likeCount']
                except:
                    v_likes = -1
                v_views = list_of_trending_videos[j]['statistics']['viewCount']
                v_comments = list_of_trending_videos[j]['statistics']['commentCount']
                duration_sec = convert_YouTube_duration_to_seconds(list_of_trending_videos[j]['contentDetails']['duration'])

                trending_df_list += [{'video_id':v_id,
                                      'video_link': v_link,
                                      'video_thumbnail': v_thumbnail,
                                     'channel_id': c_id,
                                      'channel_title': c_title,
                                      'v_description': desc,
                                      'publish_date': v_date,
                                      'video_title': v_title,
                                      'likes': v_likes,
                                      'views': v_views,
                                      'comments': v_comments,
                                      'date_fetch': date_today,
                                      'category': category_title,
                                      'duration_sec': duration_sec
                                     }]
            except:
                pass

        trending_df = pd.DataFrame(trending_df_list)
        list_of_trending_dfs += [trending_df]

        trending_df.to_pickle('youtube_indexer_folder/{}/trending_df_{}_{}.pkl'.format(date_today, category_id,date_today))

        # get the channel stats 
        channel_statistics_result = requests.get("https://www.googleapis.com/youtube/v3/channels?part=statistics&id={}&key={}".format(",".join(trending_df['channel_id']), API_KEY)).json()['items']



        channel_stats_list = []

        for z in range(len(channel_statistics_result)):
            try:
                cid = channel_statistics_result[z]['id']
                c_views = channel_statistics_result[z]['statistics']['viewCount']
                c_subs = channel_statistics_result[z]['statistics']['subscriberCount']
                c_vids = channel_statistics_result[z]['statistics']['videoCount']

                channel_stats_list += [
                    {'channel_id': cid,
                    'channel_views': c_views,
                    'channel_subs': c_subs,
                    'channel_vids': c_vids
                    }
                ]
            except:
                pass

        channel_stats_df = pd.DataFrame(channel_stats_list)
        list_of_channel_stats_dfs += [channel_stats_df]
        channel_stats_df.to_pickle('youtube_indexer_folder/{}/channel_stats_df_{}_{}.pkl'.format(date_today, category_id,date_today))

        comment_stats_df_list = []

        for video_id in trending_df['video_id']: 
            try:
                comments = requests.get('https://youtube.googleapis.com/youtube/v3/commentThreads?part=snippet&order=relevance&videoId={}&key={}'.format(video_id, API_KEY)).json()['items']


                for jj in range(len(comments)):
                    vid = comments[jj]['snippet']['videoId']
                    comment_text = comments[jj]['snippet']['topLevelComment']['snippet']['textDisplay']
                    comment_author = comments[jj]['snippet']['topLevelComment']['snippet']['authorDisplayName']
                    comment_likes = comments[jj]['snippet']['topLevelComment']['snippet']['likeCount']
                    comment_replies = comments[jj]['snippet']['totalReplyCount']

                    comment_stats_df_list += [{
                        'v_id': vid,
                        'comment_text': comment_text,
                        'comment_author': comment_author,
                        'comment_likes': comment_likes,
                        'comment_replies': comment_replies
                    }]
            except:
                pass


        comment_stats_df = pd.DataFrame(comment_stats_df_list)


        comment_stats_df.to_pickle('youtube_indexer_folder/{}/comment_stats_df_{}_{}.pkl'.format(date_today,category_id,date_today))


        list_of_comment_stats_dfs += [comment_stats_df]
    except:
        print("Error at Category Id: {} Title: {}".format(category_id, category_title))
        pass

    

all_records = []
for i in range(len(list_of_trending_dfs)):
    
    temp_comment_df = list_of_comment_stats_dfs[i].copy()
#     temp_comment_df['comment_text'] = temp_comment_df['comment_text'].apply(lambda x: clean_text(x))
    temp_comment_df = temp_comment_df.groupby('v_id')['comment_text'].apply(list).reset_index()
    temp_comment_df['comment_text'] = temp_comment_df['comment_text'].apply(lambda x: concat_str_list(x))
    
    merged_df_all_v1 = pd.merge(list_of_trending_dfs[i], list_of_channel_stats_dfs[i], how = 'left', left_on = 'channel_id', right_on='channel_id')
    merged_df_all = pd.merge(merged_df_all_v1, temp_comment_df, how = 'left', left_on = 'video_id', right_on='v_id')    
    merged_df_all['comment_text'] = merged_df_all['comment_text'].fillna('').copy()
    
    merged_df_all['publish_date'] = pd.to_datetime(merged_df_all['publish_date']).dt.date.astype(str)
    
    for z in range(len(merged_df_all)):
        all_records += [{
            # video attributes
            'objectID': merged_df_all['video_id'].iloc[z],
            'video_link': merged_df_all['video_link'].iloc[z],
            'video_thumbnail': merged_df_all['video_thumbnail'].iloc[z],
            
            'v_description': merged_df_all['v_description'].iloc[z],
            'publish_date': merged_df_all['publish_date'].iloc[z],
            'video_title': merged_df_all['video_title'].iloc[z],
            'likes': int(merged_df_all['likes'].iloc[z]),
            'views': int(merged_df_all['views'].iloc[z]),
            'comments': int(merged_df_all['comments'].iloc[z]),
            'category': merged_df_all['category'].iloc[z],
            'duration_sec': int(merged_df_all['duration_sec'].iloc[z]),
            'trending_date': merged_df_all['date_fetch'].iloc[z],

            # channel attributes
            'channel_id': merged_df_all['channel_id'].iloc[z],
            'channel_title': merged_df_all['channel_title'].iloc[z],
            'channel_views': int(merged_df_all['channel_views'].iloc[z]),
            'channel_subs': int(merged_df_all['channel_subs'].iloc[z]),
            'channel_vids': int(merged_df_all['channel_vids'].iloc[z]),

            # comments
            'top_comments_text': merged_df_all['comment_text'].iloc[z],
        }]



# build an index using algolia search
import requests
from algoliasearch.search_client import SearchClient

client = SearchClient.create( # add the algolia search info
    '',
    ''
)

index = client.init_index('reacts_finder')

for i in range(0,250, 10):
    try:
        index.save_objects(all_records[i:i + 10], {
            'autoGenerateObjectIDIfNotExist': False
        })
    except:
        print("ERROR!")
        break