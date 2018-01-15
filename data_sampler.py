from credentials import *
import pymongo
from urllib.error import HTTPError
connection = pymongo.MongoClient('localhost',27017)
connection.tweetlistener.authenticate('tweetwriter',tweetwriterpw,mechanism='SCRAM-SHA-1')
db = connection['tweetlistener']
tweet_catcher = db['notap_tweets']
import glob
import wget
from datetime import datetime as dt
from bson.objectid import ObjectId
import pandas as pd
import os
import pytz

filter_retweets = False

last_index = pd.read_pickle('processed_tweets.pkl').last_valid_index()
#last_index = 2789
image_list = glob.glob('images/*.jpg')

def pic_retrieve(url='',tweet_id=''):
    try:
        name = 'images/{}{}'.format(tweet_id,url[-4:])
        if name not in image_list:
            print('Retrieving {}'.format(name))
            wget.download(url,out=name)
        else:
            #print('Already Retrieved image for {}'.format(tweet_id))
            return
    except HTTPError:
        print('Could not retrieve image from...')
        print(url)
        return
    return

for x in tweet_catcher.find():
    tweet_catcher.update_one({'_id': x['_id']},
                             {'$set':{'new_date':dt.strptime(x['created_at'],
                                                                   '%a %b %d %H:%M:%S +0000 %Y').replace(tzinfo=pytz.UTC)}})

gen_time = dt(2017, 9, 1)
dummy_id = ObjectId.from_datetime(gen_time)

limited_data_retrieval = tweet_catcher.find({'new_date':{'$lt': dt(2017,9,1)}})
records = []
for x in limited_data_retrieval:
    tweet_id = x['_id']
    if filter_retweets:
        if 'retweeted_status' in x:
            status = x['retweeted_status']
        else:
            status = x
    else:
        status = x
    row =[]
    if filter_retweets:
        if str(status['text']).startswith('RT'):
            continue
    row.append(tweet_id) # Tweet ID
    row.append(status['created_at']) #  creation date
    row.append(status['text']) #  text
    row.append(status['lang']) #  language
    row.append(status['retweet_count']) # retweet count
    row.append(status['favorite_count'])
    if 'urls' in status['entities']:
        row.append([url['expanded_url'] for url in status['entities']['urls']])
    else:
        row.append([])
    if 'media' in status['entities']:
        pic_list = [pic['media_url'] for pic in status['entities']['media']]
        row.append(pic_list)
        for pic in pic_list:
            pic_retrieve(url=pic,tweet_id=str(tweet_id))
    else:
        row.append([])
    row.append(status['user']['id']) # Status User ID
    row.append(status['user']['screen_name']) #  Status screen name

    if 'retweeted_status' in x:
        is_retweet = True
        retweeting_user = x['user']['screen_name']
        retweeting_user_id = x['user']['id']
    else:
        is_retweet = False
        retweeting_user = None
        retweeting_user_id = None
    row.append(is_retweet)
    row.append(retweeting_user)
    row.append(retweeting_user_id)
    records.append(row)

columns = ['tweet_id',
           'creation_date',
           'text',
           'language',
           'retweet_count',
           'favorite_count',
           'urls',
           'image_urls',
           'status_user_id',
           'status_user_screen_name',
           'is_retweet?',
           'retweeting_user_screen_name',
           'retweeting_user_id']
df = pd.DataFrame(records,columns=columns)


filename = 'tweet_data/{}_NoTap_Data_increment.xls'.format(dt.now().strftime('%y-%m-%d'))

if not os.path.exists(filename):
    print('Updating exports')
    df_increment = df.loc[last_index+1:]
    df_increment.to_excel(filename)
    if filter_retweets:
        df.to_pickle('retweets_extracted_processed_tweets.pkl')
    else:
        df.to_pickle('retweets_not_extracted.pkl')
else:
    print('Already produced data today')