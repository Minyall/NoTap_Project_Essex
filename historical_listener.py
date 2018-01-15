import tweepy
from credentials import *
import pymongo
connection = pymongo.MongoClient('localhost',27017)
connection.tweetlistener.authenticate('tweetwriter',tweetwriterpw,mechanism='SCRAM-SHA-1')
db = connection['tweetlistener']
tweet_catcher = db['notap_tweets']


auth = tweepy.OAuthHandler(CONSUMER_KEY,CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY,ACCESS_SECRET)
api = tweepy.API(auth,wait_on_rate_limit=True, wait_on_rate_limit_notify=True)

for status in (tweepy.Cursor(api.search,q='#notap', lang='it')).items():
    tweet_catcher.update_one({'_id': status._json['id']}, {'$setOnInsert': status._json}, upsert=True)


