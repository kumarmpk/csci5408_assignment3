import tweepy
import json
import time
from pymongo import MongoClient
import sys
from pyspark import SparkContext
import datetime


api_key = 'GS7fF6DxxffpqpuzTm3o1nOEE'
api_key_secret = 'oDFd2A3HUi2h9FtWjzSXmrETL7TI8IL15O7PaqC7VhgWM6eWn5'
access_token = "1318220534644154368-BZieizTHamgXd6CVKYvInYP07E1l8L"
access_token_secret = "Y0KItWZLsti2k3rC5QSq8eGzX5AHV7YVjDirP0OBxQ39D"

authorization = tweepy.OAuthHandler(api_key, api_key_secret)
authorization.set_access_token(access_token, access_token_secret)

connection = tweepy.API(authorization, wait_on_rate_limit=True)

keyword_list = []
final_dict = {}
search_keyword_list = ["Storm", "Winter", "Canada",
                       "Temperature", "Flu", "Snow", "Indoor", "Safety"]

client = MongoClient(
    'mongodb+srv://user:5408pass3@data-assignment3.xtllw.mongodb.net/RawDb?retryWrites=true&w=majority')
rawdb_database = client.get_database('RawDb')


class StreamAPI(tweepy.StreamListener):
    def __init__(self, time_limit=2):
        self.start_time = time.time()
        self.limit = time_limit
        super(StreamAPI, self).__init__()

    def on_data(self, raw_data):
        tweet_dict = json.loads(raw_data)
        keyword_list.append(tweet_dict)
        if (time.time() - self.start_time) > self.limit:
            return False

    def on_error(self, status):
        print(status)
        return False


try:
    now = datetime.datetime.now()
    print("Current date and time : ")
    print(now.strftime("%Y-%m-%d %H:%M:%S"))
    for keyword in search_keyword_list:
        keyword_list = []
        for tweet in tweepy.Cursor(connection.search, q=keyword, lang='en').items(3):
            tweet_dict = tweet._json
            if(tweet_dict['text'] and tweet_dict['user']['location'] and tweet_dict['user']['created_at']):
                keyword_list.append(tweet_dict)

        print('keyword: ', keyword)
        print('count from search: ', len(keyword_list))

        stream_listener = StreamAPI()
        stream = tweepy.Stream(auth=connection.auth, listener=stream_listener)
        stream.filter(track=keyword)

        print('after streaming length: ', len(keyword_list))
        final_dict[keyword] = keyword_list
        now = datetime.datetime.now()
        print("Current date and time : ")
        print(now.strftime("%Y-%m-%d %H:%M:%S"))

except:
    print("Retrieving tweet error: ", sys.exc_info()[0])


try:
    for keyword in search_keyword_list:
        if len(final_dict[keyword]) > 0:
            rawdb_records = rawdb_database[keyword]
            rawdb_records.delete_many({})
            insert_list = final_dict[keyword][0:600]
            rawdb_records.insert_many(insert_list)
            print('tweet raw db done.', keyword)
            now = datetime.datetime.now()
            print("Current date and time : ")
            print(now.strftime("%Y-%m-%d %H:%M:%S"))
except:
    print("Inserting into rawDB error: ", sys.exc_info()[0])
