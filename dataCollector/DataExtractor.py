from TweetsListner import TweetsListner
import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import collections
from collections import defaultdict
import json
import time

class DataExtractor:
    def __init__(self):
        self.consumer_key ="pUVDi94pxaNgc214PQrhpPUwa"
        self.consumer_secret ="U8juTSRI2RazqQRIqlqp1qoWcaYVxH7bi2Ka3BKhdk5rPYcIVD"
        self.access_token ="994457489709125632-7jsHa4jzK2lN7ZoEi6Muv7aeNIWPDmp"
        self.access_token_secret ="FiJ0Budbxd3bvXi596vSK8Gc2ShmwoCeVTvtPDxLZTBo1"
        #for now just filter on some userId
        #add your user id to this list if you want to filter your tweets
        self.userIds = ["994457489709125632","1048372701788868608","1048374435370164230"]

    def create_tweet_listner(self):
        tweetListener = TweetsListner(self.consumer_key,self.consumer_secret,self.access_token,self.access_token_secret)
        #auth = OAuthHandler(consumer_key, consumer_secret)
        #auth.set_access_token(access_token, access_token_secret)

        #userIds = ["1048372701788868608"]
        api = tweepy.API(tweetListener.auth)
        

        #tweet_mode = extended, so that we get a full tweets, useful if we have some advanced sue case. 
        twitter_stream = Stream(tweetListener.auth, tweetListener,tweet_mode='extended')
        #twitter_stream.filter(follow=self.userIds)

        #location filtering to analyse the trending #tags in a gvien regeion
        
        twitter_stream.filter(locations=[
        -130.78125, -31.3536369415, 140.625, 63.8600358954
        ])
        

   

@classmethod
def parse(cls, api, raw):
    print ("parse called")
    status = cls.first_parse(api, raw)
    setattr(status, 'json', json.dumps(raw))
    return status
    

   
if __name__ == '__main__':
    DataExtractor().create_tweet_listner()