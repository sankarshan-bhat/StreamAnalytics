import tweepy
from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
from custom_filter import get_hash_tags,get_user_mentions
import json
import requests
import sys
sys.path.append("..")
from kafkaProducer import Producer

class TweetsListner(StreamListener):

	def __init__(self,consumer_key,consumer_secret,access_token,access_token_secret):
		self.auth = OAuthHandler(consumer_key, consumer_secret)
		self.auth.set_access_token(access_token, access_token_secret)
		self.kafka_producer = Producer()
 
	def on_data(self, data):
		try:
			self.tweet_text_data = json.loads(data)
			if 'extended_tweet' in self.tweet_text_data :
				#print( "extended_tweet")
				self.tweet_text = self.tweet_text_data['extended_tweet']['full_text']
			else:
				self.tweet_text = self.tweet_text_data.get("text")
				#print( "text")
			self.tweet_id = self.tweet_text_data.get("id_str")
			#print (self.tweet_text)
			#print (self.tweet_id)

			# this is simple message format, We can change it later depending on the use case
			#self.json_data = {"userid":self.tweet_id,"tweet_data":self.tweet_text_data,"source":"twitter","type":""}

			list_of_hashTags = get_hash_tags(self.tweet_text_data)
			list_of_user_mentions = get_user_mentions(self.tweet_text_data)
			#print("Tweet Text: ",self.tweet_text)
			#print("List of HashTags Returned: ",list_of_hashTags)
			#self.json_data = json.dumps(tweet_data)
			#print(json.dumps(self.json_data))

			#ToDo : Need to test the Kafka producer, some issue in the config so need to fix the kafka broker config first
			#uncomment this
			for hash_tag in list_of_hashTags:
				print("HashTag: ",hash_tag["text"])
				self.kafka_producer.produceMessage(hash_tag["text"], "HashTags")

			for user_mention in list_of_user_mentions:
				print("User_Mention: ", user_mention["screen_name"])
				self.kafka_producer.produceMessage(user_mention["screen_name"],"UserMention")

			return True
		except BaseException as e:
		    print("Error on_data: %s" % str(e))
		return True
 
	def on_error(self, status):
		print(status)
		return True

@classmethod
def parse(cls, api, raw):
	status = cls.first_parse(api, raw)
	setattr(status, 'json', json.dumps(raw))
	return status