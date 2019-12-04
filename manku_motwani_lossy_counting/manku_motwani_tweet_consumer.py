from kafka import KafkaConsumer
from manku_motwani_algo import *
from threading import Thread
import json
from timeloop import Timeloop
from StreamAnalytics.manku_motwani_lossy_counting.manku_motwani_algo import manku_motwani_algo
from datetime import timedelta
from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndTimestamp
import datetime

tl = Timeloop()

class manku_motwani_tweet_consumer:

  def consumerMessage(self):
    print("Inside Consumer")
    consumer = KafkaConsumer('UserMention',bootstrap_servers=['localhost:9092'])
    print(manku_motwani_tweet_consumer.manku_motwani)
    for message in consumer:
      r_msg = str(message.value.decode("utf-8"))
      tweet_text = json.loads(r_msg)
  #    print(tweet_text)
      manku_motwani_tweet_consumer.manku_motwani.add(tweet_text)


  def setupTable(self,manku_motwani,topic_name, minutes=1440):
        self.consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
        self.tp = TopicPartition(topic_name, 0)
        self.cur_offset = self.consumer.end_offsets([self.tp])
        current_time = datetime.datetime.now()
        old_time = current_time - datetime.timedelta(minutes=minutes)
        old_epoch_ts = int(old_time.timestamp() * 1000)  # in miliseconds
        self.old_offsets = self.consumer.offsets_for_times({self.tp: old_epoch_ts})
        self.consumer.assign([self.tp])
        self.consumer.seek(self.tp, int(self.old_offsets[self.tp].offset))
        print("StartOffset: ",int(self.old_offsets[self.tp].offset)," EndOffset: ", int(self.cur_offset[self.tp]))
        for message in self.consumer:
            if int(message.offset) >= int(self.cur_offset[self.tp]):
                break
            r_msg = str(message.value.decode("utf-8"))
            tweet_text = json.loads(r_msg)
            manku_motwani.add(tweet_text)
        return None

@tl.job(interval=timedelta(minutes=5))
def getTopK():
    manku_motwani_tweet_consumer_object = manku_motwani_tweet_consumer()
    manku_motwani_User_Mentions = manku_motwani_algo(0.01)
    manku_motwani_tweet_consumer_object.setupTable(manku_motwani=manku_motwani_User_Mentions,topic_name="UserMention",minutes=5)
    #print(manku_motwani_User_Mentions)
    print("Result after round for UserMentions: ", manku_motwani_User_Mentions.get(10))
    manku_motwani_HashTags = manku_motwani_algo(0.01)
    manku_motwani_tweet_consumer_object.setupTable(manku_motwani=manku_motwani_HashTags, topic_name="HashTags", minutes=5)
    #print(manku_motwani_HashTags)
    print("Result after round for Hashtags: ", manku_motwani_HashTags.get(10))
    print()

# @tl.job(interval=timedelta(minutes=2))
# def getWithSupport():
#     print(manku_motwani_tweet_consumer.manku_motwani)
#     print("Result after round for UserMentions: ", manku_motwani_tweet_consumer.manku_motwani.get_with_support("UserMentions", 5,0.02))
#     print("Result after round for Hashtags: ",manku_motwani_tweet_consumer.manku_motwani.get_with_support("HashTags",5,0.02))
#     print()


# def threaded_function():
#     manku_motwani_tweet_consumer().consumerMessage()
#
if __name__ == '__main__':
 tl.start(block=True)
# #
# #
# # thread = Thread(target=threaded_function)
# # thread.start()
# # tl.start(block=True)

