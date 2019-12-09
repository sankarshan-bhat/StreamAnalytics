
from memory_profiler import profile
import json
import gc
from timeloop import Timeloop
from datetime import timedelta
from kafka import KafkaConsumer
from kafka.structs import TopicPartition, OffsetAndTimestamp
import datetime
print(__name__)
from manku_motwani_algo import *

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
        number_of_msg_in_stream = int(self.cur_offset[self.tp]) - int(self.old_offsets[self.tp].offset)
        print("Count of Messages: ",number_of_msg_in_stream)
        for message in self.consumer:
            if int(message.offset) >= int(self.cur_offset[self.tp]):
                break
            r_msg = str(message.value.decode("utf-8"))
            tweet_text = json.loads(r_msg)
            manku_motwani.add(tweet_text)
        return None




@tl.job(interval=timedelta(minutes=2))
#@profile
def ourCustomaryFunction():
    getTopK()
    getWithSupport()
    gc.collect()

#@profile
def getTopK():
    manku_motwani_tweet_consumer_object = manku_motwani_tweet_consumer()
    manku_motwani_User_Mentions = manku_motwani_algo(0.001)
    manku_motwani_tweet_consumer_object.setupTable(manku_motwani=manku_motwani_User_Mentions,topic_name="UserMention",minutes=1440)
    #print(manku_motwani_User_Mentions)
    print("Result after round for UserMentions: ", manku_motwani_User_Mentions.get(10))
    manku_motwani_HashTags = manku_motwani_algo(0.0005)
    manku_motwani_tweet_consumer_object.setupTable(manku_motwani=manku_motwani_HashTags, topic_name="HashTags", minutes=10)
    #print(manku_motwani_HashTags)
    print("Result after round for Hashtags: ", manku_motwani_HashTags.get(10))
    print()

#@profile
def getWithSupport():
    print("For Support ")
    manku_motwani_tweet_consumer_object = manku_motwani_tweet_consumer()
    #manku_motwani_User_Mentions = manku_motwani_algo(0.0001)
    #manku_motwani_tweet_consumer_object.setupTable(manku_motwani=manku_motwani_User_Mentions, topic_name="UserMention",
    #                                               minutes=10)
    # print(manku_motwani_User_Mentions)
    #print("Result after round for UserMentions: ", manku_motwani_User_Mentions.get_with_support(0.005))
    start_time = int(datetime.datetime.now().timestamp()*1000)
    manku_motwani_HashTags = manku_motwani_algo(0.001)

    manku_motwani_tweet_consumer_object.setupTable(manku_motwani=manku_motwani_HashTags, topic_name="UserMention",
                                                   minutes=120)
    # print(manku_motwani_HashTags)
    print("Result after round for Hashtags: ", manku_motwani_HashTags.get_with_support(0.005))
    end_time = int(datetime.datetime.now().timestamp() * 1000)
    print("Time Taken: ",(end_time-start_time))
    print()



if __name__ == '__main__':
 #tl.start(block=True)
 #getWithSupport()
 #getTopK()
 getWithSupport()

