from kafka import KafkaConsumer
import json
import time
import datetime
from kafka.structs import TopicPartition, OffsetAndTimestamp
import sys
from misra_gries_algo import MisraGries
import operator
#from memory_profiler import profile

class TweetsConsumer:

  # @profile
  def consumerMessage(self,use_case,desired_freq):
    print ("inside consumer")
    time1 = int (datetime.datetime.now().timestamp()*1000)
    self.consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
    #consumer = KafkaConsumer('test',bootstrap_servers=['localhost:9092'],value_deserializer=lambda m: json.loads(m.decode('ascii')),auto_offset_reset='earliest', enable_auto_commit=False)
    if use_case == "HashTags":
      #get current offset and store it and procees unitl offset reaches this value
      
      self.tp = TopicPartition("HashTags", 0)
      self.cur_offset = self.consumer.end_offsets([self.tp])
      print ("cur offset",self.cur_offset[self.tp])

      #timestamp corresponds to cur time - 24 hours
      cur_time = int(round(time.time() * 1000))
      current_time = datetime.datetime.now() 
      old_time = current_time - datetime.timedelta(minutes=180)
      old_epoch_ts = int(old_time.timestamp() * 1000)# in miliseconds
     
      #get the offset corresponds to old timestamp
      self.old_offsets = self.consumer.offsets_for_times({self.tp: old_epoch_ts})

      #reset the consumer offset
      self.consumer.assign([self.tp])
      self.consumer.seek(self.tp, int(self.old_offsets[self.tp].offset))

      print ("reset offset")
      print (self.old_offsets[self.tp].offset)

      print("offset diff(no of messages",self.cur_offset[self.tp]-self.old_offsets[self.tp].offset)
      print ("----------------")
      #create instance of misra-gries algo
      number_of_msg_in_stream =  int(self.cur_offset[self.tp])-int(self.old_offsets[self.tp].offset)
      self.misra_gries = MisraGries(number_of_msg_in_stream,desired_freq)


    elif use_case == "UserMention":

      #get current offset
      self.tp = TopicPartition("UserMention", 0)
      self.cur_offset = self.consumer.end_offsets([self.tp])
      print ("cur offset",self.cur_offset[self.tp])

      #timestamp corresponds to cur time - 24 hours
      cur_time = int(round(time.time() * 1000))
      current_time = datetime.datetime.now() 
      old_time = current_time - datetime.timedelta(minutes=15)
      old_epoch_ts = int(old_time.timestamp() * 1000)# in miliseconds
     
      #get the offset corresponds to old timestamp
      self.old_offsets = self.consumer.offsets_for_times({self.tp: old_epoch_ts})

      #reset the consumer offset
      self.consumer.assign([self.tp])
      self.consumer.seek(self.tp, int(self.old_offsets[self.tp].offset))

      print ("reset offset")
      print (self.old_offsets[self.tp].offset)

      #create instance of misra-gries algo
      number_of_msg_in_stream =  int(self.cur_offset[self.tp])- int(self.old_offsets[self.tp].offset)
      self.misra_gries = MisraGries(number_of_msg_in_stream,desired_freq)

    
    print (self.consumer)

    for message in self.consumer:
      '''
      if the current offset reaches the latest offset stored prevously the break
      '''
      if int(message.offset) >= int(self.cur_offset[self.tp]):
        break

      r_msg = str(message.value.decode("utf-8"))
      tweet_text = json.loads(r_msg)
      #print (tweet_text)
      #print ( message.partition, message.offset)
      self.misra_gries.process_item(tweet_text)
      
      # print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
      #                                     message.offset, message.key,
      #                                     message.value))

    #second pass to count the exact frequency
    self.accurate_freq_items = {}
    msg_frequent_item = self.misra_gries.get_frequet_items()
    #print (msg_frequent_item)
    #reset the consumer offset
    self.consumer.assign([self.tp])
    self.consumer.seek(self.tp, int(self.old_offsets[self.tp].offset))

    #print ("reset offset")
    #print (self.old_offsets[self.tp].offset)
    for message in self.consumer:
      '''
      if the current offset reaches the latest offset stored prevously the break
      '''
      if int(message.offset) >= int(self.cur_offset[self.tp]):
        break

      r_msg = str(message.value.decode("utf-8"))
      tweet_text = json.loads(r_msg)
      if tweet_text in msg_frequent_item:
        if tweet_text in self.accurate_freq_items:
          self.accurate_freq_items[tweet_text] += 1
        else:
          self.accurate_freq_items[tweet_text] = 1

    print ("final accurate result")
    #print (self.accurate_freq_items)
    self.accurate_freq_items = sorted(self.accurate_freq_items.items(), key=operator.itemgetter(1), reverse = True)
    print (self.accurate_freq_items)
    time2 = int (datetime.datetime.now().timestamp()*1000)
    print ("Run Time",(time2-time1))




if __name__ == '__main__':
  if len(sys.argv) < 3:
    sys.exit(1)
  use_case = sys.argv[1]
  desired_freq = int(sys.argv[2])

  TweetsConsumer().consumerMessage(use_case,desired_freq)