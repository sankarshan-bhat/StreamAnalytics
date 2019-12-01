from kafka import KafkaConsumer
import json
import time
import datetime
from kafka.structs import TopicPartition, OffsetAndTimestamp



class TweetsConsumer:
  def consumerMessage(self):
    print ("inside consumer")
    self.consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])

    '''
    get current offset and store it then break if this offset is reached
    '''
    self.tp = TopicPartition("test", 0)
    cur_offset = self.consumer.end_offsets([self.tp])
    print ("cur offset",cur_offset[self.tp])
    cur_time = int(round(time.time() * 1000))
    current_time = datetime.datetime.now()  # use datetime.datetime.utcnow() for UTC time
    one_hr_ago = current_time - datetime.timedelta(seconds=15)
    one_hr_ago_epoch_ts = int(one_hr_ago.timestamp() * 1000)  # in miliseconds
   
    offsets = self.consumer.offsets_for_times({self.tp: one_hr_ago_epoch_ts})
    self.consumer.assign([TopicPartition('test', 0)])
    self.consumer.seek(self.tp, int(offsets[self.tp].offset))
    print ("offset")
    print (offsets)
    print (offsets[self.tp].offset)
    # cur_offset = self.consumer.end_offsets([self.tp])
    # print ("old offset",cur_offset)
    
    print (self.consumer)
    #consumer = KafkaConsumer('test',bootstrap_servers=['localhost:9092'],value_deserializer=lambda m: json.loads(m.decode('ascii')),auto_offset_reset='earliest', enable_auto_commit=False)
    for message in self.consumer:
      #consumer.assign([self.tp])
      #consumer.seek(self.tp, int(offsets[self.tp].offset))  
      if int(message.offset) >= int(cur_offset[self.tp]):
        break
      r_msg = str(message.value.decode("utf-8"))
      tweet_text = json.loads(r_msg)
      print (tweet_text)
      print ( message.partition, message.offset)
      
      # print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
      #                                     message.offset, message.key,
      #                                     message.value))



TweetsConsumer().consumerMessage()