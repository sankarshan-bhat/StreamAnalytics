from kafka import KafkaConsumer
import json


class TweetsConsumer:
  def consumerMessage(self):
    print ("inside consumer")
    consumer = KafkaConsumer('test',bootstrap_servers=['localhost:9092'])
    print (consumer)
    #consumer = KafkaConsumer('test',bootstrap_servers=['localhost:9092'],value_deserializer=lambda m: json.loads(m.decode('ascii')),auto_offset_reset='earliest', enable_auto_commit=False)
    for message in consumer:
      r_msg = str(message.value.decode("utf-8"))
      tweet_text = json.loads(r_msg)
      print (tweet_text)
      
      print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))

TweetsConsumer().consumerMessage()