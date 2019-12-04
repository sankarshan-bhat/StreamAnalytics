from kafka import KafkaConsumer
from manku_motwani_algo import *
from threading import Thread
import json
from timeloop import Timeloop
from StreamAnalytics.misra_gries.misra_gries_algo import misra_gries_algo
from datetime import timedelta

tl = Timeloop()

class misra_gries_consumer:
  misra_gries = MisraGries(10,100000)

  def consumerMessage(self):
    print("Inside misra_gries_consumer")
    consumer = KafkaConsumer('HashTags',bootstrap_servers=['localhost:9092'])
    print(misra_gries_consumer.misra_gries)
    for message in consumer:
      r_msg = str(message.value.decode("utf-8"))
      tweet_text = json.loads(r_msg)
      print(tweet_text)
      misra_gries_consumer.misra_gries.process_item(tweet_text)


@tl.job(interval=timedelta(seconds=10))
def getResult():
    print(misra_gries_consumer.misra_gries)
    print("Result after round: ", misra_gries_consumer.misra_gries.get_frequet_items())
    print()

def threaded_function():
    misra_gries_consumer().consumerMessage()

if __name__ == '__main__':
    thread = Thread(target=threaded_function)
    thread.start()
    tl.start(block=True)

