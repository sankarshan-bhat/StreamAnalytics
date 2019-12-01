from kafka import KafkaConsumer
from manku_motwani_algo import *
from threading import Thread
import json
from timeloop import Timeloop
from StreamAnalytics.manku_motwani_lossy_counting.manku_motwani_algo import manku_motwani_algo
from datetime import timedelta

tl = Timeloop()

class manku_motwani_tweet_consumer:
  manku_motwani = manku_motwani_algo(0.01, 5)

  def consumerMessage(self):
    print("Inside Consumer")
    consumer = KafkaConsumer('UserMention',bootstrap_servers=['localhost:9092'])
    print(manku_motwani_tweet_consumer.manku_motwani)
    for message in consumer:
      r_msg = str(message.value.decode("utf-8"))
      tweet_text = json.loads(r_msg)
  #    print(tweet_text)
      manku_motwani_tweet_consumer.manku_motwani.add(tweet_text)


@tl.job(interval=timedelta(seconds=10))
def getResult():
    print(manku_motwani_tweet_consumer.manku_motwani)
    print("Result after round: ", manku_motwani_tweet_consumer.manku_motwani.get())
    print()

def threaded_function():
    manku_motwani_tweet_consumer().consumerMessage()

if __name__ == '__main__':
    thread = Thread(target=threaded_function)
    thread.start()
    tl.start(block=True)

