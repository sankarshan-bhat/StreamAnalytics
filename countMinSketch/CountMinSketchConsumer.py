from kafka import KafkaConsumer
import json
import time
import datetime
from kafka.structs import TopicPartition, OffsetAndTimestamp
import sys
from CountMinSketch import HeavyHitters
import operator
from memory_profiler import profile

class CountMinSketchConsumer:

    #@profile
    def consumerMessage(self, use_case):
        #print("inside consumer")
        self.consumer = KafkaConsumer(bootstrap_servers=['localhost:9092'])
        if use_case == "HashTags":
            # get current offset and store it and process unitl offset reaches this value
            self.tp = TopicPartition("HashTags", 0)
            self.cur_offset = self.consumer.end_offsets([self.tp])

            # timestamp corresponding to current time - 24 hours
            current_time = datetime.datetime.now()
            old_time = current_time - datetime.timedelta(minutes=60)
            old_epoch_ts = int(old_time.timestamp() * 1000)  # in miliseconds

            # get the offset corresponding to old timestamp
            self.old_offsets = self.consumer.offsets_for_times(
                {self.tp: old_epoch_ts})

            print("offset diff(no of messages)",self.cur_offset[self.tp]-self.old_offsets[self.tp].offset)

            # reset the consumer offset
            self.consumer.assign([self.tp])
            self.consumer.seek(self.tp, int(self.old_offsets[self.tp].offset))

            # print("reset offset")
            # print(self.old_offsets[self.tp].offset)

            # create instance of misra-gries algo
            self.count_min_sketch = HeavyHitters(
                width=1000, depth=10, num_hitters=20)

        elif use_case == "UserMention":

            # get current offset
            self.tp = TopicPartition("UserMention", 0)
            self.cur_offset = self.consumer.end_offsets([self.tp])
            #print("cur offset", self.cur_offset[self.tp])

            # timestamp corresponds to cur time - 24 hours
            cur_time = int(round(time.time() * 1000))
            current_time = datetime.datetime.now()
            old_time = current_time - datetime.timedelta(minutes=60)
            old_epoch_ts = int(old_time.timestamp() * 1000)  # in miliseconds

            # get the offset corresponds to old timestamp
            self.old_offsets = self.consumer.offsets_for_times(
                {self.tp: old_epoch_ts})

            # reset the consumer offset
            self.consumer.assign([self.tp])
            self.consumer.seek(self.tp, int(self.old_offsets[self.tp].offset))

            # print("reset offset")
            # print(self.old_offsets[self.tp].offset)

            # create instance of misra-gries algo
            self.count_min_sketch = HeavyHitters(
                width=1000, depth=10, num_hitters=20)

        for message in self.consumer:
            '''
            if the current offset reaches the latest offset stored prevously the break
            '''
            if int(message.offset) >= int(self.cur_offset[self.tp]):
                break

            r_msg = str(message.value.decode("utf-8"))
            tweet_text = json.loads(r_msg)
            #print("Tweet text: ", tweet_text)
            # if tweet_text['tweet_data'] is not None and 'entities' in tweet_text['tweet_data'] and 'hashtags' in tweet_text['tweet_data']['entities']:
            #     if use_case == "HashTags":
            #         eois = tweet_text['tweet_data']['entities']['hashtags']
            #     else:
            #         eois = tweet_text['tweet_data']['entities']['user_mentions']
            #     if len(eois) > 0:
            #         for eoi in eois:
            #             if use_case == "HashTags":
            #                 eoi = eoi['text']
            #             else:
            #                 eoi = eoi['name']
            #             print(eoi, "\n")
            self.count_min_sketch.add_alt(tweet_text, self.count_min_sketch.hashes(tweet_text, 10), 1)

        msg_frequent_item = self.count_min_sketch.heavyhitters
        msg_frequent_item = sorted(msg_frequent_item.items(), key=operator.itemgetter(1), reverse=True)
        print("Most frequent item: ", msg_frequent_item)
      


if __name__ == '__main__':
    # if len(sys.argv) < 1:
    #     sys.exit(1)
    # use_case = sys.argv[1]
    # CountMinSketchConsumer().consumerMessage(use_case)
    start_time = int(datetime.datetime.now().timestamp()*1000)
    CountMinSketchConsumer().consumerMessage("HashTags")
    end_time = int(datetime.datetime.now().timestamp()*1000)
    print("Time Taken",(end_time-start_time))
