from collections import Counter


class MisraGries(object):

    def __init__(self, total_items,desired_freq):
        self.desired_frequency = desired_freq
        self.total_stream_items = total_items
        self.buffer_size = int(self.total_stream_items/self.desired_frequency)
        self.bucket = Counter()

    def process_item(self,item):
        if item in self.bucket:
                self.bucket.update([item])
        elif len(self.bucket) < self.buffer_size - 1:
                self.bucket[item] = 1
        else:
            for l in list(self.bucket):
                self.bucket[l] -= 1
                if self.bucket[l] == 0:
                        del self.bucket[l]

    def get_frequet_items(self):
       return self.bucket


    def estimate_frequency(self, item):
        return self.bucket[item] if item in self.bucket else 0