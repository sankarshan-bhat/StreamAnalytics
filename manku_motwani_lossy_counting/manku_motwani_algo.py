import operator
from functools import cmp_to_key
import math

class manku_motwani_algo:

    def __init__(self,errorLimit):
        self.bucketSize = math.ceil((1/(errorLimit)))
        self.errorLimit = errorLimit
        self.currentBucket = 0
        self.buckets = {}
        self.count = 0
        self.toBeRemoved = []

    def delete(self):
        self.toBeRemoved.clear()
        for key in self.buckets.keys():
            (objectCounter,delta) = self.buckets.get(key)
            if objectCounter+delta<self.currentBucket :
                self.toBeRemoved.append(key)

        for item in self.toBeRemoved:
            del self.buckets[item]

    def add (self, input_data):
        self.count = self.count+1;
        self.currentBucket = math.ceil(self.count/self.bucketSize)
        if input_data not in self.buckets:
            self.currentBucket = math.ceil(self.count/self.bucketSize)
            bucket_object = (0,self.currentBucket-1)
            self.buckets[input_data] = bucket_object
        (objectCounter, delta) = self.buckets[input_data]
        self.buckets[input_data] = (objectCounter+1, delta)
        if(self.count%self.bucketSize==0):
            self.delete()

    def truncate(self, tempList,maxFrequentItems):
        sorted(tempList, key=cmp_to_key(self.compare))
        tempListReturn = [None]*min(maxFrequentItems,len(tempList))
        for i in range(0,min(maxFrequentItems,len(tempList))):
            tempListReturn[i] = tempList[i]
        return tempListReturn

    def compare(self, item1, item2):
        (key1, objectCounter1) = item1
        (key2, objectCounter2) = item2
        if objectCounter1 > objectCounter2:
            return 1

        if objectCounter2 > objectCounter1:
            return -1
        return 0

    def get(self,maxFrequentItems):
        count = 0
        tempList = []
        for key in self.buckets.keys():
            count = count + 1
            (objectCounter,delta) = self.buckets[key]
            tempList.append((key,objectCounter))
            if(count%1000==0):
                self.truncate(tempList,maxFrequentItems)
        return self.truncate(tempList,maxFrequentItems)

    def get_with_support(self,support):
       count = 0
       tempList = {}
       for key in self.buckets.keys():
           count = count + 1
           (objectCounter, delta) = self.buckets[key]
           if objectCounter>(support-self.errorLimit)*self.count:
               tempList[key] = objectCounter
       tempList = sorted(tempList.items(),key=operator.itemgetter(1),reverse=True)
       return tempList





