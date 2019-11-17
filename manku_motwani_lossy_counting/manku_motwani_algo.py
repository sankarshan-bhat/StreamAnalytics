from functools import cmp_to_key
import math

class manku_motwani_algo:

    def __init__(self,errorLimit,maxFrequentItems):
        self.bucketSize = int((1/errorLimit))
        self.errorLimit = errorLimit
        self.currentBucket = 0
        self.buckets = {}
        self.maxFrequentItems = maxFrequentItems
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
        self.currentBucket = int(self.count/self.bucketSize + 1)
        if input_data not in self.buckets:
            self.currentBucket = int(self.count/self.bucketSize)
            bucket_object = (0,self.currentBucket-1)
            self.buckets[input_data] = bucket_object
        (objectCounter, delta) = self.buckets[input_data]
        self.buckets[input_data] = (objectCounter+1, delta)
        self.delete()

    def truncate(self, tempList):
        sorted(tempList, key=cmp_to_key(self.compare))
        tempListReturn = [None]*min(self.maxFrequentItems,len(tempList))
        for i in range(0,min(self.maxFrequentItems,len(tempList))):
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

    def get(self):
        count = 0
        tempList = []
        for key in self.buckets.keys():
            count = count + 1
            (objectCounter,delta) = self.buckets[key]
            tempList.append((key,objectCounter))
            if(count%1000==0):
                self.truncate(tempList)

        return self.truncate(tempList)

    def get_with_support(self,support):
       count = 0
       tempList = []
       for key in self.buckets.keys():
           count = count + 1
           (objectCounter, delta) = self.buckets[key]
           if objectCounter>(support-self.errorLimit)*self.count*0.01:
               tempList.append((key, objectCounter))
       return tempList





