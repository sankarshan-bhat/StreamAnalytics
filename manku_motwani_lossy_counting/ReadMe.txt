How to Run Manku_Motwani_lossy_counting

In Mac

1. Start Zookeeper
    a.) Go into Kafka installation Directory
    b.) run the command 'bin/zookeeper-server-start.sh config/zookeeper.properties'

2. Start Kafka
    a.) Go into kafka installation Directory
    b.) run the command 'bin/kafka-server-start.sh config/server.properties'

3. Run the file 'DataExtractor.py' to get data from twitter stream and store it in kafka

4. Run the program manku_motwani_tweet_consumer to read the data from kafka as a consumer and produce results as different thread.

