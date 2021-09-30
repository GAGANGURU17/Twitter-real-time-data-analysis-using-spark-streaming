## Twitter-real-time-data-analysis-using-spark-streaming
# Project Description
For This Big-data Project, we took Twitter API for Real-Time Data Processing from https://developer.twitter.com/en/docs/tutorials/stream-tweets-in-real-time. Firstly we have to check that Zookeeper server is up and running ,and then check the Kafka server. Using Apache Kafka first we Created a topic in Jupyter notebook and produce the topic to subscribers with the help of producer and consumers consumes that particular topic. Then we created the DStreams to store the real time Twitter Data and process that Data using Dstreams operations.

# Technologies Used
Python 3.6
Kafka 2.8.0
Spark 2.4.4
Pyspark 2.4.8
Git/GitHub
kafka-python 2.0.2

# Requirements
One of the first requirements is to get access to the streaming data; in this case, real-time tweets. Twitter provides a very convenient API to fetch tweets in a streaming manner. In addition, I also used Kafka to buffer the tweets before processing. Kafka provides a distributed queuing service which can be used to store the data when the data creation rate is more than processing rate. It also has several other uses.

# Main Libraries
tweepy: interact with the Twitter Streaming API and create a live data streaming pipeline with Twitter
pyspark: preprocess the twitter data (Python's Spark library)

# Project Setup
# Installing Required Python Libraries I have provided a text file containing the required python packages: requirements.txt
To install all of these at once, simply run (only missing packages will be installed):

$ sudo pip install -r requirements.txt
Installing and Initializing Kafka Download and extract the latest binary from https://kafka.apache.org/downloads.html
# Start zookeeper service:
$ bin/zookeeper-server-start.sh config/zookeeper.properties
# Start kafka service:
$ bin/kafka-server-start.sh config/server.properties
# Create a topic named twitterstream in kafka:
$ bin/kafka-topics.sh --create --zookeeper --partitions 1 --topic twitterstream localhost:2181 --replication-factor 1
# Using the Twitter Streaming API In order to download the tweets from twitter streaming API and push them to kafka queue, I have created a python script app.py. The script will need your twitter authentication tokens (keys).
Once you have your authentication tokens, create or update the twitter-app-credentials.txt with these credentials.

# After updating the text file with your twitter keys, you can start downloading tweets from the twitter stream API and push them to the twitterstream topic in Kafka. Do this by running the script as follows:

$ python app.py
Note: This program should be kept running for collecting tweets.

# To check if the data is landing in Kafka:
$ bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic twitterstream --from-beginning
# Running the Stream Analysis Program:
$ SPARK_HOME/bin/spark-submit --packages org.apache.spark:spark-streaming-kafka_2.10:1.5.1 twitterStream.py
