import tweepy
from kafka import KafkaProducer
import json

# Twitter API credentials
consumer_key = "YOUR_CONSUMER_KEY"
consumer_secret = "YOUR_CONSUMER_SECRET"
access_token = "YOUR_ACCESS_TOKEN"
access_token_secret = "YOUR_ACCESS_TOKEN_SECRET"

# Kafka configuration
kafka_broker = "localhost:9092"
kafka_topic = "twitter_topic"

# Create Kafka producer
producer = KafkaProducer(bootstrap_servers=[kafka_broker],
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

# Twitter Stream Listener
class TwitterStreamListener(tweepy.StreamListener):
    def on_status(self, status):
        tweet = {
            'text': status.text,
            'user': status.user.screen_name,
            'created_at': str(status.created_at),
            'hashtags': [hashtag['text'] for hashtag in status.entities['hashtags']]
        }
        producer.send(kafka_topic, tweet)
        print(f"Tweet sent to Kafka: {tweet}")

    def on_error(self, status_code):
        if status_code == 420:
            return False

# Authenticate with Twitter
auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)

# Create and start the stream
stream_listener = TwitterStreamListener()
stream = tweepy.Stream(auth=auth, listener=stream_listener)
stream.filter(track=['python', 'java', 'scala', 'spark', 'kafka'])  # Add your desired tracking keywords
