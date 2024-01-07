import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs


def run_twitter_etl():
    access_key = "XLDknoo82F8r4h5e1NbZcNIGp"
    access_secret = "LhxLoXJOy3MWnKq9WxvolxfomYiIcmVoCFkFtwBzMmRnHbmmys"
    consumer_key = "1467184744039395339-B0z0dDGnuAO6AYg9j17U171xQotG0W"
    consumer_secret = "sU63ZR1dS4wzB9LFQGPHbHJVjEulBpvJm2m03jdQ1U0Da"

    # The first step is to create a connection between the code and the twitter API
    # The code below will create the connection between API and twitter

    # Twitter authentication
    auth = tweepy.OAuthHandler(access_key, access_secret)
    auth.set_access_token(consumer_key, consumer_secret)

    # # # Creating an API object
    # By creating this object enables us to have access to the function in tweepy
    api = tweepy.API(auth)
    # If you want to get particular information from a specific user we use user_timeline
    tweets = api.user_timeline(screen_name='@WorldBank',
                               # 200 is the maximum allowed count
                               count=200,
                               include_rts=False,
                               # Necessary to keep full_text
                               # otherwise only the first 140 words are extracted
                               tweet_mode='extended'
                               )


    list = []
    for tweet in tweets:
        text = tweet.json["full_text"]

        refined_tweet = {"user": tweet.user.screen_name,
                         'text': text,
                         'favorite_count': tweet.favorite_count,
                         'retweet_count': tweet.retweet_count,
                         'created_at': tweet.created_at}

        list.append(refined_tweet)

    df = pd.DataFrame(list)
    df.to_csv('WorldBank_data_tweets.csv')
