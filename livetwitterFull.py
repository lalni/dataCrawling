#!/opt/conda/bin/python3

from multiprocessing.connection import wait
from socket import timeout
from numpy import result_type
from requests import get
import requests
from datetime import datetime, timedelta
import time
import re
import sys
import pandas as pd
import json
from tqdm import tqdm
import os
import pytz
from operator import itemgetter
from sentiment import Sentiment
import aiohttp
import asyncio
import tweepy
import oauthlib
import tweepy
import configparser
import oauth2
import urllib
import base64
import math
from collections import defaultdict



utc=pytz.UTC

testdir = os.getcwd() + '/' + os.path.abspath('')
os.chdir('/tmp/jupyterhub/admin/Data/SocialMedia')

if testdir != "":
    testdir = testdir + '/'
sys.path.append(testdir + "..")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(''))))

import dxapi

from DataLoader import DataLoader


class livetwitter(DataLoader):
    def __init__(self, symbols):
        DataLoader.__init__(self, symbols)
        self.api_keys = ["UNBX4FLnCCwu7xcwCwm1PdeHI","Mrg6xFZBOiG3AQpml1jlh51qb"]
        self.api_key_secrets = ["lvoDNOwc2xD6UEqsHrDV1rvW08QHp2HjIW6H2pPSHpDo2G8nKB","nlqFBbLJOY5U9iBMYiqE0sfE6dDV0SG5VfDYdkC7w0foWnZalz"]
        self.bearer_tokens = [self.get_bearer_token(self.api_keys[i],self.api_key_secrets[i]) for i in range(len(self.api_keys))]
        
        self.platform = "LiveTwitter"
        self.twitter_accounts = self.get_twitter_accounts()
        self.data = None
        self.symbols = symbols
        self.tweet_list = []
        self.sentiment = Sentiment()        
        self.bearer_token = self.bearer_tokens[1]
    
    
    def get_bearer_token(self, api_key, api_key_secret):
        OAUTH2_TOKEN = 'https://api.twitter.com/oauth2/token'

        # enconde consumer key
        consumer_key = urllib.parse.quote(api_key)
        # encode consumer secret
        consumer_secret = urllib.parse.quote(api_key_secret)
        # create bearer token
        bearer_token = consumer_key + ':' + consumer_secret
        # base64 encode the token
        base64_encoded_bearer_token = base64.b64encode(bearer_token.encode('utf-8'))
        # set headers
        headers = {
            "Authorization": "Basic " + base64_encoded_bearer_token.decode('utf-8') + "",
            "Content-Type": "application/x-www-form-urlencoded;charset=UTF-8",
            "Content-Length": "29"}

        response = requests.post(OAUTH2_TOKEN, headers=headers, data={'grant_type': 'client_credentials'})
        to_json = response.json()
        return to_json['access_token']

        
    def get_twitter_accounts(self):
        aggregate_df = pd.read_csv('twitter_accounts.csv')
        return aggregate_df
    
    
    async def fetch(self, session, url, params={}):
        """fetch url asynchronously"""
        data = None
        async with session.get(url, params=params) as response:
            if response.status != 200:
                text = await response.text()
                print("cannot retrieve %s: status: %d, reason: %s" % (url, response.status, text))
            else :
                data = await response.json()
        return data

    
    def get_timeline(self,user_id):
        timeline = []

        url = f'https://api.twitter.com/2/users/{user_id}/tweets'
        print(url)
        headers = {
            'Authorization':'Bearer ' + 'AAAAAAAAAAAAAAAAAAAAAJnVdwEAAAAAm716fHTzu7Le8M4kQxuJCiX4quo%3DjY1qVf9w6fKaceyKwZaIzdnGZXGOTTI4KmElJ45kVFuTysv5Q9'
        }
        more_pages=True
        pagination_token=None
        params={'max_results':100,'tweet.fields':'conversation_id','exclude':'retweets'}

        while more_pages:
            r = requests.get(
                url,headers=headers,params=params
            )
            response = json.loads(r.text)
            for tweet in response['data']:
                timeline.append(tweet)

            try:
                pagination_token = response['meta']['next_token']
            except:
                more_pages = False
                continue
            params['pagination_token'] =pagination_token

        return timeline

    
    
    async def get_timelines(self,user_ids):
        headers = {
                'Authorization':'Bearer ' + self.bearer_token
        }
        session = aiohttp.ClientSession(headers=headers)

        tasks = []
        batch = []

        for user_id in user_ids:
            batch.append( f'https://api.twitter.com/2/users/{user_id}/tweets')
        for url in batch:
            task = asyncio.create_task(self.fetch(session, url))
            tasks.append(task)
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        await session.close()
        return responses

    async def get_users_stats(self,user_ids):
        headers = {
                'Authorization':'Bearer ' + self.bearer_token
        }
        
        params = {
            'user.fields': 'public_metrics,created_at',
        }

        session = aiohttp.ClientSession(headers=headers)

        tasks = []
        batch = []

        for user_id in user_ids:
            batch.append( f'https://api.twitter.com/2/users/{user_id}/')
        for url in batch:
            task = asyncio.create_task(self.fetch(session, url,params=params))
            tasks.append(task)
            
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        await session.close()
        return responses

    def get_user_stats(self,user_id):
        url = f'https://api.twitter.com/2/users/{user_id}/'
        headers = {
            'Authorization':'Bearer ' + self.bearer_token
        }
        params = {
            'user.fields': 'public_metrics,created_at',
        }

        r = requests.get(
            url,headers=headers,params=params
        )
        return json.loads(r.text)
    
    
    async def get_tweets_lists(self,tweet_ids_lists):
        headers = {
                'Authorization':'Bearer ' + self.bearer_token
        }
        
        session = aiohttp.ClientSession(headers=headers)

        tasks = []

        #for tweet_id_list in tweet_ids_lists:
        for tweet_id_list in self.chunkify(tweet_ids_lists,math.ceil(len(tweet_ids_lists)/100)):
            params = {
                'tweet.fields': 'public_metrics,created_at',
                "ids": ",".join(tweet_id_list)
            }
            task = asyncio.create_task(self.fetch(session, 'https://api.twitter.com/2/tweets/',params=params))
            tasks.append(task)

        responses = await asyncio.gather(*tasks, return_exceptions=True)
        await session.close()
        return responses

    def get_tweets(self,tweet_id_list):
        url = 'https://api.twitter.com/2/tweets/'
        headers = {
            'Authorization':'Bearer ' + self.bearer_token
        }
        params = {
            'tweet.fields': 'public_metrics,created_at',
            "ids": ",".join(tweet_id_list)
        }

        r = requests.get(
            url,headers=headers,params=params
        )
        """
        temp['data'][0]['public_metrics']['retweet_count']
        temp['data'][0]['public_metrics']['reply_count']
        temp['data'][0]['public_metrics']['like_count']
        temp['data'][0]['text']
        temp['data'][0]['created_at']
        """
        return json.loads(r.text)

    def chunkify(self,lst, n):
        return [lst[i::n] for i in range(n)]
    
    def flatten(self, tweets_list):
        tweets = [] 
        for i in range(len(tweets_list)):
            iteration = tweets_list[i]['data']
            for tweet in iteration:
                tweets.append(tweet)
        return tweets
    
    
    def get_thread_count(self, timeline):
        
        thread_counts = {}
        thread_counts = defaultdict(lambda:1,thread_counts)

        current_conversation=''
        for tweet in timeline:
            if tweet['conversation_id'] == current_conversation:
                thread_counts[tweet['conversation_id']] = thread_counts.get(tweet['conversation_id'], 0) +1
            else:
                current_conversation=tweet['conversation_id']
        return thread_counts
    
    def get_data(self, start_time=datetime.datetime.now() - timedelta(days=7), end_time=datetime.datetime.now()):
        length = len(self.twitter_accounts) #Changed
        
        user_ids = [self.twitter_accounts[self.twitter_accounts['symbol']==symbol]['id'].values[0] for symbol in self.symbols]
        usernames = [self.twitter_accounts[self.twitter_accounts['symbol']==symbol]['username'].values[0] for symbol in self.symbols]
        #timelines = asyncio.run(self.get_timelines(user_ids))
        users_stats = asyncio.run(self.get_users_stats(user_ids))
        tweet_ids_lists=[]
#         for timeline in timelines:
#             tweet_ids_lists.append([tweet['id'] for tweet in timeline['data']])
            
        #tweets_list = asyncio.run(self.get_tweets_lists(tweet_ids_lists))
        
        for i in tqdm(range(len(self.symbols))):
            timeline = self.get_timeline(user_ids[i])
            thread_counts = self.get_thread_count(timeline)
            
            symbol = self.symbols[i]
            #timeline = timelines[i]
            user_id = user_ids[i]
            user_stats = users_stats[i]
            tweet_ids = [str(tweet['id']) for tweet in timeline]
            tweets_list = asyncio.run(self.get_tweets_lists(tweet_ids))
            #tweet_ids = tweet_ids_lists[i] # returns the ids of tweets.
            tweets = self.flatten(tweets_list)
            try:
                user = self.twitter_accounts[self.twitter_accounts['symbol']==symbol]['username'].values[0]
                assert(user_id== self.twitter_accounts[self.twitter_accounts['symbol']==symbol]['id'].values[0])
            except KeyError:
                print(symbol + " has no associated user_id")
                continue
            followers_count = user_stats['data']['public_metrics']['followers_count']
            tweets_count = user_stats['data']['public_metrics']['tweet_count']
            
            
            #speed_measurable_tweets = [tweet for tweet in tweets if tweet['public_metrics']['retweet_count'] < 100]
            #retweet_speeds = self.retweet_speeds(speed_measurable_tweets)

            curr_time = pd.Timestamp.now()
            print(tweets)
            for tweet in tweets:
                tweet_body = tweet['text']
                tweet_time =  tweet['created_at']
                public_metrics = tweet['public_metrics']
                retweet_count = public_metrics['retweet_count']
                thread_count = thread_counts[tweet['id']]
                
                if retweet_count > 100:
                    retweet_speed = -1
                else:
                    retweet_speed = -1 #retweet_speeds[tweet['id']]
                    
                reply_count = public_metrics['reply_count']
                like_count = public_metrics['like_count']
                
                temp_list = (user, symbol, reply_count, tweet_body, tweet_time, retweet_count, like_count, followers_count, tweets_count, curr_time, thread_count, retweet_speed)
                self.tweet_list.append(temp_list)
        self.data = self.tweet_list

    def is_reply(self,tweet_id):
        #is part of thread or reply to another person.
        tweet = self.api.get_status(tweet_id)
        return tweet.in_reply_to_status_id is not None
    
    
#     def get_retweets(self,tweet_id):
#         url = f'https://api.twitter.com/1.1/statuses/retweets/{tweet_id}.json'
#         headers = {
#             'Authorization':'Bearer ' + "AAAAAAAAAAAAAAAAAAAAAGI8dgEAAAAAIgsxhslQGKqXt76peJIJGLeRe2U%3D807FwgPOk3yGNBgD8z2KnCSVkCsUHoVKSBtrMRyp4KAkYiHxkV"
#         }
#         params={"count":100}

#         r = requests.get(
#             url,headers=headers,params=params
#         )
#         return json.loads(r.text)
    
    
    
    async def get_retweets(self,tweets):
        headers = {
                'Authorization':'Bearer ' + self.bearer_token
        }
        session = aiohttp.ClientSession(headers=headers)

        tasks = []
        params={"count":100}

        for tweet in tweets:
            task = asyncio.create_task(self.fetch(session, f'https://api.twitter.com/1.1/statuses/retweets/{tweet["id"]}.json', params))
            tasks.append(task)
        responses = await asyncio.gather(*tasks, return_exceptions=True)
        await session.close()
        return responses

    
    def retweet_speeds(self, tweets, minutes=10):
        """
        input: tweets list where each item is an object with created_at and id attributes
        output: dictionary of tweet_id to retweet_speed
        """
        
        retweets_lists = asyncio.run(self.get_retweets(tweets))
        speeds = []
        for i,retweets in enumerate(retweets_lists):
            speed = 0
            for retweet in retweets:
                if (pd.to_datetime(retweet['created_at']) - timedelta(minutes=minutes)) < pd.to_datetime(tweets[i]['created_at']):
                    speed+=1
            speeds.append(speed)

        return dict(zip([tweet['id'] for tweet in tweets],speeds))


        
    def load_data(self):
        
        #sort by tweet_time
        self.data = sorted(self.data, key=itemgetter(4))


        # Timebase URL specification, pattern is "dxtick://<host>:<port>"
        timebase = 'dxtick://timebase:8011'

        dateparse = lambda x: datetime.strptime(x, "%Y-%m-%d")

        try:
            # Create timebase connection
            db = dxapi.TickDb.createFromUrl(timebase)

            # Open in read-write mode
            db.open(False)

            print('Connected to ' + timebase)

            # Define name of the stream    
            streamKey = 'Twitter2'

            # Get stream from the timebase
            stream = db.getStream(streamKey)

            # Create a Message Loader for the selected stream and provide loading options
            loader = stream.createLoader(dxapi.LoadingOptions())

            # Create message
            betaMessage = dxapi.InstrumentMessage()

            # Define message type name according to the Timebase schema type name
            # For the polymorphic streams, each message should have defined typeName to distinct messages on Timebase Server level.
        #     onchainMessage.typeName = 'deltix.timebase.api.messages.universal.Onchain'
            betaMessage.typeName = 'deltix.timebase.api.universal.Twitter2'

            print('Start loading to ' + streamKey)
            #                temp_list = [user, id, symbol, reply_count, tweet_body, tweet_posted, retweet_count, like_count, followers_count, tweets_count, friends_count, curr_time, created_date, retweet_speed]

            for row in self.data:
                # get current time in UTC
                symbol=row[1]
                now = pd.to_datetime(row[4]).tz_convert(None) - datetime(1970, 1, 1)

                # Define message timestamp as Epoch time in nanoseconds 
                ns = now.total_seconds() * 1e9 + now.microseconds * 1000;
                betaMessage.symbol = symbol + "USDT"
                betaMessage.replies = float(row[2])
                betaMessage.retweets = float(row[5])
                betaMessage.likes = float(row[6])
                betaMessage.tweet = str(row[3])
                betaMessage.followers = float(row[7])
                betaMessage.thread_count = int(row[-2])
                betaMessage.number_of_tweets = float(row[8])
                betaMessage.observation_time = pd.Timestamp.now().timestamp()
                try:
                    betaMessage.sentiment = self.sentiment.predict(str(row[3]))[0]
                else:
                    betaMessage.sentiment = 0
                #retweet speed
                if row[-1] is None:
                    continue
                else:
                    betaMessage.retweet_speed = float(row[-1])

                betaMessage.instrumentType = 'CUSTOM'

                betaMessage.timestamp = ns 

                # Define other message properties

                # Send message
                loader.send(betaMessage)

            # close Message Loader
            loader.close()
            loader = None

        finally:
            # database connection should be closed anyway
            if db.isOpen():
                db.close()
                print("Connection " + timebase + " closed.")
                
    
    def create_database(self):
        """
        Create database, columns are all assumed to be FLOAT DECIMAL type, column names are generated from self.data dataframe
        """
        try:
            # Timebase URL specification, pattern is "dxtick://<host>:<port>"
            timebase = 'dxtick://timebase:8011'
            db = dxapi.TickDb.createFromUrl(timebase)
            db.open(False)
            new_line = ',\n'
            barsQQL = f"""CREATE DURABLE STREAM "Twitter2" 'Twitter2' (
                CLASS "deltix.timebase.api.universal.Twitter2" 'Twitter2'(
                    "replies" 'replies' FLOAT DECIMAL,
                    "retweets" 'retweets' FLOAT DECIMAL,
                    "likes" 'likes' FLOAT DECIMAL,
                    "tweet" 'tweet' VARCHAR,
                    "followers" 'followers' FLOAT DECIMAL,
                    "number_of_tweets" 'number_of_tweets' FLOAT DECIMAL,
                    "observation_time" 'observation_time' FLOAT DECIMAL,
                    "retweet_speed" 'retweet_speed' FLOAT DECIMAL,
                    "thread_count" 'thread_count' INTEGER,
                    "sentiment" 'sentiment' INTEGER
                    
                );
            )
            OPTIONS (FIXEDTYPE; PERIODICITY = '1I'; HIGHAVAILABILITY = TRUE)
            COMMENT 'Stream to store Twitter data'
            """

            cursor = db.executeQuery(barsQQL)
            try:
                if (cursor.next()):
                    message = cursor.getMessage()
                    print('Query result: ' + message.messageText)
            finally:
                if (cursor != None):
                    cursor.close()

        finally:  # database connection should be closed anyway
            if (db.isOpen()):
                db.close()
            print("Connection " + timebase + " closed.")


    
    
if __name__ == "__main__":
    tokens = 'BTC ETH ZEC XEM WAVES SC XMR XLM XRP DOGE BNT OMG ZEN STORJ CVC BAT IOTA ANT QTUM ZRX BTM LRC MANA KNC ADA ENJ THETA IOST ZIL MKR REN ONT RVN XTZ LPT BTT CRO ATOM MATIC RSR FTM SNX ALGO LUNA CHZ BAND SOL JST TRB SLP CELO UMA COMP FIL BAL WNXM YFI KSM SRM CRV SAND SUSHI EGLD SWRV PERP GALA UNI AVAX FLM AAVE ALPHA NEAR AXS BADGER GRT LON 1INCH MIR MASK DORA CONV CFX SHIB XCH ICP CSPR MINA CQT AGLD DYDX YGG ENS NFT PEOPLE BICO IMX KISHU STARL TORN'.split(' ')
    for token in tokens[1:]:
        object = livetwitter(symbols=[token]) 
        object.get_data()
        object.load_data()
