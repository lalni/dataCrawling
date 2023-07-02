import requests
import json
from requests.structures import CaseInsensitiveDict
import pandas as pd 
import os,sys 
from tqdm import tqdm
import pytz
from datetime import datetime
from operator import itemgetter


testdir = os.getcwd() + '/' + os.path.abspath('')
os.chdir('/tmp/jupyterhub/admin/Data/SocialMedia')

if testdir != "":
    testdir = testdir + '/'
sys.path.append(testdir + "..")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(''))))

import dxapi


class RedditMentions:
    def __init__(self):
        self.platform = "RedditMentions"
        self.data =  [] 
        self.symbols = self.get_symbols()
        
    def get_symbols(self):

        # returns a list of coins available on reddit 
        url = 'https://redditcoins.app/api/subs_and_coins'
        response = requests.get(url) 
        json = response.json()
        return json['coins']
 
    def get_data(self, start_date, end_date, granularity):
        "start_date : 2021-01-01, end_date : 2022-02-01, granularity:(H|2H|6H|12H|D|W)"
        
        for i in tqdm(range(len(self.symbols[7:]))):
            coin = self.symbols[i]
            url = 'https://redditcoins.app/api/volume/cryptocurrency/{}?start={}&end={}&ups=0&submissions=true&comments=true&granularity={}'.format(coin,start_date, end_date, granularity)
            response = requests.get(url).json()["data"]
            for item in response: 
                time = item['time']
                volume = item['volume']
                self.data.append([coin, time, volume])

    
    def load_data(self):
        self.data = sorted(self.data, key=itemgetter(1))
        
        # Timebase URL specification, pattern is "dxtick://<host>:<port>"
        timebase = 'dxtick://timebase:8011'

        dateparse = lambda x: datetime.strptime(x, "%Y-%m-%d")
        try:
            db = dxapi.TickDb.createFromUrl(timebase)
            
            # Open in read-write mode
            db.open(False)

            print('Connected to ' + timebase)

            # Define name of the stream    
            streamKey = 'RedditMentions'

            # Get stream from the timebase
            stream = db.getStream(streamKey)

            # Create a Message Loader for the selected stream and provide loading options
            loader = stream.createLoader(dxapi.LoadingOptions())

            # Create message
            betaMessage = dxapi.InstrumentMessage()

            # Define message type name according to the Timebase schema type name
            # For the polymorphic streams, each message should have defined typeName to distinct messages on Timebase Server level.
        #     onchainMessage.typeName = 'deltix.timebase.api.messages.universal.Onchain'
            betaMessage.typeName = 'deltix.timebase.api.universal.RedditMentions'
            print('Start loading to ' + streamKey)
            
            for row in self.data:
                # get current time in UTC
                symbol=row[0]
                now = pd.to_datetime(row[1]) - datetime(1970, 1, 1)
                # Define message timestamp as Epoch time in nanoseconds 
                ns = now.total_seconds() * 1e9 + now.microseconds * 1000;
                betaMessage.symbol = symbol + "USDT"
                betaMessage.comments = row[2]

                betaMessage.instrumentType = 'CUSTOM'

                betaMessage.timestamp = ns 

                # Define other message properties lol,toxic,saved,comments


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
        Create database
        """
        try:
            # Timebase URL specification, pattern is "dxtick://<host>:<port>"
            timebase = 'dxtick://timebase:8011'
            db = dxapi.TickDb.createFromUrl(timebase)
            db.open(False)
            new_line = ',\n'
            barsQQL = f"""CREATE DURABLE STREAM "RedditMentions" 'RedditMentions' (
                CLASS "deltix.timebase.api.universal.RedditMentions" 'RedditMentions'(
                    "comments" 'comments' INTEGER

                );
            )
            OPTIONS (FIXEDTYPE; PERIODICITY = '1I'; HIGHAVAILABILITY = TRUE)
            COMMENT 'Stream to store RedditMentions data'
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
    redditMentions = RedditMentions()
    redditMentions.create_database()
    redditMentions.get_data("2021-04-30", "2022-06-23", "H")
    redditMentions.load_data()