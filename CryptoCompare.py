from DataLoader import DataLoader
import pandas as pd
import datetime
import requests
import json
import pytz
from collections import defaultdict
from tqdm import tqdm
import pandas as pd

import sys, os, struct
import csv
from time import time
testdir = os.getcwd() + '/' + os.path.abspath('')
if testdir != "":
    testdir = testdir + '/'
sys.path.append(testdir + "..")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(''))))

import dxapi

class CryptoCompare(DataLoader):
    """
    This is a script to extract information from CryptoCompare to be loaded into timebase
    
    See CryptoCompare documentation for API reference: https://min-api.cryptocompare.com/documentation
    
    """
    def __init__(self, symbols):
        DataLoader.__init__(self, symbols)
        self.platform = 'CryptoCompare'
        self.symbols = symbols
        self.token = '16cdbb1cf4ea5a627019ee97ba48e0dec10d0143f171c34575731515927f85bf' 
        self.symbol_mapping = self.get_symbol_mapping()
        self.data = None
        
    def get_data(self, start_time=datetime.datetime.now() - datetime.timedelta(days=7), end_time=datetime.datetime.now()):
        dataframes=[]
        for symbol in self.symbols:
            try:
                daily_data = self.get_daily_data(symbol)
                dataframes.append(self.get_daily_data(symbol))
            except:
                #unable to get data for that symbol
                print(symbol + " has no data")
                continue
        self.data = pd.concat(dataframes)
        
    def get_symbol_mapping(self):
        """
        Access coin id for a symbol
        coin_data = get_coin_data()
        BTC_id = coin_data['Data']['BTC']['Id']
        """
        try:
            with open('cryptocompare/coinlist.json') as f:
                jsonn = json.load(f)
                print('load from file')
        except:
            url= f'https://min-api.cryptocompare.com/data/all/coinlist?api_key={self.token}'
            r = requests.get(url)
            jsonn = json.loads(r.text) 
        return jsonn

    def get_daily_data(self, symbol):
        """
        Ingest social media metrics (twitter, reddit, facebook, cryptocompare's own page views)
        on a daily frequency. Max limit is 2000 ~= 5 years, therefore one API call is currently (2022)
        enough to retrieve the whole history.
        
        input:
            symbol: String 'BTC', 'ETH'... stc
            
        output:
            pandas DataFrame, see https://min-api.cryptocompare.com/documentation?key=Social&cat=historicalDaySocialStats -> Response > Format for column names
            
        """
    
        coinId = self.symbol_mapping['Data'][symbol]['Id']
        url = f'https://min-api.cryptocompare.com/data/social/coin/histo/day?api_key={self.token}'
        payload = {'limit': 2000, 'coinId': coinId}
        r = requests.get(url, params=payload)
        jsonn = json.loads(r.text)
        data = jsonn['Data']
        result = []
        for datum in data:
            if sum(datum.values()) == datum['time']:#i.e. every value is 0 except time
                continue
            else:
                datum['symbol']=symbol
                datum['time']= pd.to_datetime(datum['time'], unit='s')
                result.append(datum)
        return pd.DataFrame(result)


    def load_data(self):
        """
        loads data from the get_data method into timebase
        
        """
        
        #save local copy before upload to timebase
        self.data.to_csv("cryptocompare.csv")
        
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
            streamKey = 'CryptoCompare'

            # Get stream from the timebase
            stream = db.getStream(streamKey)

            # Create a Message Loader for the selected stream and provide loading options
            loader = stream.createLoader(dxapi.LoadingOptions())

            # Create message
            betaMessage = dxapi.InstrumentMessage()

            # Define message type name according to the Timebase schema type name
            # For the polymorphic streams, each message should have defined typeName to distinct messages on Timebase Server level.
        #     onchainMessage.typeName = 'deltix.timebase.api.messages.universal.Onchain'
            betaMessage.typeName = 'deltix.timebase.api.universal.CryptoCompare'

            print('Start loading to ' + streamKey)
            print(self.data.columns[2:-1])
            for index, row in self.data.iterrows():
                # get current time in UTC
                symbol=row['symbol']
                betaMessage.symbol = symbol + "USDT"

                now = pd.to_datetime(row['time']) - datetime.datetime(1970, 1, 1)

                # Define message timestamp as Epoch time in nanoseconds 
                ns = now.total_seconds() * 1e9 + now.microseconds * 1000;
                betaMessage.timestamp = ns 
                
                #first and last column are 'time' and 'symbol' respectively
                for column in self.data.columns[2:-1]:
                    setattr(betaMessage,column, row[column])
                # Define other message properties
                betaMessage.instrumentType = 'CUSTOM'

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
            barsQQL = f"""CREATE DURABLE STREAM "Twitter" 'Twitter' (
                CLASS "deltix.timebase.api.universal.Twitter" 'Twitter'(
                    {new_line.join(['"'+col+'" '+ "'"+col+"' FLOAT DECIMAL" for col in self.data.columns[2:-1]])}
                );
            )
            OPTIONS (FIXEDTYPE; PERIODICITY = '1I'; HIGHAVAILABILITY = TRUE)
            COMMENT 'Stream to store Discord server data'
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
    tokens = 'ETH ZEC XEM WAVES SC XMR XLM BTC XRP DOGE BNT OMG ZEN STORJ CVC BAT IOTA ANT QTUM ZRX BTM LRC MANA KNC ADA ENJ THETA IOST ZIL MKR REN ONT RVN XTZ LPT BTT CRO ATOM MATIC RSR FTM SNX ALGO LUNA CHZ BAND SOL JST TRB SLP CELO UMA COMP FIL BAL WNXM YFI KSM YFII SRM CRV SAND SUSHI EGLD SWRV PERP GALA UNI AVAX FLM AAVE ALPHA NEAR AXS BADGER GRT LON 1INCH MIR MASK DORA CONV CFX SHIB XCH ICP CSPR MINA CQT AGLD DYDX YGG ENS NFT PEOPLE BICO IMX KISHU STARL TORN'.split(' ')
    CryptoCompareDataLoader = CryptoCompare(symbols=tokens)
    CryptoCompareDataLoader.get_data(start_time=datetime.datetime.now() - datetime.timedelta(days=1460))
    CryptoCompareDataLoader.create_database()
    CryptoCompareDataLoader.load_data()