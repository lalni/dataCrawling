#!/opt/conda/bin/python3


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
import unicodedata
from sentiment import Sentiment

os.chdir('/tmp/jupyterhub/admin/Data/SocialMedia')

testdir = os.getcwd() + '/' + os.path.abspath('')
if testdir != "":
    testdir = testdir + '/'
sys.path.append(testdir + "..")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(''))))

utc=pytz.UTC

import dxapi

class DiscordAnnouncements(DataLoader):
    
    def __init__(self, symbols):
        DataLoader.__init__(self, symbols)
        self.platform = 'DiscordAnnouncements'
        
        self.channels = {"ETH":{"Guild":"714888181740339261", "Announcements":"887696410567385138"}, 
        "ZEC":{"Guild":"669694001464737815", "Announcements": ""},
        "XEM":{"Guild":"856325968096133191", "Announcements": "884290675221364746"},
        "WAVES" : {"Guild":"420933539375087617","Announcements": "420938904120721410"},
        "SC" : {"Guild":"809849352516141067","Announcements": "809858289164484648"},
        "XMR": {"Guild":"368922875329249314","Announcements": "810584106635231312"},
        "XRP" :{"Guild":"886050993802985492","Announcements": ""},
        "DOGE" : {"Guild":"163608137457336320","Announcements": "169088443073298432"},
        "BNT": {"Guild":"476133894043729930","Announcements": "757952055905353850"},
        "ZEN": {"Guild":"334085157441110017","Announcements": "374594909329686529"},
        "CVC": {"Guild":"854862988157517834","Announcements": "854863348280590336"},
        "IOTA": {"Guild":"397872799483428865","Announcements": "398069502060789761"},
        "ANT": {"Guild":"672466989217873929","Announcements": "674689982279057418"},
        "ZRX": {"Guild":"435912040142602260","Announcements": "649318558701977624"},
        "BTM": {"Guild":"389607108926111744","Announcements": "389607108926111746"},
        "LRC": {"Guild":"488848270525857792","Announcements": "684546507336056840"},
        "MANA":{"Guild":"417796904760639509","Announcements": "433376431603580970"},
        "KNC": {"Guild":"608934314960224276","Announcements": "684611297526153226"},
        "ENJ": {"Guild":"783393889548501023","Announcements": "783393941117861958"},
        "THETA": {"Guild":"808417775978676265","Announcements": "808438938448166982"},
        "ZIL": {"Guild":"370992535725932544","Announcements": "610654674319835137"},
        "REN": {"Guild":"559933846800564226","Announcements": "559938040840060930"},
        "ONT": {"Guild":"400884201773334540","Announcements": ""},
        "RVN": {"Guild":"429127343165145089","Announcements": "429134388882833408"},
        "XTZ": {"Guild":"840231135694946324","Announcements": ""},
        "LPT": {"Guild":"423160867534929930","Announcements": "428351836609576972"},
        "CRO": {"Guild":"372888022452011008","Announcements": "940705475165577226"},
        "ATOM": {"Guild":"669268347736686612","Announcements": "669488112807706625"},
        "MATIC": {"Guild":"635865020172861441","Announcements": "648889327530672128"}, 
        "RSR": {"Guild":"832015054609055774","Announcements": "920041059566882859"}, 
        "FTM": {"Guild":"582931087005843466","Announcements": "582931498865393686"},
        "SNX": {"Guild":"413890591840272394","Announcements": "479848656460316676"},
        "ALGO":{"Guild":"491256308461207573","Announcements": "491322846761451550"},
        "LUNA": {"Guild":"983359798059892766","Announcements": "983359798361854027"},
        "BAND": {"Guild":"694023152795385948","Announcements": "699847780126031882"},
        "SOL": {"Guild":"428295358100013066","Announcements": "510137859995598863"}, 
        "TRB": {"Guild":"461602746336935936","Announcements": "953404140942405692"},
        "SLP": {"Guild":"410537146672349205","Announcements": "414758518101639188"},
        "CELO":{"Guild":"600834479145353243","Announcements": "600841646632075274"},
        "UMA":{"Guild":"718590743446290492","Announcements": "971001705602887710"},
        "COMP":{"Guild":"402910780124561410","Announcements": "402911280475799552"},
        "BAL":{"Guild":"638460494168064021","Announcements": ""},
        "WNXM":{"Guild":"496296560624140298","Announcements": "496314334222352386"},
        "YFI":{"Guild":"734804446353031319","Announcements": "735617936206594249"},
        "KSM":{"Guild":"771178421522268191","Announcements": "771178422889087013"},
        "SRM":{"Guild":"739225212658122886","Announcements": "884572841285156884"},
        "CRV":{"Guild":"729808684359876718","Announcements": "729810461888872509"},
        "SAND":{"Guild":"497312527093334036","Announcements": "497327985234411520"},
        "SUSHI":{"Guild":"748031363935895552","Announcements": "748478893929005066"},
        "SWRV":{"Guild":"751609290002923580","Announcements": "751609997422493819"},
        "PERP":{"Guild":"687397941383659579","Announcements": "739471229706043478"},
        "GALA":{"Guild":"692403822265368626","Announcements": "694726186638770236"},
        "UNI":{"Guild":"597638925346930701","Announcements": "673373498290470932"},
        "FLM":{"Guild":"748375438467793036","Announcements": "755445263077867670"},
        "AAVE":{"Guild":"602826299974877205","Announcements": "619170408352055306"},
        "ALPHA":{"Guild":"758322648215322625","Announcements": "758323596694257684"},
        "NEAR":{"Guild":"490367152054992913","Announcements": "494277489779277854"},
        "AXS":{"Guild":"410537146672349205","Announcements": "414758518101639188"},
        "BADGER":{"Guild":"743271185751474307","Announcements": "903050920970051604"},
        "GRT":{"Guild":"438038660412342282","Announcements": "438070074700464128"},
        "LON":{"Guild":"749875871535595550","Announcements": "751297827560488991"},
        "1INCH":{"Guild":"730042542229422121","Announcements": "730290710120759296"},
        "MIR":{"Guild":"784009721447317504","Announcements": ""},
        "MASK":{"Guild":"757597809993056387","Announcements": "757599901327818813"},
        "CONV":{"Guild":"836788116030357544","Announcements": "837141775909912586"},
        "CFX":{"Guild":"707952293412339843","Announcements": "707952293856673883"},
        "SHIB":{"Guild":"740287152843128944","Announcements": "861879016172290048"},
        "ICP":{"Guild":"748416164832608337","Announcements": "835862464193101834"},
        "CSPR":{"Guild":"615596155992145953","Announcements": "904041376755376208"},
        "MINA":{"Guild":"484437221055922177","Announcements": "484437221055922177"},
        "CQT":{"Guild":"715804406842392586","Announcements": "938495226174120017"},
        "AGLD":{"Guild":"880899217973968917","Announcements": "882430298871169124"},
        "DYDX":{"Guild":"724804754382782534","Announcements": "724805710411726868"},
        "YGG":{"Guild":"768039848094334987","Announcements": "768039848094334987"},
        "ENS":{"Guild":"742384562646286509","Announcements": "895131054858461224"},
        "NFT":{"Guild":"814206710524280832","Announcements": "814237077045772328"},
        "IMX":{"Guild":"765480457256042496","Announcements": "765871633990287390"},
        "STARL":{"Guild":"873247646268157972","Announcements": "886170560369926154"},
        "TORN":{"Guild":"791603868696969276","Announcements": ""}}
                        
                        
        self.token = 'OTcwNjE3NzIyMjk3NzgyMzEz.Ym-lCw.CKNP2Cf2gO6qlCCo_yftlceG7og' 
        self.latest_posts = {}
        self.sentiment = Sentiment()        

        self.data = []
        
    def get_emoji_name(self, emoji):
        try:
            return unicodedata.name(emoji)
        except:
            return emoji
    
    def get_data(self, start_time=datetime.datetime.now() - datetime.timedelta(days=7), end_time=datetime.datetime.now()):
        for i in tqdm(range(len(self.symbols))):
            symbol = self.symbols[i]
            if symbol not in self.channels.keys():
                continue
            
            channel = self.channels[symbol]['Announcements']
            more_messages=True
            while more_messages:
                messages = self.retrieve_messages(channel)

                more_messages = len(messages)>1 and pd.to_datetime(messages[-1]['timestamp']) > utc.localize(start_time)
                for message in messages:
                    information = {}

                    information['time'] = pd.to_datetime(message['timestamp'])
                    information['symbol'] = symbol
                    information['text']= message['content']
                    information['notify_all'] = message['mention_everyone']
                    try:
                        information['sentiment'] = self.sentiment.predict(information['text'])[0]
                    except:
                        information['sentiment'] = 0

                    try:
                        information['reactions'] = message['reactions'][0]['count']
                        information['topreaction'] = self.get_emoji_name(message['reactions'][0]['emoji']['name'])
                    except KeyError:
                        information['reactions']=0
                        information['topreaction'] = ''

                    #prepend data
                    self.data.insert(0, information)

    def retrieve_messages(self,channelid):
        headers = {
            'authorization': self.token
        }
        url = f'https://discord.com/api/v9/channels/{channelid}/messages?limit=100'
        if self.latest_posts.get(channelid,None) is None:
            new_url=url
        else:
            new_url = url + f'&before={self.latest_posts[channelid]}'
        print(new_url)

        r = requests.get(
            new_url,headers=headers
        )
        try:
            jsonn = json.loads(r.text)
            self.latest_posts[channelid] = jsonn[-1]['id']

            return jsonn
        except:
            print(channelid, r.text)
            return []

        
    def get_emoji_name(self,emoji):
        try:
            return unicodedata.name(emoji)
        except:
            return emoji

        
    def load_data(self):
        
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
            streamKey = 'DiscordAnnouncements'

            # Get stream from the timebase
            stream = db.getStream(streamKey)

            # Create a Message Loader for the selected stream and provide loading options
            loader = stream.createLoader(dxapi.LoadingOptions())

            # Create message
            betaMessage = dxapi.InstrumentMessage()

            # Define message type name according to the Timebase schema type name
            # For the polymorphic streams, each message should have defined typeName to distinct messages on Timebase Server level.
        #     onchainMessage.typeName = 'deltix.timebase.api.messages.universal.Onchain'
            betaMessage.typeName = 'deltix.timebase.api.universal.DiscordAnnouncements'

            print('Start loading to ' + streamKey)

            for row in self.data:
                # get current time in UTC
                symbol=row['symbol']

                now = pd.to_datetime(row['time']) - utc.localize(pd.to_datetime('01/01/1970'))

                # Define message timestamp as Epoch time in nanoseconds 
                ns = now.total_seconds() * 1e9 + now.microseconds * 1000;
                betaMessage.symbol = symbol + "USDT"
                betaMessage.text = row['text']
                betaMessage.notify_all = row['notify_all']
                betaMessage.sentiment = row['sentiment']
                betaMessage.reactions = row['reactions']
                betaMessage.topreaction = row['topreaction']
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
        try:
            import dxapi
            # Timebase URL specification, pattern is "dxtick://<host>:<port>"
            timebase = 'dxtick://timebase:8011'
            db = dxapi.TickDb.createFromUrl(timebase)
            db.open(False)
            barsQQL = f"""CREATE DURABLE STREAM "DiscordAnnouncements" 'DiscordAnnouncements' (
                CLASS "deltix.timebase.api.universal.DiscordAnnouncements" 'DiscordAnnouncements'(
                    "text" 'text' VARCHAR,
                    "reactions" 'reactions' INTEGER,
                    "topreaction" 'topreaction' VARCHAR,
                    "sentiment" 'sentiment' INTEGER,
                    "notify_all" 'notify_all' BOOLEAN
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
    
    tokens = """BTC ETH ZEC XEM WAVES SC XMR XLM XRP DOGE BNT OMG ZEN STORJ CVC BAT IOTA ANT QTUM ZRX BTM LRC MANA KNC ADA ENJ THETA IOST ZIL MKR REN ONT RVN XTZ LPT BTT CRO ATOM MATIC RSR FTM SNX ALGO LUNA CHZ BAND SOL JST TRB SLP CELO UMA COMP FIL BAL WNXM YFI KSM YFII SRM CRV SAND SUSHI EGLD SWRV PERP GALA UNI AVAX FLM AAVE ALPHA NEAR AXS BADGER GRT LON 1INCH MIR MASK DORA CONV CFX SHIB XCH ICP CSPR MINA CQT AGLD DYDX YGG ENS NFT PEOPLE BICO IMX KISHU STARL TORN""".split(" ")
    for i in tqdm(range(len(tokens))):
        token = tokens[i]
        print(token)
        try:
            discordAnnouncements = DiscordAnnouncements(symbols=[token])
            discordAnnouncements.create_database()
            discordAnnouncements.get_data(start_time=datetime.datetime.now() - datetime.timedelta(days=1460))
            discordAnnouncements.load_data()
        except Exception as e:
            print(token + ' exception: ' + str(e))
            pass
