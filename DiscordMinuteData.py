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
from operator import itemgetter

import sys, os, struct
import csv
from time import time

os.chdir('/tmp/jupyterhub/admin/Data/SocialMedia')

testdir = os.getcwd() + '/' + os.path.abspath('')
if testdir != "":
    testdir = testdir + '/'
sys.path.append(testdir + "..")
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(''))))

utc=pytz.UTC

import dxapi

class Discord(DataLoader):
    
    def __init__(self, symbols):
        DataLoader.__init__(self, symbols)
        self.platform = 'Discord'
        
        self.channels=  {"ETH":{"Guild":"714888181740339261"},
        "ZEC":{"Guild":"669694001464737815"},
        "XEM":{"Guild":"856325968096133191"},
        "WAVES" : {"Guild":"420933539375087617"},
        "SC" : {"Guild":"809849352516141067"},
        "XMR": {"Guild":"368922875329249314"},
        "XRP" :{"Guild":"886050993802985492"},
        "DOGE" : {"Guild":"163608137457336320"},
        "BNT": {"Guild":"476133894043729930"},
        "ZEN": {"Guild":"334085157441110017"},
        "CVC": {"Guild":"854862988157517834"},
        "IOTA": {"Guild":"397872799483428865"},
        "ANT": {"Guild":"672466989217873929"},
        "ZRX": {"Guild":"435912040142602260"},
        "BTM": {"Guild":"389607108926111744"},
        "LRC": {"Guild":"488848270525857792"},
        "MANA":{"Guild":"417796904760639509"},
        "KNC": {"Guild":"608934314960224276"},
        "ENJ": {"Guild":"783393889548501023"},
        "THETA": {"Guild":"808417775978676265"},
        "ZIL": {"Guild":"370992535725932544"},
        "REN": {"Guild":"559933846800564226"},
        "ONT": {"Guild":"400884201773334540"},
        "RVN": {"Guild":"429127343165145089"},
        "XTZ": {"Guild":"840231135694946324"},
        "LPT": {"Guild":"423160867534929930"},
        "CRO": {"Guild":"372888022452011008"},
        "ATOM": {"Guild":"669268347736686612"},
        "MATIC": {"Guild":"635865020172861441"}, 
        "RSR": {"Guild":"832015054609055774"}, 
        "FTM": {"Guild":"582931087005843466"},
        "SNX": {"Guild":"413890591840272394"},
        "ALGO":{"Guild":"491256308461207573"},
        "LUNA": {"Guild":"983359798059892766"},
        "BAND": {"Guild":"694023152795385948"},
        "SOL": {"Guild":"428295358100013066"}, 
        "TRB": {"Guild":"461602746336935936"},
        "SLP": {"Guild":"410537146672349205"},
        "CELO":{"Guild":"600834479145353243"},
        "UMA":{"Guild":"718590743446290492"},
        "COMP":{"Guild":"402910780124561410"},
        "BAL":{"Guild":"638460494168064021"},
        "WNXM":{"Guild":"496296560624140298"},
        "YFI":{"Guild":"734804446353031319"},
        "KSM":{"Guild":"771178421522268191"},
        "SRM":{"Guild":"739225212658122886"},
        "CRV":{"Guild":"729808684359876718"},
        "SAND":{"Guild":"497312527093334036"},
        "SUSHI":{"Guild":"748031363935895552"},
        "SWRV":{"Guild":"751609290002923580"},
        "PERP":{"Guild":"687397941383659579"},
        "GALA":{"Guild":"692403822265368626"},
        "UNI":{"Guild":"597638925346930701"},
        "FLM":{"Guild":"748375438467793036"},
        "AAVE":{"Guild":"602826299974877205"},
        "ALPHA":{"Guild":"758322648215322625"},
        "NEAR":{"Guild":"490367152054992913"},
        "AXS":{"Guild":"410537146672349205"},
        "BADGER":{"Guild":"743271185751474307"},
        "GRT":{"Guild":"438038660412342282"},
        "LON":{"Guild":"749875871535595550"},
        "1INCH":{"Guild":"730042542229422121"},
        "MIR":{"Guild":"784009721447317504"},
        "MASK":{"Guild":"757597809993056387"},
        "CONV":{"Guild":"836788116030357544"},
        "CFX":{"Guild":"707952293412339843"},
        "SHIB":{"Guild":"740287152843128944"},
        "ICP":{"Guild":"748416164832608337"},
        "CSPR":{"Guild":"615596155992145953"},
        "MINA":{"Guild":"484437221055922177"},
        "CQT":{"Guild":"715804406842392586"},
        "AGLD":{"Guild":"880899217973968917"},
        "DYDX":{"Guild":"724804754382782534"},
        "YGG":{"Guild":"768039848094334987"},
        "ENS":{"Guild":"742384562646286509"},
        "NFT":{"Guild":"814206710524280832"},
        "IMX":{"Guild":"765480457256042496"},
        "STARL":{"Guild":"873247646268157972"},
        "TORN":{"Guild":"791603868696969276"}}


                        
                        
                        
        self.token = 'OTcwNjE3NzIyMjk3NzgyMzEz.Ym-lCw.CKNP2Cf2gO6qlCCo_yftlceG7og' 
        self.latest_posts = {}

        self.server_data = pd.DataFrame(columns=['time','symbol','subscribers','active','num_messages','num_announcements','channels'])
        self.server_data = []
        #self.data = pd.DataFrame(columns=['time','symbol','platform','text','views'])

    def get_data(self, start_time=datetime.datetime.now() - datetime.timedelta(days=7), end_time=datetime.datetime.now()):
        for i in tqdm(range(len(self.symbols))):
            symbol = self.symbols[i]
            if symbol not in self.channels.keys():
                continue

            self.get_server_statistics(symbol)

            author_counts = {}
            author_counts = defaultdict(lambda:[], author_counts)
            message_counts = {}
            message_counts = defaultdict(lambda:0,message_counts)
            announcement_counts = {}
            announcement_counts = defaultdict(lambda:0,message_counts)

            for i in tqdm(range(len(self.channels[symbol]['Channels']))):
                channel = self.channels[symbol]['Channels'][i]
                more_messages=True
                while more_messages:
                    messages = self.retrieve_messages(channel)
 
                    more_messages = len(messages)>1 and pd.to_datetime(messages[-1]['timestamp']) > utc.localize(start_time)
                    for message in messages:
                        time = pd.to_datetime(message['timestamp'])
                
                        #Skip todays date as results will only be partial
                        author = message['author']['username']
                        time = pd.to_datetime(message['timestamp'])
                        date = time.ceil('min')
                        if channel == self.channels[symbol].get('announcement_channel', None):
                            announcement_counts[date]=announcement_counts[date]+1
                        message_counts[date]=message_counts[date]+1
                        author_counts[date].append(author)
                        text = message['content']
            
            number_of_channels = len(self.channels[symbol]['Channels'])

            for time in author_counts.keys():
                self.server_data.append([time,symbol,self.channels[symbol]['subscribers'], len(set(author_counts[time])), message_counts[time], announcement_counts[time], number_of_channels])



    def get_server_statistics(self,symbol):
        """
        self.server_data = pd.Dataframe(columns=['time','symbol','subscribers','active','num_messages','channels'])
        """
        headers = {
            'authorization': self.token
        }
        serverid = self.channels[symbol]['Guild']
        r = requests.get(
            f'https://discord.com/api/v9/guilds/{serverid}?with_counts=true',headers=headers
        )
        try:
            jsonn = json.loads(r.text) 
            result = {'subscribers':jsonn['approximate_member_count'],
                'active':jsonn['approximate_presence_count'],
                'date':datetime.datetime.now().date()}
            self.channels[symbol]['subscribers'] = result['subscribers']
            self.channels[symbol]['active'] = result['active']

        except:
            self.channels[symbol]['subscribers'] = 0
            self.channels[symbol]['active'] = 0
            print('ERROR' + r.text)
            
        r = requests.get(f'https://discord.com/api/v9/guilds/{serverid}/channels',headers=headers)

        self.channels[symbol]['Channels']=[]
        try:
            channels = json.loads(r.text)
            for channel in channels:
                self.channels[symbol]['Channels'].append(channel['id'])
                if 'announcement' in channel['name'].lower():
                    self.channels[symbol]['announcement_channel'] = channel['id']
        except:
            print("ERROR-CHANNEL "+r.text)

    def retrieve_messages(self,channelid):
        headers = {
            'authorization': self.token
        }
        url = f'https://discord.com/api/v9/channels/{channelid}/messages?limit=100'
        if self.latest_posts.get(channelid,None) is None:
            new_url=url
        else:
            new_url = url + f'&before={self.latest_posts[channelid]}'

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

    def load_data(self):

#         self.server_data = pd.read_csv("server_data.csv")
        #self.server_data['time'] = pd.to_datetime(self.server_data['time'])
        
        self.server_data = sorted(self.server_data, key=itemgetter(0))
        
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
            streamKey = 'DiscordMinute'

            # Get stream from the timebase
            stream = db.getStream(streamKey)

            # Create a Message Loader for the selected stream and provide loading options
            loader = stream.createLoader(dxapi.LoadingOptions())

            # Create message
            betaMessage = dxapi.InstrumentMessage()

            # Define message type name according to the Timebase schema type name
            # For the polymorphic streams, each message should have defined typeName to distinct messages on Timebase Server level.
        #     onchainMessage.typeName = 'deltix.timebase.api.messages.universal.Onchain'
            betaMessage.typeName = 'deltix.timebase.api.universal.DiscordMinute'

            print('Start loading to ' + streamKey)

            for row in self.server_data:
                # get current time in UTC
                symbol=row[1]

                now = row[0] - utc.localize(pd.to_datetime('01/01/1970'))

                # Define message timestamp as Epoch time in nanoseconds 
                ns = now.total_seconds() * 1e9 + now.microseconds * 1000;
                betaMessage.symbol = symbol + "USDT"
                betaMessage.subscribers = float(row[2])
                betaMessage.active = float(row[3])
                betaMessage.messages = float(row[4])
                betaMessage.announcements = float(row[5])
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
            barsQQL = f"""CREATE DURABLE STREAM "DiscordMinute" 'DiscordMinute' (
                CLASS "deltix.timebase.api.universal.DiscordMinute" 'DiscordMinute'(
                    "subscribers" 'subscribers' FLOAT DECIMAL,
                    "active" 'active' FLOAT DECIMAL,
                    "messages" 'messages' FLOAT DECIMAL,
                    "announcements" 'announcements' FLOAT DECIMAL
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
            
        DiscordDataLoader = Discord(symbols=[token])
        if token == 'BTC':
            DiscordDataLoader.create_database()
        DiscordDataLoader.get_data(start_time=datetime.datetime.now() - datetime.timedelta(days=1460))
        DiscordDataLoader.load_data()
