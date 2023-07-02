import datetime
import pandas as pd

class DataLoader:

    def __init__(self,  symbols):
        self.associated_words = {"BTC":['BTC','Bitcoin'], "ETH":["ETH","Ethereum"]}
        self.data = pd.DataFrame(columns=['time','symbol','platform','text','views'])
        self.platform = "dataloader"
        self.symbols =  symbols


    def get_data(self,start_time, end_time):
        """
        args:
            symbol : String to denote token 
            start_time: Datetime.Datetime denote start time for filtering social media
            end_time: Datetime.Datetime denote end time for filtering social media
        returns:
            None - data is loaded into self.data object
        """
        for symbol in self.symbols:
            for word in self.associated_words.get(symbol, [symbol]):
                """
                response = api.call(word, start_time, end_time)
                time = response.post_time
                text = response.text
                views = response.user.subscribers

                self.data.loc[len(self.data.index)] = [time, symbol, self.platform, text, views]
                """

                #call api
                pass
        
        return

    def load_data(self):
        """
        to be replaced by timeseries function
        """
        self.data.to_csv(self.platform+"_data.csv")

if __name__ == "__main__":
    dataLoader = DataLoader(symbols=['ETH','BTC'])
    dataLoader.get_data(start_time=datetime.datetime.now() - datetime.timedelta(days=7), end_time=datetime.datetime.now())
    dataLoader.load_data()
