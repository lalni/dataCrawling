from DataLoader import DataLoader
import pandas as pd
import numpy as np
import datetime

BTC_tweet = pd.read_csv('BTC_tweet.csv')
 
 
class Twitter(DataLoader):
	def __init__(self, symbols):
		DataLoader.__init__(self, symbols)
		self.platform = 'Twitter'
	def get_data(self, start_time=datetime.datetime.now() - datetime.timedelta(days=7), end_time=datetime.datetime.now()):
		for symbol in self.symbols:
			for word in self.associated_words.get(symbol, [symbol]):
				contains = BTC_tweet['text'].str.contains(word, na=False, case=False)
				BTC_tweet.insert(1,'contains',contains) 
				indices = np.where(BTC_tweet['contains'])
				for b in indices:
					dict = {}
					dict.update({'time': BTC_tweet['date'][b]})
					dict.update({'symbol': symbol})
					dict.update({'platform': self.platform})
					dict.update({'text': BTC_tweet['text'][b]})
					dict.update({'views' : BTC_tweet['user_followers'][b]})
					self.data = pd.concat([self.data, pd.DataFrame(dict)])
				BTC_tweet.__delitem__('contains')
				print(len(self.data))
		self.data.drop_duplicates()
		 
if __name__ == "__main__":
	TwitterDataLoader = Twitter(symbols=['BTC', 'ETH'])
	TwitterDataLoader.get_data()
	TwitterDataLoader.load_data()
