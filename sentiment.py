from transformers import pipeline


class Sentiment:
    def __init__(self):
        self.classifier = pipeline("text-classification", model="ProsusAI/finbert")
        self.labels = {'negative':-1,"neutral":0,"positive":1}
        
    def predict(self,sentences):
        """
        inputs:
            sentences, list of strings to interpret sentiment for.
        output:
            list of dictionaries which return best prediction with confidence score
            e.g.
                    [{'label': 'neutral', 'score': 0.8891165852546692}, {'label': 'positive', 'score': 0.6572639346122742}]

        """
        #shorten input sequence (maximum 512 characters)
        return [self.labels[label['label']] for label in self.classifier(sentences)]


    
if __name__ == "__main__":
    #Example usage below
    
    sentiment = Sentiment()
    custom_tweets = ["""I see that some people are worried or anxious about the #Bitcoin market price.

    My advice: stop looking at the graph and enjoy life. If you invested in #BTC your investment is safe and its value will immensely grow after the bear market.

    Patience is the key.""","looks like a good buy."]
    
    print(sentiment.predict(custom_tweets))
