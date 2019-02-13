
import nltk 
import math
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
import sys
import requests
nltk.download ('vader_lexicon')

list_all = ['#cpc','#lpc',"#ndp", "#pcpo", "#ondp","#democrats", "#republican","#bccp", " #olp", "#pq",
         '#apple','#microsoft', '#google',"#ibm","#tesla", "#target","#walmart","#aon","#salesforce", "#at&t",
         '#cnn','#cbc','#cp24','#nbc','#hbo','#hgtv', "#ctv", "#globaltv","#historychannel","#discoverychannel",
         '#katyperry',"#justinbieber", "#rihanna", '#taylorswift','#ladygaga', '#jtimberlake','#shakira',"#selenagomez","#demilovato","#britneyspears",
         '#nytimes','#wsj','#latimes','#usatoday','#denverpost','#thestar',"#newsday","#theglobeandmail","#nationalpost",'#torontosun']

pol_list = [ '#cpc','#lpc',"#ndp", "#pcpo", "#ondp","#democrats", "#republican","#bccp", " #olp", "#pq"]
com_list = [ '#apple','#microsoft', '#google',"#ibm","#tesla", "#target","#walmart","#aon","#salesforce", "#at&t"]
tv_list =  [ '#cnn','#cbc','#cp24','#nbc','#hbo','#hgtv', "#ctv", "#globaltv","#historychannel","#discoverychannel" ]
art_list = ['#katyperry',"#justinbieber", "#rihanna", '#taylorswift','#ladygaga', '#jtimberlake','#shakira',"#selenagomez","#demilovato","#britneyspears"]
news_list=  ['#nytimes','#wsj','#latimes','#usatoday','#denverpost','#thestar',"#newsday","#theglobeandmail","#nationalpost",'#torontosun']
# create spark configuration
conf = SparkConf()
conf.setAppName("TwitterStreamApp")
# create spark context with the above configuration
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
# create the Streaming Context from spark context, interval size 2 seconds
ssc = StreamingContext(sc, 2)
# setting a checkpoint for RDD recovery (necessary for updateStateByKey)
ssc.checkpoint("checkpoint_TwitterApp")
# read data from port 9009
dataStream = ssc.socketTextStream("twitter",9009)


sia  = SIA()
# map each hashtag to be a pair of (hashtag,1)

def tag_finder (x):
    for tag in list_all:
        if tag in x:
            return tag

# This will do sentiment analysis on the line which has the tags listed above
def filter_func(x):
    if any (tag in x for tag in list_all): # if the hashtag in the word , then do sentimental analysis
        return (tag_finder (x), sia.polarity_scores(x))
    else:
        x="" # if the word does not contain hastag, then create tag and empty dictionary to make the map fuction works
        return ("#empty", sia.polarity_scores(x))
words = dataStream.map(filter_func)


# assigning new key with max value based  sentimental analysis values of hashtag
def aggregate_tags_count(new_values, total):
    result=dict();
    #The keys of hastag are new , then compare the values of keys to find the max and assign it as new key
    if total is None or len(total)==0 and len(new_values)>=1:
        result['compound'] = max_dict(new_values,'compound')
        result['neg'] = max_dict(new_values,'neg')
        result['pos'] = max_dict(new_values,'pos')
        result['neu'] = max_dict(new_values,'neu')
    #The keys of hastag is already present in total, then compare the new key values and the old key values, find the max values and assign it as key
    if total is not None and len(total)==4  and len(new_values)>=1:
       # print (' I am at this 2')
        result['compound'] = max(max_dict(new_values,'compound'),total.get('compound'))
        result['neg'] = max(max_dict(new_values,'neg'),total.get('neg'))
        result['pos'] = max(max_dict(new_values,'pos'),total.get('pos'))
        result['neu'] = max(max_dict(new_values,'neu'),total.get('neu'))
#  The keys of hastag is already present in total and no new keys, then preserve the old key values
    if total is not None and len(total)==4  and  not new_values:
        result['compound'] = total.get('compound')
        result['neg'] = total.get('neg')
        result['pos'] = total.get('pos')
        result['neu'] = total.get('neu')
    

    return result

#find max in collection of dict ( since sia.polarity_scores(x) return dictionary we have to do this to compare values )
def max_dict(new_values, key):
    max_num = 0.00000
    for i in range(len(new_values)):
        max_num = max(max_num, new_values[i].get(key))
    return max_num


# do the aggregation, note that now this is a sequence of RDDs
hashtag_totals = words.updateStateByKey(aggregate_tags_count)

# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    try:
        # sort the results alphabtically
        sorted_rdd = rdd.sortBy(lambda x:x[0], True)
        # transform rdd to list
        list = sorted_rdd.collect()
   
        # open output file
        with open("storage.txt", "w") as file:
            # we only consider an hashtag value if it is EXACTLY like we wanted it
            # and then we write to output file
            for elem in list:
                    if elem[0] is not "#empty":
                        if elem[0] in pol_list:
                            file.write('Politics '+ elem[0] + ' ' + str(elem[1]) + '\n')
                        if elem[0] in com_list:
                            file.write('Companies '+ elem[0] + ' ' + str(elem[1]) + '\n')
                        if elem[0] in tv_list:
                            file.write('Televisions '+ elem[0] + ' ' + str(elem[1]) + '\n')
                        if elem[0] in art_list:
                            file.write('Artists '+ elem[0] + ' ' + str(elem[1]) + '\n')
                        if elem[0] in news_list:
                            file.write('News '+ elem[0] + ' ' + str(elem[1]) + '\n')



                
        print('RDD collected') # print statement for the spark terminal
        e = sys.exc_info()[0]
        print("Error: %s" % e)



# do this for every single interval
hashtag_totals.foreachRDD(process_interval)



# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()



