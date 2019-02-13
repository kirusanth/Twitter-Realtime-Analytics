"""
    This Spark app connects to a script running on another (Docker) machine
    on port 9009 that provides a stream of raw tweets text. That stream is
    meant to be read and processed here, where top trending hashtags are
    identified. Both apps are designed to be run in Docker containers.

    To execute this in a Docker container, do:
    
        docker run -it -v $PWD:/app --link twitter:twitter eecsyorku/eecs4415

    and inside the docker:

        spark-submit spark_app.py

    For more instructions on how to run, refer to final tutorial 8 slides.

    Made for: EECS 4415 - Big Data Systems (York University EECS dept.)
    Modified by: Tilemachos Pechlivanoglou
    Based on: https://www.toptal.com/apache/apache-spark-streaming-twitter
    Original author: Hanee' Medhat

"""
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
sad_list=  ['#sad','#mood',"#depressed","#cry"]
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
words = dataStream.map(lambda x: (x, sia.polarity_scores(x)))
words.pprint(10)

words = words.filter()


#hashtags = words.filter(lambda w: '#cpc' in w.lower().lower() or '#lpc' in w.lower().lower() or "#ndp" in w.lower().lower()  or "#pcpo" in w.lower().lower() or "#" in w.lower() or "#democrats" in w.lower()  or "#republican" in w.lower() or "#bccp" in w.lower() or " #olp" in w.lower() or  "#pq" in w.lower() or 
#    '#apple' in w.lower() or '#microsoft' in w.lower() or '#google' in w.lower() or "#ibm" in w.lower() or "#tesla" in w.lower() or "#target" in w.lower() or "#walmart" in w.lower() or "#aon" in w.lower() or "#salesforce" in w.lower() or  "#at&t" in w.lower() or
#    '#cnn' in w.lower() or '#cbc' in w.lower()  or '#cp24' in w.lower() or '#nbc' in w.lower() or '#hbo' in w.lower() or '#hgtv' in w.lower() or  "#ctv" in w.lower() or  "#globaltv" in w.lower() or "#historychannel" in w.lower() or "#discoverychannel" in w.lower() or
#    '#katyperry' in w.lower() or "#justinbieber" in w.lower() or  "#rihanna" in w.lower() or  '#taylorswift' in w.lower() or '#ladygaga' in w.lower() or  '#jtimberlake' in w.lower() or '#shakira' in w.lower() or "#selenagomez" in w.lower() or "#demilovato" in w.lower() or "#britneyspears" in w.lower() or
#    '#nytimes' in w.lower() or '#wsj' in w.lower() or '#latimes' in w.lower() or '#usatoday' in w.lower() or '#denverpost' in w.lower() or '#thestar' in w.lower() or "#newsday" in w.lower() or "#theglobeandmail" in w.lower() or "#nationalpost" in w.lower() or '#torontosun' in w.lower() or "#sad" in w.lower() or '#mood' in w.lower() or "#depressed" in w.lower() or"#cry" in w.lower() )


#hashtag_counts.pprint(5)

# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total):
    #print("START OF THE TEST")
    #print(new_values)
    #if new_values is not None:
     #   print ("newValues SIZE" + str(len(new_values)))
    #print('total')
    #print(total)
    
    #if total is not None:
     #   print ("TOTal SIZE" + str(len(total)))
    #print("*****************************START OF THE CASE******************************")
    result=dict();
    if total is None or len(total)==0 and len(new_values)>=1:
        #print (' I am at this 1')
        result['compound'] = max_dict(new_values,'compound')
        result['neg'] = max_dict(new_values,'neg')
        result['pos'] = max_dict(new_values,'pos')
        result['neu'] = max_dict(new_values,'neu')
    if total is not None and len(total)==4  and len(new_values)>=1:
       # print (' I am at this 2')
        result['compound'] = max(max_dict(new_values,'compound'),total.get('compound'))
        result['neg'] = max(max_dict(new_values,'neg'),total.get('neg'))
        result['pos'] = max(max_dict(new_values,'pos'),total.get('pos'))
        result['neu'] = max(max_dict(new_values,'neu'),total.get('neu'))
    if total is not None and len(total)==4  and  not new_values:
      #  print (' I am at this 3')
        result['compound'] = total.get('compound')
        result['neg'] = total.get('neg')
        result['pos'] = total.get('pos')
        result['neu'] = total.get('neu')
    
    #print("*****************************END OF THE CASE******************************")

    #print('result')
    #print(result)
    #print("END OF THE TEST")
    return result

#find max in collection of dict 
def max_dict(new_values, key):
    max_num = 0.00000
    for i in range(len(new_values)):
        max_num = max(max_num, new_values[i].get(key))
    return max_num


# do the aggregation, note that now this is a sequence of RDDs
hashtag_totals = words.updateStateByKey(aggregate_tags_count)

# process a single time interval
# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    try:
        # sort counts (ascending) in this time instance
        sorted_rdd = rdd.sortBy(lambda x:x[0], True)
        # transform rdd to list
        list = sorted_rdd.collect()
   
        # open output file
        with open("stream_output_partb.txt", "w") as file:
            # we only consider an hashtag value if it is EXACTLY like we wanted it
            # and then we write to output file
            for elem in list:
                    file.write(' Tweet : '+elem[0] + ' score: ' + str(elem[1]) + '\n')
                
        print('RDD collected')
    except:
        e = sys.exc_info()[0]
        print("Error: %s" % e)



# do this for every single interval
hashtag_totals.foreachRDD(process_interval)



# start the streaming computation
ssc.start()
# wait for the streaming to finish
ssc.awaitTermination()