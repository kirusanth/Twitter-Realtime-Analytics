from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row,SQLContext
import sys
import requests
import re

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

# get lines


track = ['#NBA', '#Knicks', '#WeTheNorth', '#TrueToAtlanta', '#Cavs']


def update_fuction (line):
    print(">>>>>>>>>>>>>>>>>>>>>>> LINe <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
    print (line)
    if "apple" in line:
        print(">>>>>>>>>>>>>>>>>>>>>>> ENDSDADSAD <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")

        return line
    else:
        return "empty"
# filter the words to get only hashtags
hashtags = dataStream.filter(update_fuction )
print(">>>>>>>>>>>>>>>>>>>>>>> HASHTAGS <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
hashtags.pprint()
print(">>>>>>>>>>>>>>>>>>>>>>> I AM <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")


# map each hashtag to be a pair of (hashtag,1)
hashtag_counts = hashtags.map(lambda x: (x, 1))

# adding the count of each hashtag to its last count
def aggregate_tags_count(new_values, total_sum):
    return sum(new_values) + (total_sum or 0)

# do the aggregation, note that now this is a sequence of RDDs
hashtag_totals = hashtag_counts.updateStateByKey(aggregate_tags_count)

# process a single time interval
def process_interval(time, rdd):
    # print a separator
    print("----------- %s -----------" % str(time))
    try:
        # sort counts (ascending) in this time instance
        sorted_rdd = rdd.sortBy(lambda x:x[1], False)
        # transform rdd to list
        list = sorted_rdd.collect()
        # open output file
        with open("QA.txt", "w+") as file:
            # we only consider an hashtag value if it is EXACTLY like we wanted it
            # and then we write to output file
            for elem in list:
                if elem[0] in track:
                    file.write(elem[0] + ' ' + str(elem[1]) + '\n')
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
