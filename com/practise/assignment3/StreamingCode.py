#It cannot run correctly on cluster we need dynamice broadcast variables

import time
import json
from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext


def filter_language(tweet):
    if tweet['lang'] == 'en':
        return True
    return False

#boiler plate configuration
configuration = SparkConf().setMaster("local[*]").setAppName("apachelog")
sparkContext = SparkContext(conf = configuration)
ssc = StreamingContext(sparkContext, 30)
IP = "localhost"
Port = 5555
stream = ssc.socketTextStream(IP, Port)



#business code
jsonTweetsRdd = stream.map(lambda v: json.loads(v))
englishTweetsRdd = jsonTweetsRdd.filter(filter_language)

countedValueRdd = englishTweetsRdd.map(lambda tweet: (tweet['user']['location'],1)).reduceByKey(lambda x, y: x + y)

countedValueRdd.pprint()
countedValueRdd.saveAsTextFiles("./tweets/%f" % time.time())

# Start the streaming context
ssc.start()
ssc.awaitTermination()


