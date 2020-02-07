from pyspark import SparkConf, SparkContext


#splitter function
def cusomterKeyvalueGenerator(line):
    words = line.split(',')
    return (int(words[0]),float (words[2]))




#boiler plate code for setting up spark
configuration = SparkConf().setMaster("local").setAppName("CustomerSpending")
sparkcontext = SparkContext(conf = configuration)


customerDataLinesRdd = sparkcontext.textFile("customer-orders.csv")
customerKeyvaluePairRdd = customerDataLinesRdd.map(cusomterKeyvalueGenerator)

amountAddedRdd = customerKeyvaluePairRdd.reduceByKey(lambda x , y: x+y)

amountSpentByCustomer =  amountAddedRdd.collect()

for amount in amountSpentByCustomer:
    print(amount)





