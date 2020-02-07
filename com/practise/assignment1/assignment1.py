from pyspark import SparkConf, SparkContext
import re
from datetime import datetime

#Commented for furthur discussion
def keyValueGeneratorForIp(line):
    splittedLineWithIp = re.split('(^\S+\.[\S+\.]+\S+)\s', line)
    splittedLineWithTime = re.split('\[(.*)]', line)
    datetime_object = datetime.strptime(splittedLineWithTime[1].replace('[', '').replace('+0000','').strip(), '%d/%B/%Y:%H:%M:%S')
    return (splittedLineWithIp[1],datetime_object.strftime("%d/%m/%Y %H:%M:%S"))


def keyValueGeneratorForIpWithCount(line):
    splittedLineWithIp = re.split('(^\S+\.[\S+\.]+\S+)\s', line)
    splittedLineWithTime = re.split('\[(.*)]', line)
    return (splittedLineWithIp[1],1)

configuration = SparkConf().setMaster("local[*]").setAppName("apachelog")
sparkContext = SparkContext(conf = configuration)
inputLogFile = sparkContext.textFile("apache-log.txt")
rowsWithNoUrl = inputLogFile.filter(lambda row: "http" not in row)
keyValuePairofIP = rowsWithNoUrl.map(keyValueGeneratorForIpWithCount)
countedValue = keyValuePairofIP.reduceByKey(lambda x,y: x+y)
counter = countedValue.collect()
for count in counter:
    print(count)


#Discuss this later
#groupedByKeyRdd = keyValuePairofIP.groupByKey()
#calculate average session time #see at the end
#averageofSession = keyValuePairofIP.reduceByKey()









