from pyspark import SparkConf, SparkContext
import os

conf = SparkConf().setMaster("local").setAppName("My App")
sc = SparkContext(conf = conf)

log4jLogger = sc._jvm.org.apache.log4j
log = log4jLogger.LogManager.getLogger(__name__)

contentRDD = sc.textFile("1342-0.txt")

lines = contentRDD.filter(lambda x: len(x.strip()) > 0)

print(lines.count())

def hasBennet(line):
    return "Bennet" in line

bennetLines = lines.filter(hasBennet)

print(bennetLines.count())
print(bennetLines.take(3))

words = lines.flatMap(lambda x: x.split(" "))
wordcount = words.map(lambda word: (word, 1)) \
        .reduceByKey(lambda x,y: x + y) \
        .map(lambda x: (x[1], x[0])) \
        .sortByKey(False) \

for word in wordcount.collect():
    print(word)

if not os.path.isdir("wordcount"):
    wordcount.saveAsTextFile("wordcount")
else :
    log.warn("wordcount path already exists")

sc.stop()
