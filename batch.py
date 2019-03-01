import sys

from pyspark import SparkContext
from pyspark.sql.context import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import StructType
from pyspark.streaming import StreamingContext
from pyspark.sql import SparkSession

sc = SparkContext("local[*]", appName="Spark-Flight-Data")
#ssc = StreamingContext(sc, 3)   #Streaming will execute in each 3 seconds
#sqlContext = SQLContext(sc)
    
#log4jLogger = sc._jvm.org.apache.log4j
#log = log4jLogger.LogManager.getLogger(__name__)

spark = SparkSession(sc)

def main():
    flightData2015 = spark \
            .read \
            .option("inferSchema", "true") \
            .option("header", "true") \
            .csv("flight-data/csv/2015-summary.csv")

    flightData2015.printSchema()

    print(flightData2015.take(3))
    
    flightData2015.sort("count").explain()

    spark.conf.set("spark.sql.shuffle.partitions", "5") # instead of the default 200
    print(flightData2015.sort("count").take(2))

if __name__ == "__main__":
    main()
