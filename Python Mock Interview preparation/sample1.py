from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions
from pyspark.sql.types import *
if __name__ == '__main__':
    sparkconf = SparkConf().setAppName("Spark Context Init").setMaster("local[*]")
    sc = SparkContext(conf=sparkconf)
    print(sc)
    spark = SparkSession.builder.appName("Spark Session Init").master("local[*]").getOrCreate()
    print(spark)