from pyspark import SparkContext,SparkConf
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
if __name__ == '__main__':
    sparkconf = SparkConf().setAppName("Spark Context Init").setMaster("local[*]")
    sc = SparkContext(conf=sparkconf)
    print(sc)
    spark = SparkSession.builder.appName("Spark Session Init").master("local[*]").getOrCreate()
    print(spark)


# schema1 =StructType([StructField("ID",IntegerType(),True),
#                     StructField("firstname",StringType(),True),
#                     StructField("lastname",StringType(),True),
#                     StructField("city",StringType(),True),
#                     StructField("Date", DateType(),True),
#                     StructField("EndDate", DateType(),True)])

# trialdf = spark.read.csv('C:\Spark\GARAGE DF\sc.txt',inferSchema=True,header=True)

# trialdf.show()

# trialdf.distinct().orderBy('salary').show()

# trialdf.dropDuplicates(['name','surnmae']).orderBy('age').show()
#
# trialdf.select('name','age').orderBy('name','age').show()
#
# trialdf.select('name','age').sort('age','name').show()
#

# trialdf.groupBy('age').sum('salary').show()

# trialdf.groupBy('age').max('salary').show()

# trialdf.groupBy('age').min('salary').show()

# trialdf.select(trialdf.name.substr(1,3),trialdf.surnmae.substr(1,5)).show()

# trialdf.na.fill('Empty**').show()

# trialdf.na.fill('unknown',['city']).show()

# trialdf.select(f.concat(trialdf['name'],trialdf['surnmae'])).show()

# trialdf.agg(f.countDistinct('name')).show()

# trialdf.select('name').distinct().show()


pardf = spark.read.parquet('C:\\Spark\GARAGE DF\\blocks-0012738509-0012739509.parquet',inferSchema=True,header=True)
pardf.show()

pardf.select(pardf.columns[5:15]).show()