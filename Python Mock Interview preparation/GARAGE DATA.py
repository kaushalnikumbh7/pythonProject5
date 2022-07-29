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


empdf = spark.read.csv('C:\Spark\GARAGE DF\employee.csv',header=True,inferSchema=True)


empdf.show()
#
# empdf.withColumn('EDOJ',to_date('EDOJ','dd-MM-yyyy')).withColumn('TodayDate',f.current_date()).show()
# empdf.printSchema()

# empdf = empdf.withColumn('EDOJ',empdf['EDOJ'].cast(DateType())).withColumn('EDOL',empdf['EDOL'].cast(DateType()))
# empdf.printSchema()

# empdf.select(f.date_add(empdf.EDOJ,5).alias('NextDay')).show()

# empdf.withColumn('Todaysdate',f.current_date()).show()

# empdf.withColumn('Into50',col('ESAL')*50).show()

# empdf.withColumn('copiedcolumn',col('ESAL')* -1).show()

# empdf.withColumn('Country',lit('India')).show()

# empdf.withColumnRenamed('EADD','Address').show()


custdf = spark.read.csv('C:\Spark\GARAGE DF\customer.csv',header=True,inferSchema=True)
custdf.show()

custdf.createOrReplaceTempView('customer')

# custdf1 = spark.sql('select * from customer where C_CREDITDAYS!=20')

# custdf1.show()

# custdf.filter(custdf.CADD=='LONDON').show()

# custdf.filter(custdf.CADD.isin('LONDON')).show()

# custdf.filter(~custdf.CADD.isin('LONDON')).show()

# custdf.select(custdf.columns[1:5]).show()

serdetdf = spark.read.csv('C:\Spark\GARAGE DF\ser_det.csv',header=True,inferSchema=True)
serdetdf.show()

empdf.join(serdetdf,empdf.EID == serdetdf.EID,'inner').show()

empdf.distinct().join(serdetdf,empdf.EID == serdetdf.EID,'left').show()

empdf.join(serdetdf,empdf.EID == serdetdf.EID,'right').show()