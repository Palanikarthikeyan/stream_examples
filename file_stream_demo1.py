from pyspark.sql import SparkSession
from pyspark.sql.types import (StructType,StructField,StringType,DoubleType)

spark = SparkSession.builder.appName("FileStreamExample").getOrCreate()

schema = StructType([StructField("transaction_id",StringType(),True),
                     StructField("customer",StringType(),True),
                     StructField("amount",DoubleType(),True)])

# Read files continuously

df = spark.readStream.schema(schema).option("header",True).csv("Demo/")

# display incoming records
query = df.writeStream.format("console").outputMode("append").start() # display to monitor(console)
query.awaitTermination()

'''
Run:
Open another terminal
Create a new Folder called Demo/
create sample file csv file inside the Demo/
When ever new file is created inside the Demo/ directory - spark do processing
'''
