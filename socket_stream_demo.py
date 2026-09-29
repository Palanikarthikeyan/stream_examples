'''
Telecom towers continuously send live recharge (or) usage logs
Example live stream: 101,raj,12.34,399

Demo: socket streaming
'''
# 1st - import std. pyspark module
from pyspark.sql import SparkSession
from pyspark.sql.functions import split,col

# 2nd - Create spark session object
spark = SparkSession.builder.appName("TelcomSocketStreaming").getOrCreate()

# 3rd - Read streaming data from socket - incoming data from socket source
socket_df = spark.readStream.format("socket").option("host","localhost").option("port",9999).load()


# 4th - Split columns
telecom_df = socket_df.select(
        split(col("value"),",").getItem(0).alias("customer_id"),
        split(col("value"),",").getItem(1).alias("name"),
        split(col("value"),",").getItem(2).alias("data_usage_gb"),
        split(col("value"),",").getItem(3).alias("recharge_amount")
        )
# 5th - filter recharge >400
filtered_df = telecom_df.filter(col("recharge_amount") > 400)

# 6th - output to cosole
query = filtered_df.writeStream.outputMode("append").format("console").start()
query.awaitTermination()

## Run
# Open another terminal
# nc -l 9999 
# 101,raj,12.34,401
