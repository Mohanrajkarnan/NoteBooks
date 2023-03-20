# Databricks notebook source
from pyspark.sql.functions import *

df = spark.readStream.format("rate").option("rowsPerSecond", 3).load()
final_df = df.withColumn("result", col("value"))

# COMMAND ----------

#final_df.writeStream.format("parquet").option("checkpointLocation", "/mnt/data/streams/checkpoint").start("/mnt/data/streams/output")
#spark.read.format("parquet").load("/mnt/data/streams/output").count()

# COMMAND ----------

spark.read.format("parquet").load("/mnt/data/streams/output").groupBy('timestamp').agg(sum("value").alias("count_of_value")).show()

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/data/streams/output")

# COMMAND ----------

# value >200 = completed
# value >300 = processing
# value >400 = inprorgress
from pyspark.sql.functions import when
df1 = df.withColumn("order_status",when(df.value < 200 , "Completed").when(df.value < 300 , "Processing").otherwise("Not Completed"))

# COMMAND ----------

df1.write.mode("append").partitionBy("order_status").format("delta").save("/mnt/data/streams/Curated/parquetFormat")

# COMMAND ----------

spark.read.format("delta").load("/mnt/data/streams/Curated/parquetFormat").groupBy("order_status").count().show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## ---------------- Netflix Data ---------------------

# COMMAND ----------

dbutils.fs.mount(
    source = 'wasbs://data@adlsdev0123.blob.core.windows.net',
    mount_point = '/mnt/data',
    extra_configs = {'fs.azure.account.key.adlsdev0123.blob.core.windows.net' : dbutils.secrets.get('databricks-demo-scope' , 'databricksdemovalut')}
)



# COMMAND ----------

dbutils.fs.ls("/mnt/data")

# COMMAND ----------

dbutils.fs.unmount("/mnt/data")

# COMMAND ----------


