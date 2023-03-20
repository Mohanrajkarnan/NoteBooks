# Databricks notebook source
def add(a,b):
    return a+b

# COMMAND ----------

def _readExcel(filePath,sheet):
    sparkDF = spark.read.format("com.crealytics.spark.excel") \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .option("dataAddress", f"'{sheet}'!A1") \
        .load(filePath)
    return sparkDF

# COMMAND ----------


