from pyspark.sql import SparkSession
import pandas as pd

dataFile = "/tmp/data/DataSample.csv"
cleanFile = "/tmp/data/DataSampleClean.csv"
spark = SparkSession.builder.appName("CleanApp").getOrCreate()

data = spark.read.csv(path=dataFile, sep=',', header=True)
filterCol = [' TimeSt', 'Latitude', 'Longitude']
counts = data.groupBy(filterCol).count().filter("`count`==1")
dataFilter = data.join(counts, filterCol).dropDuplicates()
pdf = dataFilter.select("*").toPandas()
pdf.to_csv(cleanFile, index=False)

spark.stop()
