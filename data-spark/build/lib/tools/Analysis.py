from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, avg, abs, max, count
from pyspark.sql.types import DoubleType
import pandas as pd

spark = SparkSession.builder.appName("Label").getOrCreate()
joinPoiFile = "/tmp/data/JoinPio.csv"
avgDevFile = "/tmp/data/AvgDevPio.csv"
circleFile = "/tmp/data/CirclePio.csv"

joinPoiData = spark.read.csv(path=joinPoiFile, sep=',', header=True, inferSchema=True)
avgData = joinPoiData.groupBy(['POIID']).agg(avg(col("Dist")).alias("avg")).select(col('POIID').alias('AvgPOIID'), 'avg')
#avgData.show()
joinAvgData = joinPoiData.join(avgData, joinPoiData.POIID == avgData.AvgPOIID).select('POIID', 'Dist', 'avg')
devData = joinAvgData.groupBy(['POIID','avg']).agg(avg(abs(col("Dist")-col("avg"))).alias("dev"))
#devData.show()
pdf = devData.select('*').toPandas()
pdf.to_csv(avgDevFile, index=False)

# redius is the max distance in each requests assigned to a POI
circleData = joinPoiData.groupBy(['POIID']).agg(max(col("Dist")).alias("radius"), (count(col("_ID"))/(max(col("Dist"))*max(col("Dist")))).alias("density"))
circleData.show()
pdf = circleData.select('*').toPandas()
pdf.to_csv(circleFile, index=False)

spark.stop()
