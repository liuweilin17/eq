from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, struct, min
from pyspark.sql.types import DoubleType
from math import sin, cos, sqrt, atan2, radians
import pandas as pd

def printDf(df):
    print('----------------------------------------------')
    df.show()
    line=df.count()
    print(line)
    print('----------------------------------------------')

def calDist(lat1, lon1, lat2, lon2):
    # approximate radius of earth in km
    R = 6373.0
    lat1, lon1, lat2, lon2 = float(lat1), float(lon1), float(lat2), float(lon2)
    dlon = float(lon2) - float(lon1)
    dlat = float(lat2) - float(lat1)
    a = sin(dlat / 2)**2 + cos(lat1) * cos(lat2) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    distance = R * c
    return distance

calDistUdf = udf(calDist, DoubleType())
spark = SparkSession.builder.appName("Label").getOrCreate()
cleanFile = "/tmp/data/DataSampleClean.csv"
poiFile = "/tmp/data/POIList.csv"
joinPoiFile = "/tmp/data/JoinPio.csv"

cleanData = spark.read.csv(path=cleanFile, sep=',', header=True)
poiData = spark.read.csv(path=poiFile, sep=',', header=True)
poiData = poiData.select(col("POIID"), col(" Latitude").alias("PoiLatitude"), col("Longitude").alias("PoiLongitude"))

joinData = cleanData.crossJoin(poiData).select("*", calDistUdf("Latitude", "Longitude", "PoiLatitude", "PoiLongitude").alias("Dist"))
colNames = cleanData.schema.names
colNames.remove("count")
cols = colNames + ["min.POIID", "min.Dist"]
labelData = joinData.groupBy(colNames).agg(min(struct(col("Dist"), col("POIID"))).alias("min")).select(*cols)
#printDf(labelData)
pdf = labelData.select("*").toPandas()
pdf.to_csv(joinPoiFile, index=False)

spark.stop()
