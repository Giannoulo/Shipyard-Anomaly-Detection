
import findspark
findspark.init()

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
import pandas as pd
from haversine import haversine

def port_visit(row):

    lat = row['lat']
    lon = row['lon']

    port_list = br_ports.value
    visitors_list = []
    for i in port_list:
        distance = haversine((lat,lon), (i[1], i[0]), miles=False)
        if (distance < 0.5):
             visitors_list.append(row['sourcemmsi'])
             break

    return visitors_list




# Create the spark session object
conf = SparkConf().setAppName('Shipyard')
# sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=conf).getOrCreate()

#Read the input data to an RDD
# ais_data = sc.textFile('file:///C:\\Users\\SinQ\\PycharmProjects\\shipyard\\nari_dynamic_1million.csv')
# ais_data = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load('file:///C:\\Users\\SinQ\\PycharmProjects\\shipyard\\nari_dynamic_1million.csv')

#Read the ports csv, turn it to a list of tuples and broadcast it to the workers
ports_df = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load('file:///C:\\Users\\SinQ\\PycharmProjects\\shipyard\\ports.csv')
port_tuples = [tuple(x) for x in ports_df.collect()]
br_ports = spark.sparkContext.broadcast(port_tuples)

#Check if the point is close to a port and save the mmsi
# port_visitors = ais_data.rdd.flatMap(port_visit)
#
#
# unique_visitors = port_visitors.distinct()
# unique_list = unique_visitors.collect()

for x in br_ports.value:
    print type(x[0]),type(x[1])

