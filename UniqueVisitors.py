
# Extract the mmsi for the ships that have been to a port of brittany
# at least once

from pyspark import SparkConf
from pyspark.sql import SparkSession
from haversine import haversine

#The function to use in the flatmap stage
def port_visit(row):


    lat = row['lat']
    lon = row['lon']

    port_list = br_ports.value
    visitors_list = []
    for i in port_list:
        distance = haversine((lat,lon), (i[1], i[0]), miles=False)
        if (distance < 0.5):
             visitors_list.append((row['sourcemmsi'],1))
             break

    return visitors_list




# Create the spark session object
conf = SparkConf().setAppName('Shipyard')
spark = SparkSession.builder.config(conf=conf).getOrCreate()

#Read the input data to a dataframe
ais_data = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load('hdfs://Master1/user/nari_dynamic.csv')

#Read the ports csv, turn it to a list of tuples and broadcast it to the workers
ports_df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load('hdfs://Master1/user/ports.csv')

port_tuples = [tuple(x) for x in ports_df.collect()]
br_ports = spark.sparkContext.broadcast(port_tuples)

#Check if the point is close to a port and return a tuple with the mmsi and value 1
port_visitors = ais_data.rdd.flatMap(port_visit)

#Return a tuple with the mmsi and the number of points close to a port
unique_visitors = port_visitors.reduceByKey(lambda a, b : a+b)

#Save to a text file
unique_visitors.coalesce(1).saveAsTextFile('/unique_visitors')

spark.stop()

