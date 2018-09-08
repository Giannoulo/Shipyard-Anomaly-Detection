

from pyspark import SparkConf
from pyspark.sql import SparkSession
from haversine import haversine
from pyspark.sql import functions as sf
from pyspark.sql import Row




#Function to check if the point is close to a port
def docked(row):

    row_lat = row['lat']
    row_lon = row['lon']
    row_t = row['t']
    row_mmsi = row['sourcemmsi']

    port_list = br_ports.value
    is_docked = False

    for i in port_list:
        distance = haversine((row_lat, row_lon), (i[1], i[0]), miles=False)
        if distance < 0.5:
            is_docked = True

    if is_docked:
        return Row(sourcemmsi=row_mmsi, lon=row_lon, lat=row_lat, t=row_t, docked=1)
    else:
        return Row(sourcemmsi=row_mmsi, lon=row_lon, lat=row_lat, t=row_t, docked=0)



#Format the Row elements of the RDD to list of lists
def pretty(row):

    pretty_mmsi = row['sourcemmsi']
    row_list =[]
    for i in range(0 , len(row['collect_list(struct)'])):
        row_list.append([pretty_mmsi\
                        , row['collect_list(struct)'][i]['lon']\
                        , row['collect_list(struct)'][i]['lat']\
                        , row['collect_list(struct)'][i]['t']\
                        ,row['collect_list(struct)'][i]['docked']])

    return [row_list] #In a list because it will get flatten



def trips(list):

    trip = []
    trip_list = []
    list.sort(key=lambda point: point[3])#Sort the list by timestamp

    for i in range(0, len(list)):
        if list[i][4] == 1:
            trip_start= list[i]






# Create the spark session object
conf = SparkConf().setAppName('Shipyard')
spark = SparkSession.builder.config(conf=conf).getOrCreate()



#Read the input data to a dataframe
ais_data = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load('hdfs://Master1/user/nari_dynamic.csv')\
    .repartition(50)



#Read the ports csv, turn it to a list of tuples and broadcast it to the workers
ports_df = spark.read.format("csv")\
    .option("header", "true")\
    .option("inferSchema", "true")\
    .load('hdfs://Master1/user/ports.csv')

port_tuples = [tuple(x) for x in ports_df.collect()]
br_ports = spark.sparkContext.broadcast(port_tuples)



#Read the visitors file turn it to a list and broadcast it to the workers
visitors = spark.read.format('text')\
    .load('hdfs://Master1/user/visitors')


#Keep only the mmsi with more than one port visit
visitors_list=[]
for x in visitors.collect():
    if (int(x[0].split(",")[1].strip(')')) > 1):
        visitors_list.append(int(x[0].split(",")[0].strip('(')))

#Broadcasted the list of visitors mmsi's
br_visitors = spark.sparkContext.broadcast(visitors_list)

#Keep the points that belong to the visitors
visitor_data = ais_data.drop('navigationalstatus', 'rateofturn', 'speedoverground', 'courseoverground', 'trueheading')\
    .filter(ais_data['sourcemmsi'].isin(br_visitors.value))


#For each row in the rdd check if the point is close to a port and add the value 1 to the docked column
#or 0 if it isnt.
visitor_docked_data = visitor_data.rdd.map(docked)

#Group the rows by the mmsi
grouped_mmsi = spark.createDataFrame(visitor_docked_data)\
    .withColumn("struct", sf.struct("lon", "lat", 't', 'docked'))\
    .groupBy("sourcemmsi")\
    .agg(sf.collect_list("struct"))


#Format the Rdd to lists of lists and split into trips
tripsRDD = grouped_mmsi.rdd.flatMap(pretty).flatMap(trips)

    .coalesce(3).saveAsTextFile('hdfs://Master1/user/visitor_')

spark.stop()

