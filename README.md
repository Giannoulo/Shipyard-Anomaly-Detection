# Shipyard-Anomaly-Detection

Try to find regions during a ships voyage where the AIS beacon is turned off(dark regions). 
By using pySpark calculate these points where two concecutive beacon pings have anusual delays.

First Step: Find the trips. Trip is defined as all the AIS pings in two concecutive port visits. The data are from Brittany so
by checking if a ship has been close to a port of Brittany(ports.csv), the port visitors are isolated. [UniqueVisitors.py](https://github.com/Giannoulo/Shipyard-Anomaly-Detection/blob/master/UniqueVisitors.py)

Second Step: Next split the pings into trips. Find the points where the beacon is turned off. [DisitorData.py](https://github.com/Giannoulo/Shipyard-Anomaly-Detection/blob/master/VisitorsData.py)
