# Shipyard-Anomaly-Detection

Try to find regions during a ships voyage where the AIS beacon is turned off(dark regions). 
By using pySpark calculate these points where two concecutive beacon pings have anusual delays.
![AIS data](https://github.com/Giannoulo/Shipyard-Anomaly-Detection/blob/master/Capture.JPG "AIS Data")





First Step: Find the trips. Trip is defined as all the AIS pings in two concecutive port visits. The data are from Brittany so
by checking if a ship has been close to a port of Brittany(ports.csv), the port visitors are isolated. [UniqueVisitors.py](https://github.com/Giannoulo/Shipyard-Anomaly-Detection/blob/master/UniqueVisitors.py)




Second Step: Next split the pings into trips. Find the points where the beacon is turned off. [DisitorData.py](https://github.com/Giannoulo/Shipyard-Anomaly-Detection/blob/master/VisitorsData.py)




All the data for 1km port radius. Green is for pings inside a trip, yellow for the starter and ending pings of a trip and red for the starter and ending pings of a dark region.
![Trips](https://github.com/Giannoulo/Shipyard-Anomaly-Detection/blob/master/closeup-1km-alltime.PNG "Trips")




For radius of 1km from a port and delay between pings  3hr > delay > 1hr:
![Red pings](https://github.com/Giannoulo/Shipyard-Anomaly-Detection/blob/master/turnoff.PNG "Anomalies")
