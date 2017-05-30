# Databricks notebook source
# MAGIC %md 
# MAGIC #### Preparation
# MAGIC 
# MAGIC | Databricks Metadata        | Value           |
# MAGIC | ------------- |:-------------|
# MAGIC | Apache Spark Version | 2.1+  |
# MAGIC | Recommended Cluster Size     | 2 rxlarge      |
# MAGIC | Modified By     | Myles Baker      |
# MAGIC | Date     | March 6, 2017      |
# MAGIC | Requirements     | <ol><li>Attach GraphFrames Library</li><li>Run: `%run "./Federal Demo Prep"`</li><li>Run: `on_time_flight_demo()`</li></ol>|
# MAGIC ___

# COMMAND ----------

# MAGIC %run "../Federal Demo Prep"

# COMMAND ----------

on_time_flight_demo()

# COMMAND ----------

# MAGIC %md # On-Time Flight Performance with GraphFrames for Apache Spark
# MAGIC This notebook provides an analysis of On-Time Flight Performance and Departure Delays data using GraphFrames for Apache Spark.
# MAGIC 
# MAGIC Note: Links require Internet connection
# MAGIC 
# MAGIC Source Data: 
# MAGIC * [OpenFlights: Airport, airline and route data](http://openflights.org/data.html)
# MAGIC * [United States Department of Transportation: Bureau of Transportation Statistics (TranStats)](http://www.transtats.bts.gov/DL_SelectFields.asp?Table_ID=236&DB_Short_Name=On-Time)
# MAGIC  * Note, the data used here was extracted from the US DOT:BTS between 1/1/2014 and 3/31/2014*
# MAGIC 
# MAGIC References:
# MAGIC * [GraphFrames User Guide](http://graphframes.github.io/user-guide.html)
# MAGIC * [GraphFrames: DataFrame-based Graphs (GitHub)](https://github.com/graphframes/graphframes)
# MAGIC * [D3 Airports Example](http://mbostock.github.io/d3/talk/20111116/airports.html)

# COMMAND ----------

# Set File Paths
tripdelaysFilePath = "/mnt/databricks-federal/flights/departuredelays.csv"
airportsnaFilePath = "/mnt/databricks-federal/flights/airport-codes-na.txt"

# Obtain airports dataset
airportsna = sqlContext.read.format("com.databricks.spark.csv").options(header='true', inferschema='true', delimiter='\t').load(airportsnaFilePath)
airportsna.registerTempTable("airports_na")

# Obtain departure Delays data
departureDelays = sqlContext.read.format("com.databricks.spark.csv").options(header='true').load(tripdelaysFilePath)
departureDelays.registerTempTable("departureDelays")
departureDelays.cache()

# Available IATA codes from the departuredelays sample dataset
tripIATA = sqlContext.sql("select distinct iata from (select distinct origin as iata from departureDelays union all select distinct destination as iata from departureDelays) a")
tripIATA.registerTempTable("tripIATA")

# Only include airports with atleast one trip from the departureDelays dataset
airports = sqlContext.sql("select f.IATA, f.City, f.State, f.Country from airports_na f join tripIATA t on t.IATA = f.IATA")
airports.registerTempTable("airports")
airports.cache()

# COMMAND ----------

# Build `departureDelays_geo` DataFrame
#  Obtain key attributes such as Date of flight, delays, distance, and airport information (Origin, Destination)  
departureDelays_geo = sqlContext.sql("select cast(f.date as int) as tripid, cast(concat(concat(concat(concat(concat(concat('2014-', concat(concat(substr(cast(f.date as string), 1, 2), '-')), substr(cast(f.date as string), 3, 2)), ' '), substr(cast(f.date as string), 5, 2)), ':'), substr(cast(f.date as string), 7, 2)), ':00') as timestamp) as `localdate`, cast(f.delay as int), cast(f.distance as int), f.origin as src, f.destination as dst, o.city as city_src, d.city as city_dst, o.state as state_src, d.state as state_dst from departuredelays f join airports o on o.iata = f.origin join airports d on d.iata = f.destination") 

# RegisterTempTable
departureDelays_geo.registerTempTable("departureDelays_geo")

# Cache and Count
departureDelays_geo.cache()
departureDelays_geo.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Building the Graph
# MAGIC Now that we've imported our data, we're going to need to build our graph. To do so we're going to do two things. We are going to build the structure of the vertices (or nodes) and we're going to build the structure of the edges. What's awesome about GraphFrames is that this process is incredibly simple. 
# MAGIC * Rename IATA airport code to **id** in the Vertices Table
# MAGIC * Start and End airports to **src** and **dst** for the Edges Table (flights)
# MAGIC 
# MAGIC These are required naming conventions for vertices and edges in GraphFrames as of the time of this writing (Feb. 2016).

# COMMAND ----------

# MAGIC %md **WARNING:** If the graphframes package, required in the cell below, is not installed, follow the instructions [here](http://cdn2.hubspot.net/hubfs/438089/notebooks/help/Setup_graphframes_package.html).

# COMMAND ----------

# Note, ensure you have already installed the GraphFrames spack-package
from pyspark.sql.functions import *
from graphframes import *

# Create Vertices (airports) and Edges (flights)
tripVertices = airports.withColumnRenamed("IATA", "id").distinct()
tripEdges = departureDelays_geo.select("tripid", "delay", "src", "dst", "city_dst", "state_dst")

# Cache Vertices and Edges
tripEdges.cache()
tripVertices.cache()

# COMMAND ----------

# Vertices
#   The vertices of our graph are the airports
display(tripVertices)

# COMMAND ----------

# Edges
#  The edges of our graph are the flights between airports
display(tripEdges)

# COMMAND ----------

# Build `tripGraph` GraphFrame
#  This GraphFrame builds up on the vertices and edges based on our trips (flights)
tripGraph = GraphFrame(tripVertices, tripEdges)
print tripGraph

# Build `tripGraphPrime` GraphFrame
#   This graphframe contains a smaller subset of data to make it easier to display motifs and subgraphs (below)
tripEdgesPrime = departureDelays_geo.select("tripid", "delay", "src", "dst")
tripGraphPrime = GraphFrame(tripVertices, tripEdgesPrime)

# COMMAND ----------

# MAGIC %md ## Simple Queries
# MAGIC Let's start with a set of simple graph queries to understand flight performance and departure delays

# COMMAND ----------

# MAGIC %md #### Determine the number of airports and trips

# COMMAND ----------

print "Airports: %d" % tripGraph.vertices.count()
print "Trips: %d" % tripGraph.edges.count()


# COMMAND ----------

# MAGIC %md #### Determining the longest delay in this dataset

# COMMAND ----------

# Finding the longest Delay
longestDelay = tripGraph.edges.groupBy().max("delay")
display(longestDelay)

# COMMAND ----------

# MAGIC %md #### Determining the number of delayed vs. on-time / early flights

# COMMAND ----------

# Determining number of on-time / early flights vs. delayed flights
print "On-time / Early Flights: %d" % tripGraph.edges.filter("delay <= 0").count()
print "Delayed Flights: %d" % tripGraph.edges.filter("delay > 0").count()

# COMMAND ----------

# MAGIC %md #### What flights departing SFO are most likely to have significant delays
# MAGIC Note, delay can be <= 0 meaning the flight left on time or early

# COMMAND ----------

tripGraph.edges\
  .filter("src = 'SFO' and delay > 0")\
  .groupBy("src", "dst")\
  .avg("delay")\
  .sort(desc("avg(delay)"))

# COMMAND ----------

display(tripGraph.edges.filter("src = 'SFO' and delay > 0").groupBy("src", "dst").avg("delay").sort(desc("avg(delay)")))

# COMMAND ----------

# MAGIC %md #### What destinations tend to have delays

# COMMAND ----------

# After displaying tripDelays, use Plot Options to set `state_dst` as a Key.
tripDelays = tripGraph.edges.filter("delay > 0")
display(tripDelays)

# COMMAND ----------

# MAGIC %md #### What destinations tend to have significant delays departing from SEA

# COMMAND ----------

# States with the longest cumulative delays (with individual delays > 100 minutes) (origin: Seattle)
display(tripGraph.edges.filter("src = 'SEA' and delay > 100"))

# COMMAND ----------

# MAGIC %md 
# MAGIC ## Vertex Degrees
# MAGIC * `inDegrees`: Incoming connections to the airport
# MAGIC * `outDegrees`: Outgoing connections from the airport 
# MAGIC * `degrees`: Total connections to and from the airport
# MAGIC 
# MAGIC Reviewing the various properties of the property graph to understand the incoming and outgoing connections between airports.

# COMMAND ----------

# Degrees
#  The number of degrees - the number of incoming and outgoing connections - for various airports within this sample dataset
display(tripGraph.degrees.sort(desc("degree")).limit(20))

# COMMAND ----------

# MAGIC %md ## City / Flight Relationships through Motif Finding
# MAGIC To more easily understand the complex relationship of city airports and their flights with each other, we can use motifs to find patterns of airports (i.e. vertices) connected by flights (i.e. edges). The result is a DataFrame in which the column names are given by the motif keys.

# COMMAND ----------

# MAGIC %md #### What delays might we blame on SFO

# COMMAND ----------

# Using tripGraphPrime to more easily display 
#   - The associated edge (ab, bc) relationships 
#   - With the different the city / airports (a, b, c) where SFO is the connecting city (b)
#   - Ensuring that flight ab (i.e. the flight to SFO) occured before flight bc (i.e. flight leaving SFO)
#   - Note, TripID was generated based on time in the format of MMDDHHMM converted to int
#       - Therefore bc.tripid < ab.tripid + 10000 means the second flight (bc) occured within approx a day of the first flight (ab)
# Note: In reality, we would need to be more careful to link trips ab and bc.
motifs = tripGraphPrime.find("(a)-[ab]->(b); (b)-[bc]->(c)")\
  .filter("(b.id = 'SFO') and (ab.delay > 500 or bc.delay > 500) and bc.tripid > ab.tripid and bc.tripid < ab.tripid + 10000")
display(motifs)

# COMMAND ----------

# MAGIC %md ## Determining Airport Ranking using PageRank
# MAGIC There are a large number of flights and connections through these various airports included in this Departure Delay Dataset.  Using the `pageRank` algorithm, Spark iteratively traverses the graph and determines a rough estimate of how important the airport is.

# COMMAND ----------

# Determining Airport ranking of importance using `pageRank`
ranks = tripGraph.pageRank(resetProbability=0.15, maxIter=5)
display(ranks.vertices.orderBy(ranks.vertices.pagerank.desc()).limit(20))

# COMMAND ----------

# MAGIC %md ## Most popular flights (single city hops)
# MAGIC Using the `tripGraph`, we can quickly determine what are the most popular single city hop flights

# COMMAND ----------

# Determine the most popular flights (single city hops)
import pyspark.sql.functions as func
topTrips = tripGraph \
  .edges \
  .groupBy("src", "dst") \
  .agg(func.count("delay").alias("trips")) 

# COMMAND ----------

# Show the top 20 most popular flights (single city hops)
display(topTrips.orderBy(topTrips.trips.desc()).limit(20))

# COMMAND ----------

# MAGIC %md ## Top Transfer Cities
# MAGIC Many airports are used as transfer points instead of the final Destination.  An easy way to calculate this is by calculating the ratio of inDegree (the number of flights to the airport) / outDegree (the number of flights leaving the airport).  Values close to 1 may indicate many transfers, whereas values < 1 indicate many outgoing flights and > 1 indicate many incoming flights.  Note, this is a simple calculation that does not take into account of timing or scheduling of flights, just the overall aggregate number within the dataset.

# COMMAND ----------

# Calculate the inDeg (flights into the airport) and outDeg (flights leaving the airport)
inDeg = tripGraph.inDegrees
outDeg = tripGraph.outDegrees

# Calculate the degreeRatio (inDeg/outDeg)
degreeRatio = inDeg.join(outDeg, inDeg.id == outDeg.id) \
  .drop(outDeg.id) \
  .selectExpr("id", "double(inDegree)/double(outDegree) as degreeRatio") \
  .cache()

# Join back to the `airports` DataFrame (instead of registering temp table as above)
nonTransferAirports = degreeRatio.join(airports, degreeRatio.id == airports.IATA) \
  .selectExpr("id", "city", "degreeRatio") \
  .filter("degreeRatio < .9 or degreeRatio > 1.1")

# List out the city airports which have abnormal degree ratios.
display(nonTransferAirports)

# COMMAND ----------

# Join back to the `airports` DataFrame (instead of registering temp table as above)
transferAirports = degreeRatio.join(airports, degreeRatio.id == airports.IATA) \
  .selectExpr("id", "city", "degreeRatio") \
  .filter("degreeRatio between 0.9 and 1.1")
  
# List out the top 10 transfer city airports
display(transferAirports.orderBy("degreeRatio").limit(10))

# COMMAND ----------

# MAGIC %md ## Breadth First Search 
# MAGIC Breadth-first search (BFS) is designed to traverse the graph to quickly find the desired vertices (i.e. airports) and edges (i.e flights).  Let's try to find the shortest number of connections between cities based on the dataset.  Note, these examples do not take into account of time or distance, just hops between cities.

# COMMAND ----------

# Example 1: Direct Seattle to San Francisco 
filteredPaths = tripGraph.bfs(
  fromExpr = "id = 'SEA'",
  toExpr = "id = 'SFO'",
  maxPathLength = 1)
display(filteredPaths)

# COMMAND ----------

# MAGIC %md As you can see, there are a number of direct flights between Seattle and San Francisco.

# COMMAND ----------

# Example 2: Direct San Francisco and Buffalo
filteredPaths = tripGraph.bfs(
  fromExpr = "id = 'SFO'",
  toExpr = "id = 'BUF'",
  maxPathLength = 1)
display(filteredPaths)

# COMMAND ----------

# MAGIC %md But there are no direct flights between San Francisco and Buffalo.

# COMMAND ----------

# Example 2a: Flying from San Francisco to Buffalo
filteredPaths = tripGraph.bfs(
  fromExpr = "id = 'SFO'",
  toExpr = "id = 'BUF'",
  maxPathLength = 2)
display(filteredPaths)

# COMMAND ----------

# MAGIC %md But there are flights from San Francisco to Buffalo with Minneapolis as the transfer point.

# COMMAND ----------

# MAGIC %md ## D3 Visualization
# MAGIC Use `D3.js` to visualize complex data relationships

# COMMAND ----------

# MAGIC %md #### Visualize Flights Departing Without Delay

# COMMAND ----------

# MAGIC %scala
# MAGIC /* 
# MAGIC On-time and Early Arrivals
# MAGIC graphs.force(
# MAGIC   height = 800,
# MAGIC   width = 1200,
# MAGIC   clicks = sql("""select src, dst as dest, count(1) as count from departureDelays_geo where delay <= 0 group by src, dst""").as[Edge])
# MAGIC */

# COMMAND ----------

displayHTML("""<img src=files/databricks-federal/force-directed.gif>""")

# COMMAND ----------

# MAGIC %md #### Visualize Delayed Trips Departing from the West Coast
# MAGIC 
# MAGIC Notice that most of the delayed trips are with Western US cities

# COMMAND ----------

# MAGIC %scala
# MAGIC /* 
# MAGIC Delayed Trips from CA, OR, and/or WA
# MAGIC graphs.force(
# MAGIC   height = 800,
# MAGIC   width = 1200,
# MAGIC   clicks = sql("""select src, dst as dest, count(1) as count from departureDelays_geo where state_src in ('CA', 'OR', 'WA') and delay > 0 group by src, dst""").as[Edge])
# MAGIC */

# COMMAND ----------

displayHTML("""<img src=files/databricks-federal/flight_delays.gif>""")

# COMMAND ----------

# MAGIC %md #### Visualize All Flights (from this dataset)

# COMMAND ----------

# MAGIC %scala
# MAGIC /* 
# MAGIC Trips (from DepartureDelays Dataset)
# MAGIC graphs.force(
# MAGIC   height = 800,
# MAGIC   width = 1200,
# MAGIC   clicks = sql("""select src, dst as dest, count(1) as count from departureDelays_geo group by src, dst""").as[Edge])
# MAGIC */

# COMMAND ----------

displayHTML("""<img src=files/databricks-federal/all_flights.gif>""")

# COMMAND ----------

