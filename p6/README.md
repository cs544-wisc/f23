# DRAFT!  Don't start yet.

# P6 (6% of grade): Cassandra, Weather Data

## Overview

NOAA (National Oceanic and Atmospheric Administration) collects
weather data from all over the world.  In this project, you'll explore
how you could (1) store this data in Cassandra, (2) write a server for
data collection, and (3) analyze the collected data via Spark. You can
find the datasets we are going to be using in the `datasets` directory
of `p6`. 

We'll also explore read/write availability tradeoffs.  When always
want sensors to be able to upload data, but it is OK if we cannot
always read the latest stats (we prefer an error over inconsistent
results).

Learning objectives:
* create a schema for a Cassandra table that uses a partition key, cluster key, and static column
* create custom Cassandra types
* create custom Spark UDFs (user defined functions)
* configure queries to tradeoff read/write availability
* refresh a stale cache

Before starting, please review the [general project directions](../projects.md). 

## Corrections/Clarifications

* none yet

## Cluster Setup

We provide the Dockerfile and docker-compose.yml for this project.
You can run the following:

* `docker build image . -t p6-base`
* `docker compose up -d`

It will probably take 1-2 minutes for the cluster to be ready.  You
can `docker exec` into one of the containers and run `nodetool
status`.  When the cluster is ready, it should show something like
this:

```
Datacenter: datacenter1
=======================
Status=Up/Down
|/ State=Normal/Leaving/Joining/Moving
--  Address     Load       Tokens  Owns (effective)  Host ID                               Rack 
UN  172.27.0.4  70.28 KiB  16      64.1%             90d9e6d3-6632-4721-a78b-75d65c673db1  rack1
UN  172.27.0.3  70.26 KiB  16      65.9%             635d1361-5675-4399-89fa-f5624df4a960  rack1
UN  172.27.0.2  70.28 KiB  16      70.0%             8936a80e-c6b2-42ef-b54d-4160ff08857d  rack1
```

When the cluster isn't ready yet, that command will usually show a
Java error and stack trace.

The `p6-db-1` container will be running JupyterLab as well.  Check the
Docker port forwarding configuration and setup a tunnel to connect via
your browser.

Create a notebook `p6.ipynb` in the `/nb` directory for your work.
Use the same format for answers as in past projects (e.g., `#q1`).

## Part 1: Station Data

#### Schema Creation

In your notebook, first connect to the Cassandra cluster:

```python
from cassandra.cluster import Cluster
cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
cass = cluster.connect()
```

Then write code to do the following:

* drop a `weather` keyspace if it already exists
* create a `weather` keyspace with 3x replication
* inside `weather`, create a `station_record` type containing two ints: `tmin` and `tmax`
* inside `weather`, create a `stations` table

The `stations` table should have four columns: `id` (text), `name` (text), `date` (date), `record` (weather.station_record):

* `id` is a partition key and corresponds to a station's ID (like 'USC00470273')
* `date` is a cluster key, ascending
* `name` is a static field (because there is only one name per ID).  Example: 'UW ARBORETUM - MADISON'
* `record` is a regular field because there will be many records per station partition.

#### Q1: What is the Schema of `stations`?

You can execute `describe table weather.stations` and extract the `create_statement` as your answer.  It should look something like this:

```python
"CREATE TABLE weather.stations (\n id text,\n date date,\n
name text static,\n record station_record,\n PRIMARY KEY (id, date)\n)
WITH CLUSTERING ORDER BY (date ASC)\n AND additional_write_policy =
'99p'\n AND bloom_filter_fp_chance = 0.01\n AND caching = {'keys':
'ALL', 'rows_per_partition': 'NONE'}\n AND cdc = false\n AND comment =
''\n AND compaction = {'class':
'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy',
'max_threshold': '32', 'min_threshold': '4'}\n AND compression =
{'chunk_length_in_kb': '16', 'class':
'org.apache.cassandra.io.compress.LZ4Compressor'}\n AND memtable =
'default'\n AND crc_check_chance = 1.0\n AND default_time_to_live =
0\n AND extensions = {}\n AND gc_grace_seconds = 864000\n AND
max_index_interval = 2048\n AND memtable_flush_period_in_ms = 0\n AND
min_index_interval = 128\n AND read_repair = 'BLOCKING'\n AND
speculative_retry = '99p';"
```

#### Station Data

Create a local Spark session like this:

```python
from pyspark.sql import SparkSession
spark = (SparkSession.builder
         .appName("p6")
         .config('spark.jars.packages', 'com.datastax.spark:spark-cassandra-connector_2.12:3.4.0')
         .config("spark.sql.extensions", "com.datastax.spark.connector.CassandraSparkExtensions")
         .getOrCreate())
```

Remember that with a local deployment, the executor runs in the same
container as your notebook.  This means local file paths will work,
and you won't use HDFS for anything in this project.

Use Spark and `SUBSTRING` to extract `ID`, `STATE`, and `NAME` from
`nb/ghcnd-stations.txt`.  Reference the documentation to determine the
offsets
(this contains format descriptions for several different files, so be
sure you're reading about the correct one):

https://www.ncei.noaa.gov/pub/data/ghcn/daily/readme.txt

Review the lecture demos where we used Spark to extract the station
IDs from this text file:
https://github.com/cs544-wisc/f23/blob/main/lec/22-spark

Filter your results to the state of Wisconsin, collect the rows in
your notebook so you can loop over them, and do an `INSERT` into your
`weather.stations` table for each station ID and name.

#### Q2: what is the name corresponding to station ID `USW00014837`?

Write a Cassandra query to obtain the answer.

#### Q3: what is the token for the `USC00470273` station?

#### Q4: what is the first vnode token in the ring following the token for `USC00470273`?

You can use
[check_output](https://docs.python.org/3/library/subprocess.html#using-the-subprocess-module)
to run `nodetool ring`. Then write some code to parse the output, loop
over the ring and find the correct vnode.

Handle the case where the ring "wraps around" (meaning the row token is bigger than any vnode).

## Part 2: Weather Data

#### Server

Now you'll write gRPC-based `nb/server.py` file that receives
temperature data and records it to `weather.stations`.  You could
imagine various sensor devices acting as clients that make gRPC calls
to `server.py` to record data, but for simplicity we'll make the
client calls from `p5.ipynb`.

Build the `station.proto` we provide to get `station_pb2.py` and
`station_pb2_grpc` (consider reviewing P3 for how to do this).

In `server.py`, implement the interface from
`station_pb2_grpc.StationServicer`.  RecordTemps will insert new
temperature highs/lows to `weather.stations`.  `StationMax` will
return the maximum `tmax` ever seen for the given station.

Each call should use a prepared statement to insert or access data in
`weather.stations`.  It could be something like this:

```python
insert_statement = cass.prepare("????")
insert_statement.consistency_level = ConsistencyLevel.ONE
max_statement = cass.prepare("????")
max_statement.consistency_level = ????
```

Note that W = 1 (`ConsistencyLevel.ONE`) because we prioritize high
write availability.  The thought is that real sensors might not have
much space to save old data that hasn't been uploaded, so we want to
accept writes whenever possible.

Choose R so that R + W > RF.  We want to avoid a situation where a
`StationMax` returns a smaller temperature than one previously added
with `RecordTemps`; it would be better to return an error message if
necessary.

If execute of either prepared statement raises a `ValueError` or
`cassandra.Unavailable` exception, `server.py` should return a
response with the `error` string set to something informative.

Launch your server in the same container as your notebook.  There are
multiple ways you could do this, one options is with `docker exec`:

```
docker exec -it p6-db-1 python3 /nb/server.py
```

#### Data Upload

Unzip `records.zip` to get `records.parquet`.  In your `p6.ipnynb`
notebook, use Spark to load this and re-arrange the data so that there
is (a) one row per station/date combination, and (b) tmin and tmax
columns.  You can ignore other measurements.

Collect and loop over the results, making a call to the server with
for each row to insert the measurements to the database.

### Client

Implement the `simulate_station` function which takes in a `station_id` and then adds data from that station to `weather.stations`. Your implementation should ensure 
that for a given `sensor_id`, it should only record data for the **days in `2022` which we have both the `TMIN` and `TMAX` values**. Further details about the 
function behaviour is provided in the starter code. 

#### Q5: what is the max temperature ever seen for station USW00014837?

The code in your notebook cell should make an RPC call to your server to obtain the answer.

## Part 3: Spark Analysis

Next we are going to be configuring our spark session such that we have access to the table in Cassandra. The starter code already includes code to read the table using the session. You can find this code in the cell after the Part 3 header. 

Next implement the following functionality in the cell with comment "Create weather view":
* Create a view called `weather2022` that contains all 2022 data from `cassandra.weather.stations`.
* Cache `weather2022`.
* Register a UDF (user-defined function) that takes a TMIN or TMAX number and returns the temperature the Farheneith. Note that the default unit of temperature of a TMIN or TMAX number is ten degrees (i.e. if we have a TMAX of `1` that is equivalent to `10 °C`). 

Verify your implementation by running `spark.sql("show tables").show()` that it produces the following output:
```
+---------+-----------+-----------+
|namespace|  tableName|isTemporary|
+---------+-----------+-----------+
|         |weather2022|       true|
+---------+-----------+-----------+
```

#### Q4: what were the daily highs and lows at Madison's airport in 2022?

Madison airport has a station id of USW00014837. Using this information and `weather2022` determine the following information:
* Determine the date that had the lowest `tmin` temperature and get that `tmin` value in Farheneith
* Determine the date that had the highest `tmax` temperature and get that `tmax` value in Farheneith

Your output should look like (numbers may differ):
```
Date 2020-10-10 has lowest tmin of -32 F
Date 2023-04-12 has highest max of 13 F
```

Write your code in the cell with the comment "Q4 Ans". 


#### Q5: what is the correlation between maximum temperatures in Madison and Milwaukee?

Use station id of USW00014837 for Madison and USW00014839 for Milwauke. Checkout the [coor function](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.functions.corr.html) provided by pyspark, which has already been imported for you. Write your code in the cell with the comment "Q5 Ans" and ensure that prints out the coorelation rounded to 2 decimal places. The output of running the cell should be something like this (number might be different):
```
Coorelation of 0.21
```

## Part 4: Disaster Strikes

**Before starting this part, kill either the `p6-db-2` or the `p6-db-3` container.**

#### Q6: Does get_max still work?

Try to get the maximum for sensor USW00014837 using the `get_maximum` function. Record the response of the function call in the cell with the comment "Q6 Ans". 

#### Q7: Does simulate_sensor still work?

Try to add data for sensor USC00477115 using the `simulate_sensor` function. Record the response of the function call in the cell with the comment "Q7 Ans". 

#### Q8 how does refreshing the stale cache change the number of rows in weather2022?

Get the number of rows in weather2022, refresh the cache, then get the count again. Your output should look like:
```
Before refresh: 1460
After refresh: 1825
```

Note that we're only counting regular rows of data (not per-partition data in partition keys and static columns), so you can use something
`COUNT(record)` to only count rows where record is not NULL. Write your code in the cell with the comment "Q8 Ans"

## Submission

We should be able to run the following on your submission to create the mini cluster:

```
docker-compose up
```

We should then be able to open `http://localhost:5000/lab`, find your notebook, and run it.

We also be using an autograder to verify your solution which you can run yourself by running the following command in the `p6` directory:
```
python3 autograder.py
```

If we want to see how the autograder works as well as to see the result produced when running your notebooks, run the command in the `p6` directory. 
```
python3 autograder.py -g
```
This will create a `p6_results` dir in your `tester` directory and you can find the results in `p6/autograder_results`. 

## Approximate Rubric:

The following is approximately how we will grade, but we may make changes if we overlooked an important part of the specification or did
not consider a common mistake.

1. [x/1] question 1 (part 1)
2. [x/1] question 2 (part 1)
3. [x/1] **server** (part 2)
4. [x/1] question 3 (part 2)
5. [x/1] **view/caching/UDF** (part 3)
6. [x/1] question 4 (part 3)
7. [x/1] question 5 (part 3)
8. [x/1] question 6 (part 4)
9. [x/1] question 7 (part 4)
10. [x/1] question 8 (part 4)
