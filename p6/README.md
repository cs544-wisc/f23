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
* create Cassandra schemas involving partition keys and cluster keys
* move data between Spark and Cassandra
* configure queries to achieve a tradeoff between read and write availability

Before starting, please review the [general project directions](../projects.md). 

## Corrections/Clarifications

* none yet

## Cluster Setup

We have provided you with a `setup.sh` which will download and setup all the necessary files for this project. 
Note that you might need to give `setup.sh` executable permission before running it. You can do this using:
```
wget https://raw.githubusercontent.com/cs544-wisc/f23/main/p6/setup.sh -O setup.sh
chmod u+x setup.sh
./setup.sh
```

We provide the Dockerfile and docker-compose.yml for this project.
You can run the following:

* `docker build . -t p6-base`
* `docker compose up -d`

The `p6-db-1` container will be running JupyterLab as well.  Check the
Docker port forwarding configuration and setup a tunnel to connect via
your browser.

Create a notebook `p6.ipynb` in the `/nb` directory for your work.
Use the same format for answers as in past projects (e.g., `#q1`).

It generally takes around 1 to 2 minutes fro the Cassandra cluster to be ready. Write a cell like this:
```python3
!nodetool status
```

and if the cluster is ready, it will produce an output like this:
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

If the cluster is not ready it will generally show an error. If this occurs then wait a little bit and rerun the command
and keep doing so until you see that the cluster is ready. 

## Part 1: Station Data

#### Schema Creation

Connect to the Cassandra cluster using this code:

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

If you did this correctly, then running the query `SELECT COUNT(*) FROM weather.stations`
should produce "1313" as the output.

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
client calls from `p6.ipynb`. 

Build the `station.proto` we provide to get `station_pb2.py` and
`station_pb2_grpc` (consider reviewing P3 for how to do this).

In `server.py`, implement the interface from
`station_pb2_grpc.StationServicer`.  RecordTemps will insert new
temperature highs/lows to `weather.stations`. `StationMax` will
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
accept writes whenever possible. **Host your server on port 5440**.

Choose R so that R + W > RF.  We want to avoid a situation where a
`StationMax` returns a smaller temperature than one previously added
with `RecordTemps`; it would be better to return an error message if
necessary.

Launch your server in the same container as your notebook.  There are
multiple ways you could do this -- one option is with `docker exec`:

```
docker exec -it p6-db-1 python3 /nb/server.py
```

#### Error Handling

Note that `RecordTempsReply` and `StationMaxReply` both have a string
field called `error`.

For a successful request, these should contain the empty string, `""`.

If executing a Cassandra statement raises a `cassandra.Unavailable`
except `e`, then the `error` should have a string like this:

```python
'need 3 replicas, but only have 2'
```

The specific for the error message can be found in
`e.required_replicas` and `e.alive_replicas`, respectively.

Sometimes `cassandra.Unavailable` is wrapped inside a
`cassandra.cluster.NoHostAvailable` exception.  If you catch a
`NoHostAvailable` exception `e`, loop over the values in the
`e.errors` dictionary.  If any of these values are of type
`cassandra.Unavailable`, use it to generate the same error as earlier
(like 'need 3 replicas, but only have 1').

For other errors/exceptions, you can decide what the `error` message
should be (we recommend choosing something that will help you debug).

#### Data Upload

Unzip `records.zip` to get `records.parquet`.  In your `p6.ipnynb`
notebook, use Spark to load this and re-arrange the data so that there
is (a) one row per station/date combination, and (b) tmin and tmax
columns.  You can ignore other measurements.

Collect and loop over the results, making a call to the server with
for each row to insert the measurements to the database.

Change number types and date formats as necessary.

#### Q5: what is the max temperature ever seen for station USW00014837?

The code in your notebook cell should make an RPC call to your server
to obtain the answer.

## Part 3: Spark Analysis

#### `stations` view

Create a temporary view in Spark named `stations` that corresponds to
the `stations` table in Cassandra.

Hint: you already enabled `CassandraSparkExtensions` when creating
your Spark session, so you can create a Spark DataFrame corresponding
to a Cassandra table like this:

```python
spark.read.format("org.apache.spark.sql.cassandra")
.option("spark.cassandra.connection.host", "p6-db-1,p6-db-2,p6-db-3")
.option("keyspace", ????)
.option("table", ????)
.load()
```

#### Q6: what tables/views are available in the Spark catalog?

You can answer with this:

```python
spark.catalog.listTables()
```

#### Q7: what is the average difference between tmax and tmin, for each of the four stations that have temperature records?

Use Spark to compute the answer, and convert to `dict` for your output, like this:

```python
{'USW00014839': 89.6986301369863,
 'USR0000WDDG': 102.06849315068493,
 'USW00014837': 105.62739726027397,
 'USW00014898': 102.93698630136986}
```

## Part 4: Disaster Strikes

**Important:** run a `docker` command to kill the `p6-db-2` container.

#### Q8: what does `nodetool status` output?

You can use the `! COMMAND` technique to show the output in a cell.

#### Q9: if you make a `StationMax` RPC call, what does the `error` field contain in `StationMaxReply` reply?

Choose any station you like for the call.

#### Q10: if you make a `RecordTempsRequest` RPC call, what does `error` contain in the `RecordTempsReply` reply?

Make up some data (station, date, tmin and tmax).  Inserts should
happen with `ConsistencyLevel.ONE`, so this ought to work, meaning the
empty string is the expected result for `error`.

## Submission

We should be able to run the following on your submission to create the mini cluster:

```
docker compose up -d
```

We should the be able to launch your server like this:

```
docker exec -it p6-db-1 python3 /nb/server.py
```

We should also be able to open `http://localhost:5000/lab`, find your notebook, and run it.

## Testing:

We also be using an autograder to verify your solution which you can run yourself by running the following command in the `p6` directory:
```
python3 autograder.py
```

This will create a `autograder_result` directory with the following content:
* `result.ipynb` : This will contain the result of the autograder running your notebook. You can look at this to debug your code
* `autograder.out` : This will contain both the stdout and stderr from running the autograder. You can examine this if you run into bugs with the autograder.
* `server.out` : This will contain the stdout and stderr from running your server code. You can examine this to debug your server code
