# DRAFT!  Don't start yet.

# P4 (6% of grade): HDFS Replication

## Overview

**Remember to switch to an E2 Medium for this project:** [VM schedule](../projects.md#compute-setup).

HDFS can *partition* large files into blocks to share the storage
across many workers, and it can *replicate* those blocks so that data
is not lost even if some workers die.

In this project, you'll deploy a small HDFS cluster and upload a large
file to it, with different replication settings.  You'll write Python
code to read the file.  When data is partially lost (due to a node
failing), your code will recover as much data as possible from the
damaged file.

Learning objectives:
* use the HDFS command line client to upload files
* use the webhdfs API (https://hadoop.apache.org/docs/r1.0.4/webhdfs.html) to read files
* measure the impact buffering has on read performance
* relate replication count to space efficiency and fault tolerance

Before starting, please review the [general project directions](../projects.md).

## Corrections/Clarifications

* None yet

## Part 1: HDFS Deployment and Data Upload

#### Cluster

For this project, you'll deploy a small cluster of containers, one
with Jupyter, one with an HDFS NameNode, and two with HDFS DataNodes.

TODO: memory limits in compose file...

We have given you `docker-compose.yml` for starting the cluster, but
you need to build some images first.  Start with the following:

```
docker build . -f hdfs.Dockerfile -t p4-hdfs
docker build . -f notebook.Dockerfile -t p4-nb
```

The second image depends on the first one (`p4-hdfs`) -- you can see
this by checking the `FROM` line in "notebook.Dockerfile".

The compose file also needs `p4-nn` (NameNode) and `p4-dn` (DataNode)
images.  Create Dockerfiles for these that can be built like this:

```
docker build . -f namenode.Dockerfile -t p4-nn
docker build . -f datanode.Dockerfile -t p4-dn
```

Requirements:
* like `p4-nb`, both these should use `p4-hdfs` as a base
* `namenode.Dockerfile` should run two commands, `hdfs namenode -format` and `hdfs namenode -D dfs.namenode.stale.datanode.interval=10000 -D dfs.namenode.heartbeat.recheck-interval=30000 -fs hdfs://boss:9000`
* `datanode.Dockerfile` should just run `hdfs datanode -D dfs.datanode.data.dir=/var/datanode -fs hdfs://boss:9000`

You can use `docker compose up -d` to start your mini cluster.  You
can run `docker compose kill; docker compose rm -f` to stop and delete
all the containers in your cluster as needed.  For simplicity, we
recommend this rather than restarting a single container when you need
to change something as it avoids some tricky issues with HDFS.  For
example, if you just restart+reformat the container with the NameNode,
the old DataNodes will not work with the new NameNode without a more
complicated process/config.

#### Q1: how many live DataNodes are in the cluster?

Write a cell like this:

```
#q1
! SHELL COMMAND TO CHECK
```

The shell command should generate a report by passing some arguments
to `hdfs dfsadmin`.  The output should contain a line like this:

```
...
Live datanodes (2):
...
```

#### Data Upload

Connect to JupyterLab running in the `p4-nb` container, and create a
notebook called `p4a.ipynb` in the "/nb" directory (we'll do some
later work in another notebook, `p4b.ipynb`).

Write some code (Python or shell) that downloads
https://pages.cs.wisc.edu/~harter/cs544/data/hdma-wi-2021.csv if it
hasn't already been downloaded.

Next, use two `hdfs dfs -cp` commands to upload this same file to HDFS
twice, to the following locations:

* `hdfs://boss:9000/single.csv`
* `hdfs://boss:9000/double.csv`

In both cases, use a 1MB block size (`dfs.block.size`), and
replication (`dfs.replication`) of 1 and 2 for `single.csv` and
`double.csv`, respectively.

#### Q2: what are the logical and physical sizes of the CSV files?

Run a `du` command with `hdfs dfs` to see.

You should see something like this:

```
166.8 M  333.7 M  hdfs://main:9000/double.csv
166.8 M  166.8 M  hdfs://main:9000/single.csv
```

The first columns show the logical and physical sizes.  The two CSVs
contain the same data, so the have the same logical sizes.  Note the
difference in physical size due to replication, though.

## Part 2: Block Locations

If you correctly configured the block size, single.csv should have 167
blocks, some of which will be stored on each of your two
Datanodes. Your job is to write some Python code to count how many
blocks are stored on each worker by using the webhdfs interface.

Read about the `OPEN` call here:
https://hadoop.apache.org/docs/r1.0.4/webhdfs.html#OPEN.

Adapt the curl examples to use `requests.get` in Python instead.  Your URLs will be like this:

```
http://boss:9870/webhdfs/v1/single.csv?op=OPEN&offset=????
```

Note that `boss:9870` is the NameNode, which will reply with a redirection response that sends you to a Datanode for the actual data.

If you pass `allow_redirects=False` to `requests.get` and look at the
`.headers` of the Namenode's repsonse, you will be able to infer which Datanode stores that data corresponding to a specific offset in the file.  Loop over offsets corresponding to the start of each block (your blocksize is 1MB, so the offsets will be 0, 1MB, 2MB, etc).

Construct a dictionary named `per_worker_block_count_single_csv` (or use the trick in the comments). Your dictionary keys should be the web addresses / ports  of each datanote as shown below. Your result should look like the following that shows how many blocks of `single.csv` are on each Datanode:

```
{'http://70d2c4b6ccee:9864/webhdfs/v1/single.csv': 92,
 'http://890e8d910f92:9864/webhdfs/v1/single.csv': 75}
```

Your data will probably be distributed differently between the two, and you will almost certainly have container names that are different than `70d2c4b6ccee` and `890e8d910f92`. Run cell 2.1 and add up your blocks to sanity check your answer. 

## Part 3: Reading the Data

In this part, you'll make a new reader class that makes it easy to loop over the lines in an HDFS file.

You'll do this by inheriting from the `io.RawIOBase` class: https://docs.python.org/3/library/io.html#class-hierarchy.  Here is some starter code:

```python
import io

class hdfsFile(io.RawIOBase):
    def __init__(self, path):
        self.path = path
        self.offset = 0
        self.length = 0 # TODO

    def readable(self):
        return True

    def readinto(self, b):
        return 0 # TODO
```

In the end, somebody should be able to loop over the lines in an HDFS file like this:

```python
for line in io.BufferedReader(hdfsFile("single.csv")):
    line = str(line, "utf-8")
    print(line)
```


Implementation:

* use `GETFILESTATUS` (https://hadoop.apache.org/docs/r1.0.4/webhdfs.html#GETFILESTATUS) to correctly set `self.length` in the constructor
* whenever `readinto` is called, read some data at position `self.offset` from the HDFS file: https://hadoop.apache.org/docs/r1.0.4/webhdfs.html#OPEN
* the data you read should be put into `b`.   The type of `b` will generally be a `memoryview` which is like a fixed-size list of bytes.  You can use slices to put values here (something like `b[0:3] = b'abc'`).  When you place values in b, place them at the beginning of `b` like so `b[:len(values)] = values`. Since `b` is fixed size, you should use `len(b)` to determine how much data to request from HDFS.
* `readinto` should return the number of bytes written into `b` -- this will usually be the `len(b)`, but not always (for example, when you get to the end of the HDFS file), so base it on how much data you get from webhdfs.
* before returning from `readinto`, increase `self.offset` so that the next read picks up where the previous one left off
* if `self.offset >= self.length`, you know you're at the end of the file, so `readinto` should return 0 without calling to webhdfs

Use your class to loop over every line of single.csv.

Count how many lines contain the text "Single Family" and how many contain "Multifamily". Your counts should look similar to the following example:

```
Counts from single.csv
Single Family: 444874
Multi Family: 2493
Seconds: 24.33926248550415
```

We provide you with template code and variables that you will need to use. After you implement your code, make sure you check your answers using cells 3.1 - 3.7. 


Note that by default `io.BufferedReader` uses 8KB for buffering, which creates many small reads to HDFS, so your code will be unreasonably slow.  Experiment with setting `bs1` and `bs2` to different values and notice how the different buffer sizes change the amount of processing time. 

Your code should show at least two different sizes you tried (and the resulting times).

## Part 4: Disaster Strikes

Switch to `p3-part2.ipynb`. The rest of your project development will occur there. 


You have two datanodes.  What do you think will happen to `single.csv` and `double.csv` if one of these nodes dies?

Find out by manually running a `docker kill <CONTAINER NAME>` command on your VM to abruptly stop one of the Datanodes.

Wait until the Namenode realizes the Datanode has died before proceeding.  Run `!hdfs dfsadmin -fs hdfs://main:9000/ -report` (cell 4.1) in your notebook to see when this happens.  Before proceeding, the report should show one dead Datanode, something like the following:

<details>
<summary>Expand</summary>
<pre>
Configured Capacity: 83111043072 (77.40 GB)
Present Capacity: 25509035132 (23.76 GB)
DFS Remaining: 24980049920 (23.26 GB)
DFS Used: 528985212 (504.48 MB)
DFS Used%: 2.07%
Replicated Blocks:
	Under replicated blocks: 0
	Blocks with corrupt replicas: 0
	Missing blocks: 0
	Missing blocks (with replication factor 1): 0
	Low redundancy blocks with highest priority to recover: 0
	Pending deletion blocks: 0
Erasure Coded Block Groups: 
	Low redundancy block groups: 0
	Block groups with corrupt internal blocks: 0
	Missing block groups: 0
	Low redundancy blocks with highest priority to recover: 0
	Pending deletion blocks: 0

Live datanodes (1):

Name: 172.19.0.4:9866 (spark-lecture-worker-1.cs544net)
Hostname: 890e8d910f92
Decommission Status : Normal
Configured Capacity: 41555521536 (38.70 GB)
DFS Used: 255594721 (243.75 MB)
Non DFS Used: 28793143071 (26.82 GB)
DFS Remaining: 12490006528 (11.63 GB)
DFS Used%: 0.62%
DFS Remaining%: 30.06%
Configured Cache Capacity: 0 (0 B)
Cache Used: 0 (0 B)
Cache Remaining: 0 (0 B)
Cache Used%: 100.00%
Cache Remaining%: 0.00%
Xceivers: 1
Last contact: Sat Dec 31 20:07:46 GMT 2022
Last Block Report: Sat Dec 31 20:04:00 GMT 2022
Num of Blocks: 242

Dead datanodes (1):

Name: 172.19.0.3:9866 (172.19.0.3)
Hostname: 70d2c4b6ccee
Decommission Status : Normal
Configured Capacity: 41555521536 (38.70 GB)
DFS Used: 273390491 (260.73 MB)
Non DFS Used: 28775310437 (26.80 GB)
DFS Remaining: 12490043392 (11.63 GB)
DFS Used%: 0.66%
DFS Remaining%: 30.06%
Configured Cache Capacity: 0 (0 B)
Cache Used: 0 (0 B)
Cache Remaining: 0 (0 B)
Cache Used%: 100.00%
Cache Remaining%: 0.00%
Xceivers: 1
Last contact: Sat Dec 31 20:06:15 GMT 2022
Last Block Report: Sat Dec 31 20:04:00 GMT 2022
Num of Blocks: 259
</pre>
</details>

<br>
Note that HDFS datanodes use heartbeats to inform the namenode of its liveness. That is, the datanodes send a small dummy message (heartbeat) periodically (every 3 seconds by default) to inform the namenode of its presence. Recall that when we start the namenode, we specify `dfs.namenode.stale.datanode.interval=10000` and `dfs.namenode.heartbeat.recheck-interval=30000`. The first says that the namenode considers the datanode stale if it does not receive its heartbeat for 10 seconds (10000 ms) and the second says that it will consider the datanode dead after another 30 seconds. Hence, if you configure your cluster correctly, the namenode will become aware of the loss of datanode within 40 seconds after you killed the datanode. 

Run your code from part 3 again that counts the multi and single family dwellings in new cells.  Do so on both double.csv and single.csv. We again provide template code and variables for you to insert your implementation in. 

double.csv should work just like before.  You will have lost some blocks from single.csv, but modify `readinto` in your `hdfsFile` class so that it still returns as much data as possible.  If reading a block
from webhdfs fails, `readinto` should do the following:

1. put `\n` into the `b` buffer
2. move `self.offset` forward to the start of the next block
3. return 1 (because `\n` is a single character)

You should get some prints like this:

```
Counts from double.csv
Single Family: 444874
Multi Family: 2493
```

AND

```
Counts from single.csv
Single: 200608
Multi: 929
```

Observe that we're still getting some lines from single.csv, but only about half as many as before the data loss (exact counts will depend on how many blocks each datanode was storing).

## Submission

We should be able to run the following on your submission to create the mini cluster:

```
docker build -t p3-image ./image
docker compose up
```

We should then be able to open `http://localhost:5000/lab`, find your
notebook, and run it.

## Tester:

TODO
