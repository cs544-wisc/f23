# P5 (6% of grade): Spark, Loan Applications

## Overview

In P5, we'll use Spark to analyze loan applications in WI.  You'll
load your data to Hive tables and views so you can easily query them.
The big table (loans) has many IDs in columns; you'll need to join
these against other tables or views to determine the meaning of these
IDs.  In addition, you'll practice training a Decision Tree model to
predict loan approval.

**Important:** you'll answer 10 questions in P5.  Write
  each question and it's number (e.g., "#q1: ...") as a comment in your
  notebook prior to each answer so we can easily search your notebook
  and give you credit for your answers.

Learning objectives:

* use Spark's RDD, DataFrame, and SQL interfaces to answer question about data
* load data into Hive for querying with Spark
* optimize queries with bucketing and caching
* train a Decision Tree model

Before starting, please revisit the [general project directions](../projects.md).

## Corrections/Clarifications

* none yet

## Cluster Setup

### Virtual Machine

~4 GB is barely enough for P5. Brefore you start, take a moment to enable a 1
GB swap file to supplement.  A swap file is on storage, but acts
as extra memory. This has performance implications as storage is
much slower than RAM (as what we have studied in class).  


```
# https://www.digitalocean.com/community/tutorials/how-to-add-swap-space-on-ubuntu-22-04
sudo fallocate -l 1G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
# htop should show 1 GB of swap beneath memory
```

### Containers

For this project, you'll deploy a small cluster of containers:

* `p5-nb` (1 Jupyter container)
* `p5-nn` (1 NameNode container)
* `p5-dn` (1 DataNode container)
* `p5-boss` (1 Spark boss container)
* `p5-worker` (2 Spark worker containers)

You should be able to build all these images like this:

```
docker build . -f p5-base.Dockerfile -t p5-base
docker build . -f notebook.Dockerfile -t p5-nb
docker build . -f namenode.Dockerfile -t p5-nn
docker build . -f datanode.Dockerfile -t p5-dn
docker build . -f boss.Dockerfile -t p5-boss
docker build . -f worker.Dockerfile -t p5-worker
```

We provide most of the Dockerfiles mentioned above, but you'll need to write 
`boss.Dockerfile` and `worker.Dockerfile` yourself. These Dockerfiles will invoke
the Spark boss and workers and will use `p5-base` as their base Docker image.

To start the Spark boss and workers, you will need to run the `start-master.sh` 
and `start-worker.sh
spark://boss:7077 -c 1 -m 512M` commands respectively (you'll need to specify
the full path to these .sh scripts). These scripts launch the Spark
boss and workers in the background and then exit. Make sure that the containers
do not exit along with the script and instead keep running until manually stopped.

You should then be able to use the `docker-compose.yml` we proved to
run `docker compose up -d`.  Wait a bit and make sure all containers
are still running.  If some are starting up and then exiting, troubleshoot
the reason before proceeding.

## Data Setup

### Virtual Machine

The Docker Compose setup maps a	`nb` directory into your Jupyter
container.  Within `nb`, you need to create a subdirectory called
`data` and fill it with some CSVs you'll use for the project.

You can	run the	following on your VM (not in any container):

```
wget https://pages.cs.wisc.edu/~harter/cs544/data/hdma-wi-2021.zip
wget https://pages.cs.wisc.edu/~harter/cs544/data/arid2017_to_lei_xref_csv.zip
wget https://pages.cs.wisc.edu/~harter/cs544/data/code_sheets.zip
mkdir -p nb/data
unzip -o hdma-wi-2021.zip -d nb/data
unzip -o arid2017_to_lei_xref_csv.zip -d nb/data
unzip -o code_sheets.zip -d nb/data
```

You'll probably	need to	change some permissions	(`chmod`) or run as
root (`sudo su`) to be able to do this.

### Jupyter Container

Connect to JupyterLab inside your container, and create a notebook
called `p5.ipynb`.  Put your name(s) in a comment at the top.

Run a shell command (`hdfs dfs -D dfs.replication=1 -cp -f data/*.csv
hdfs://nn:9000/` in a cell to upload the CSVs from the local file
system to HDFS).

## Part 1: Filtering: RDDs, DataFrames, and Spark

Inside your `p5.ipynb` notebook, create a Spark session (note we're enabling
Hive on HDFS):

```python
from pyspark.sql import SparkSession
spark = (SparkSession.builder.appName("cs544")
         .master("spark://boss:7077")
         .config("spark.executor.memory", "512M")
         .config("spark.sql.warehouse.dir", "hdfs://nn:9000/user/hive/warehouse")
         .enableHiveSupport()
         .getOrCreate())
```

#### Q1: how many banks contain the word "first" in their name, ignoring case?  Use an **RDD** to answer.

The `arid2017_to_lei_xref_csv.csv` contains the banks, so you can use the following to read it to a DataFrame.

```python
# TODO: modify to treat the first row as a header
# TODO: modify to infer the schema
banks_df = spark.read.csv("hdfs://nn:9000/arid2017_to_lei_xref_csv.csv")
```

From this DataFrame, you can use `banks_df.rdd` to get the underlying
RDD you need for this question.

Use a `filter` transformation (that takes a Python lambda) with a
`count` action to get the answer.

As an practice, you could run this:

```python
rows = df.rdd.take(3)
```

The try to extract the name from one of the rows with a little Python
code.  This will help you determine how to write your lambda (which
will take a `Row` and return a boolean).

Use the some format as previous projects for all your notebook answers
(last line of a cell contains an expression giving the answer, and the
cell starts with a "#q1" comment, or similar).

#### Q2: how many banks contain the word "first" in their name, ignoring case?  Use a **DataFrame** to answer.

This is the same as Q1, but now you must operate on `banks_df` itself,
without directly accessing the underlying `RDD`.

DataFrames also have `filter` transformations and `count` actions, but
`filter` takes a string containing a condition.  The condition uses
the same syntax as a condition in SQL, so these resources may help:

* https://www.w3schools.com/sql/sql_like.asp
* https://www.w3schools.com/sql/func_sqlserver_lower.asp

#### Q3: how many banks contain the word "first" in their name, ignoring case?  Use **Spark SQL** to answer.

To write a SQL query to answer this, we first need to load into a Hive
table.  You can do so with this:

```python
banks_df.write.saveAsTable("banks", mode="overwrite")
```

Now you can use `spark.sql(????)` with a SQL query you write to get
the answer.  This call will return the results as a Spark DataFrame --
you'll need to do a little extra Python work to get this out as a
single int for your answer.

## Part 2: Hive Data Warehouse

#### `loans` table

You have already added a `banks` table to Hive (using the command we
shared with you).

Now, write similar code `hdma-wi-2021.csv` into a table called
`loans`.

Note that `writeTable` will produce one or more Parquet files in
`hdfs://nn:9000/user/hive/warehouse` (look back at how you created
your Spark session to see this).

Use
[`bucketBy`](https://spark.apache.org/docs/3.1.1/api/python/reference/api/pyspark.sql.DataFrameWriter.bucketBy.html)
with your `writeTable` call to create 8 buckets on column
`county_code`.  This means that your written data will be broken into
8 buckets/groups, and all the rows will the same county will be in the
same bucket/group.  This will make some queries faster (for example,
if you `GROUP BY` on `county_code`, Spark might be able to avoid
shuffling/exchanging data across partitions/machines).

#### Other views

Use `createOrReplaceTempView` to create Hive views for each of the names in this list:

```python
["ethnicity", "race", "sex", "states", "counties", "tracts", "action_taken",
 "denial_reason", "loan_type", "loan_purpose", "preapproval", "property_type"]
```

The contents should correspond to the CSV files of the same name in
HDFS.  Don't forget about headers and schema inference!

#### Q4: what tables are in our warehouse?

You can use `spark.sql("SHOW TABLES").show()` to see your tables in the warehouse as follows. 

```
+---------+-------------+-----------+
|namespace|    tableName|isTemporary|
+---------+-------------+-----------+
|  default|        banks|      false|
|  default|        loans|      false|
|         | action_taken|       true|
|         |     counties|       true|
|         |denial_reason|       true|
|         |    ethnicity|       true|
|         | loan_purpose|       true|
|         |    loan_type|       true|
|         |  preapproval|       true|
|         |property_type|       true|
|         |         race|       true|
|         |          sex|       true|
|         |       states|       true|
|         |       tracts|       true|
+---------+-------------+-----------+
```
Answer with a Python dict looks like this
```python
{'banks': False, 
 'loans': False, 
 'action_taken': True, 
 'counties': True, 
 'denial_reason': True, 
 'ethnicity': True, 
 'loan_purpose': True, 
 'loan_type': True, 
 'preapproval': True, 
 'property_type': True, 
 'race': True, 
 'sex': True, 
 'states': True, 
 'tracts': True
 }
```


#### Q5: how many loan applications has the bank "University of Wisconsin Credit Union" received in 2020 in this dataset?

Use an `INNER JOIN` between `banks` (`banks.lei_2020`) and `loans` (`loans.lei`) to answer this
question.  `lei` in `loans` lets you identify the bank.  Filter on
`respondent_name` (do NOT hardcode the LEI).

#### Q6: what does `.explain("formatted")` tell us about how Spark executes Q5?

Show the output, then write comments (which we will manually grade) explaining the following:

1. Which table is sent to every executor via a `BroadcastExchange` operation?
2. Does the plan involve `HashAggregate`s (depending on how you write the query, it may or may not)?  If so, which ones?

## Part 3: Grouping Rows

#### Q7: what are the average interest rates for Wells Fargo applications for the ten counties where Wells Fargo receives the most applications?

Answer with a Python `dict` that looks like this:

```python
{'Milwaukee': 3.1173465727097907,
 'Waukesha': 2.8758225602027756,
 'Washington': 2.851009389671362,
 'Dane': 2.890674955595027,
 'Brown': 3.010949119373777,
 'Racine': 3.099783715012723,
 'Outagamie': 2.979661835748792,
 'Winnebago': 3.0284761904761908,
 'Ozaukee': 2.8673765432098772,
 'Sheboygan': 2.995511111111111}
```

The cell following your answer should have a plot that looks like this:

<img src="q7.png" width=500>

The bars are sorted by the number of applications in each county (for
example, most applications are in Milwaukee, Waukesha is second most,
etc).

#### Q8: when computing a MEAN aggregate per group of loans, under what situation (when) do we require network I/O between the `partial_mean` and `mean` operations?

Write some simple `GROUP BY` queries on `loans` and call `.explain()`.
Try grouping by the `county_code`. Then try grouping by the `lei`
column.

If a network transfer (network I/O) is necessary for one query but not the other,
write a comment explaining why.  You might want to look back at how
you loaded the data to a Hive table earlier.

Write your answer as a Python comment.

## Part 4: Machine Learning

The objective of Part 4 is to use the given loan dataset to train a
Decision Tree model that can predict outcomes of loan applications
(approved or not). Recall that a loan is approve if `action_taken` is
"Loan originated".

We call our label `approval`, indicating the whether of a loan
application is approved or not (`1` for approved, `0` otherwise). And
for this exercise, we will use the features `loan_amount`, `income`,
`interest_rate` in `loans` table for prediction.

First, as a prepartory step, get the features and label from the loans
table into a new dataframe `df`. Cast the `loan_amount` and `income`
columns to `double` type and fill missing values by 0.0.

Then split `df` as follows:

```python
# deterministic split
train, test = df.randomSplit([0.8, 0.2], seed=41) 
```

Cache the `train` DataFrame.

#### Q9. How many loans are approved (`approval = 1`) in the `train` DataFrame?
Answer with a single number.

#### Q10. What is the accuracy of the decision tree classifier of depth 5 on the test dataset?

You'll need to train a decision tree first.  Start with some imports:

```python
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import DecisionTreeClassifier
```

Use the VectorAssembler to combine the feature columns `loan_amount`,
`income`, `interest_rate` into a single column.

Train a `DecisionTreeClassifier` of max depth 5 (and default arguments
for other parameters) on your training data to predict `approved`
based on the features.

Use the model to make predictions on the test data.  What is the
*accuracy* (fraction of times the model is correct)?

## Submission

Be sure to include a comment at the top of notebook with your name (or both names if you worked as partners).

We should be able to run the following on your submission to directly create the mini cluster:

```
docker compose up -d
```

We should then be able to open `http://localhost:5000/lab`, find your
notebook, and run it.

## Testing


After copying `../tester.py`, `../nbutils.py`, and `autograde.py` to
your repository (where your `nb/` directory is located), you can check
your notebook answers with this command:

```sh
python3 autograde.py
```

For the autograder to work, for each question, please include a line of comment at the beginning of code cell that outputs the answer. For example, the code cell for question 7 should look like
```python
#q7
...
```

Of course, the checker only looks at the answers, not how you got them, so there may be further deductions (especially in the case of hardcoding answers). Moreover, Q6 and Q8 will be manually graded after your submission, so the autograder will not give you any feedback on them (it always says `PASS`)!
