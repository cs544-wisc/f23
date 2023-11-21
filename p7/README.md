# DRAFT!  Don't start yet.

# P7 (6% of grade): Kafka, Weather Data

## Overview

For this project, imagine a scenario where you are receiving daily
weather data for a given location. Your task is to populate this data
into a Kafka stream using a *producer* Python program. A *consumer*
Python program consumes data from the stream to produce JSON files
with summary stats, for use on a web dashboard (you don't need to
build the dashboard yourself). Later in the project, the consumer
itself acts as a producer to populate certain messages into another
Kafka topic. Finally, you will visualize some of the data collected by
the consumer.

For simplicity, we use a single Kafka broker instead of using a
cluster.  A single producer will generate weather data in an infinite
loop at an accelerated rate (1 day per 0.5 second). Finally, consumers
will be different processes, launching from the same Python program.

Learning objectives:
* write code for Kafka producers
* write code for Kafka consumers
* apply streaming techniques to achive "exactly once" semantics
* write to different Kafka topics
* use manual and automatic assignment of Kafka topics and partitions
* ensure atomic writes to files

Before starting, please review the [general project directions](../projects.md).

## Clarifications/Correction
* none yet

## Container setup

Start by creating a `files` directory in your repository. Your Python programs and generated files must be stored in this directory.
Next, build a `p7` docker image with Kafka installed using the provided Dockerfile.
Run the Kafka broker in the background using:

```
docker run -d -v ./files:/files --name=p7 p7
```

You'll be creating three programs, `producer.py`, `debug.py`, and
`consumer.py`.  You can launch these in the container like this:
`docker exec -it p7 python3 /files/<path_of_program>`.  This will run
the program in the foreground, making it easier to debug.

All the programs you write for this projects will run forever, or
until manually killed.

## Part 1: Kafka Producer

### Topic Initialization

Create a `files/producer.py` that creates a `temperatures` topic with 4
partitions and 1 replica.  If the topic already existed, it should
first be deleted.

Feel free to use/adapt the following:

```python
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import UnknownTopicOrPartitionError

broker = 'localhost:9092'
admin_client = KafkaAdminClient(bootstrap_servers=[broker])

try:
    admin_client.delete_topics(["temperatures"])
    print("Deleted topics successfully")
except UnknownTopicOrPartitionError:
    print("Cannot delete topic/s (may not exist yet)")

time.sleep(3) # Deletion sometimes takes a while to reflect

temperatures_topic = NewTopic(name="temperatures", num_partitions=4, replication_factor=1)
admin_client.create_topics([temperatures_topic])

print("Topics:", admin_client.list_topics())
```

### Weather Generation

Using the provided `weather.py` file, you can infinitely generate
daily weather data starting from 1990-01-01 for a specific location
(loosely modelled around weather of Dane County). Copy `weather.py` to
your `files` directory and try generating some data using the
ufollowing code snippet. This will generate the weather at an
accelarated rate of 1 day per 0.5 second:

```python
import weather

# Runs infinitely because the weather never ends
for date, degrees in weather.get_next_weather(delay_sec=0.5):
    print(date, degrees) # date, max_temperature
```

Note: The above snippet is just for testing, don't include it in your submission.

Instead of printing the weather, create a KafkaProducer to send the
reports to the `temperatures` topic.

For the value format, use protobufs.  To start, create a protobuf file
`report.proto` in `files` with a `Report` message having the following
entries, and build it to get a `???_pb2.py` file (review P3 for how to
do this if necessary):

* string **date** (format "YYYY-MM-DD") - Date of the observation
* double **degrees**: Observed max-temperature on this date

### Requirements
1. Use a setting so that the producer retries up to 10 times when `send` requests fail
2. Use a setting so that the producer's `send` calls are not acknowledged until all in-sync replicas have received the data
3. When publishing to the `temperatures` stream, use the string representation of the month ('January', 'February', ...) as the message's `key`
4. Use a `.SerializeToString()` call to convert a protobuf object to bytes (not a string, despite the name)

### Running in Background

When your producer is finished, consider running it in the background indefinitely:

```
docker exec -d p7 python3 /files/producer.py
```

## Part 2: Kafka Debug Consumer

Create a `files/debug.py` program that initializes a KafkaConsumer.  It
could be in a consumer group named "debug".

The consumer should subscribe to the "temperatures" topic; let the
broker automatically assign the partitions.

The consumer should NOT seek to the beginning.  The consumer should
loop over messages forever, printing dictionaries corresponding to
each message, like the following:

```
...
{'partition': 2, 'key': 'December', 'date': '2000-12-26', 'degrees': 31.5235}
{'partition': 2, 'key': 'December', 'date': '2000-12-27', 'degrees': 35.5621}
{'partition': 2, 'key': 'December', 'date': '2000-12-28', 'degrees': 4.6093}
{'partition': 2, 'key': 'December', 'date': '2000-12-29', 'degrees': 26.3698}
{'partition': 2, 'key': 'December', 'date': '2000-12-30', 'degrees': 41.9125}
{'partition': 2, 'key': 'December', 'date': '2000-12-31', 'degrees': 46.1511}
{'partition': 2, 'key': 'January', 'date': '2001-01-01', 'degrees': 40.391}
...
```

Use your `debug.py` to verify your producer is writing to the stream
as expected.

## Part 3: Kafka Stats Consumer

Now you'll write a `files/consumer.py` that computes stats on the topic,
outputing results to JSON files after each batch.

### Partition Files

`consumer.py` will use manual partition assignment.  If it is launched
as `docker exec -it p7 python3 /files/consumer.py 0 2`, it should
assign partitions 0 and 2 of the `temperatures` topic.

Next, you will run 2 consumer processes to process the data from the
`temperatures` stream.  These consumers will generate JSON files
summarizing temperature data for different months. Write the consumer code in a new Python file named `consumer.py` within the `files` directory

Overview:
* there are 12 months but only 4 partitions, so naturally some partitions will correspond to data from multiple months
* each partition will correspond to one JSON file named `partition-N.json` (where N is the partition number). 
* there will be 4 JSON files, but we might launch fewer than 4 consumer.py processes, so each process should be capable of keeping multiple JSON files updated

When a consumer launches that is responsible for partition N, it
should check whether `partition-N.json` exists.  If it does not exist,
your consumer should first initialize it to `{"partition": N,
"offset": 0}`.

Your consumer should then load `partition-N.json` to a Python
dictionary.  To manage each partition in memory, you might want a
dictionary where each key is a partition number and each value is a
dictionary with data for that partition.

When the `partition-N.json` files are loaded, your consumer should
`seek` on each partition to the offset specified in the file.

### Message Processing

TODO


The per-partition JSON files store year-wise summaries of months that
map to the corresponding partition. Following is an example of a JSON
file corresponding to partition 2 (due to several factors you'll
probably never see this exact data):

```json
{
  "partition": 2,
  "offset": 4117,

  "January": {
    "1990": {
      "count": 31,
      "sum": 599,
      "avg": 19.322580645161292,
      "end": "1990-01-31",
      "start": "1990-01-01"
    },
    "1991": {
      "count": 31,
      "sum": 178,
      "avg": 5.741935483870968,
      "end": "1991-01-31",
      "start": "1991-01-01"
    }
  },

  "April": {
    "1990": {
      "count": 30,
      "sum": 1113,
      "avg": 37.1,
      "end": "1990-04-30",
      "start": "1990-04-01"
    },
    "1991": {
      "count": 30,
      "sum": 1149,
      "avg": 38.3,
      "end": "1991-04-30",
      "start": "1991-04-01"
    }
  }
}
```

The JSON file has the following keys at the top/outermost level:

* `partition`: the partition number in the temperatures stream, from which the stats were computed.
* `offset`: the partition offset up to which the consumer completed reading to produce this file (sort of like a checkpoint)
* month-key: one key for each month in the partition (in the above example, months "January" and "April")

Each month-key ("January", "April", etc.) maps to another set of keys
representing the years for which weather data was captured. For each
year mapped to a month, there's a collection of fields that summarizes
the weather for that month in that specific year. For a given year,
this summary includes:

* `count`: the number of days for which data is available.
* `sum`: sum of temperatures seen so far (yes, this is an odd metric by itself)
* `avg`: the `sum/count`. This is the only reason we record the sum - so we can recompute the average on a running basis without having to remember and loop over all temperatures each time the file is updated
* `start`: the date of the *first* measurement for the corresponding
month and year combination
* `end`: the date of the *last* measurement for the corresponding
month and year combination

In the sample JSON, under "January" for the year "1990", the count is
31 (indicating data for all 31 days of January was recorded), the sum
is 599, the avg is approximately 19.32, and the data collection period
is from "1990-01-01" to "1990-01-31". This pattern repeats for each
month and year combination in the dataset.

### `temperatures` Consumer

Let us now create the consume method to read the messages from Kafka.
You can use the following starter code for your consumer processes
(recall we want two consumers for the 4 partitions in `temperatures`):

```python
def consume(partition_nums_to_consume, max_iterations):
    consumer = ????
    # TODO: create list of TopicPartition objects
    consumer.assign(????)

    # PART 1: initialization
    
    # key=partition num, value=snapshot dict
    partitions = {} 

    # TODO: load partitions from JSON files (if they exist) or create fresh dicts
    # TODO: if offsets were specified in previous JSON files, the consumer
    #       should seek to those; else, seek to offset 0.

    # PART 2: process batches
    iteration_count = 0
    while max_iterations is None or iteration_count < max_iterations:
        batch = consumer.poll(1000) # 1s timeout
        for topic, messages in batch.items():
            # perhaps create a separate function for the following?  You decide.
            # TODO: update the partitions based on new messages
            # TODO: save the data back to the JSON file
        
        # Run infinitely if 'max_iterations' is None
        if max_iterations is not None: iteration_count += 1

    print("exiting")
```

Note that instead of when the `max_iterations` argument of the
`consume` method is `None`, the consumer runs infinitely. However, if
a numeric value is passed, the consumer only runs for the specified
number of iterations. Typically, the consumer will run infinitely.

Now we will perform **two rounds** of running the consumers. In the
first round, the consumers run 30 iterations each and then stop. In
the next round, we run the consumers without any limit on the number
of iteration (so `max_iterations` is None) and leave them running in
the background. The point here is to practice writing your consumers
so that if they die and get restarted, the new consumers can pickup
where the previous ones left off, while avoiding both these problems:

* missing some messages
* double counting some messages

Your code for running these two rounds would look like this:
```python3
# Round 1: Run consumers for 30 iterations
  # Create thread to run consumer 1 for 30 iterations
  # Create thread to run consumer 2 for 30 iterations
  # Run both consumer processes (parallelly)
  # Wait for both consumer processes to complete (simulates consumers dying)

# Round 2: Run consumers for infinite iterations
  # Same as round 1, except no limit on iterations
  # Do not join these processes, let them run infinitely in the background
```

### Required: Exactly-Once Kafka Messages

Unless the producer repeatedly fails to write a message, each
message/measurement should be counted exactly once in the output stats.

To avoid **undercounting**:
* the producer must use an ackowledgement option so that `send` is not considered successful until all in-sync replicas have received the data
* the produce must use an option to retry up to 10 times upon failure to receive such a message

To avoid **overcounting**:
* when a consumer updates a JSON file, it must record the current read offset in the file (you can get this with `consumer.position(???)`)
* when a new consumer starts, it must start reading from the last offset (you can do this with `consumer.seek(????)`)
* when a consumer reads a message, it should ignore it if the date is <= the previous date processed (the `end` entry in the station dict will help with this).  Remember that producers retry when they don't get an ack, but it's possible for an ack to be lost after a successful write. So retry could produce duplicates in the stream. To highlight this, the weather generation function has a 5% chance of generating a duplicated value, so keep this in mind. Other than these duplicated entries, you may assume all other messages are generated in order.

## Part 4: Visualizing Yearly Trends

In this part, we will visualize the weather summary, for a given month, stored in the partition json files. Read the partition files to identify which file contains weather data for the month of **January**. For this month, plot a bar chart with its x-axis representing the years over which weather summary data exists. The y-axis should represent the average temperature for each year in January. Save the graph in the `files` directory under the name `yearly_trend.png`

Since data is continuously being produced in the background, your plot will keep getting outdated and will need refreshing. Use the below structure to re-generate the plot every 30 seconds to reflect the updated data in the generated png:

```python
target_month = "January"
# TODO: Find path of partition file storing summary for January
partition_file_with_tgt = ???

fig, ax = plt.subplots()
# Keep refreshing the plot after 5 seconds
while True:
    time.sleep(5)

    try:
      # TODO: Read January's yearly temperatures from 'partition_file_with_tgt'
    except Exception as e:
      print("Failed reading partition for January, retrying...")
      continue    

    # Clear the previous plot data
    ax.clear()
    # Update the plot with new data
    ax.bar(x, y)
    ax.set_xlabel('Year')
    ax.set_ylabel('Avg. Max Temperature for January')
    ax.set_title('Avg. Max Temperatures per year')
    plt.xticks(rotation=90)

    # Save the plot
    plt.savefig('/files/yearly_trends.png')
    print("Refreshed Graph")
```

![Yearly trends plot](./yearly_trends.png "Yearly trends plot for January")

### Atomic File Writes

Remember that we're producing the JSON files so somebody else (not
you) can use them to build a web dashboard. We are also using the JSON files to continously plot graphs. What if the dashboard app or the graph plotting program tries to read the JSON file at the same time your consumer is updating
the file? It's possible the dashboard or plotting app could read an
incomprehensible mix of old and new data.

To prevent such partial writes, the proper technique is to write
a new version of the data to a different file.  For example, say the
original file is `F.txt` -- you might write the new version to
`F.txt.tmp`.  After the new data has been completely written, you can
rename F.txt.tmp to F.txt.  This atomically replaces the file
contents. Anybody trying to read it will see all old data or all new
data. Make these changes to your `save_dict_to_json` function that
is responsible for saving summary JSON files (both partition JSON files
and birthday.json file)

```python
path = ????
path2 = path + ".tmp"
with open(path2, "w") as f:
    # TODO: write the data
    os.rename(path2, path)
```

Note that this only provides atomicity when the system doesn't crash.
If the computer crashes and restarts, it's possible some of the writes
for the new file might only have been buffered in memory, not yet
written to the storage device.  Feel free to read about `fsync` if
you're curious about this scenario.

## Submission
All your code and generated files (partition json files and graphs) should be in a directory named `files` within your repository.
Your generated files (partition JSON files, graph, birthday JSON file) must contain data for atleast 3 years starting from 1990.

We should be able to run the following on your submission to build and run the required image:

```
# To build the image
docker build . -t p7

# To run the kafka broker
docker run -d -v ./files:/files p7

# To run the producer program
docker exec -it <container_name> python3 /files/producer.py
# To run the consumer program
docker exec -it <container_name> python3 /files/consumer.py
```

Verify that your submission repo has a structure similar to this:

```
.
├── Dockerfile
└── files
    ├── producer.py
    ├── consumer.py
    ├── report.proto
    ├── partition-0.json
    ├── partition-1.json
    ├── partition-2.json
    ├── partition-3.json
    ├── yearly_trends.png
    ├── birthday.json
    ├── report_pb2.ipynb
    └── weather.py
```

# Testing
We will be using an autograder to verify your solution which you can run yourself by running the following command in the p7 directory:

```
python3 autograde.py
```