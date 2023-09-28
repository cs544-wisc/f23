# DRAFT! Don't start yet.

# P3 (6% of grade): Key/Value Store Service

## Overview

In the last project, we focused on creating a PyTorch regression model. We will now build upon this to provide an interface to use this model via a service that is able to be shared by code running on different computers.

In this project, you'll build a simple [model serving](https://deci.ai/deep-learning-glossary/model-serving/) server that that will be able to serve a regression model, like the one made in P2. Clients will be able to communicate with your server via RPC calls.

Because executing the regression model may become expensive as the model grows in size, you will implement a caching mechanism for your service such that the model does not need to recompute the output of the same input. For this caching mechanism, you'll need to use threads and locking for efficiency and safety.

Learning objectives:

-   Communicate between clients and servers via gRPC
-   Cache expensive compute results
-   Use locking for safety
-   Measure performance statistics like cache hit rate and tail latency
-   Deploy your code in a Docker container

Before starting, please review the [general project directions](../projects.md).

## Corrections/Clarifications

N/A

## Part 1: ModelServer class

You will implement the following equation:

$y=\mathbf{a}\cdot \mathbf{x}$

where $\mathbf{a}$ are some coefficients and $\mathbf{x}$ some data.

Write a class with two methods: `SetCoefs(coefs)` and `Predict(x)` to perform dot product. Each parameter should be an array. Also check that the lengths match in `Predict`.

### `SetCoefs(coefs)`

Your class should store these coefficients and invalidate the entire cache (more on this in a bit).

### `Predict(x)`

Your server will able to predict the output of some given input vector corresponding to the inputs to be predicted on. Define the computation to be a simple dot-product of `coefs` and `x`.
After computing the output, your server will store the output in a cache using `x` as the cache key (more on this in the next section, but you may use something like `dict`) for subsequent lookup. Please use `np.round` to 4 decimal places since very similar inputs will map to the same output (see also: [floating point limitations](https://docs.python.org/3/tutorial/floatingpoint.html)).

Return an object with the following fields:

-   `output`: the output of the dot product
-   `cache_hit`: a boolean indicating whether or not the value was read from the cache

If there is some kind of exception or `x` does not match do not match the expected length, return unsuccessfully with an error message.

### Caching

Though our model is small, a more complex model may be computationally slow, especially in the case where the model requires a GPU (or even a cluster of computers!) to run.

Add an [LRU cache](<https://en.wikipedia.org/wiki/Cache_replacement_policies#Least_recently_used_(LRU)>) to _remember_ previously computed outputs for specific keys. Note that dictionary keys _must_ be hashable, and `np.array`s are not hashable.

Requirements:

-   The cache should hold a maximum of 10 entries
-   Check if a key is present during prediction before calculating with the model
-   Use `cache_hit=True` or `cache_hit=False` to indicate whether or not the value was read from the cache during prediction

### Manual Testing

Use `test_modelserver.py` and verify that it produces the expected output indicated by the comments.

## Part 2: gRPC Interface

Read this guide for gRPC with Python:

-   https://grpc.io/docs/languages/python/quickstart/
-   https://grpc.io/docs/languages/python/basics/

Install the tools (be sure to upgrade pip first, as described in the directions):

```shell
pip3 install grpcio grpcio-tools
```

Create a file called `modelserver.proto` containing a service called `ModelServer`.
Specify `syntax="proto3";` at the top of your file.
`ModelServer` will contain 2 RPCs:

1. `SetCoefs`
    - Parameters: `coefs` (`repeated float`)
    - Returns: `success` (`bool`) and `error` (`string`)
2. `Predict`
    - Parameters: `inputs` (`repeated float`)
    - Returns: `output` (`float`), `cache_hit` (`bool`), and `error` (`string`)

You can build your `.proto` with:

```shell
python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. modelserver.proto
```

Then, verify `modelserver_pb2_grpc.py` was generated.

## Part 3: gRPC Server Implementation

Create a `server.py` file and add a class that inherits from `modelserver_pb2_grpc.ModelServerServicer`. It should override the two methods. Please re-use the `ModelServer` implementations from **Part 1** here (it is recommended to change the class definition and method signatures to match the `ModelServerServicer` base class).

At the end, add a `serve()` function and call it (adapting the [example from the gRPC documentation](https://grpc.io/docs/languages/python/basics/#starting-the-server)).

Your server should:

-   Have 4 worker threads
-   Register with `modelserver_pb2_grpc.add_ModelServerServicer_to_server`
-   Use port 5440

### Locking

Your server will have 4 threads, so use a [Lock](https://docs.python.org/3/library/threading.html#threading.Lock) when accessing **any** global variables from `SetCoefs` or `Predict`

The lock should:

-   Protect any access to shared structures
-   Get released at the end of each call, even in the event of any raised exceptions

### Manual Testing

Use `test_server.py` and verify that it produces the expected output indicated by the comments.

## Part 4: gRPC Client - Random Workload

The client should start some threads or processes that send random requests to the server. The port of the server should be specified on the command line, like this:

```shell
python3 client.py 5440
```

Note that the random workload is _only_ run if `client.py` is invoked directly (and not if imported in another `.py` file).

Requirements:

-   There should be 8 threads

-   Each thread should send 1000 random requests to the server
-   For each request, randomly decide between `SetCoefs` (0.1) and `Predict` (0.9) proportionally
-   For each `SetCoefs` request, randomly choose a coefficient vector of length 10 with random floats between -10 and 10
-   For `Predict` requests, select from predefined list of 20 different inputs (you can implement this as first initializing a 20x10 random uniform matrix and then selecting a random row during `Predict`, or something equivalent).

The client should then print the statistics in JSON format at the end:

```json
{
    "cache_hit_rate": 0,
    "p50_response_time": 0,
    "p99_response_time": 0
}
```

Feel free to use `numpy` for computing percentiles.

## Part 5: gRPC Client - Text File Workload

If your client is invoked in the following way: `python3 client.py 5440 workload.json`, your client should:

1. Parse the file as JSON
2. Read each element in the `requests` array
    - Example element: `{"type": "SetCoefs", "value": {"coefs": [1, 2, 3]}}`
3. Make the corresponding gRPC call to the server (based on `type`), setting the `value` of the request as necessary
4. Print out the Response

## Part 6: Docker Deployment

You should write a `Dockerfile` to build an image that runs your server.

Your Docker image should:

-   Build via `docker build -t p3 .`
-   Run via `docker run -p 54321:5440 p3` (i.e., you can map any external port to the internal port of 5440)

## Submission

You should organize and commit your files such that we can run the code in your repo like this:

```shell
python3 -m grpc_tools.protoc -I=. --python_out=. --grpc_python_out=. modelserver.proto
docker build -t p3 .
export PORT=54321
docker run -d -p $PORT:5440 p3
python3 client.py $PORT > statistics.json
python3 client.py $PORT workload.json
python3 autograder.py
```

Be sure to test the above, and commit the `statistics.json` file you generated in the process.

## Approximate Rubric

The following is approximately how we will grade, but we may make changes if we overlooked an important part of the specification or did not consider a common mistake.

1. [X/1] `SetCoefs` and `Predict` have specified interface
2. [X/1] `SetCoefs` is logically correct
3. [X/1] `Predict` is logically correct
4. [X/1] `Predict` **caching** is correct
5. [X/1] `SetCoefs` and `Predict` **locking** is correct
6. [X/1] Server listens on **port 5440** with **4 workers**
7. [X/1] Client sends requests to the **specified port** from **8 threads**
8. [X/1] Random workload follows specification
9. [X/1] Workload statistics are correctly computed
10. [X/1] Text file workload is properly read, exucuted, and printed
11. [X/1] **Dockerfile** correctly containerizes the server
