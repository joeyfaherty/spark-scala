Spark

Contains functions that let you import data set from distributed datastore like HDFS or S3, and lets you simply and efficiently, process that data.

Same code can be run locally as on a large cluster.

Driver Program (Spark Context) -> Spark devs concern

Cluster Manager (Spark, YARN, Mesos)

Executor Process (each will have its cache, tasks)


Claims: 
Runs 100x faster than Map-Reduce in memory, or 10x faster on disk.
DAG (directed acyclic graph) optimizes workflows

Code in Python, Java, Scala
Built around one main concept, Resilient Distributed Dataset (RRD)


Spark Core

Spark Streaming - real time data processing
Spark SQL - run SQL like queries on mass data
MLLib - Machine Learning, routines etc
GraphX - Graph Networks, Social Networks

Why Scala?
* Scala's functioning programming model is a good fit for distributed processing
* Gives you fast performance (Scala compiles to Java bytecode)
* Less code & boilerplate than java
* Python is slow in comparison

RRDs: Resilient Distributed Dataset

Abstracts away all the complexity such as 
- recovery (when one node goes down that a new one comes up and pick up where it left off)
- distributed (decides how to split up your data horizontally across nodes)
- is just a dataset (basically just a huge data set, row after row)

Spark Context
Is created by our driver program
Is responsible for making the RDDs resilient and distributed
Creates RDDs
Spark Shell creates a spark context object for you

sc.parallelize(List(1,2,3,4)) -> plain integer data set -> creates an RRD

sc.textFile("file://home/user/joey/users.txt") -> load data from local FS
or -> s3n:// -> load data from AWS
or -> hdfs:// -> load data from HDFS

Transforming RDDs:
map
applys a function to an entire an RDD. Produces a new transformed RDD (Immutable)
flatmap
filter -> Boolean Predicate
distinct -> returns distinct rows
sample -> random sample of dataset
union, intersection, subtract, cartesian
process between two RDDs


Key Value RRDs:
example, number of friends by age in a network
Tuple: if the value of the RRD has 2 values

reduceByKey(): combines values of the same key using some function.
eg. rrd.reduceByKey((a,b) => a + b) would get the total sum.
we do it one by one (a,b) because of the distributed nature of the RRD we need to operate one at a time

groupByKey(): Collects all the values of a key into a list
sortByKey(): sorts an RRD by a key
keys(), values() creates an RRD of just the keys or just the values

if you are only working with the values use: (more efficient and better syntactically)
mapValues()
flatmapValues()
