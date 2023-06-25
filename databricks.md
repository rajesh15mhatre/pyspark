# What is Azure Data Bricks
- It's a platform where different teams can collaborate like data Engineers, Data scientists, and Analysts. there is a specific workspace provided in Databricks

# Core concepts of Apache Spark 1
## Architecture
The cluster contains below components
- Driver program (machine) is the spark script
- Driver program sends the data to the cluster manager 
- Cluster manager sends data to Worker nodes
- Worker nodes have multiple executor but databricks only has one executor under one worker node
- Inside executor has many cores which work in parallel 
-  RDD created for each transformation like a filter, group by and executed as per DAG (directed acyclic graphs) 
- Using DAG physical execution plan is created
- When there is no shuffle (transfer data between nodes) involved then 1 stage is created to perform the transformation on a subset of data
- the plan is executed only when we try to retrieve the data its called lazy evaluation
- Say if your executor is 4GB then 300mb is reserved for spark internal task
- Spark memory has execution and storage memory for transformations
- User memory used for UDFs
- tasks are smaller pieces of work that are equal to no. of partition

# Core concepts of Apache Spark 2
RDD: Resilient Distributred Disk. RDDs can be created by variety of ways and are lowest level API available. It is a orignal data structure of Apache Spark. Dataframe API is superset of RDD functionality. The RDD(not R) and dataframe API is available in java, scala and python. dataframes are similar to pandas
- RDD is immutable and thus cannot be changed and only new RDD is created from the previous.
- Using spark context we create a rdd.
```
from pyspark import SparkContext
rdd = sc.textFile("c:/file.txt")
```
- create df
```
df = spark.read.csv(path="path", sep=",",header=True)
```  
- Transformation vs action:
  - Transformation means all the business logic
    - read, filter, groupBy, Flatmap, Mappartition, soryBy, sample, randomSplit, Union, Intersection, Distinct, Zip, Coalsce, Repartition, zipWithIndex
  - action is when we request output by performing transformation
    - display, count, first, reduce, collect, aggregate, fold, take, ForEach, Top, collectAsMap, saveAsTextFile, Mean, Min, Max, takeSample
- Narrow Transformations: Where each input partition will contribute to only one output partition. each partition of the parent RDD is used by at most one partition of the child RDD.
  - Union, filter 
- Wide Transformations: Where input partition contributes to many output partitions. Multiple child RDD partitions may depend on a single-parent RDD partition.
##  Classify below transformations into wide or narrow
- mappartition, groupBy, reducebyKey, join, distinct, intersect, flatmap, filter, union

# Databricks cluster creation and configuration
- Excutoer cores Runs in parallel
- Under **compute** tab -> click on create cluster
  - All-purpose clusters: dev tasks 
  - job cluster: prod task
  - Give name
- select mode:
  - high concurrency: concurrent and no support to scala. Suitable for multiple users
  - standard: single user, all language support
  - Single node: cluster with no worker. for small data volume
- Select Databricks runtime version:
  - Standard: go for std, select 2nd latest, check versions of spark and programming language
  - Genomics
  - ML
- check **photon** for a lot of sql queries used and optimize workload
- Worker Type
  - Choose a worker machine and the number of workers which equals one executor
  - No of worker equals to no of partitions
  - Avoid HDD
  - complex operation select storage optimize
- Driver types: select VM
- We can terminate after a certain time to save money
- We can write extra configuration on the spark config text box
- check spot instances to save money
- under Logging select log path
- init script  bootstrap script
-  Click on Create a cluster
-  We can mention libraries PyPI, maven, etc e wish to install in the library tab
-  We can edit the configuration by using the edit link, and can also access a JSON file to create clusters using cluster API via Powershell.

## Read data from different format
- Under **_Data_ on navigation** -> select DBFS
- tables-> upload file -> copy path
- `Display(df)` command shows better output than `show()`
- Avro files creates partition which help data retrieval as sparks read subset of data based on query

## What is DBFS
- Databriks File System(DBFS) is a distributed file system mounted into Databricks workspace and available on Databricks clusters.
- DBFS is an abstraction on top of scalable object store 
- Databricks recommends using your own storage mount on data bricks like Azure blob storage in order to have more control on data. The DBFS root is not intended for the production data.
- **DBFS Root**
  - The default storage location on DBFS is known as the DBFS root.
  - Several types of data are stored in the following DBFS rot locations:
    - 




# Code snippets
- read file
```
df = spark.read.csv(path="path", sep=",",header=True)
```

```
%scala

val testJson = spark.read.json("/tmp/test.json")
// read multiline json
val mldf  = spark.read.option("multiline", "true").json("/tmp/test.json")

```
- Read parquet files
```
%scala

case class MyCaseClass(key: String, group: String, value: Int, someints: Seq[Int], somemap: Map[String, Int])
//parallerlize creats rdd and .toDF() creates dataframe
val dataframe = sc.parallerize(Array(MyCaseClass("a", "vowels", 1, Array(1), Map("a" -> 1)),
Array(MyCaseClass("a", "vowels", 1, Array(1), Map("a" -> 1)))
).toDF()
dataframe.write.mode("overwrite").parquet("/tmp/testParquet")

```

- Read Avro format
```
%scala

val df = Seq((2012, 8, "Batman", 8.8),
(2012, 8, "hero", 7,3))
.toDF("year", "month", "title", "rating")
//different files will be created under  year dir =-->  month dir as we have partiotion by those 
df.write.mode("overwrite").partitionBy("year", "month").format("avro").save("/tmp/test")

```


- Create JSON file in Scala
```
%scala

dbutils.fs.put("/tmp/test.json", """
{"string":"string1","int":1,"array":[1,2],"dict":{"key":"value1"}}
{"string":"string2","int":2,"array":[2,3],"dict":{"key":"value2"}}
""", true)
```





