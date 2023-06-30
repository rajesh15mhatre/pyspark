## Partition in data bricks:
![image](https://github.com/rajesh15mhatre/pyspark/assets/15013611/99992409-b82f-4689-b58c-8e4bd6d47fbc)
- create  a partition in Python 
![image](https://github.com/rajesh15mhatre/pyspark/assets/15013611/a7cd2d30-e789-4fde-bd27-8b22e1d6aa56)
- Above command will split the file in multiple partition/directories:
- ![image](https://github.com/rajesh15mhatre/pyspark/assets/15013611/92f49868-a6aa-4dd0-80c6-ec4e0d79e914)
- Partition using SQL
![image](https://github.com/rajesh15mhatre/pyspark/assets/15013611/900de801-f8f6-4d79-a09e-1f0a17cf9539)

 # What is the delta table
 all the tables you create after version 8 is delta table.


 ## Create a delta table from  dataframe scala
```
df.write.format("delta").mode("overview").save("abfss://container@user.pathtofile");
```
delta table gets stored in parquet format with a snappy compression method.
along with table dela_log dir gets generated to store transaction logs

##  Read delta file  in scala

```
val  read_format = "delta"
val  load_path - "abdss://textcontainer@test1223.dfs.core.windows.net/tagetDeltaTable"

val read_delta - spark
.read
.format(read_format)
.load(load_path)
```

## Fetch delta table history

```
import  io.deltaa.tables._
val deltaTable = DeltaTable.forPath(spark, "abdss://textcontainer@test1223.dfs.core.windows.net/tagetDeltaTable")
val fulHistoryDF = deltaTable.history()
display(fullHistoryDF)

```


## Read Databrics sample dataset in python
```
read_format= 'delta'
load_path = '/databricks-datasets/learning-spark-v2/people/people-10m.delta'
 # Load data
people = spark.read \
    .format(read_format) \
    .load(load_path)

# Show result
display(people)
```

## Write DataFrame as Databricks Delta data in python
```
# Define the output format, output mode, columns to partition by, and the output path
# unmanaged tables
write_format = 'delta'
write_mode = 'overwrite'
partition_by = 'gender'
save_path = '/tmp/delta/people-10m'

# Write the data to its target
people.write \
    .fomat(write_format) \
    .partitionBy(partition_by) \
    .mode(write_mode) \
    .save(save_path)

```

## Create a delta file without specifying the path  then its create a table under databricks tables
```
# Managed table
# For example tables and Databrixks determine the location of the data. to get the location, you can use the DESCRIBE DETAIL statement, for example

peiple.write \
    .format("delta") \
    .saveAsTable("people10m") \
display(spark.sql("SELECT * FROM people10m"))
```

## Drop table 

```
table_name = 'people10m'
display (spark.sql("DROP TABLE IF EXISTS " + table_name))

```

## Query table and df
```
# query table
display(spark.table(table_name).select("id", "salary").orderBy("salary", ascending= False))
# query df
display(spark.table(table_name).select("gender").orderBy("gender", ascending= False).groupBy('gender').count())
```

# timetravel/ versioning in delta table

- File contents
 - parquet files
 - _delta_log dir
  - json file
  - crc checkpoint file
- versioning steps
 - connect to datalake
 -  Read file from datalake using scala
```
val read_format = 'delta'
val load_path = "abfdss://testcontainer@test.dfs.core.windows.net/test"
// Load files
val read_delta = spark
  .read
  .format(read_format)
  .load(load_path)
display(read_delta)
```
  - ferch history
```
import io.delta.tables._

val deltaTable = DeltaTable.forPath(spark, "abfdss://testcontainer@test.dfs.core.windows.net/test")

val fillHistoryDF = deltaTabel.history()
display(fullHistoryDF)
```
  - Read csv file from datalake and overwrite deltatable
```
//Readd file
val df_datalake =spark.read.format("csv).option("header", "true").load("abfss://testcontainer@test.dfs.core.windows.net/sample.csv")
display(df_datalake)
//  overwrite
df_datalake.wrirte.format.("delta").mode("overwrite").save("abfdss://testcontainer@test.dfs.core.windows.net/test");
```

  - Read delta file to see the updated contents using the above command and also check the history
  - Read the previous version of the delta file using  version numbers  and also by using the timestamp
```
%python 
df2 = spark.read.format("delta").option("versionAsOf", 0).load.("path")
display(df2)
//Take timestamp from history command
timestamp_string = "2020-02-01T12:34:00.000"
df2 = spark.read.format("delta").option("timestampAsOf", timestamp_string).load.("path")
display(df2)
```

Using SQL also we can fetch  the previous versions
```
%sql
SELECT * FROM delta.'pathToDelta' VERSION AS OF 0;
SELECT * FROM delta.'pathToDelta TIMESTAMP AS OF  '2020-02-01T12:34:00.000';
```

Restore the previous version of delta file using the version and timestamp

```
%sql 

RESTORE TABLE delta.'pathtoDelta' TO VERSION AS OF 3;
--RESTORE TABLE delta.'pathtoDelta' TO TIMESTAMP AS OF '2020-02-01T12:34:00.000';
```
  

  
  
  
  
# Vaccume command









