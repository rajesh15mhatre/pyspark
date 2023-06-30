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
SELECT * FROM delta.'abfdss://testcontainer@test.dfs.core.windows.net/test' VERSION AS OF 0;
SELECT * FROM delta.'abfdss://testcontainer@test.dfs.core.windows.net/test' TIMESTAMP AS OF  '2020-02-01T12:34:00.000';
```

Restore the previous version of delta file using the version and timestamp

```
%sql 

RESTORE TABLE delta.'abfdss://testcontainer@test.dfs.core.windows.net/test' TO VERSION AS OF 3;
--RESTORE TABLE delta.'abfdss://testcontainer@test.dfs.core.windows.net/test' TO TIMESTAMP AS OF '2020-02-01T12:34:00.000';
```
  
## Vacuum on delta  table 
- Vacuum command deletes a set of files to which delta table is not referring
 - vacuum command does not trigger automatically, do we need to run it manually or can be scheduled
-  Default retention threshold for the files is 7 days, 168 hours
- vacuum deletes only data files not log files
- Log files are deleted automatically and asynchronously after checkpoint operations
- The default retention period for log files is 30 days, configurable using delta.logRetensionDuration property

```
%sql 
SET spark.databricks.delta.retensionDurationCheck.enabled = True;
```

- Vacuum run command to **list files** which will get deleted. This command will give an error as we have set the retention period is 0 and the default is 164 hours which is 7 days. we can suppress error by changing the property to False given in the above command
```
%sql
VACCUM delta.'abfss://testcontainer@test.dfs.core.windows.net/test' RETAIN 0 HOURS DRY RUN
```

- Vacuum run command to **delete files**
```
%sql
VACCUM delta.'abfdss://testcontainer@test.dfs.core.windows.net/test' RETAIN 0 HOURS
```

## Merge command in delta table

INSERT or UPDATE parquet: 7-step process\
1. Identify the new row to be inserted
2. Identify the rows that will be replaced(i.e. updated)
3. Identify all of the rows that are not impacted by the insert or update
4. Create a new temp based on all three insert statements
5. Delete the original table (and all of those associated files)
6. "Rename" the temp table back to the original table name
7. Drop the temp table

- Read sample delta file from DBFS 

```
read_format = 'delta'
load_path = '/databricks-datasets.learning-spark-v2/people/people-10m.delta'

people = spark.read \
   .format(read_format)
   .load(load_path)
   
display(people)

```

- Saving a delta table from the file data 

```
%sql
people.write \
    .format("delta") \
	.saveAsTable("people10m")

display(spark.sql("SELECT * FROM people10m"))

```

- Create another table with the same schema for the merge operation
  - Managed vs unmanaged: Tables created with a specified LOCATION are considered unmanaged by the meta store. Unlike a managed table, where no path is specified, an unmanaged table's files are not deleted when you DROP the table.

```
%sql
CREATE TABLE default.people10m_upload01 (
  id INT,
  firstName STRING,
  middleName STRING,
  lastName STRING,
  gender STRING,
  birthDate TIMESTAMP,
  ssn STRING,
  salary INT
)USING DELTA
```


- Insert the same records from the Databricks sample data set into  the newly created table
```
%sql

INSERT INTO default.people10m_upload01 VALUES
 (9999997, 'billy', 'Tom', 'Lupit', 'M', '1992-09-17T12:01:00,000+000', '954-954-3453', 55250),
 (9999998, 'Harry', 'Jack', 'Thon', 'M', '1982-19-27T12:01:00,000+000', '954-122-3453', 155250),
 (9999999, 'Tim', 'bily', 'Smith', 'M', '1985-19-27T12:01:00,000+000', '974-122-3487', 15253),
```

- Now merge both the new and old table
  - if records are present then after matching keys in the target table the update will happen else insert
```
%sql
MERGE INTO default.people10m
USING default.people10m_upload01
ON default.people10m.id = default.people10m_upload01.id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```


## Schema evolution in delta table

 SCema evolution is a feature that allows users to easily change a table's current schema to accommodate data that is changing over time. Most commonly, it's used when performing an append or overwrite operation, to automatically adapt the schema to include one or more new columns
 
 
 ```
 %python
 DELTALAKE_SILVER_PATH = "/ml/loan_by_state_delta"
 # read a sample parquet file
 lspq_path = "/databricks-datasets/samples/lending_club/parquet/"
 # Read file
 data = spark.read.parquet(lspq_path)
# Takeout sample data
(loan_stats, loan_stats_rest) = data.randomSplit([0.01, 0.99]), seed = 123)

# select needed columns
 loan_by_state = loan_stat.select("addr_state", "load_status")groupBy("addr_state").count()

# create a table 
loan_by_state.createOrReplaceTempView("loan_by_state")
display(loan_by_state)

 ```

- create a table from aggregated data

```
%sql 
CREATE TABLE loan_by_state_delta
USING delta
LOCATION "/ml/loan_by_state_delta"
AS SELECT * FROM loan_by_state;

--View delta lake table
SELECT * FROM loan_by_state_delta;

```
- create a new table with a different schema
```
loans = sql("SELECT addr_state, cast(rand(10)*count as bigint) as count, cast(rand(10) * 10000 * count as double) as amount from loan_by_state_delta")

display(loans)

```

- append new data with different schema into the existing delta table 
- Operation will show an error if:
   - data column name is different and also the case is different
   - contains an additional column
   - column with different data types like date columns having integer values

```
loans.format("delta").mode("append").save(DELTALAKE_SILVER_PATH)

``

You will give error analysisException  bcoz of schema mismatch 

- Appends new data without error using option option("mergeSchema", "true")

```
loans.option("mergeSchema", "true").format("delta").mode("append").save(DELTALAKE_SILVER_PATH)

``
We can also turn off errors by using `spark.databricks.delta.schema.autoMerge = True` into spark config






