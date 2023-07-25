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

```

You will give error analysisException  bcoz of schema mismatch 

- Appends new data without error using option option("mergeSchema", "true")

```
loans.option("mergeSchema", "true").format("delta").mode("append").save(DELTALAKE_SILVER_PATH)

```
We can also turn off errors by using `spark.databricks.delta.schema.autoMerge = True` into spark config


## Transform Batch and stream data

- Create a new DB and use it 
```
db = "deltadb"

spark.sql(f"CREATE DATABASE IF NOT EXISTS {db})")
spark.sql(f"USE {db}")

```
- create a folder and add a parquet file under DBFS

```
%sh mkdir -p /dbfs/tmp/delta_demo/loans_parquet/; wget -O /dbfs/tmp/delta_demo/loans_parquet/loans.parquet http://pages.databricks.com/rs.094-YMS-629/images.SAISEUI19-loan-risks.snappy.parquet

```

- lit(literal) is used to give any hard-coded value
- read the file add new 2 columns and save it as a delta table
```
parquet-path = "file:dbfs/tmp/delta_demo/loans_parquet/"

df = (spark.read.format("parquet").load(parquet_path)
    .withColumn("type", lit("batch"))
	.withColumn("timestamp", current_timestamp())

df.write.format.("delta").mode("overwrite").saveAsTable("loans_delta")

```


create another delta table from the parquet file

```
%sql
CREATE TABLE loans_delta2
USING delta
AS SELECT * FROM parquet.'/tmp/delta_demo/loans_parquet`


```

9.56
use convert to delta to convert the Parquet file to Delta Lake format in place( means the file will become delta table )

```
%sql
CONVERT TO DELTA parquet.'/tmp/delta_demo/loans_parquet`
```

- quey delta table
```
%sql
SELECT COUNT(*) FROM loans_delta'
```
- use show 
```
spark.sql("select count(*) from loans_delta").show()

spark.sql("select * from loans_delta").show(3)
```

- streaming function

```
%python
# User-defined fucntion to generate random state
@udf(retuenType=StringType())
def random_state():
   return str(random.choice(["CA", "TX", "NY", "WA"])
   
# Function to start a streaming query witha stream of randomly generetd load data and append to the parquet table
def generete_and_append_data_stream(table_format, table_name, schema_ok=False, type="batch")"
    stream_data = (spark.readStream.format("rate").option('rowsPerSecond", 500).load()
	.withColumn("loan_id", 1000+ col("value"))
	.withColumn("funded_amnt", (rand() * 5000 + 5000).cast("integer"))
	.withColumn("paidAmount", col("funded_amnt") - (rand() * 2000))
	.withColumn("addr_state", ramdom)state())
	.withColumn("type", lit(type)))

if schema_ok:
    stream_data = .select("loan_id", "funded_amnt", "paidAmount", "addr_state", "type", "timestamp")

query = (stream_data.writeStream
    .format(tableFormat)
	.option("checkpointLocation", my_checkpoint_dir())
	.trigger(processingTime = "5 seconds")
	.table(table_name)
	
return query

```

- live query streaming table
Stop the execution or else it keeps reading continuously
```
display(spark.readStream.format("delta").tabel("loans_delta").groupBy("type").count().orderBy("type"))

```
- group by time window

```
display(spark.readStream.format("delta").tabel("loans_delta").groupBy("type", window("timestamp", "10 seconds")).count().orderBy("window"))

```

- custom function to stop all streams or can be stopped by clicking stop stream button
```
# function to stop all streaming queries
def stop_all_streams():
    print("stopping all streams")
	for s in spark.stream.active:
	    try:
			s.stop()
		except:
			pass
	print("stopped all streams")
	dbutils.fs.rm('/tmp/delta_demo/chkpt/", True)
	
def cleanup_paths_and_tables():
    dbutils.fs.rm("/tmp/delta_demo/", True)
	dbutils.fs.rm("file:dbfs/tmp/delta_demo/loans_parquet/", True)
	
	for table in ["deltadb.loans_parquet", "deltadb.loans_delta", "deltadb.loans_delta2"]:
	    spark.sql(f" DROP TABLE IF EXISTS {table}")
		
```

- fetch history

```
%sql DESCRIBE HISTORY loans_delta
```

- data frame with extra column and add the data 

```
form pyspark.sql.types import
import datetime
from datetime import *

# Generate new_dat with additional columns
new_column = [StructFiels("credit_score", IntegerType(), True)]
new_schema = StructType(spark.table("loans_delta").schema.fields + new_colum)

data = [(999997, 10000, 13338.5, "CA", "batch", datetime.now(), 649),
(999998, 20000, 14498.5, "NY", "batch", datetime.now(), 702)]

new_data = spark.creaetDataFrame(dat, new_schema)
new_data.printSchema()

```

-  Merge new data frame with new column  to delta table 
you will get the error - schema enforcement 
in order to merge use option - schema evolution

```
new.data.write.format("delta").mode("append").saveAsTable("loans_delta")

new.data.write.format("delta").mode("append").option("mergeSchema", True).saveAsTable("loans_delta")

```

- show the first version of the file
```
spark.sql("SELECT * FROM  loans_delta VERSION AS OF 0").SHOW(3)
spark.sql("SELECT COUNT(*) FROM  loans_delta VERSION AS OF 0").SHOW(3)

```

## Parameterized notebook to productionise job


- Creating widgets and imports
```
from pyspark.sql.functions import col, max, dense_rank
from pyspark.sql.window import Window

dbutils.widgets.dropdown("database", "TESTDATABASE", ["TESTDATABASE"])
dbutils.widgets.dropdown("schema", "SCHEMA", ["SCHEMA"])
dbutils.widgets.dropdown("warehouse", "WAREHOUSE", ["WAREHOUSE"])
dbutils.widgets.dropdown("table", "Airlines", ["Airlines"])

```

- Connect to Snowflake
```
ooptions= {
	"sfUrl": "snowflakeurl",
	"sfUser": "user",
	"sfPassword": "pass",
	"sfDatabase": dbutils.widget.get("database"),
	"sfSchema": dbutils.widget.get("schema"),
	"sfWarehouse: dbutils.widget.get("warehouse")
}
```

- filer and group by on df
```
df = airlines_df.filter(lower(airlines_df.IsDepDelayed) == "yes")

df_DelayedAggCount = df.groupby("UniqueCarrier").agg({"IsDepDelayed": "count"})
```


- windows function
```
windowSpec = Window.partitionBy().orderBy(desc("count(IsDepDelayed)"))

df_top5 = df_DelayedAggCount.withColumn("denserank", dense_rank().over(windowSpec)) \
.where(col("denserank")<=5).select("UniqueCarrier","count(IsDepDelayed)")

```

- Write to Snowflake using the parameter
```
df_top5.write.format("snowflake").options(**options).mode("overwrite").option("dbtable", dbutils.widget.get("table")).save()
```

- Create a job 
workflow - create job - give task name - select notebook - select cluster 
in the same window, you will get the parameter option 



## Databricks jobs

Under Workflow, we have below options
- jobs
- Job Runs
- Delta live tables

we can connect to Git 
we can schedule job  which supports cron syntax
Also, add an email to send job status notification
We can give permission to other people/groups for the job

- While creating a job we can find the below options in advanced settings
  - Add dependent libraries
  - Edit notification
  - Edit retry policy
  - Edit timeout

## Databricks CI/CD

your azure dev ops account and Databricks account should be the same Microsoft account 

- Goto Azure DevOps - create projects -
give name - click Create 

- under repo - click on Intialise to create a repo

- we can establish ci/cd in two ways
 1. Legacy way
  - goto databricks -> workspace -> user-> select any script 
  - Under revision history, you will get all previous script versions and an option to link the script to git (this is an old feature)
  - not a recommended option as all the users must need to create a DevOps account and link the script to the repo
 
 
 2. Preferred way
  - goto data bricks repo - users - add repo  - paste path of git repository created under azure devops (We can use any provider like GitHub, bitbucket)
  - repo dir will be visible 
  - now push/pull options are available under options of repo folder having branch name).
  - Select the git option to see the changes
  
  - for the schedule notebook, you can add a release folder (with any name) to the user workspace and keep only the latest notes which will run on production 
  - We can also restrict certain commands for repos  under the data bricks admin console - repo section 
  
- create pipelines to automatically pull the latest file to release/schedule
  - goto DevOps - pipelines - create pipelines
  - select versioning tools - select project
  - select yaml
  - Yaml file has all variables like VM, software to install like data brick CLI,  credential, branch name, code to delete existing files from release dire and paste new from latest branch
  - this file will get saved and repo 
  - We can write file types to ignore in a file named .artifactignore
  - 

  # medallion architecture and change data feed
- A medallion architecture is a data design pattern used to logically organize data in a lakehouse, with the goal of incrementally and progressively improving the structure and quality of data as its architectures are sometimes also referred to as **multi-hop architecture**.

There are three forms of data 
- Bronze: Raw data where the schema is not needed
- Silver: Where we define the structure, enforce a schema, and evolve the schema as needed
- Gold : Deliver continuously updated, clean data to downstream users and apps

**Change Data Feed(CFD)**: feature allows Delta table to track row-level changes between versions  of a Delta table. When enabled on the Delta table, the runtime records "change events" for all the data written into the table. This includes the troe data along with metadata indicating whether the specified toe was inserted, deleted, or updated.

The changes data feed is not enabled by default we can set it while creating a table or by altering the table

```
CREATE TABLE student (id INT, name STRING, age INT) TBLPROPERTIES (delta.enabledChangeDataFeed=true)

ALTER TABLE myDeltaTable SET TBLPROPERTIES (delta.enabledChangeDataFeed = true)

set spark.databricks.delta.properties.defaults.enabledChangeDataFeed = true;

```

Use cases: The following use cases should drive when you enable the change data feed:
- Silver and Gold Tables: Improve Delta performance by processing only row-level changes following initial MERGE, UPDATE, or DELETE operation to accelerate and simplify ETL and ELT.

- Materialised view: Create up-to-date, aggregated views of information for use in BI and analytics without having to reprocess the full underlying tables, instead updating only where changes have come through.

- Transmit changes: Send change of data feed to the downstream system such as Kafaka or RDBMS that can use to incrementally process in later stages of data pipelines.

- Audit Trail: Capture the change data feed as a Delta table provides perpetual storage and efficient query capability to see all changes over time, including when deletes occur and what updates were made.


-- Fetch  table changes of delta table with commit version 1
```
SELECT * FROM table_changes('table_name', 1)

```


- To fetch data from change feed for a specific commit say 2 from an upstream table to feed the downstream table 

```
MERGE INTO downstream_table d
USING
	(SELECT * FROM table_changes('upstream_table', 2))
	AS u
ON u.id = d.id 
WHEN MATCHED THEN
	UPDATE SET d.col = u.col
WHEN NOT MATCHED
	THEN INSERT (col1, col2) VALUES (val1, val2)
```

- Using the change data feed we can get the previous version of the upstream table which has data feed enabled and populate the downstream tables which do not have data feed enabled


## Change data feed demo

-- Write data feed changes in df
```
changes_df = spark.read.format("delta").option("readChangeData", True).option("startingVersion", 2).table("tablename")
```

##35.  Join strategy in spark 
- Spark decides what algorithm will be used for joining the data in the phase of physical planning, where each node in the logical plan has to be converted to one or more operators in the physical plan using so-called strategies. The strategy responsible for planning the join is called Join selection.
Five types of strategies
- Broadcast Hash join: If your df is up to a certain threshold in size then It is considered as smaller df. Then smaller df is available in memory to each node and it gets joined with a larger df.
  - Using spark config we can get/set the threshold memory value in bytes
```
%scala
spark.conf.get("spark.sql.autoBroadcastJoinThreshold")
spark.conf.set("spark.sql.autoBroadcastJoinThreshold", 10485760)
```
  - Get joined query execution plan. which tell strategy used by join
```
dfJoined.queryExecution.executedPlan
```
  - We can explicitly tell which df to be broadcasted (broadcasted df wiil be stored in memory of each node)
```
val dfJoined = df1.join(broadcast(df2), $"id1" === $"id2")
```
  - Broad cast Hash Join is the **fastest join algorithm (as shuffle is not involved)** when the following criteria are met
    - Works only for equi join
    - Works for all joins except for full outer joins
    - Broadcast Hasj join works when a dataset is small enough that it can be broadcasted and hashed.
    - broadcast Has join doesn't work well if the dataset that is being broadcasted is big
    - If the size of the broadcasted dataset is big, it could become network-intensive operations and cause your jo execution to slow down
    - If the size of the broadcasted dataset is big, you would get an OutOfMemory exception when Spark builds the Hash table on the data. Because the hash table will be kept in memory.

- Shuffle Hash join
  - 
- Shuffle sort Merge join
- Cartesian Join
- Broadcasted Nested Loop Join

