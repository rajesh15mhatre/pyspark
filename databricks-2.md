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


