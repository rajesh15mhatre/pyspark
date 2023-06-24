# What is Azure Data Bricks
- It's a platform where different teams can collaborate like data Engineers, Data scientists, Analysts. ther is specific worspace provided in databricks

# Core concepts of Apache Spark 1
## Architecture
- Driver program (machine) is the spark script
- Driver program sends the data to the cluster manager 
- Cluster manager sends data to Worker nodes
- Worker nodes has mutiple executor but for databriks only has one executor under one woker node
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






















