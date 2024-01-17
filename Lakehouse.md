https://github.com/LearningJournal/Apache-Spark-and-Databricks-Stream-Processing-in-Lakehouse/tree/main

8. Batch to stream processing
- DE pipeline/workflow
  - Injest = Preapre landing zone from where data will move to bronze layer
  - Prepare and process - prepare high-quality data table by performing DQ, enrich data
  - REsult - as per consumer needs
-JOb frequency
  - Monthly to hourly managable
  - 15min to asap need time for housekeeping/cleanup processing time and volume   
- Incremetal data processing
