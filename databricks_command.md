
## Reading data
#### Read text file 
```
from pyspark.sql.functions import explode, split
        lines  = (spark.readStream   # <--
                .format("text")
                .option("lineSep", ".")
                .load(f"{self.base_data_dir}/text")
                )
```
#### Read JSON file

```
from pyspark.sql.functions import input_file_name
        df = ( spark.readStream
                        .format("json")
                        .schema(self.getSchema())
                        .load(f"{self.base_data_dir}/Data/json")
                        .withColumn("filename", input_file_name())
        )

```
