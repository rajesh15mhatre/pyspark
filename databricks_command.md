
## Reading data
#### from text file 
```
from pyspark.sql.functions import explode, split
        lines  = (spark.readStream   # <--
                .format("text")
                .option("lineSep", ".")
                .load(f"{self.base_data_dir}/text")
                )
```
