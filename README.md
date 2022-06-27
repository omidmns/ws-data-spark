# Work Sample for Data Aspect, PySpark Variant

## Environment setup

You can run `docker-compose up` and follow the prompt to open the Jupyter Notebook UI (looks like `http://127.0.0.1:8888/?token=<SOME_TOKEN>`).

The given `data/` directory mounts as a Docker volume at `~/data/` for easy access:

```python
import os
from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').getOrCreate()
df = spark.read.options(
    header='True',
    inferSchema='True',
    delimiter=',',
).csv(os.path.expanduser('~/data/DataSample.csv'))
```

