
from pyspark.sql import *

# Create Spark Config
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

# Import dependencies ZIP
sc.addPyFile('jobs/src.zip')

# Import from dependencies
from load import load_data


if __name__ == "__main__":

    # Load data
    df = load_data(sc, [2015, 2016, 2017])

    # Print first 10 elements
    for x in df.take(10):
        print(x)

