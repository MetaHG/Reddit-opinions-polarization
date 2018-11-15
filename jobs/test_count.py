
from pyspark.sql import *

# Create Spark Config
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

# Import dependencies ZIP
sc.addPyFile('src/load.py')

# Import from dependencies
from load import load_data


if __name__ == "__main__":

    # Load data
    rdd, df = load_data(sc, [2005])

    print("The selected dataset has {} entries.".format(df.count()))