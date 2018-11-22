
from pyspark.sql import *
import pyspark.sql.functions as func
from pyspark.sql.types import *


import os

# Create Spark Config
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

# if os.environ['RUN_MODE'] == "CLUSTER":
    # dir = "./__pyfiles__/"
# else:
    # dir = "./src/"

# print(os.environ)

# execfile(dir + "load.py")

from pyspark.sql.types import *
from pyspark import SQLContext
import json

if __name__ == "__main__":

    print(os.listdir("./tokenizers.zip"))

    # rdd, df = load_data(sc, sample=0.002)
