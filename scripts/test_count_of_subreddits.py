
from pyspark.sql import *


if __name__ == "__main__":

    spark = SparkSession.builder.getOrCreate()
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    sc = spark.sparkContext

    # Load dataset
    df = sc.textFile("hdfs:///datasets/reddit_data/2017/")

    # Print first 10 elements
    for x in df.take(10):
        print(x)

