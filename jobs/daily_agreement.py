from pyspark.sql import *
import pyspark.sql.functions as func
from pyspark.sql.types import *

# Create Spark Config
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

# Import dependencies ZIP
execfile('__pyfiles__/load.py')
# execfile('src/load.py')

from pyspark.sql.types import *
from pyspark import SQLContext
import json

if __name__ == "__main__":

    _, df = load_data(sc, sample=None)

    df = df.withColumn('created', func.from_unixtime(df['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType()))

    df.registerTempTable("comments")

    daily_agreement = spark.sql("""
    SELECT created, agreement_factor, AVG(agreement_factor) OVER (
        ORDER BY created
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS agreement_factor_60d_avg
    FROM (
        SELECT 
            created,
            COUNT(*) AS count,
            SUM(CASE WHEN score > 0 THEN 1 ELSE 0 END) AS count_pos,
            SUM(CASE WHEN score < 0 THEN 1 ELSE 0 END) AS count_neg,
            (SUM(CASE WHEN score > 0 THEN 1 ELSE 0 END) / COUNT(*)) AS agreement_factor
        FROM comments
        WHERE score < -3 OR score > 5
        GROUP BY created
        HAVING COUNT(*) > 5
    )
    """)

    daily_agreement.write.mode('overwrite').parquet('daily_agreement.parquet')


