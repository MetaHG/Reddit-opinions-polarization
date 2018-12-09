# coding: utf-8

###
### Imports
###
import pyspark
from pyspark.sql import *
import pyspark.sql.functions as func
from pyspark.sql.types import *

# Create spark session
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

# Standard library
from collections import Counter
import os
import numpy as np

def count_matches(msg_grams, ref_grams_counter, ref_grams_intensity=None):
    msg_grams_joined = [' '.join(msg_gram) for msg_gram in msg_grams]
    print(msg_grams_joined)
    msg_grams_counter = Counter(msg_grams_joined)
    res_counter = msg_grams_counter & ref_grams_counter
    
    if ref_grams_intensity is not None:
        res_intensity = dict()
        for w, occ in res_counter.items():
            res_intensity[w] = occ * ref_grams_intensity[w]
    
    count = sum(res_counter.values())
    if ref_grams_intensity is None:
        return count
    else:
        intensity = sum(res_intensity.values())
        return {'count':float(count), 'intensity':intensity}
    
def df_count_matches(gram_counter, sql_fun_name):
    udf = func.udf(lambda c: count_matches(c, gram_counter), IntegerType())
    spark.udf.register(sql_fun_name, udf)

def df_count_matches_intensity(gram_counter, intensity_dict, sql_fun_name):
    udf = func.udf(lambda c: count_matches(c, gram_counter, intensity_dict), MapType(StringType(), FloatType()))
    spark.udf.register(sql_fun_name, udf)