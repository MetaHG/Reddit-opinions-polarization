from pyspark.sql import *
import pyspark.sql.functions as func
from pyspark.sql.types import *
from pyspark import SQLContext

# Create Spark Config
spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.sql.session.timeZone', 'UTC')
sc = spark.sparkContext

from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer

import nltk
nltk.download('wordnet')

en_stop = set(stopwords.words('english'))
en_lemmatizer = WordNetLemmatizer()
sc = spark.sparkContext

#for both variables to be available on all nodes.
sc.broadcast(en_stop)
sc.broadcast(en_lemmatizer)

# Import dependencies ZIP
# sc.addPyFile('src/load.py')
# Import dependencies ZIP
# sc.addPyFile('src/lda.py')

execfile("./__pyfiles__/load.py")
execfile("./__pyfiles__/lda.py")

from pyspark.sql.types import *
from pyspark import SQLContext
import json
import datetime

if __name__ == "__main__":

    import load
    import lda

    _, df = load_data(sc, filter=[2015, 2016])
    df = df.withColumn('created', func.from_unixtime(df['created_utc'], 'yyyy-MM-dd HH:mm:ss.SS').cast(DateType()))
    comments = df.select('link_id','body','created', 'subreddit')
    start_date = datetime.date(year=2015, month=11, day=7)
    end_date = datetime.date(year=2016, month=11, day=8)
    year_b4_election_news_comments = comments.filter(comments.created > start_date).filter(comments.created < end_date).filter(comments.subreddit == 'news')
    
    cleaned_preprocessed = dataset_cleaning_and_preprocessing(year_b4_election_news_comments, en_stop, en_lemmatizer)
    individual_keys = cleaned_preprocessed.keys().distinct().collect()

    lda_res = [lda_and_min_date(cleaned_preprocessed.filter(lambda r: r[0] == post_id), 1, 3) for post_id in individual_keys]

    sqlContext = SQLContext(sc)
    res_df = sc.parallelize(lda_res).toDF().selectExpr("_1 as topic", "_2 as date")
    res_df.write.mode('overwrite').parquet('2016_news_lda.parquet')