import pandas as pd
import numpy as np
import scipy as sp
import seaborn as sns
import datetime as dt
import matplotlib
import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap

import findspark
findspark.init()

import pyspark
from pyspark.sql import *
import pyspark.sql.functions as func
from pyspark.sql.types import *


# Create spark session
spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext


def spark_to_pandas(spark_metrics):
    metrics_pd = spark_metrics.toPandas()
    metrics_pd = metrics_pd.set_index('creation_date')
    metrics_pd = metrics_pd.sort_index()
    return metrics_pd

def get_metrics(filename, sql_query, sql_table_name):
    spark_metrics = spark.read.load(filename)
    spark_metrics.registerTempTable(sql_table_name)
    avg_metrics = spark.sql(sql_query)

    pd_metrics = spark_to_pandas(spark_metrics)
    pd_metrics_avg = spark_to_pandas(avg_metrics)
    pd_metrics_n = pd_metrics.div(pd_metrics['msg_count'], axis=0)
    pd_metrics_avg_n = pd_metrics_avg.div(pd_metrics_avg['msg_count_60d_avg'], axis=0)
    
    return pd_metrics_n, pd_metrics_avg_n, pd_metrics, pd_metrics_avg



###
### NLTK 
###

nltk_full_name = '../data/nlp_nltk_metrics_daily_full_0.01.parquet/'
nltk_15_17_name = '../data/nlp_nltk_metrics_daily_15_17_0.1.parquet/'

nltk_sql_table_name = 'nltk_metrics'
nltk_sql_query = (f"""
SELECT
    creation_date,
    
    AVG(msg_count) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS msg_count_60d_avg,

    AVG(sum_nltk_negativity) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS nltk_negativity_60d_avg,

    AVG(sum_nltk_neutrality) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS nltk_neutrality_60d_avg,
    
    AVG(sum_nltk_positivity) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS nltk_positivity_60d_avg

FROM {nltk_sql_table_name}
""")

def plot_nltk_metrics(filename, sql_query, sql_table_name):
    m_n, m_avg_n, m, m_avg = get_metrics(filename, sql_query, sql_table_name)
    
    def plot_averages(neg=True, neu=True, pos=True):
        plt.figure(figsize=(20, 10))
        title = None
        labels = []
        if neg:
            title = 'Negativity avg'
            labels.append('Negativity avg')
            plt.plot(m_avg_n['nltk_negativity_60d_avg'])
        if neu:
            title = 'Neutrality avg' if title is None else title + ' VS Neutrality avg'
            labels.append('Neutrality avg')
            plt.plot(m_avg_n['nltk_neutrality_60d_avg'])
        if pos:
            title = 'Positivity avg' if title is None else title + ' VS Positivity avg'
            labels.append('Positivity avg')
            plt.plot(m_avg_n['nltk_positivity_60d_avg'])
        plt.title(title + ' scores over time')
        plt.xlabel('Date')
        plt.ylabel('Score')
        plt.legend(labels)
    
    def plot_stat(neg=False, neu=False, pos=False):
        if neg or neu or pos:
            plt.figure(figsize=(20, 10))
        if neg:
            plt.plot(m_n['sum_nltk_negativity'])
            plt.plot(m_avg_n['nltk_negativity_60d_avg'])
            plt.title('Negativity scores over time')
            plt.legend(['Negativity', 'Negativity avg'])
        elif neu:
            plt.plot(m_n['sum_nltk_neutrality'])
            plt.plot(m_avg_n['nltk_neutrality_60d_avg'])
            plt.title('Neutrality scores over time')
            plt.legend(['Neutrality', 'Neutrality avg'])
        elif pos:
            plt.plot(m_n['sum_nltk_positivity'])
            plt.plot(m_avg_n['nltk_positivity_60d_avg'])
            plt.title('Positivity scores over time')
            plt.legend(['Positivity', 'Positivity avg'])
        else:
            print('Please set one of the following parameter to True: [neg, neu, pos]')
            return
        
        plt.xlabel('Date')
        plt.ylabel('Score')
            
    return plot_averages, plot_stat


def get_plot_nltk(full=True):
    if full:
        return plot_nltk_metrics(nltk_full_name, nltk_sql_query, nltk_sql_table_name)
    else:
        return plot_nltk_metrics(nltk_15_17_name, nltk_sql_query, nltk_sql_table_name)


###
### TextBlob
###

blob_full_name = '../data/nlp_blob_metrics_daily_full_0.01.parquet/'
blob_15_17_name = '../data/nlp_blob_metrics_daily_15_17_0.1.parquet/'

blob_sql_table_name = 'blob_metrics'
blob_sql_query = (f"""
SELECT
    creation_date,
    
    AVG(msg_count) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS msg_count_60d_avg,

    AVG(sum_blob_polarity) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS text_blob_polarity_60d_avg,

    AVG(sum_blob_subjectivity) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS text_blob_subjectivity_60d_avg

FROM {blob_sql_table_name}
""")

def plot_blob_metrics(filename, sql_query, sql_table_name):
    m_n, m_avg_n, m, m_avg = get_metrics(filename, sql_query, sql_table_name)
    
    def plot_stat(pol=False, pol_avg=False, subj=False, subj_avg=False):
        if pol_avg or pol or subj_avg or subj:
            plt.figure(figsize=(20, 10))
            title = None
            labels = []
            if pol:
                plt.plot(m_n['sum_blob_polarity'])
                title = 'Polarity'
                labels.append('Polarity')
            if pol_avg:
                plt.plot(m_avg_n['text_blob_polarity_60d_avg'])
                title = 'Polarity avg' if title is None else title + ' VS Polarity avg'
                labels.append('Polarity avg')
            if subj:
                plt.plot(m_n['sum_blob_subjectivity'])
                title = 'Subjectivity' if title is None else title + ' VS Subjectivity'
                labels.append('Subjectivity')
            if subj_avg:
                plt.plot(m_avg_n['text_blob_subjectivity_60d_avg'])
                title = 'Subjectivity avg' if title is None else title + ' VS Subjectivity avg'
                labels.append('Subjectivity avg')
            plt.title(title + ' scores over time')
            plt.xlabel('Date')
            plt.ylabel('Score')
            plt.legend(labels)
        else:
            print('Please set one of the following parameter to True: [pol, pol_avg, subj, subj_avg]')
            
    return plot_stat


def get_plot_blob(full=True):
    if full:
        return plot_blob_metrics(blob_full_name, blob_sql_query, blob_sql_table_name)
    else:
        return plot_blob_metrics(blob_15_17_name, blob_sql_query, blob_sql_table_name)


###
### Bad words
###

bw_full_name = '../data/nlp_bw_metrics_daily_full_0.01.parquet/'
bw_15_17_name = '../data/nlp_bw_metrics_daily_15_17_0.1.parquet/'

bw_sql_table_name = 'bw_metrics'
bw_sql_query = (f"""
SELECT
    creation_date,
    
    AVG(msg_count) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS msg_count_60d_avg,

    AVG(sum_nb_bw_matches) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS nb_bw_matches_60d_avg

FROM {bw_sql_table_name}
""")

def plot_bw_metrics(filename, sql_query, sql_table_name):
    m_n, m_avg_n, m, m_avg = get_metrics(filename, sql_query, sql_table_name)
    
    def plot_stat(bw=False, bw_avg=False):
        if bw or bw_avg:
            plt.figure(figsize=(20, 10))
            title = None
            labels = []
            if bw:
                plt.plot(m_n['sum_nb_bw_matches'])
                title = 'Bad words'
                labels.append('Bad words')
            if bw_avg:
                plt.plot(m_avg_n['nb_bw_matches_60d_avg'])
                title = 'Bad words avg' if title is None else title + ' VS Bad words avg'
                labels.append('Bad words avg')
                
            plt.title(title + ' scores over time')
            plt.xlabel('Date')
            plt.ylabel('Score')
            plt.legend(labels)
        else:
            print('Please set one of the following parameter to True: [bw, bw_avg]')
            
    return plot_stat


def get_plot_bw(full=True):
    if full:
        return plot_bw_metrics(bw_full_name, bw_sql_query, bw_sql_table_name)
    else:
        return plot_bw_metrics(bw_15_17_name, bw_sql_query, bw_sql_table_name)


###
### Hate words
###

hw_full_name = '../data/nlp_hw_metrics_daily_full_0.01.parquet/'
hw_15_17_name = '../data/nlp_hw_metrics_daily_15_17_0.1.parquet/'

hw_sql_table_name = 'hw_metrics'
hw_sql_query = (f"""
SELECT
    creation_date,
    
    AVG(msg_count) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS msg_count_60d_avg,

    AVG(sum_nb_hw_matches) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS nb_hw_matches_60d_avg

FROM {hw_sql_table_name}
""")

def plot_hw_metrics(filename, sql_query, sql_table_name):
    m_n, m_avg_n, m, m_avg = get_metrics(filename, sql_query, sql_table_name)
    
    def plot_stat(hw=False, hw_avg=False):
        if hw or hw_avg:
            plt.figure(figsize=(20, 10))
            title = None
            labels = []
            if hw:
                plt.plot(m_n['sum_nb_hw_matches'])
                title = 'Hate words'
                labels.append('Hate words')
            if hw_avg:
                plt.plot(m_avg_n['nb_hw_matches_60d_avg'])
                title = 'Hate words avg' if title is None else title + ' VS Hate words avg'
                labels.append('Hate words avg')
            plt.title(title + ' scores over time')
            plt.xlabel('Date')
            plt.ylabel('Score')
            plt.legend(labels)
        else:
            print('Please set one of the following parameter to True: [hw, hw_avg]')
            
    return plot_stat


def get_plot_hw(full=True):
    if full:
        return plot_hw_metrics(hw_full_name, hw_sql_query, hw_sql_table_name)
    else:
        return plot_hw_metrics(hw_15_17_name, hw_sql_query, hw_sql_table_name)



###
### Refined hate words
###

ref_hw_full_name = '../data/nlp_ref_hw_metrics_daily_full_0.01.parquet/'
ref_hw_15_17_name = '../data/nlp_ref_hw_metrics_daily_15_17_0.1.parquet/'

ref_hw_sql_table_name = 'ref_hw_metrics'
ref_hw_sql_query = (f"""
SELECT
    creation_date,
    
    AVG(msg_count) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS msg_count_60d_avg,
    
    AVG(sum_hw_ref_intensity) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS hw_ref_intensity_60d_avg,

    AVG(sum_nb_hw_ref_matches) OVER (
        ORDER BY creation_date
        RANGE BETWEEN 30 PRECEDING AND 30 FOLLOWING
    ) AS nb_hw_ref_matches_60d_avg

FROM {ref_hw_sql_table_name}
""")

def plot_ref_hw_metrics(filename, sql_query, sql_table_name):
    m_n, m_avg_n, m, m_avg = get_metrics(filename, sql_query, sql_table_name)
    
    def plot_stat(ref_hw=False, ref_hw_avg=False, intensity=False, intensity_avg=False):
        if ref_hw or ref_hw_avg or intensity or intensity_avg:
            plt.figure(figsize=(20, 10))
            title = None
            labels = []
            if ref_hw:
                plt.plot(m_n['sum_nb_hw_ref_matches'])
                title = 'Hate words refined'
                labels.append('Hate words refined')
            if ref_hw_avg:
                plt.plot(m_avg_n['nb_hw_ref_matches_60d_avg'])
                title = 'Hate words refined avg' if title is None else title + ' VS Hate words refined avg'
                labels.append('Hate words refined avg')
            if intensity:
                plt.plot(m_n['sum_hw_ref_intensity'])
                title = 'Hate words refined intensity' if title is None else title + ' VS Hate words refined intensity'
                labels.append('Hate words refined intensity')
            if intensity_avg:
                plt.plot(m_avg_n['hw_ref_intensity_60d_avg'])
                title = 'Hate words refined intensity avg' if title is None else title + ' VS Hate words refined intensity avg'
                labels.append('Hate words refined intensity avg')
            
            plt.title(title + ' scores over time')
            plt.xlabel('Date')
            plt.ylabel('Score')
            plt.legend(labels)
        else:
            print('Please set one of the following parameter to True: [ref_hw, ref_hw_avg, intensity, intensity_avg]')
            
    return plot_stat


def get_plot_ref_hw(full=True):
    if full:
        return plot_ref_hw_metrics(ref_hw_full_name, ref_hw_sql_query, ref_hw_sql_table_name)
    else:
        return plot_ref_hw_metrics(ref_hw_15_17_name, ref_hw_sql_query, ref_hw_sql_table_name)


###
### Utils
###

def get_all_metrics(avg=False, remove_outliers=True):
    nltk_n, nltk_avg_n, _, _ = get_metrics(nltk_full_name, nltk_sql_query, nltk_sql_table_name)
    blob_n, blob_avg_n, _, _ = get_metrics(blob_full_name, blob_sql_query, blob_sql_table_name)
    bw_n, bw_avg_n, _, _ = get_metrics(bw_full_name, bw_sql_query, bw_sql_table_name)
    hw_n, hw_avg_n, _, _ = get_metrics(hw_full_name, hw_sql_query, hw_sql_table_name)
    hw_ref_n, hw_ref_avg_n, _, _ = get_metrics(ref_hw_full_name, ref_hw_sql_query, ref_hw_sql_table_name)
    
    if not avg:
        nlp_n = pd.concat([nltk_n, blob_n, bw_n, hw_n, hw_ref_n], axis=1, sort=True).drop('msg_count', axis=1)
    else:
        nlp_n = pd.concat([nltk_avg_n, blob_avg_n, bw_avg_n, hw_avg_n, hw_ref_avg_n], axis=1, sort=True).drop('msg_count', axis=1)

    nlp_n.columns = ['neg', 'neu', 'pos', 'pol', 'subj', 'bw', 'hw', 'hw_ref', 'intensity']
    nlp_n = nlp_n.drop('intensity', axis=1)

    if remove_outliers:
        # Remove outliers assuming gaussian distribution
        nlp_n = nlp_n[(sp.stats.zscore(nlp_n) < 3).all(axis=1)]

    return nlp_n