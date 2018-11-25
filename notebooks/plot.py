
import matplotlib.pyplot as plt
import pandas as pd

def plot_daily_metric(df, key, ax=None):

    if ax is None:
        ax = df[[key, key + "_60d_avg"]].plot()
    else:
        df[[key, key + "_60d_avg"]].plot(ax=ax)

    ax.set_xlabel("Date")

    if key == "count_of_comments":
        legend = "Daily comments"
    elif key == "count_of_users":
        legend = "Daily active users"
    elif key == "prop_of_controversial":
        legend = "Daily prop. of controversial comments"
    else:
        legend = key

    ax.legend([legend, legend + " (60-day average)"])

def plot_daily_metrics(df, key1, key2):

    fig, ax = plt.subplots(figsize=(15, 5), ncols=2)

    _ = plot_daily_metric(df, key1, ax[0])
    _ = plot_daily_metric(df, key2, ax[1])


def plot_scores(score_metrics):

    fig, ax = plt.subplots(figsize=(15, 5), ncols=2)

    df = score_metrics.sort_index()
    positives = df.loc[df.index >= 0]
    negatives = df.loc[df.index < 0]
    negatives = negatives.set_index(negatives.index.map(lambda x: -x))
    negatives = negatives.sort_index()

    _ = positives.plot(ax=ax[0], logy=True, logx=True, title='Positive score distribution', legend=False)
    _ = negatives.plot(ax=ax[1], logy=True, logx=True, title='Negative score distribution', legend=False)
    
def plot_frequency_of_topics(dataset, topics):
    current_df = dataset
    idx = pd.period_range(min(current_df.created), max(current_df.created)).to_series()
    ts_df = pd.DataFrame({'created': list(idx), 'dummy': [0 for i in range(len(idx))]}).set_index('created')
    ts_df.index = ts_df.index.to_timestamp()

    for t in topics:
        topic_df = current_df[current_df.topic == t][['created', 'topic']]
        toplot = topic_df.groupby(['created']).agg(len).rename({'topic':t}, axis=1)
        ts_df = ts_df.join(toplot, how='left')

    ts_df = ts_df.drop(columns='dummy').fillna(0)
    ax = ts_df.plot(title='Number of post per topic through time', figsize=(15,8))
    ax.set_xlabel('Time')
    ax.set_ylabel('Occurence')