
import matplotlib.pyplot as plt

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