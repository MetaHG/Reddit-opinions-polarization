import matplotlib.pyplot as plt
import matplotlib.ticker as ticker

def plot_metric_distrib(ax, metric, main_title, title, col):
    metric.hist(col, bins=100, ax=ax)
    ax.set_title(main_title + ' ' + title)
    ax.set_xlabel(title)
    ax.set_ylabel('frequency')


def plot_metrics_distrib(metrics_list, main_titles_list, plots, figsize=(15, 30)):
    fig, axes = plt.subplots(nrows=len(plots), ncols=len(metrics_list), figsize=figsize)
    
    for i, plot in enumerate(plots):
        for j, metric in enumerate(metrics_list):
            plot_metric_distrib(axes[i, j], metric, main_titles_list[j], *plot)


def plot_corr_mat(ax, data, main_title, corr_name, opt_name=''):
    ax.matshow(data.corr())
    ax.set_title(main_title + ' ' + corr_name + ' ' + opt_name + 'correlation matrix')
    ax.xaxis.set_major_locator(ticker.MultipleLocator(1))
    ax.yaxis.set_major_locator(ticker.MultipleLocator(1))
    ax.set_xticklabels(data.columns.values, rotation=90)
    ax.set_yticklabels(data.columns.values)
    ax.xaxis.set_ticks_position('bottom')


def plot_corr_mats(metrics_list, main_title, corr_names, opt_names_list=None):
    fig, axes = plt.subplots(nrows=len(corr_names), ncols=len(metrics_list), figsize=(15, 20), constrained_layout=True)
    
    if (len(metrics_list) == 1):
        for i, corr in enumerate(corr_names):
            plot_corr_mat(axes[i], metrics_list[0], main_title, corr, opt_names_list[i] if opt_names_list is not None else '')
    elif (len(corr_names) == 1):
        for i, metric in enumerate(metrics_list):
            plot_corr_mat(axes[i], metric, main_title, corr_names[0], opt_names_list[i] if opt_names_list is not None else '')
    else:
        for i, metric in enumerate(metrics_list):
            for j, corr in enumerate(corr_names):
                plot_corr_mat(axes[i, j], metric, main_title, corr, opt_names_list[i] if opt_names_list is not None else '')


def plot_metric(metrics, ax, main_title, title1, title2, col1, col2):
    metrics.plot.scatter(col1, col2, alpha=0.2, c='b', ax=ax)
    ax.set_title(main_title + ': ' + title1 + ' VS ' + title2)
    ax.set_xlabel(title1)
    ax.set_ylabel(title2)
    

def plot_metrics(metrics_list, titles_list, plots):
    fig, axes = plt.subplots(nrows=len(plots), ncols=len(metrics_list), figsize=(15, 80))

    if (len(metrics_list) == 1):
        for i, plot in enumerate(plots):
            plot_metric(metrics_list[0], axes[i], titles_list[0], *plot)
    else:
        for i, plot in enumerate(plots):
            for j, metric in enumerate(metrics_list):
                plot_metric(metric, axes[i, j], titles_list[j], *plot)


###
### Specific functions
###

def plot_daily_distrib(metrics_list, main_titles_list):
    plots = [
        ('negativity', 'neg'),
        ('neutrality', 'neu'),
        ('positivity', 'pos'),
        ('agreement factor', 'agreement_factor'),
        ('total score', 'total_score')
    ]
    
    plot_metrics_distrib(metrics_list, main_titles_list, plots)


def plot_subreddit_distrib(metrics_list, main_titles_list):
    plots = [
        ('negativity', 'neg'),
        ('neutrality', 'neu'),
        ('positivity', 'pos'),
        ('agreement factor', 'agreement_factor')
    ]
    
    plot_metrics_distrib(metrics_list, main_titles_list, plots)


def plot_nlp_distrib(metrics_list, main_titles_list):
    plots = [
        ('negativity', 'neg'),
        ('neutrality', 'neu'),
        ('positivity', 'pos'),
        ('polarity', 'pol'),
        ('subjectivity', 'subj'),
        ('bad words', 'bw'),
        ('hate words', 'hw'),
        ('hate words refined', 'hw_ref'),
        ('hate words refined intensity', 'intensity')
    ]

    plot_metrics_distrib(metrics_list, main_titles_list, plots, figsize=(15, 50))
            

def plot_daily_metrics(metrics_list, titles_list):
    plots = [('negativity', 'agreement factor', 'neg', 'agreement_factor'),
            ('neutrality', 'agreement factor', 'neu', 'agreement_factor'),
            ('positivity', 'agreement factor', 'pos', 'agreement_factor'),
            ('polarity', 'agreement factor', 'pol', 'agreement_factor'),
            ('subjectivity', 'agreement factor', 'subj', 'agreement_factor'),
            ('bad words', 'agreement factor', 'bw', 'agreement_factor'),
            ('hate words', 'agreement factor', 'hw', 'agreement_factor'),
            ('refined hate words', 'agreement factor', 'hw_ref', 'agreement_factor'),
            ('total score', 'agreement factor', 'total_score', 'agreement_factor'),
            ('positivity', 'total score', 'pos', 'total_score')]
    
    plot_metrics(metrics_list, titles_list, plots)


def plot_subreddit_metrics(metrics_list, titles_list):
    plots = [('negativity', 'agreement factor', 'neg', 'agreement_factor'),
            ('neutrality', 'agreement factor', 'neu', 'agreement_factor'),
            ('positivity', 'agreement factor', 'pos', 'agreement_factor'),
            ('polarity', 'agreement factor', 'pol', 'agreement_factor'),
            ('subjectivity', 'agreement factor', 'subj', 'agreement_factor'),
            ('bad words', 'agreement factor', 'bw', 'agreement_factor'),
            ('hate words', 'agreement factor', 'hw', 'agreement_factor'),
            ('refined hate words', 'agreement factor', 'hw_ref', 'agreement_factor'),
            ('negativity', 'positivity', 'neg', 'pos'),
            ('negativity', 'bad words', 'neg', 'bw')]
    
    plot_metrics(metrics_list, titles_list, plots)

def plot_nlp_metrics(metrics_list, titles_list):
    plots = [('negativity', 'positivity', 'neg', 'pos'),
            ('negativity', 'neutrality', 'neg', 'neu'),
            ('positivity', 'neutrality', 'pos', 'neu'),
            ('polarity', 'negativity', 'pol', 'neg'),
            ('polarity', 'positivity', 'pol', 'pos'),
            ('polarity', 'subjectivity', 'pol', 'subj'),
            ('negativity', 'bad words', 'neg', 'bw'),
            ('negativity', 'hate words', 'neg', 'hw'),
            ('negativity', 'hate words refined', 'neg', 'hw_ref')]
    
    plot_metrics(metrics_list, titles_list, plots)
