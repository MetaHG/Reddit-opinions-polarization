

def fetch_metric_for_group(metric, group):
    return spark.sql("""
        SELECT {}
        FROM nlp JOIN subreddits ON LOWER(nlp.subreddit) == subreddits.subreddit
        WHERE subreddits.Group == '{}'
    """.format(metric, group)).toPandas()

def fetch_metric_for_subgroup(metric, subgroup):
    return spark.sql("""
        SELECT {}
        FROM nlp JOIN subreddits ON LOWER(nlp.subreddit) == subreddits.subreddit
        WHERE subreddits.Subgroup == '{}'
    """.format(metric, subgroup)).toPandas()

def create_group_metrics(groups, metrics):
    return { metric: [ fetch_metric_for_group(metric, group) for group in groups ] for metric in metrics }

def create_subgroup_metrics(subgroups, metrics):
    return { metric: [ fetch_metric_for_subgroup(metric, subgroup) for subgroup in subgroups ] for metric in metrics }

def load_subreddit_list(name, subgroup=None):

    with open('../data/subreddits/' + name + '.txt') as f:
        lines = f.readlines()

    group = ""
    subreddit_list_df = pd.DataFrame(data=[], columns=['type'])

    for line in lines:

        if line == '\n':
            continue

        if line[0] == '#':
            group = line[2:].strip('\n')
            continue

        if subgroup != None and group not in subgroup:
            continue

        subreddit = line[3:].strip('\n').lower()

        subreddit_list_df.loc[subreddit] = group

    return subreddit_list_df
