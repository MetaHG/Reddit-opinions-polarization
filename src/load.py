


def load_data(sc, filter=None, sample=True):

    if filter is None:

        df = sc.textFile("hdfs:///datasets/reddit_data/*/*")

    elif type(filter) is list:

        directories = ["hdfs:///datasets/reddit_data/{}/*".format(year) for year in filter]
        df = sc.textFile(",".join(directories))

    elif type(filter) is dict:

        directories = [["hdfs:///datasets/reddit_data/{}/{}".format(year, month)
                        for month in months] for (year, months) in filter]

        df = sc.textFile(",".join(directories))

    else:

        raise NotImplementedError("filter must be either an array, a dictionary or None.")

    return df


