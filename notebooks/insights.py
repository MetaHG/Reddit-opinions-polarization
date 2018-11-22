
from IPython.display import Markdown, display

def printmd(string):
    display(Markdown(string))

def insights_comments(metrics):

    printmd(
    "> The dataset contains a total of **{:,}** comments ranging from "
    "**{:%m/%d/%Y}** to **{:%m/%d/%Y}**.".format(
        # metrics['count_of_comments'][0],
        # metrics['count_of_comments'],
        metrics['count_of_comments'][0],
        metrics['first_date'][0],
        metrics['last_date'][0],
    ))

def insights_posts(metrics):

    printmd(
    "> There is a total of **{:,}** posts on Reddit with at least one reply.".format(
        metrics['count_of_posts'][0]
    ))

def insights_removed(metrics):

    printmd(
    "> A total of **{:,}** comments were deleted by their "
    "authors, which is **{:.2f}%** of the dataset.".format(
        metrics['count_of_deleted'][0],
        metrics['count_of_deleted'][0] / metrics['count_of_comments'][0] * 100,
    ))

def insights_deleted(metrics):

    printmd(
    "> A total of **{:,}** comments were removed by the moderation "
    "teams, which is **{:.2f}%** of the dataset.".format(
        metrics['count_of_removed'][0],
        metrics['count_of_removed'][0] / metrics['count_of_comments'][0] * 100,
    ))

def insights_comments_levels(metrics):

    printmd(
    "> Out of all the comments, **{:,}** (**{:.2f}%** of the dataset) are top-"
    "level comments. This makes an average of **{:.2f}** top-level comments per post. "
    "The other **{:,}** (**{:.2f}%**) are replies to other comments, with a "
    "maximum depth of **{}**.".format(
        metrics['count_top'][0],
        metrics['count_top'][0] / metrics['count_of_comments'][0] * 100,
        metrics['count_top'][0] / metrics['count_of_posts'][0],
        metrics['count_low'][0],
        metrics['count_low'][0] / metrics['count_of_comments'][0] * 100,
        "[MISSING: max_depth]"
    ))
