# Opinion Polarization on Reddit

# Abstract

In the recent months following the 2016 American election, many media outlets have argued about an increase in social divisiveness and the polarization of political opinions. The intensification of political debates as well as a growing inability to find common grounds are seen as direct consequences of this divide.

Many of these organization point at modern social media as a potential cause of this polarization. Their inherent filter bubbles and a promotion of extreme opinions create fertile ground for disrespectful debate, especially when it meets little consequence.

Through an analysis of the wide range of political and social discussions available on Reddit, our goal is to measure and analyze how this polarization of opinions has evolved over time in order to confirm or disprove this hypothesis. After defining clear metrics for social divisiveness and controversy, we would like to compare them among the different communities of Reddit, as well as understand what topics are the most contentious.

# Research questions

- What direct metrics can be used to quantify divisiveness?
- Are people more divided on the internet today?
- What topics are the most divisive?
- Are vulgarity, lack of respect, lack of empathy correlated with divisiveness?
- Which communities feature the most polarized discussions?
- What are personal characteristics of individuals with very polarized opinions?
- Could we infer some reasons and consequences of this division based on the data?

# Dataset

## URLs

- http://academictorrents.com/details/85a5bd50e4c365f8df70240ffd4ecc7dec59912b
- https://www.reddit.com/r/datasets/comments/3bxlg7/i_have_every_publicly_available_reddit_comment/
-

## Description & Motivation

Our main dataset will be the Reddit dataset containing comments from December 2005 to March 2017. If needed, we might try and collect additional data for the end of 2017. The dataset is available at and a detailed description is made at: . Moreover, we might use additional NLP databases based on the metrics used to analyze the text but this will be decided in the second Milestone.

We think of using uniquely this dataset in order to test our research hypothesis for the following reasons :


- Reddit is a global forum which is organized through user-constituted communities centered around specific topics (a.k.a. “subreddits”). As such, it is easy to follow the evolution of social behaviors according to the topic the members discuss. Focusing on specific topics might help understand if disrespectful behavior stems from the subject or the medium on which the subject is shared.


- Reddit allows users to interact with content through a simple system of feedback: each user can “downvote” or “upvote” a content to express whether it was appreciated or not. According to how much positively a content was rated, Reddit will give it more coverage on the platform. Reddit also allows users to sort through content according to their amount of upvote or downvote. For example, sorting by “controversial” will rank all posts which achieved a high number of both downvotes and upvotes. Such sorting might be useful for our research, as we could observe divided discussions within the platform.


- The dataset provided is highly comprehensive, as it features all of the comments made publicly available from the creation of the site in 2005 until early 2017. This means that we have at our disposal ten years of internet history. This can be useful to determine how trends evolved and fluctuate over a long period of time.

Considering how the Reddit dataset is organized, each reddit comment is stored in JSON format according to the following convention :

{

    "gilded":0,
    "retrieved_on":1425124228,
    "ups":3,
    "subreddit_id":"t5_2s30g",
    "edited":false,
    "controversiality":0,
    "parent_id":"t1_cnapn0k",
    "subreddit":"AskMen",
    "body":"I can't agree with passing the blame, but I'm glad to hear it's at least helping you with the anxiety. I went the other direction and started taking responsibility for everything. I had to realize that people make mistakes including myself and it's gonna be alright. I don't have to be shackled to my mistakes and I don't have to be afraid of making them. ",
    "created_utc":"1420070668",
    "downs":0,
    "score":3,
    "author":”TheDukeofEtown",
    "archived":false,
    "id":"cnasd6x",
    "score_hidden":false,
    "name":"t1_cnasd6x",
    "link_id":"t3_2qyhmp"
  }

We get for each comment a rich amount of metadata which will be useful for our project. The first basic metric that could characterize division of opinion, are the “ups” and “downs” attributes, which denote the number of upvotes and downvotes the comment received by other reddit users. A comment which received a large equivalent amount of up and down votes means that it greatly divides the set of all reddit users reading it. Also, chances are that the opinons expressed in subcomments will be greatly divided.

Although the dataset is huge (350GB), we will focus on specific communities (subreddits) and topics in order to better assess divisiveness and its expression within a community.

# A list of internal milestones up until project milestone 2

1. Study different metrics and find a suitable measure of divisiveness. These metrics can either be direct (e.g. reddit’s “controversial” score) or indirect (measuring vulgarity, lack of respect, etc…).
2. Early results of sentiment analysis on reddit comments (NLP).
3. Choose years, communities and topics of interest. Motivate the choices made. Note: Google trends might be used in order to find the timeframe and topics that might have caused contentious debates on the net.
4. Implement tools to extract chosen topics of interest or communities from the dataset.
5. Create initial time series featuring the overall divisiveness over time.
6. Rank subreddits & topics chosen according to their polarization.

# Questions for TAa

- For some metrics we want to use, we will have to do some natural language processing. Do you have any tips on how to proceed efficiently?
- Did you understand what we meant by divisiveness/polarization of opinion through this milestone?
- Is our bottom up approach (starting with a selection of specific topics and timeframe then work toward more general ideas and findings) a good start? Or would we benefit of trying to rather do a top-down approach (consider the whole reddit dataset and then work toward specific events)?
