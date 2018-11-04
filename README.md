# Opinion Polarization on Reddit

## Abstract

In the recent months following the 2016 American election, many media outlets have argued about an increase in social divisiveness and the polarization of political opinions. The intensification of political debates as well as a growing inability to find common grounds are seen as direct consequences of this divide.

Many of these organization point at modern social media as a potential cause of this polarization. Their inherent filter bubbles and a promotion of extreme opinions create fertile ground for disrespectful debate, especially when it meets little consequence.

Through an analysis of the wide range of political and social discussions available on Reddit, our goal is to measure and analyze how this polarization of opinions has evolved over time in order to confirm or disprove this hypothesis. After defining clear metrics for social divisiveness and controversy, we would like to compare them among the different communities of Reddit, as well as understand what topics are the most contentious.

## Research questions

- What direct metrics can be used to quantify divisiveness?
- Are people more divided on the internet today?
- What topics are the most divisive?
- Are vulgarity, lack of respect, lack of empathy correlated with divisiveness?
- Which communities feature the most polarized discussions?
- What are personal characteristics of individuals with very polarized opinions?
- Could we infer some reasons and consequences of this division based on the data?

## Dataset

Our main dataset will be the [Reddit dataset](http://academictorrents.com/details/85a5bd50e4c365f8df70240ffd4ecc7dec59912b) containing comments from December 2005 to March 2017. Additional description can be found [here](https://www.reddit.com/r/datasets/comments/3bxlg7/i_have_every_publicly_available_reddit_comment/). The main reasons why we decided to go with Reddit instead of another social media platform (e.g. Twitter) are the following.

- Reddit is a global forum which is organized through user-constituted communities centered around specific topics (a.k.a. “subreddits”). As such, it is easy to follow the evolution of social behaviors according to the topic the members discuss. 

- Reddit allows users to interact with content through a simple system of feedback: each user can “downvote” or “upvote” a content to express whether it was appreciated or not. Reddit also provides additional metrics such as a "controversy" score (controversial topics typically have a high number of both upvotes and downvotes).

- The dataset provided is highly comprehensive, as it features all of the comments made publicly available from the creation of the site in 2005 until early 2017. This can be useful to determine how trends evolved and fluctuate over a long period of time.

Although the dataset is huge (350GB), we will focus on specific communities (subreddits) and topics in order to better assess divisiveness and its expression within a community.

Finally, we might use additional NLP databases based on the metrics used to analyze the text but this will be decided in the second Milestone.

## A list of internal milestones up until project milestone 2

1. Study different metrics and find a suitable measure of divisiveness. These metrics can either be direct (e.g. reddit’s “controversial” score) or indirect (measuring vulgarity, lack of respect, etc…).
2. Early results of sentiment analysis on reddit comments (NLP).
3. Choose years, communities and topics of interest. Motivate the choices made. Note: Google trends might be used in order to find the timeframe and topics that might have caused contentious debates on the net.
4. Implement tools to extract chosen topics of interest or communities from the dataset.
5. Create initial time series featuring the overall divisiveness over time.
6. Rank subreddits & topics chosen according to their polarization.

## Questions for TAa

- For some metrics we want to use, we will have to do some natural language processing. Do you have any tips on how to proceed efficiently?
- Did you understand what we meant by divisiveness/polarization of opinion through this milestone?
- Is our bottom up approach (starting with a selection of specific topics and timeframe then work toward more general ideas and findings) a good start? Or would we benefit of trying to rather do a top-down approach (consider the whole reddit dataset and then work toward specific events)?
