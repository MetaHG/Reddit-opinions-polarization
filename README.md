# Opinions polarization analysis on the internet through Reddit social media over the years 2005 to 2015

# Abstract
In the recent months following the 2016 American election, many media outlets have argued about an increase in social divisiveness and the polarization of political opinions. The intensification of political debates as well as a growing inability to find common grounds are seen as direct consequences of this divide.

Many of these organization point at modern social media as a potential cause of this polarization. Their inherent filter bubbles and a promotion of extreme opinions create fertile ground for disrespectful debate, especially when it meets little consequence.

Through an analysis of the wide range of political and social discussions available on Reddit, we wish to measure and analyze how this polarization of opinions has evolved over time in order to confirm or disprove this hypothesis. After defining clear metrics for social divisiveness and controversy, we would like to compare them among the different communities of Reddit, as well as understand what topics are the most contentious.

# Research questions
- How can we correctly quantify divisiveness, meanness, empathy, or vulgarity on the internet?
- How are these correlated?
- Are people less respectful on the internet today?
- What topics brings the most division / disrespect?
- Which subreddits feature the most disrespectful and/or polarized discussions?
- Could we infer some reasons and consequences of this division based on the data?

# Dataset
We will use the reddit publicly available comments dump collected in July 2015 here : https://www.reddit.com/r/datasets/comments/3bxlg7/i_have_every_publicly_available_reddit_comment/

We think of using uniquely this dataset in order to test our research hypothesis for the following reasons :

- Reddit is a global forum which decentralizes itself through specific topics maintained by user constituted communities, which are called “subreddits”. As such, it is easy to follow the evolution of social behaviors according to the topic the members discuss. Focusing on specific topics might help understand if disrespectful behavior stems from the subject or the medium on which the subject is shared.
- Reddit allows users to interact with content through a simple system of feedback: each user can down vote or up vote a content to express wether it was appreciated or not. According to how much positively a content was rated, Reddit will give it more coverage on the platform. Reddit also allows user to sort through content according to the measure of up vote or down vote. For example, sorting by “controversial” will rank all posts which achieved a high number of both downvotes and upvotes. Such sorting might be useful for our research, as we could observe divided discussions within the platform.
- The dataset provided is highly comprehensive, as it features all of the comments made publicly available from the creation of the site in 2005 until the date of the dump which was made in 2015. This means that we have at our disposal ten years of internet history seen through Reddit’s eyes. This can be useful in order to determine wether the divisiveness or disrespect has fluctuated through the years.

However, we do not completely exclude the idea of using the Twitter dataset as the Twitter community is not necessarily the same as the Reddit community. Moreover, Twitter has a total character number per message restriction which Reddit does not have. This could lead to different polarization level than the one observed on Reddit and this could be an interesting point to study. Hashtags could be used to split messages into topics.

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
1. Study the different suitable metrics to find a good measure of divisiveness.
2. Choosing years / communities / topics of interest, and motivate the choices made. Google trends might be used in order to find the timeframe and topics that might have caused contentious debates on the net.
3. Program the tools used to extract the chosen topics of interest from the dataset.
4. High-level descriptions of the data based on previous topics of interest.
5. Early results to orient the rest of the analysis / (Idea:) Producing some time-series graph showcasing evolution of divisiveness based on the measure mentioned in point 1 and selected communities, topic and timeframe mentioned in point 4.
6. According to early results in point 5, perform some cross-referencing using tools with a more broad coverage of the web (such as Google trends) to assess the soundness of the patterns observed only on Reddit. (i.e. checking wether a controversial subject on reddit was also controversial on the rest of the web.

# Questions for TAa
- Is our research idea well-defined enough to produce coherent results in the end?
- Did you understand what we meant by divisiveness/polarization of opinion through this milestone?
- Is our bottom up approach (starting with a selection of specific topics and timeframe then work toward more general ideas and findings) a good start? Or would we benefit of trying to rather do a top-down approach (consider the whole reddit dataset and then work toward specific events)?
