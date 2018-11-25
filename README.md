# Opinion Polarization on Reddit

## Abstract

In the recent months following the 2016 American election, many media outlets have argued about an increase in social divisiveness and the polarization of political opinions. The intensification of political debates as well as a growing inability to find common grounds are seen as direct consequences of this divide.

Many of these organizations point at modern social media as a potential cause of this polarization. Their inherent filter bubbles and a promotion of extreme opinions create fertile ground for disrespectful debate, especially when it meets little consequence.

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

- Reddit allows users to interact with content through a simple system of feedback: each user can “downvote” or “upvote” a content to express whether it was appreciated or not. Reddit also provides additional metrics such as a "controversy" score (controversial topics typically have a high number of both upvotes and downvotes). However, after exploring the dataset, we observed that the upvotes and downvotes where not collected correctly (values are missing or downvotes are always 0) and that the controversy score have a lot missing values for some time period. Our analysis is therefore affected and we will have to find other equivalent metrics.

- The dataset provided is highly comprehensive, as it features all of the comments made publicly available from the creation of the site in 2005 until early 2017. This can be useful to determine how trends evolved and fluctuate over a long period of time.

Although the dataset is huge (350GB), we will focus on specific communities (subreddits) and topics in order to better assess divisiveness and its expression within a community.

Finally, to quantify vulgarity, hate speech and lack of respect in general, we are using two additional datasets:
1. The first one consists of a list of bad words that we found here: https://data.world/sya/list-of-bad-words. Note that this dataset is noisy. Some words are not bad words (e.g. "Charlie") and some other words are not necessarily considered as bad words by everyone.
2. The second dataset consists of two different files containing hate speech words or expressions (n-grams). It can be found here: https://data.world/ml-research/automated-hate-speech-detection-hate-speech-lexicons . The first file contains hate speech words which are not necessarily real hate speech. This file is noisy. The second file contains hate speech words from the first file, but has been refined to remove words which were not considered as hate speech. This refinement was done in the context of the companion paper <sup>1</sup>.



## Organisation of the repository :


```
.
+-- _eggs_
+-- _jobs_
+--_notebooks
|  +--Basic Metrics.ipynb
|  +--DataCollection.ipynb
|  +--NLP Metrics.ipynb
|  +--nlp metrics analysis.ipynb
|  +--insight.py
|  +--plot.py
+--_report_
+--_scripts_
|  +--connect.sh
|  +--sync.sh
|  +--run.sh
|  +--fetch.sh
+--_src_
|  +--load.py
+-- README.md
```
Above is a schematic description of the structure of the repository. As we need to perform computations on the cluster using pyspark, most of the files and folders of the repo serve for this purpose. 

* The _eggs_ folder contains the python libraries we need to deploy on the cluster packaged into eggs.

* The _jobs_ folder contains the python files with the code needed to be run on the cluster for our computations.

* The _notebooks_ folder contains all the notebook that were created in order to do some data exploration and analysis. **DataCollection.ipynb** is the notebook containing the results for this milestone, and the notebook on which we will be working until the end. The other notebook were temporary notebooks used to do some smaller scale computation or testing. The two .py files on this folder are used by the main notebook (DataCollection) in order to reduce the amount of code within it.

* The _report_ folder contains the template for the final latex report.

* The _scripts_ folder contains shell script we developped in order to facilitate our interaction with the cluster: **connect.sh** allows us to connect to the cluster, **sync.sh** will copy the content from this repository into the user filespace of the cluster, **run.sh** is used to run a selected job from the folder _jobs_ and finally, **fetch.sh** is used to retrieve locally the results from our jobs computed by the cluster saved into parquet format.

* The _src_ folder contains all python code that is needed by multiple job for modularity purpose. **load.py** contains function allowing easy loading of the reddit dataset according to a year of choice. 

* The _README.md_ file is the one you are currently reading.

One last folder should get a mention even though it is absent from this repository, is the _data_ folder. This is where we store all the results from big computations that were done either on the cluster or locally but were too big to be kept on the github. This folder is syncronized between the team members using dropbox, and we don't put it on this repository due to its sheer size (more than 3GB).


## Internal steps until Milestone 3
* Refine list of subreddits to be analyzed based on existing metrics.

* Focus on the subreddits where the agreement factor is the lowest. (i.e. video games, poltical/news and cities). Produce some basic metrics on them. Interpret those metrics according to the context (i.e. subject of the subreddit).

* Run NLP on longer samples and/or subreddits chosen.

* Trying other metrics to understand potential biases in the ones chosen previously.

* Understand if [deleted] and [removed] are actually usable.

* In-depth analysis of potential reasons for the jump in agreement observed around November 2016, observe if other metrics have similar behavior.

* Study correlation between the different metrics.

* Finally, using all the previous results and evidences, conclude by answering our research questions.

## Questions for TAs
* How does the cluster schedule job? Sometimes we had to wait 1 hour for a job to be runned after being accepted, while other time this took only 20 seconds.


[1]: Davidson et al.(2017), Automated Hate Speech Detection and the Problem of Offensive Language._Proceedings of the 11th International AAAI Conference on Web and Social Media_. 
