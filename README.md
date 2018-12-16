# Opinion Polarization on Reddit

## Important Notes

* This Readme was also updated (again) with additional information. Namely, we updated the Internal Milestones section with what was done for each of these milestones, we also added a section mentionned how we split the tasks and a description of all the additional datasets we generated.
* The Data Story can be found [here](https://jfperren.github.io/Reddit-polarization-story/). It contains custom css on top of Jekyll which we tried to make responsive, so it should work on mobile. However, it is best viewed on a regular computer screen. Note that we were note sure if it had to be in a completely separate github repository or simply another folder. Just to be sure, we copied our initial code (which is found in the `docs/` folder) over to [another repository](https://github.com/jfperren/Reddit-polarization-story) which contains only the code related to the data story.
* Like in Milestone 2, the notebook with Data Collection can be found at [notebooks/DataCollection.ipynb](https://github.com/MetaHG/Reddit-opinions-polarization/blob/master/notebooks/DataCollection.ipynb).
* All additional work was added **at the end** of the notebook. Hopefully this will make it easier for you to review the new results.

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

*Note - Answers to these questions can be found at the end of DataCollection.ipynb*

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
+--_docs_
+-- _eggs_
+-- _jobs_
+--_notebooks
|  +--Basic Metrics.ipynb
|  +--DataCollection.ipynb
|  +--LDA Trump Hillary.ipynb
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

* The _docs_ folder contain the source code from our web site serving as a Data Story of the project.

* The _eggs_ folder contains the python libraries we need to deploy on the cluster packaged into eggs.

* The _jobs_ folder contains the python files with the code needed to be run on the cluster for our computations.

* The _notebooks_ folder contains all the notebook that were created in order to do some data exploration and analysis. **DataCollection.ipynb** is the notebook containing all the results from our work which were good enough to be presented. The other notebook were temporary notebooks used to do some smaller scale computation or testing. The two .py files on this folder are used by the main notebook (DataCollection) in order to reduce the amount of code within it.

* The _scripts_ folder contains shell script we developped in order to facilitate our interaction with the cluster: **connect.sh** allows us to connect to the cluster, **sync.sh** will copy the content from this repository into the user filespace of the cluster, **run.sh** is used to run a selected job from the folder _jobs_ and finally, **fetch.sh** is used to retrieve locally the results from our jobs computed by the cluster saved into parquet format.

* The _src_ folder contains all python code that is needed by multiple job for modularity purpose. **load.py** contains function allowing easy loading of the reddit dataset according to a year of choice. 

* The _README.md_ file is the one you are currently reading.

One last folder should get a mention even though it is absent from this repository, is the _data_ folder. This is where we store all the results from big computations that were done either on the cluster or locally but were too big to be kept on the github. This folder is syncronized between the team members using dropbox, and we don't put it on this repository due to its sheer size (more than 7GB).


## Internal steps until Milestone 3 (And Results)

* Refine list of subreddits to be analyzed based on existing metrics. -> *Done, we ran more computations and studies on lists of subreddits.*.

* Focus on the subreddits where the agreement factor is the lowest. (i.e. video games, poltical/news and cities). Produce some basic metrics on them. Interpret those metrics according to the context (i.e. subject of the subreddit). -> *Done, better analysis of how the metrics vary on specific subgroups of subreddits*.

* Run NLP on longer samples and/or subreddits chosen. -> *Done, still could not run on the entire dataset but got closer in terms of size of sample and was able to run without restriction in date range*.

* Trying other metrics to understand potential biases in the ones chosen previously. -> *Got some interesting results with number of subreddits visited per person per month. Also as able to fully use nlp metrics and found some good results.*.

* Understand if [deleted] and [removed] are actually usable. -> *They were not.*

* In-depth analysis of potential reasons for the jump in agreement observed around November 2016, observe if other metrics have similar behavior. -> *We found that it was exactly on November 1st 2016 (so not election day). Could not find a reason for this yet.*

* Study correlation between the different metrics. -> *Done, did not find too many interesting things there*.

* Finally, using all the previous results and evidences, conclude by answering our research questions. -> *Done in the data story and the notebook.*

## Description of Each Person's Work

**Julien Perrenoud**

* ML2 - Initial setup to access cluster, run jobs and fetch data back (bash scripts)
* ML2 - Definition of Agreement score
* ML2 - High-Level metrics (total count of comments, deleted / gold / controversial, etc...)
* ML2 - Daily metrics, including daily agreement
* ML3 - Analyze metrics on macro-communities (sports, politics, ...) -> "A Look at Communities"
* ML3 - Analyze metrics on smaller communities within politics
* ML3 - Analyze most extreme subreddits for each metric
* ML3 - Compute average count of subreddits per user per month to compare with increasing agreement
* ML3 - Setup Jekyll
* ML3 - Create custom CSS, plots and commentary for data story

**Valentin Borgeaud**


* ML2 - Read documentation about NLTK sentiment analysis + find datasets of hate speech / bad words
* ML2 - Setup cluster to accept NLTK via eggs, custom location for additional zip files, etc...
* ML2 - Create jobs for extracting positivity, negativity, vulgarity metrics measures on the dataset (on a small time window)
* ML3 - Optimize previous scripts to perform sentiment analysis on a bigger subsample of the dataset with full date range
* ML3 - Evaluate how NLP metrics evolve over time.
* ML3 - Plot the distributions of all metrics.
* ML3 - Analysis of correlations between all metrics, aggregated either by day or by subreddit.

**Cédric Viaccoz**

* ML2 - First local implementation of LDA and test on r/news in november 2016
* ML3 - Pyspark implementation of LDA
* ML3 - LDA on r/The_Donald and r/hillaryclinton with result analysis

## Description of Parquets

During the scope of this project, we generated a big amount of derivative data from the dataset. Because of the sheer size of information, we often had to store this data in parquets. Concretely, it means that if someone wants to re-run the notebook and still get the results that are displayed (in order to verify the results, for instance), one will have to download the needed parquet files.

For practical, synchronization purposes, we decided to host all the parquet files on Dropbox. One can download each of them by using the following [link](https://www.dropbox.com/sh/8b8j0k4g0rtr9g1/AADHYwW_ekdEIjDRFA122PXsa?dl=0). Moreover, below is a list of all parquets used and how they were generated.

* `2016_news_comment.parquet` - This was generated via an outdated version of `jobs/fetch_content_for_lda.py`, it contains comments from the news subreddit from the 8th of November 2015 until the 7th of november 2016. 
* `agreement_per_community.parquet` - This was generated via the `subreddit_nlp_full_0.001` and the list of subreddits (meta-category) in `data/subreddits`
* `agreement_per_subgroup.parquet` - Same as `agreement_per_community.parquet` but using the subgroup instead of meta-category.
* `daily_agreement.parquet` - This was generated via the cluster job `jobs/daily_agreement.py` directly on the full dataset
* `daily_metrics.parquet` - This was generated via the cluster job `jobs/daily_metrics.py` directly on the full dataset
* `dataset_metrics.parquet` - This was generated via the cluster job `jobs/basic_metrics.py` directly on the full dataset
* `donald_comments.parquet` - This contains all the comments available on the subreddit The_Donald. Ran directly on the cluster.
* `hillary_lda_result.parquet` - holds one result of LDA made on r/hillaryclinton the week before the election. Generated by `notebooks/LDA Trump Hillary.ipynb`
* `hillary_lda_prepro.parquet` - holds the preprocessed comments for LDA of hillaryclinton the week before the election. It is the input on which LDA was run to produce `hillary_lda_result.parquet`.
* `hillary_comments.parquet` - This contains all the comments available on the subreddit hillaryclinton starting in 2016. Ran directly on the cluster.
* `monthly_contribs.parquet` - This was generated using the script `jobs/avg_monthly_contributions` directly on the entire dataset on the cluster.
* `nlp_filtered_for_communities.parquet` - This is a lighter version of `subreddit_nlp_full_0.001` which contains only communities in `data/subreddits/`
* `nlp_per_community.parquet` - Same as above I believe.
* `oct_2016_news_comment.parquet` - Same as `2016_news_comments.parquet` but only the comments october 2016 which were preprocessed into thread for LDA purpose.
* `oneW_oneT_lda_result.parquet` - Result from milestone 2 LDA on one month of news comment. Topics were generated at the post level, with one topic per post and one word per topic.
* `sample.parquet` - This was the first parquet generated. It is basically a random subsample (`0.001`) of the whole dataset. It was used for fast iteration of multiple data explorations before running on the full dataset.
* `score_metrics.parquet`
* `subreddit_agreement.parquet` - Filtering in agreement score per subreddits in `subreddits.parquet`
* `subreddit_nlp_full_0.001.parquet` - All NLP metrics calculated directly on the cluster using 0.001 subsample
* `subreddits.parquet` - This contains a spark-ready version of the list of subreddits found in the txt files at `data/subreddits`
* `threeW_twoT_lda_result.parquet` - Same as `oneW_oneT_lda_result.parquet` except with two topics per post, and 3 words per topic.
* `trump_lda_result.parquet` - same as `hillary_lda_result.parquet` but with The_Donald comments.
* `trump_lda_prepro.parquet` - same as `hillary_lda_prepro.parquet` but with The_Donald comments.

[1]: Davidson et al.(2017), Automated Hate Speech Detection and the Problem of Offensive Language._Proceedings of the 11th International AAAI Conference on Web and Social Media_. 
