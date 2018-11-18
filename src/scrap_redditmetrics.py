from bs4 import BeautifulSoup
from math import floor
from sys import stdout
import csv
import requests
import os
import pandas as pd


RM_URL = 'http://redditmetrics.com/top/offset/%d'
data_file = '../data/reddit_metrics_sr_ranking.csv'

#parse the html tab elements of the ranking.
def get_ranking_table(url, offset):
    soup = BeautifulSoup(requests.get(url%(offset)).text, 'html.parser')
    #ranking elements are the only part of the page stored into cell elements of class name "tod"
    return soup.find_all('td', class_='tod')


#dichotomy search function to get the last_subreddit offset from reddit metrics. on 17 November 2018, 
#this number was 1209753. May need to change default parameters for future computation.
def fetch_last_subreddit_offset(start_offset = 1000000, end_offset = 1400000):
    half = lambda a, b: int(floor((a+b)/2))

    mid_offset = half(start_offset, end_offset)
    ranking_tab = get_ranking_table(RM_URL, mid_offset)
    while end_offset != (start_offset+1):
        if ranking_tab:
            start_offset = mid_offset
        else:
            end_offset = mid_offset
        mid_offset = half(start_offset, end_offset)
        ranking_tab = get_ranking_table(RM_URL, mid_offset)
    
    ranking_tab = get_ranking_table(RM_URL, start_offset)
    subreddit_left = len(ranking_tab)
    '''
    if there is more than 100 subreddit in the table of the apparent 
    last page, the default start and end offset were off from the 
    real last number. So restarting the research with doubly
    increased last_offset.
    '''
    return fetch_last_subreddit_offset(mid_offset-100, mid_offset*2) if subreddit_left/3 > 99 else mid_offset+subreddit_left


#will scrap the descending ranking of subreddit by subscribers from redditmetrics.com into the save_filename file path given as parameter
def scrap_ranking_and_save(save_filename=data_file):
	#number of subscribers are stored as string with ',' to delimits thousands. 
	remove_comma = lambda s: s.replace(',', '')
	#subreddit names always start with /r/, so we can remove it (redundancy)
	remove_r = lambda s: s[3:] 

	#only useful to print the progress of the scraping (since it is a lengthy operation)
	last_reddit_number = fetch_last_subreddit_offset()

	with open(save_filename, 'w') as out:
	    line_writer = csv.writer(out, delimiter=',')

	    #to have a header for when the data will be read on pandas.
	    line_writer.writerow(['subreddit', 'subscriber_numb'])
	    
	    #reddit metrics display the ranking 100 by 100,
	    tab_step = 100
	    offset = 0
	    ranking_tab = get_ranking_table(RM_URL, offset)
	    
	    while ranking_tab: #if ranking_tab is an empty list, this will evaluate to false.
	        for i in range(0, len(ranking_tab), 3):
	            '''
	            each table element has three parts: 
	            1. the rank (1), 
	            2. the subreddit name (/r/announcements) 
	            3.and the subscriber number (21,352,277)

	            We do not care about the ranking (as we will store it in the ranking order in the csv).
	            We are only interested in the sub number and the subreddit name 
	            (without the '/r/', as it is implied by convention).
	            '''
	            subreddit = remove_r(ranking_tab[i+1].text)
	            sub_number = remove_comma(ranking_tab[i+2].text)
	            line_writer.writerow([subreddit, sub_number])

	        #output a progress display in place, on the terminal.
	        progress = float(offset/last_reddit_number)*100.0
	        stdout.write("Scraping progress: %f%% (%d/%d)   \r" % (progress, offset, last_reddit_number))
	        stdout.flush()

	        offset += tab_step
	        ranking_tab = get_ranking_table(RM_URL, offset)

def get_ranking_as_pd_df(save_filename=data_file):

	if save_filename == data_file:
			file_path = os.path.dirname(__file__)
			save_filename = file_path + '/' + save_filename

	try:
		return pd.read_csv(save_filename, sep=',')
	except FileNotFoundError:
		print("Could not find data file, scraping the subreddit ranking from redditmetrics.com (this might take a while)")
		scrap_ranking_and_save(save_filename)
		return pd.read_csv(save_filename, sep=',')

if __name__ == '__main__':
	scrap_ranking_and_save()
