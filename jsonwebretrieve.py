import os, re, sys, time, datetime, copy, calendar
import pandas
import simplejson as json
from pattern.web import URL, extension, cache, plaintext, Newsfeed


class WebJsonRetrieval(object):
    """
        General object to retrieve json file from the web.
        Would require only the first tag so after that can str away form the dict
    """
    def __init__(self):
        """ 

        """
        ## parameters
        self.saved_json_file = r'c:\data\temptryyql.json'
        self.target_tag = '' #use to identify the json data needed

        ## Result dataframe
        self.result_json_df = pandas.DataFrame()

    def set_url(self, url_str):
        """ Set the url for the json retrieval.
            url_str (str): json url str
        """
        self.com_data_full_url = url_str

    def set_target_tag(self, target_tag):
        """ Set the target_tag for the json retrieval.
            target_tag (str): target_tag for json file
        """
        self.target_tag = target_tag
        
    def download_json(self):
        """ Download the json file from the self.com_data_full_url.
            The save file is default to the self.saved_json_file.

        """
        cache.clear()
        url = URL(self.com_data_full_url)
        f = open(self.saved_json_file, 'wb') # save as test.gif
        try:
            url_data = url.download(timeout = 50)
        except:
            url_data = ''

        f.write(url_data) 
        f.close()

    def process_json_data(self):
        """ Processed the json file for handling the announcement.

        """
        try:
            self.json_raw_data  = json.load(open(self.saved_json_file, 'r'))
        except:
            print "Problem loading the json file."
            self.json_raw_data = [{}] #return list of empty dict


    def convert_json_to_df(self):
        """ Convert json data (list of dict) to dataframe.
            Required the correct input of self.target_tag.

        """
        self.result_json_df = pandas.DataFrame(self.json_raw_data[self.target_tag]) 