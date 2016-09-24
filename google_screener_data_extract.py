"""
    Extract info from google finance stock screener.
    Author: Tan Kok Hua
    Blog: simply-python.com

    Updates:
        Jun 01 2015: Add in rename columns name functions
        May 20 2015: Update mid url as txt file

    ToDo:
        Change some of paramters names
        divdidend str: %20%26%20%28dividend_yield%20%3E%3D%200%29%20%26%20%28dividend_yield%20%3C%3D%20248%29

    Exchange str:
    SGX (default)
    NYSEMKT
    OTCMKTS
    NYSEARCA
    NASDAQ

"""
import os, re, sys, time, datetime, copy, calendar
import pandas
from pattern.web import URL, extension, cache, plaintext
from jsonwebretrieve import WebJsonRetrieval
import simplejson as json

class GoogleStockDataExtract(object):
    """
        Target url need to be rotate and join separately.
    """
    def __init__(self):
        """ 

        """
        ## url parameters for joining
        self.target_url_start = 'https://www.google.com/finance?output=json&start=0&num=3000&noIL=1&q=[%28exchange%20%3D%3D%20%22#$%22%29%20%26%20%28'
        self.target_exchange = 'SGX' #default
        #self.target_url_start = 'https://www.google.com/finance?output=json&start=0&num=3000&noIL=1&q=[%28exchange%20%3D%3D%20%22NASDAQ%22%29%20%26%20%28'
        self.target_url_end = ']&restype=company&ei=BjE7VZmkG8XwuASFn4CoDg'
        self.temp_url_mid = ''
        self.target_full_url = ''
        current_script_folder = os.path.dirname(os.path.realpath(__file__))
        self.mid_url_list_filepath = os.path.join(current_script_folder,'googlescreen_url.txt')

        with open(self.mid_url_list_filepath, 'r') as f:
            url_data =f.readlines()

        self.mid_url_list = [n.strip('\n') for n in url_data]

        ## parameters
        self.saved_json_file = r'c:\data\temptryyql.json'
        self.target_tag = 'searchresults' #use to identify the json data needed

        ## Result dataframe
        self.result_google_ext_df = pandas.DataFrame()

    @property
    def target_exchange(self):
        return self._target_exchange

    @target_exchange.setter
    def target_exchange(self, exchange):
        """ Will also set to the target url start string.
            Temporary set to SGX and NASDAQ

        """
        self._target_exchange = exchange
        self.target_url_start = self.target_url_start.replace('#$', self._target_exchange)


    def form_full_url(self):
        """ Form the url"""
        self.target_full_url = self.target_url_start + self.temp_url_mid + self.target_url_end
        
    def retrieve_stockdata(self):
        """ Retrieve the json file based on the self.target_full_url"""
        ds = WebJsonRetrieval()
        ds.set_url(self.target_full_url)
        ds.download_json() # default r'c:\data\temptryyql.json'

    def get_json_obj_fr_file(self):
        """ Return the json object from the .json file download.        
            Returns:
                (json obj): modified json object fr file.
        """

        with open(self.saved_json_file) as f:
            data_str = f.read() 
        # replace all the / then save back the file
        update_str = re.sub(r"\\",'',data_str)
        json_raw_data = json.loads(update_str)
        return json_raw_data

    def convert_json_to_df(self):
        """ Convert the retrieved data to dataframe
            Returns:
                (Dataframe obj): df formed from the json extact.
        """
        json_raw_data = self.get_json_obj_fr_file()
        
        new_data_list = []
        for n in json_raw_data['searchresults']:
            temp_stock_dict={'SYMBOL':n['ticker'],
                             'CompanyName':n['title'],
                            }
            for col_dict in n['columns']:
                if not col_dict['value'] == '-':
                    temp_stock_dict[col_dict['field']] = col_dict['value']
                
            new_data_list.append(temp_stock_dict)
            
        return pandas.DataFrame(new_data_list)        


    def retrieve_all_stock_data(self):
        """ Retreive all the stock data. Iterate all the target_url_mid1 """
        for temp_url_mid in self.mid_url_list:
            self.temp_url_mid = temp_url_mid
            self.form_full_url()
            self.retrieve_stockdata()
            temp_data_df = self.convert_json_to_df()
            if len(self.result_google_ext_df) == 0:
                self.result_google_ext_df = temp_data_df
            else:
                self.result_google_ext_df =  pandas.merge(self.result_google_ext_df, temp_data_df, on=['SYMBOL','CompanyName'])

        self.rename_columns() 

    def rename_columns(self):
        """ Rename some of columns to avoid confusion as from where the data is pulled.
            Some of names added the GS prefix to indicate resutls from google screener.
            Set to self.result_google_ext_df
        """
        self.result_google_ext_df['PE'] = self.result_google_ext_df['PE'].str.replace(',','')
        self.result_google_ext_df['PE'] = self.result_google_ext_df['PE'].astype('float')
        self.result_google_ext_df['TotalDebtToEquityYear'] = self.result_google_ext_df['TotalDebtToEquityYear'].str.replace(',','')
        self.result_google_ext_df['TotalDebtToEquityYear'] = self.result_google_ext_df['TotalDebtToEquityYear'].astype('float')
        
        self.result_google_ext_df = self.result_google_ext_df.rename(columns={'CompanyName':'GS_CompanyName',
                                                                                 'AverageVolume':'GS_AverageVolume',
                                                                                 'Volume':'GS_Volume',
                                                                                 'AINTCOV':'Interest_coverage',
                                                                                 'DividendYield':'TRAILINGANNUALDIVIDENDYIELDINPERCENT',
                                                                                 'PE':'PERATIO', 'TotalDebtToEquityYear':'TotalDebtEquity',
                                                                                  'PriceToBook':'PRICEBOOK','CurrentRatioYear':'CurrentRatio',
                                                                                })

if __name__ == '__main__':

    choice  = 2

    if choice == 2:
        hh = GoogleStockDataExtract()
        hh.target_exchange = 'NASDAQ'
        hh.retrieve_all_stock_data()
        
        print hh.result_google_ext_df.head()
        hh.result_google_ext_df.to_csv(r'c:\data\temp.csv', index =False)

