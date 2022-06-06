'''
This is a web scraping project built via Selenium
The website used is the https://www.lambertshealthcare.co.uk/ (supplement products)
'''

# essential librairies
import os
import shutil
import uuid
from sqlalchemy import create_engine
from tqdm import tqdm
import time
from datetime import date
from ufid import generate_user_friendly_id
import pandas as pd

# library that allows us to work with aws from our script
from boto.s3.connection import S3Connection
from boto.s3.key import Key

# bring in the Selenium librairies
from selenium.webdriver import Chrome 
from selenium.webdriver.common.by import By 
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.keys import Keys

from webdriver_manager.chrome import ChromeDriverManager

# GUI
from pivottablejs import pivot_ui 

# format json
import json

# import url library
import urllib.request

class Scraper:
    
    '''
    This class is used to represent a SCRAPER.
    '''
        
    # sample url = "https://www.lambertshealthcare.co.uk/"
    def __init__(self, url, path):                         
        """
        Get the HTML of a URL
        
        Parameters
        ----------
        url : str
            The URL to get the HTML of        
        """       
        self.path = str(path)                 
        self.driver = Chrome(ChromeDriverManager().install())               
        
        self.driver.get(url)
        self.driver.maximize_window()
            
    # decorator for time waiting and clicking buttons
    def timing_button_decorator(a_function):
        def wrapper(self, msg, xpath):
            """
            wrapper:
            Get any function that handles the messages and xpaths of elements such as cookies and pop ups 
            
            Parameters
            ----------
            msg : str
            xpath : XML path used for navigation through the HTML structure of the page                
            
            Returns
            -------
            clicks the window of rather cookies (accept cookies) or pop-ups (x button to close them)
                
            """                    
            xpath = str(xpath)                    
            try:
                time.sleep(3)
                WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.XPATH, xpath)))                                                                     
                clicked = self.driver.find_element(By.XPATH, xpath).click()                
                return clicked
            except :
                print(TimeoutError, msg)                
                pass # in case there are not cookies or pop ups                        
        return wrapper               
    
    def search_bar(self, msg, xpath):
            """
            Get the function that handles the search bar of website 
            
            Parameters
            ----------
            msg : str
            xpath : XML path used for navigation through the HTML structure of the page                
            
            Returns
            -------
            a.clicks on the search bar 
            b.if no search bar found --> None
                
            """                            
            xpath = str(xpath)                    
            try:
                time.sleep(1)
                element = WebDriverWait(self.driver, 3).until(EC.presence_of_element_located((By.XPATH, xpath)))                                                                     
                element.click()
                return element
            except TimeoutException:
                print(msg)
                return None                                
       
    def text_hit_enter(self, msg, xpath, text)-> None:        
            """
            text and then hit enter method 
            
            Parameters
            ----------
            msg : str
            xpath : XML path used for navigation through the HTML structure of the page                
            text : input user string
            """
            element = self.search_bar(msg, xpath)            
            element.send_keys(text)
            element.send_keys(Keys.ENTER)
        
    # method to find multiple subcategories within the category (text)                                   
    def subcategories(self, xpath):        
        return self.driver.find_element(By.XPATH, xpath)
    
    def container(self, xpath):
        # contains the parent of all product subcategories
        # just one level above of all subcategories            
        # for example vitamin c is the parent 
        self.xpath = str(xpath)
        container = self.subcategories(self.xpath)

        # find all the children of the container (one level below)
        list_subcategories = container.find_elements(By.XPATH, './div')                
        return list_subcategories       

    def the_list_of_links(self, uuid1_xpath, qnt_price_xpath, usage_xpath, product_category_xpath, complete_label_xpath):             
        """
        the basic method ---> it returns the elements of products
        
        Parameters
        ----------
        uuid1_xpath = product's unique determenistic code xpath         
        qnt_price_xpath : product's quantity and price xpath
        usage_xpath :  product's usage xpath
        product_category_xpath : product's category xpath
        complete_label_xpath : product's label with xpath
                                       
        Returns
        -------
        dictionairy of product subcategories (all turmeric range products e.g)
        in DataFrame format.
             
        """ 
        
        # to be used for labeling products       
        self.label = complete_label_xpath 
        
        # to be used for appending product links       
        self.list_of_links = []                
        
        # to be used for connecting image data with date and unique code 
        self.RDS_list_image, self.RDS_list_unique= [], [] 
                
        list_subcategories = self.container(self.xpath)
        for i in list_subcategories:
            self.list_of_links.append(i.find_element(By.TAG_NAME, 'a').get_attribute('href'))
            
        # a dictionairy with all the values of subcategories
        self.subcategories_dict = dict(uuid1 = [], uuid4 = [], link = [], quantity_and_price = [], usage = [], product_category = [])
        
        # go to each one of the above links
        # and fill in the dictionairy with the information                  
        for self.link in self.list_of_links[:2]:                        
            
            # chrome will get step by step the links of the products
            self.driver.get(self.link)                          
            
            # it will append the elements to the subcategories dictionairy
            time.sleep(2) # delay the searching (the bot is doing the job)
            self.subcategories_dict['link'].append(self.link)                        
                        
            # append the unique generated code version 4             
            uuid4 = str(uuid.uuid4())
            self.uuid4 = uuid4
            self.subcategories_dict['uuid4'].append(uuid4)
                                    
            try:
                uuid1 = self.driver.find_element(By.XPATH, uuid1_xpath)                
                self.uuid1 = uuid1.text    # reuse the unique code                 
                self.subcategories_dict['uuid1'].append(uuid1.text)                                                 
            except NoSuchElementException:    
                self.subcategories_dict['uuid1'].append('unique code not found')                                    
            try:
                quantity_and_price = self.driver.find_element(By.XPATH, qnt_price_xpath) 
                self.subcategories_dict['quantity_and_price'].append(quantity_and_price.text.split('£'))
            except NoSuchElementException:    
                self.subcategories_dict['quantity_and_price'].append('quantity or price not found')                        
            try:    
                usage = self.driver.find_element(By.XPATH, usage_xpath)                                                                    
                self.subcategories_dict['usage'].append(usage.text)
            except NoSuchElementException:    
                self.subcategories_dict['usage'].append('usage not found')
            try:    
                product_category = self.driver.find_element(By.XPATH, product_category_xpath)
                self.product_category = product_category.text # reuse the category of product                                                                    
                self.subcategories_dict['product_category'].append(product_category.text)
            except NoSuchElementException:    
                self.subcategories_dict['product_category'].append('no category of product found') 
                        
            # search for duplicate data via the method duplicates()
            self.duplicates()                  
                                                                    
        # present the info to a table format via DataFrame     
        self.df = pd.DataFrame(self.subcategories_dict)                                        
                
        return self.df
    
    def duplicates(self) -> None:                                    
        # reusable image object 
        self.image = f"{self.uuid1}_{self.product_category}"      
        
        # check if duplicate data is in the folder (not the one for uploads)             
        # call the method only if the image does not exist in the folder                                                          
        if not os.path.exists(f"{self.path}\{self.label_folder}\{self.image}.jpg"):          
            self.call_the_image_methods()                                                                                                                    
                
    def call_the_image_methods(self) -> None:        
        
        # images links                        
        self.image_source()
        
        # download images & labels of products                        
        self.images_label_download()
        
        # build the RDS lists                                             
        self.connect_images_with_date()           
    
    def image_source(self) -> None:                                                                                
        self.src_label = self.driver.find_element_by_xpath(self.label).get_attribute('src')                                 
    
    # create new folder 
    def create_store(self, label_folder) -> None:                
        self.label_folder = label_folder # reusable 
        self.label_folder_upload = f"{label_folder}_for_upload"        
        
        # one folder without the unique code version 4 for checking duplicate data        
        if not os.path.exists(f"{self.path}\{label_folder}"):
            os.makedirs(f"{self.path}\{label_folder}")
        
        # one folder with the image of product complete ready to upload for AWS S3 bucket
        # only with images        time.sleep(1)
        if not os.path.exists(f"{self.path}\{label_folder}_for_upload"):
            os.makedirs(f"{self.path}\{label_folder}_for_upload")                           

    # dump the data into the folder 
    def data_dump(self) -> None:
        time.sleep(1)                                     
        with open(f"{self.path}\{self.label_folder}\link_and_product_data.json", "w") as f:            
            json.dump(self.subcategories_dict, f)                                                          
                                            
    def images_label_download(self) -> None:                                                
        # iterate and bring all the images
        time.sleep(1)        
        urllib.request.urlretrieve(self.src_label, f"{self.path}\{self.label_folder}\{self.image}.jpg")  
        # copy the images to the folder for upload after you add the uuid4 code                                       
        shutil.copyfile(f"{self.path}\{self.label_folder}\{self.image}.jpg", f"{self.path}\{self.label_folder_upload}\{self.uuid4}_{self.image}.jpg")
        
    # a beautiful demonstration via pivot table js for further analysis    
    def my_gui(self) :                        
        self.gui = pivot_ui(self.df)
        
        # use shutil.copyfile to copy the file generated from the working storage folder (os.getcwd()) to the folder you want  
        shutil.copyfile(f"{os.getcwd()}\pivottablejs.html", f"{self.path}\{self.label_folder}\pivottablejs.html")                         
                
    def bucket_interraction(self) -> None:   
        time.sleep(1)     
        conn = S3Connection()
                        
        bucket = conn.get_bucket('scraper-aicore')

        # upload the folder created to the bucket        
        for root, dirs, files in os.walk(f"{self.path}\{self.label_folder_upload}"):            
            for name in files:                                
                path = root.split(os.path.sep)[1:]
                path.append(name)                
                key_id = os.path.join(*path)
                k = Key(bucket)
                k.key = key_id
                k.set_contents_from_filename(os.path.join(root, name))
                     
    def connect_images_with_date(self) -> None:
        # a unique and user-friendly code with 5-digit length
        self.unique = str(generate_user_friendly_id(length=5))                                              

        # append the new lines to the RDS lists (update them)
        self.RDS_list_image.append(self.image)
        self.RDS_list_unique.append(self.unique)
                
    def tabular_data(self, database_type, endpoint, password, dbapi, user, port, database) -> None:        
        today = date.today()                        
        engine = create_engine(f"{database_type}+{dbapi}://{user}:{password}@{endpoint}:{port}/{database}")
        
        # upload the dictionary with products to amazon RDS
        time.sleep(1)
        self.df.to_sql(f'{self.label_folder}', engine, if_exists='replace')
        
        # upload the RDS dictionary with unique code and date to amazon RDS
        self.df_1 = pd.DataFrame(data=self.RDS_list_image, columns=[today], index=self.RDS_list_unique)                        
        self.df_1.to_sql(f'{today}_{self.label_folder}', engine, if_exists='replace')        
        
        # access the sql table with products
        self.df_2 = pd.read_sql_table(f'{self.label_folder}', engine) 
        # access the sql table with  with unique code and date
        self.df_3 = pd.read_sql_table(f'{today}_{self.label_folder}', engine) 
        
        # print the tables created in te database
        print(self.df_2, "\n\n", self.df_3)
    
    # method to close the pop-us and accept cookies
    @timing_button_decorator
    def cookies_popups(self, msg, xpath) :
        return msg, xpath                           


"""                                                                
                                      CONTROL PANEL --- > DO ANY SCRAPING YOU WANT FROM ANY UK SUPPLEMENT SITE
                                                           (just alter the xpaths)
"""                                                                
def initiate():
                 
    # cookies accept  (it can be used for pop-ups too)                         
    bot.cookies_popups(msg = "No cookies here !", xpath = '//button[@id="onetrust-accept-btn-handler"]')    
    
    # text then hit_enter to search bar 
    bot.text_hit_enter(msg = "No search bar found !!!"  , xpath = '//input[@id="searchINPUT"]', 
                       text = 'All products')        
    
    # close possible pop-up 
    bot.cookies_popups(msg = 'No pop up found !', xpath = '//div[@class="popup-close"]')            
                    
    # call the container with the list of subcategories
    bot.container(xpath = '//div[@class="container-cols page-wrapper relative-children "]')
    
    # call the function to create folder for the images
    # enter the path you want this to be sent 
    bot.create_store(label_folder = 'All products')
    
    # call the elements (deterministic uuid1 code, quantity and price, usage, category, label)
    bot.the_list_of_links(uuid1_xpath = '//h1[@class="mt0-5 mb0 f-30 f-color6 f-bold"]',                           
                          qnt_price_xpath = '//div[@class="nogaps pt0-25 pb0-5 bd-color4 bd-bottomonly block"]', 
                          usage_xpath = '//div[@class="f-18 f-xspace f-color11 f-nobold"]',                           
                          product_category_xpath = '//span[@class="f-color2 f-brand-persist "]',
                          complete_label_xpath = '//img[@id="mainImage"]')

    # dump the data in json format in the folder 
    bot.data_dump()
    
    # call the pivot table js method
    bot.my_gui()        
    
    # start interracting with bucket 
    # upload the whole folder 
    bot.bucket_interraction()    
    
    # RDS TIME!
    bot.tabular_data(database_type = 'postgresql',
                     endpoint = 'aicoredb.ctuapz5fv9z4.eu-central-1.rds.amazonaws.com',
                     password = 'Twinperama10',
                     dbapi = 'psycopg2',
                     user = 'postgres',
                     port = 5432,
                     database = 'postgres')    
                
# run it only if it is NOT imported
if __name__ == "__main__":
    bot = Scraper(url = 'https://www.lambertshealthcare.co.uk/',    
    path = '\\Users\kon17\Downloads\AICORE_PROJECTS\FINALIZED\Web_Scraping')    
                      
    #  do scraping only once 
    for i in tqdm(range(1)):        
        initiate()    