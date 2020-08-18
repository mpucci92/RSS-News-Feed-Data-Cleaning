#!/usr/bin/env python
# coding: utf-8

# # Modules/Libraries #

# In[3]:


import os
import json
import tarfile
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
from langdetect import detect
from urllib.parse import urlparse
import regex as re


# # Loading the data as an I/O object - concatenating into a dataframe #

# In[4]:


df = pd.DataFrame()
temp = pd.read_csv("D:\\GCP DATA BACKUP\\GoogleNews\\1.5gb google\\google_data_03-30-2020 to 06-05-2020 Google RSS Data_Merged_ALLDATA.csv",iterator=True,chunksize=100000,encoding='latin-1')
df = pd.concat(temp, ignore_index=True)


# In[5]:


df = df.astype(str)


# In[6]:


# df.isna().sum()


# In[7]:


df.drop(['Unnamed: 0', 'guidislink', 'id','links','summary','summary_detail.language','summary_detail.type',
       'summary_detail.value','title_detail.base',
       'title_detail.language', 'title_detail.type', 'title_detail.value'],axis=1,inplace=True)


# In[8]:


df.dtypes


# In[177]:


df_google = pd.DataFrame()
df_google['title'] = df.title
df_google['link'] = df.link
df_google['timestamp'] = df['published']
df_google['authors'] = df['source.title']
df_google['published_parsed'] = df['published_parsed']
df_google['source_href'] = df['source.href']
df_google['search_query'] = df['summary_detail.base']


# In[178]:


query_tickers = []

for i in range(len(df_google)):
    try:
        qticker = re.sub(r'[^\w\s]','',df_google.search_query.iloc[i])
        ticker_q = re.findall(r"([A-Z]+)",qticker)[0]
        query_tickers.append(ticker_q)
    except Exception as e:
        query_tickers.append(None)
        print("Error OCCURRED")


# In[179]:


df_google['Ticker'] = query_tickers


# In[180]:


df_tickers = pd.read_csv("C:\\Users\\mpucci\\Desktop\\LJZP inv\\finBERT Git\\clean_tickers.csv")


# In[181]:


df_google = df_google.merge(df_tickers, on='Ticker', how='left')


# In[182]:


df_google = df_google.drop_duplicates(subset=['title'])


# In[183]:


df_google.reset_index(drop=True,inplace=True)


# In[184]:


df_google.head()


# In[185]:


df_google.tail()


# In[186]:


df_google = df_google.astype(str)


# In[213]:


validation_flag = []

for i in range(len(df_google)):
    try:
        if (df_google.Ticker.iloc[i] in df_google.title.iloc[i]) and (df_google.ExchangeCode.iloc[i] in df_google.title.iloc[i]) :
            validation_flag.append(1)
        elif (df_google.Ticker.iloc[i] not in df_google.title.iloc[i]) or (df_google.ExchangeCode.iloc[i] in df_google.title.iloc[i]):
            validation_flag.append(0)
        else:
            validation_flag.append(None)
    except Exception as e:
        validation_flag.append(None)


# In[214]:


df_google['validation_flag'] = validation_flag


# In[ ]:





# In[189]:


name_validation_flag = []

for i in range(len(df_google)):
    try:
        if (df_google.Name.iloc[i] in df_google.title.iloc[i]):
            name_validation_flag.append(1)
        elif (df_google.Name.iloc[i] not in df_google.title.iloc[i]):
            name_validation_flag.append(0)
        else:
            name_validation_flag.append(None)
    except Exception as e:
        name_validation_flag.append(None)


# In[190]:


df_google['name_validation_flag'] = name_validation_flag


# In[215]:


df_google.validation_flag.value_counts()


# # Normalizing the time field # 

# In[192]:


no_gmt = []
for i in range(len(df_google['timestamp'])):
    try:
        val = df_google['timestamp'].iloc[i].replace('GMT','+0000')
        no_gmt.append(val)
    except Exception as e:
        no_gmt.append(None)


# In[193]:


df_google['timestamp'] = no_gmt


# In[194]:


dateutil = []
from dateutil.parser import parse
for i in range(len(df_google['timestamp'])):
    try:
        val = parse(df_google['timestamp'].iloc[i])
        dateutil.append(val)
    except Exception as e:
         dateutil.append(None)


# In[195]:


df_google['timestamp'] = dateutil


# In[196]:


times = pd.to_datetime(df_google['timestamp'],utc=True)
df_google['timestamp'] = times


# In[197]:


df_google.head()


# In[198]:


df_google.tail()


# # Sorting the dataframe based on timestamp and resetting the index #

# In[199]:


df_google = df_google.sort_values(by=['timestamp'])


# In[200]:


df_google.reset_index(drop=True,inplace=True)


# In[216]:


df_google = df_google.astype(str)


# In[217]:


#re.findall(r"[A-Z]+",df_google.title.iloc[541])


# In[218]:


#df_google.link.iloc[639627]


# In[220]:


df_google[df_google.validation_flag == "1.0"]


# In[205]:


df_google.drop(['published_parsed','Flag'],axis=1,inplace=True)


# In[206]:


df_google.rename({'Ticker':'ticker'},inplace=True)


# In[222]:


title_tickers = {k: [] for k in range(len(df_google))}

for i in range(len(df_google)):
        for name in re.findall('\((.*?)\)',df_google.title.iloc[i]):
            if name.isupper():
                title_tickers[i].append(name)


# In[223]:


pattern = "[A-Z]{3,7}-*[A-Z]{1,7}:[ ]{0,1}[A-Z]{1,7}-*[A-Z]{1,7}\.*[A-Z]{1,7}"

for i in range(len(df_google)):
        for name in re.findall(pattern,df_google.title.iloc[i]):
            if ((name not in title_tickers[i]) and (name.isupper())):
                title_tickers[i].append(name)


# In[224]:


df_google['ticker_tags'] = pd.Series(title_tickers)


# In[225]:


df_google.to_csv("D:\GCP DATA BACKUP\GoogleNews\Final\Google_03-30-2020_06-05-2020_CLEAN.csv")


# ### Cleaning Script ###

# In[8]:


# # Importing Libraries #

# import os
# import json
# import tarfile
# import pandas as pd
# import numpy as np
# from bs4 import BeautifulSoup
# from langdetect import detect
# from urllib.parse import urlparse
# import regex as re
# from dateutil.parser import parse
# import logging
# from time import gmtime, strftime, localtime
# import datetime as datetime

# # Local Time setting
# localtime = strftime("%Y-%m-%d %H-%M-%S", localtime())

# #data_path = "D:\\GoogleNews Data\\New Tars"
# #size_path =  "D:\GoogleNews Data\New Tars"

# # data_path = "D:\\GCP DATA BACKUP\\GoogleNews\\Final"
# # dirs = os.listdir(data_path)

# dirs = ['Test'] # list of len 1 for the iterations.

# for file in dirs:
    
#     #path = data_path + "\\" + file
#     #size_test = size_path + '\\' + file
    
# #     path = "D:\\GCP DATA BACKUP\\GoogleNews\\Final\\Google_Merged_2020-08-11 16-05-18_uncleaned.csv"
# #     size_test = "D:\\GCP DATA BACKUP\\GoogleNews\\Final\\Google_Merged_2020-08-11 16-05-18_uncleaned.csv"
    
# #     try:
        
# #         if os.stat(size_test).st_size > 0:
# #             df = pd.read_csv(path,error_bad_lines=False)
# #         else:
# #             pass
    
# #     except Exception as e:
# #         pass

#     df = pd.DataFrame()
#     temp = pd.read_csv("D:\\GCP DATA BACKUP\\GoogleNews\\Final\\Google_Merged_2020-08-11 16-05-18_uncleaned.csv",iterator=True,chunksize=100000,encoding='latin-1')
#     df = pd.concat(temp, ignore_index=True)
    
#     over100names = [] #1 per day average
    
#     l1 = "24/7 Wall Street,Barron's,Benzinga,Bloomberg Markets and Finance,Bloomberg,Bloomberg Markets,Bloomberg Technology,Business Insider,Business Wire,CNBC,CNET,CNN,CNN Business,Deadline,Engadget,ETF Trends,Fast Company, Forbes, Fox Business, GeekWire"

#     l2 = "Globe News Wire,GlobeNewsWire,GuruFocus,Huffington Post,Investopedia,Invezz,Investors Business Daily,Investor Place,Kiplinger,Market Watch,MarketWatch,Morningstar Inc.,Newsfile Corp,New York Post,New York Times,PR Newswire"

#     l3 = "Reuters,Reuters UK,Reuters India,Seeking Alpha,See It Market,Skynews,TechCrunch,The Motley Fool,The Street,Wall Street Journal,Yahoo Finance,Zacks Investment Research"
    
#     for item in l1.split(','):
#         over100names.append(item)

#     for item in l2.split(','):
#         over100names.append(item)

#     for item in l3.split(','):
#         over100names.append(item)

#     company_names = list(set(over100names))

    
#     df_filtered = df[df['source.title'].isin(company_names)]
    
#     df_nd = df_filtered.drop_duplicates(subset="title")
    
#     df_nd.reset_index(inplace=True)
    
#     data_df = df_nd.loc[:,['link','published','source.href','source.title','title']]
    
#     data_df.reset_index(inplace=True)
    
#     title_tickers = {k: [] for k in range(len(data_df))}

#     for i in range(len(data_df)):
#         for name in re.findall('\((.*?)\)',data_df.title.iloc[i]):
#             if name.isupper():
#                 title_tickers[i].append(name)
                
    
#     pattern = "[A-Z]{3,7}-*[A-Z]{1,7}:[ ]{0,1}[A-Z]{1,7}-*[A-Z]{1,7}\.*[A-Z]{1,7}"

#     for i in range(len(data_df)):
#         for name in re.findall(pattern,data_df.title.iloc[i]):
#             if ((name not in title_tickers[i]) and (name.isupper())):
#                 title_tickers[i].append(name)
    
    
#     data_df['Tickers'] = pd.Series(title_tickers)
    
#     data_df.rename(columns={'link':'Link','published':'Published','source.href':'URL','source.title':'Source','title':'Title'},inplace=True)
    
#     no_gmt = []
    
#     for i in range(len(data_df['Published'])):
#         try:
#             val = data_df['Published'].iloc[i].replace('GMT','+0000')
#             no_gmt.append(val)
#         except Exception as e:
#             no_gmt.append(None)
            
#     data_df['Published'] = no_gmt       
            
#     dateutil = []
    
#     for i in range(len(data_df['Published'])):
#         try:
#             val = parse(data_df['Published'].iloc[i])
#             dateutil.append(val)
#         except Exception as e:
#              dateutil.append(None)
    
#     data_df['Published'] = dateutil
    
#     times = pd.to_datetime(data_df['Published'],utc=True)
    
#     data_df['Published'] = times

#     data_df = data_df.sort_values(by=['Published'])
    
#     data_df.drop(['index'],axis=1,inplace=True)
    
#     data_df.to_csv("D:\\GCP DATA BACKUP\\GoogleNews\\Final\\Google_%s" %(localtime) + "_CLEAN.csv")
    
# #     json_df = data_df.to_dict('records')

# #     with open("D:\\GCP DATA BACKUP\\GoogleNews\\Final\\Google_%s" %(localtime) + "_CLEAN.json", 'w') as fout:
# #         json.dump(json_df , fout)
    


# In[ ]:





# In[ ]:




