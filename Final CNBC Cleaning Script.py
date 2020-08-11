#!/usr/bin/env python
# coding: utf-8

# In[1]:


import os
import json
import tarfile
import pandas as pd
import numpy as np
from urllib.parse import urlparse
import regex as re


# In[2]:


cnbc_df = pd.read_csv("D:\GCP DATA BACKUP\CNBC\Final\CNBC_06-18-2020_08-10-2020-Unclean.csv")


# In[3]:


dates = []
publisher = []
type_ = []

for i in range(len(cnbc_df)):
    try:
        dates.append(cnbc_df.date.iloc[i].split('-')[0])
    except Exception as e:
        dates.append(None)
    
    try:
        publisher.append(cnbc_df.date.iloc[i].split('-')[1])
    except Exception as e:
        publisher.append(None)
    
    try:
        type_.append(cnbc_df.date.iloc[i].split('-')[2])
    except Exception as e:
        type_.append(None)


# In[4]:


cnbc_df['Date'] = dates
cnbc_df['Publisher'] = publisher
cnbc_df['Article_Type'] = type_


# In[6]:


cnbc_df.drop(['date'],axis=1,inplace=True)


# In[7]:


title_tickers = {k: [] for k in range(len(cnbc_df))}

for i in range(len(cnbc_df)):
        try:
            for name in re.findall('\((.*?)\)',cnbc_df.title.iloc[i]):

                if (str(name).isupper() and len(str(name)) < 8):
                    title_tickers[i].append(name)
        except Exception as e:
            print("FAIL")


# In[8]:


pattern = "[A-Z]{3,7}-*[A-Z]{1,7}:[ ]{0,1}[A-Z]{1,7}-*[A-Z]{1,7}\.*[A-Z]{1,7}"

for i in range(len(cnbc_df)):
        try:
            for name in re.findall(pattern,cnbc_df.title.iloc[i]):
                if ((name not in title_tickers[i]) and (name.isupper())):
                    title_tickers[i].append(name)
        except Exception as e:
            print("FAIL")


# In[9]:


cnbc_df['Tickers'] = pd.Series(title_tickers)


# In[11]:


flag_date = []

for i in range(len(cnbc_df.Date)):
    if 'ago' in cnbc_df.Date.iloc[i]:
        flag_date.append(1)
    else:
        flag_date.append(0)


# In[13]:


cnbc_df['Flag Date'] = flag_date


# In[15]:


# 2 CNBC
# 3 Seeking aplha
# 4 Other

flag_publisher = []

for i in range(len(cnbc_df.Date)):
    
    if 'cnbc.com' in cnbc_df.link.iloc[i]:
        flag_publisher.append(2)
    
    elif 'seekingalpha.com' in cnbc_df.link.iloc[i]:
        flag_publisher.append(3)
    
    else:
        flag_publisher.append(4)


# In[16]:


cnbc_df['Publisher Flag'] = flag_publisher


# In[17]:


cnbc_date = {k: [] for k in range(len(cnbc_df))}


# In[18]:


for i in range(len(cnbc_df)):
    
    if (('cnbc.com' in cnbc_df.link.iloc[i]) and ('video' in cnbc_df.link.iloc[i])):
        x = cnbc_df.link.iloc[i]
        string_date = '-'.join([x.split('/')[6],x.split('/')[5],x.split('/')[4]])
        
        if (bool(re.search('[a-zA-Z]', string_date))) == False:
            cnbc_date[i].append(string_date)
    
    elif 'cnbc.com' in cnbc_df.link.iloc[i]:
        x = cnbc_df.link.iloc[i]
        string_date = '-'.join([x.split('/')[5],x.split('/')[4],x.split('/')[3]])
        
        if (bool(re.search('[a-zA-Z]', string_date))) == False:
            cnbc_date[i].append(string_date)
        
    else:
        cnbc_date[i].append(None)


# In[19]:


cnbc_df['missing dates'] = pd.Series(cnbc_date)


# In[20]:


for i in range(len(cnbc_df)):
    if ((cnbc_df['Flag Date'].iloc[i] == 1) and (cnbc_df['Publisher Flag'].iloc[i] == 2)):
        cnbc_df['Date'].iloc[i] = cnbc_df['missing dates'].iloc[i]


# In[27]:


cnbc_df.drop(['Flag Date','Publisher Flag','missing dates'],axis=1,inplace=True)


# In[28]:


# import json

# json_df = cnbc_df.to_dict('records')

# with open('D:\\GCP DATA BACKUP\\CNBC\\Final\\CNBC_06-18-2020_08-10-2020-clean.json', 'w') as fout:
#            json.dump(json_df , fout)


# In[29]:


# cnbc_df.to_csv('D:\\GCP DATA BACKUP\\CNBC\\Final\\CNBC_06-18-2020_08-10-2020-clean.csv')


# In[ ]:





# In[ ]:




