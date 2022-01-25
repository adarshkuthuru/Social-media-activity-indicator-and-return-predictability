# -*- coding: utf-8 -*-
"""
Created on Thu Aug  5 12:02:47 2021

@author: adarshpl7
"""

import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
import seaborn as sns
# import math
    

#Clears console and stored variables
try:
    from IPython import get_ipython
    get_ipython().magic('clear') #console
    #get_ipython().magic('reset -f') #stored variables
except:
    pass

import os
print("Current working directory: {0}".format(os.getcwd())) #current working directory
os.chdir('C:/Users/adars/Downloads/Laptop/Semester 4/RA work/Social/') #change directory
# Print the current working directory
print("Changed working directory: {0}".format(os.getcwd()))
# Main program starts here


#method-1: import CSV/TSV files
# import csv
# with open("C:/Users/adars/Downloads/Laptop/Semester 4/RA work/Social/SocialIndicators_BroadUS_2020-10-01_2021-03-31/Ticker_Identifiers_2020-10-01_2021-03-31.tsv") as fd:
#     rd = csv.reader(fd, delimiter="\t", quotechar='"')
# for row in rd:
#         print(row)

#del(rd) #deleted variable from memory

#method-2: import CSV/TSV files        
Identifiers = pd.read_csv ("C:/Users/adars/Downloads/Laptop/Semester 4/RA work/Social/SocialIndicators_BroadUS_2020-10-01_2021-03-31/Ticker_Identifiers_2020-10-01_2021-03-31.tsv", sep = '\t')
#Identifiers.head()
#Identifiers.columns
Identifiers.iloc[1,:] #prints first row of the dataframe
Identifiers.dtypes #to get data type for all the columns in the dataframe

cc = pd.read_csv ("C:/Users/adars/Downloads/Laptop/Semester 4/RA work/Social/SocialIndicators_BroadUS_2020-10-01_2021-03-31/SocialIndicators_BroadUS_CloseToClose_2020-10-01_2021-03-31.tsv", sep = '\t')
co = pd.read_csv ("C:/Users/adars/Downloads/Laptop/Semester 4/RA work/Social/SocialIndicators_BroadUS_2020-10-01_2021-03-31/SocialIndicators_BroadUS_CloseToOpen_2020-10-01_2021-03-31.tsv", sep = '\t')
oc = pd.read_csv ("C:/Users/adars/Downloads/Laptop/Semester 4/RA work/Social/SocialIndicators_BroadUS_2020-10-01_2021-03-31/SocialIndicators_BroadUS_OpenToClose_2020-10-01_2021-03-31.tsv", sep = '\t')

#convert strings to dates
cc["Date1"]=pd.to_datetime(cc["Date"])
cc["Start Date1"]= pd.to_datetime(cc["Start Date"])
cc["End Date1"]= pd.to_datetime(cc["End Date"])

co["Date1"]=pd.to_datetime(co["Date"])
co["Start Date1"]= pd.to_datetime(co["Start Date"])
co["End Date1"]= pd.to_datetime(co["End Date"])

oc["Date1"]=pd.to_datetime(oc["Date"])
oc["Start Date1"]= pd.to_datetime(oc["Start Date"])
oc["End Date1"]= pd.to_datetime(oc["End Date"])

cc.iloc[1,:]

#############################################################################
                    #Estimating next 7 day returns
#############################################################################
russell = pd.read_csv ("C:/Users/adars/Downloads/Laptop/Semester 4/RA work/Social/Russel3000_MarketData_2020___2020-08-01_2021-04-14__2021-04-14_112631.csv", sep = ',')
russell["Date1"]=pd.to_datetime(russell["Date"])        

russell['Day1'] = russell["Date1"] + pd.Timedelta(days=1)
russell['Day7'] = russell["Date1"] + pd.Timedelta(days=7)
russell.iloc[1,:]


#heatmap of missing data
sns.heatmap(russell.isnull(),yticklabels=False,cbar=False,cmap='viridis') #heat map of missing data

#check if any values are -9999402
#test=russell[russell['Volume']==-9999402]
# test.iloc[1,:]

#replace '-9999402' values with 'NaN'
#russell=russell['pr'].replace(-9999402, np.nan) #replace by column name
russell=russell.replace(-9999402, np.nan) #replace entire dataset
# test=test.replace(-9999402, np.nan) 
# test=russell[russell['Volume'].isnull()] #subset dataframe with missing values in volume col

#if a row in 'ClosePrice' col has missing values then replace returns with 'NaN' 
russell.loc[russell["ClosePrice"].isnull(),"pr"] = np.nan
russell.loc[russell["Volume"].isnull(),"pr"] = np.nan
russell.loc[russell["MarketCapitalization"].isnull(),"pr"] = np.nan

#test=russell[russell['RIC']=='A.N']
test=russell.loc[(russell['RIC']=='A.N') | (russell['RIC']=='AA.N')]
#to run sql queries in python
# import pandasql as ps

# sqlcode = '''
# select A.cusip
# from test as A
# inner join test('Date','pr') as B on A.ric=B.ric
# where A.date1 >= B.day1 and A.date1 < B.day7
# group by A.ric
# '''
# test1 = ps.sqldf(sqlcode,locals())

import sqlite3
#Make the db in memory
conn = sqlite3.connect(':memory:')
#write the tables
# test.to_sql('test', conn, index=False)
russell.to_sql('russell', conn, index=False)

#Add next day return
russell['ret1'] = russell.groupby(['RIC'])['pr'].shift(-1)
russell1=russell.copy()
russell1.to_sql('russell1', conn, index=False)

#query to get next 7 day returns
qry = '''
    select  
        a.*, b.date1 as b_date, b.pr as b_pr
    from 
        russell as a left join russell as b
    on
        a.RIC=b.RIC and a.Day1 <= b.Date1 and b.Date1 < a.Day7 
    order by 
        a.RIC, a.Date1, b.date1
    '''
russell2 = pd.read_sql_query(qry, conn)

#query to calculate cumulative return
russell2.to_sql('russell2', conn, index=False)
qry2='''
    select  
        RIC, Date, Date1, sum(b_pr) as ret7 
    from russell2
    group by RIC, Date1
    '''
russell3 = pd.read_sql_query(qry2, conn)


#create final returns dataset
russell3.to_sql('russell3', conn, index=False)
qry3='''
    select
        a.*, b.ret7
    from 
        russell1 as a left join russell3 as b
    on
        a.RIC=b.RIC and a.Date1 = b.Date1
    order by 
        a.RIC, a.Date1
    '''
russell_final = pd.read_sql_query(qry3, conn)
