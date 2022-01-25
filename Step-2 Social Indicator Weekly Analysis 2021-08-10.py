# -*- coding: utf-8 -*-
"""
Created on Tue Aug 10 11:27:24 2021

@author: adarshpl7
"""
import pandas as pd
import numpy as np

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


#*************************************************************************************
#			UNIQUE DAILY DATES AND COMPANIES IN THE DATASET
#*************************************************************************************
dates = russell.drop_duplicates(subset = ["Date1"])
dates['ID']=1
Identifiers['ID']=1

Identifiers.to_sql('Identifiers', conn, index=False)
dates.to_sql('dates', conn, index=False)
#query to get next 7 day returns
qry4 = '''
    select  
        a.*, b.Date1
    from 
        Identifiers as a left join dates as b
    on
        a.ID=b.ID
    order by 
        a.RIC, b.Date1
    '''
date_comp = pd.read_sql_query(qry4, conn)

# *************************************************************************************
# 				Merge Russell returns with each social dataset
# *************************************************************************************
date_comp.to_sql('date_comp', conn, index=False)
cc.to_sql('cc', conn, index=False)
co.to_sql('co', conn, index=False)
oc.to_sql('oc', conn, index=False)

#merge datecomp with cc
qry5 = '''
    select  
        a.*, b.*
    from 
        date_comp as a left join cc(drop = 'ticker') as b
    on
        a.ticker=b.ticker and a.date1=b.date1
    order by 
        a.RIC, b.Date1
    '''
cc1 = pd.read_sql_query(qry5, conn)


#merge cc1 with russell_final
cc1.to_sql('cc1', conn, index=False)
qry6 = '''
    select  
        a.*, b.ret1, b.ret7
    from 
        cc1 as a left join russell_final as b
    on
        a.RIC=b.RIC and a.date1=b.date1
    order by 
        a.RIC, a.Date1
    '''
cc2 = pd.read_sql_query(qry6, conn)





