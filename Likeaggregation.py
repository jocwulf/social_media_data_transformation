'''
Created on 27.12.2016

@author: jwulf

Produce the following effect indicators per post:
o    Overall Activity change (month, 6 month, 12 month, overall)
    Overall comments and likes
    Overall comments
   Overall likes (for every like calculate the indicators, than average per comment)
   FGC likes (for every like calculate the indicators, than average per comment)
   UGC likes (for every like calculate the indicators, than average per 
'''
import glob
import pandas as pd
import numpy as np
import datetime
from pandas.core.frame import DataFrame
import os
from multiprocessing import Pool


def process_likes(filename_raw):
        
    try:
        print("starting to process file "+filename_raw+" at "+str(datetime.datetime.now()) )
        filename = filename_raw.replace(".\\",'')
        filename_out = filename.replace(".csv",'')            
        df_all = pd.read_csv(filename, index_col=None, sep="\t",header='infer', usecols=['type_details', 'user_id', 'post_id', 'post_by_pageID', 'original_post_time_unix_UTC_Zurich', 'referring_post_or_comment_id', 'status_published_unix_UTC_Zurich'], dtype={'type_details':str, 'user_id':str, 'post_id':str, 'post_by_pageID':bool, 'original_post_time_unix_UTC_Zurich':str, 'referring_post_or_comment_id':str, 'status_published_unix_UTC_Zurich':str})
        
    #to every like, include a column fgclike 
    #create new colum fgc_comment_indicator with 1 if not like and else 0
        df_all['fgc_comment_identificator'] = np.where((df_all.type_details == 'ORIGINAL_POST') | (df_all.type_details == 'comment')  | (df_all.type_details == 'commentcomment'), df_all.post_by_pageID, 0)
    
    #create new colum (like_post_id) with post_id if not like else referring_post_id
        df_all['like_post_id'] = np.where((df_all.type_details == 'ORIGINAL_POST') | (df_all.type_details == 'comment')  | (df_all.type_details == 'commentcomment'), df_all.post_id, df_all.referring_post_or_comment_id)
    
    #create new colume fgc like: group by like_post_id['fgc_comment_indicator'].transform(sum)
        df_all['fgc_like'] = df_all.groupby('like_post_id')['fgc_comment_identificator'].transform(sum)
        
        #print(df_all)
        
        #df_all['referring_user_id'] =  df_all.groupby('like_post_id').apply(lambda df_sub: df_sub.apply(lambda seri: seri['user_id'] if ((seri['type_details'] == 'ORIGINAL_POST') | (seri['type_details'] == 'comment')  | (seri['type_details'] == 'commentcomment')) else ""))
        #create a new dataframe with the post_ids as index, include user_id and time information
        df_status = df_all[['type_details', 'user_id', 'post_id', 'status_published_unix_UTC_Zurich','post_by_pageID']][(df_all.type_details == 'ORIGINAL_POST') | (df_all.type_details == 'comment')  | (df_all.type_details == 'commentcomment')].copy().set_index('post_id')

        #df_status['status_published_unix_UTC_Zurich'] = df_all['status_published_unix_UTC_Zurich'].convert_objects(convert_numeric=True)
        pd.to_numeric(df_status['status_published_unix_UTC_Zurich'],downcast='integer',errors='coerce')

        df_status.dropna(subset=['status_published_unix_UTC_Zurich'], inplace=True)

        df_status['status_published_unix_UTC_Zurich'] = df_status['status_published_unix_UTC_Zurich'].astype(float)
        #df_status['status_published_unix_UTC_Zurich_neg'] = df_status['status_published_unix_UTC_Zurich'].astype(np.int64) * (-1)       
        df_status['status_published_unix_UTC_Zurich_neg'] = df_status['status_published_unix_UTC_Zurich'] * (-1)       

        df_status['status_published_unix_UTC_Zurich'] = pd.to_datetime(df_status.status_published_unix_UTC_Zurich.astype(np.int64),unit='s')
        df_status['status_published_unix_UTC_Zurich_neg'] = pd.to_datetime(df_status.status_published_unix_UTC_Zurich_neg.astype(np.int64),unit='s')
        df_status.index.name = 'referring_post_or_comment_id'
    
    
    #delete firm likes somewhere....
    
    #do group by userid and rolling like described here http://stackoverflow.com/questions/13996302/python-rolling-functions-for-groupby-object
    #and time functions here https://github.com/pandas-dev/pandas/pull/13513
    #     df_ulikes = df_all[(df_all.type_details != 'ORIGINAL_POST') & (df_all.type_details != 'comment')  & (df_all.type_details != 'commentcomment') & (df_all.post_by_pageID != 1)]
    #         
    #     df_ulikes.index = pd.to_datetime(df_ulikes.index.astype(np.int64),unit='s')
    #     df_ulikes.sort_index(inplace=True)
    #     print(df_ulikes)
        df_all.drop(df_all[(df_all.type_details == 'ORIGINAL_POST') | (df_all.type_details == 'comment')  | (df_all.type_details == 'commentcomment') | (df_all.post_by_pageID == 1)].index, inplace=True)
        #df_all['original_post_time'] = pd.to_datetime(df_all.original_post_time_unix_UTC_Zurich.astype(np.int64),unit='s')
        #pd.to_numeric(df_all['original_post_time_unix_UTC_Zurich'],downcast='integer',errors='coerce')
        df_all['original_post_time_unix_UTC_Zurich'] = df_all['original_post_time_unix_UTC_Zurich'].convert_objects(convert_numeric=True)

        df_all.dropna(subset=['original_post_time_unix_UTC_Zurich'], inplace=True)
        
        #df_all['original_post_time_unix_UTC_Zurich'] = df_all['original_post_time_unix_UTC_Zurich'].convert_objects(convert_numeric=True)
        #df_all = df_all.dropna(subset=['original_post_time_unix_UTC_Zurich'] )
        df_all['original_post_time_unix_UTC_Zurich']= df_all['original_post_time_unix_UTC_Zurich'].astype(float)
        df_all['original_post_time_unix_UTC_Zurich_neg'] = df_all['original_post_time_unix_UTC_Zurich'] * (-1)
        df_all['original_post_time_unix_UTC_Zurich'] = pd.to_datetime(df_all['original_post_time_unix_UTC_Zurich'].astype(np.int64),unit='s')
        df_all['original_post_time_unix_UTC_Zurich_neg'] = pd.to_datetime(df_all['original_post_time_unix_UTC_Zurich_neg'].astype(np.int64),unit='s')
        #df_all.set_index('original_post_time',append=True, inplace=True)
    #     df_all.reorder_levels(['user_id','original_post_time_unix_UTC_Zurich'])

    #     df_all['numlikes_last_month'] = df_all.groupby('user_id')['user_id'].rolling(min_periods=1, window=999999999).count()
        #test = df_all['like_post_id'].rolling('365d',min_periods=1, on=df_all.original_post_time).count()
    
        #test = df_all.reset_index(level=0)
        #groupby('user_id')['like_post_id'].rolling('365d',min_periods=1).count()
        
        #START OF LIKER LEVEL INDICATION GENERATION
        
        print("starting to generate Liker-level indicators of file (forward windowing) "+filename_raw+" at "+str(datetime.datetime.now()) )
        
        df_all.drop_duplicates(subset=['user_id','original_post_time_unix_UTC_Zurich_neg'],inplace=True)
        
        #first do forward windowing with reversed time index
        df_all.set_index('original_post_time_unix_UTC_Zurich_neg',append=False, drop=False, inplace=True)
        df_all.sort_index(inplace=True) 
        
        #TODO speedup: alle mit apply zusammenfassen
        likesperuser_plustotal = df_all.groupby('user_id')['fgc_like'].rolling('3650d',min_periods=1).count()
        likesperuser_plus1y = df_all.groupby('user_id')['fgc_like'].rolling('365d',min_periods=1).count()
        likesperuser_plus1q = df_all.groupby('user_id')['fgc_like'].rolling('90d',min_periods=1).count()
        likesperuser_plus1m = df_all.groupby('user_id')['fgc_like'].rolling('30d',min_periods=1).count()
        likesperuser_plus1w = df_all.groupby('user_id')['fgc_like'].rolling('7d',min_periods=1).count() 
        
        fgc_likesperuser_plustotal = df_all.groupby('user_id')['fgc_like'].rolling('3650d',min_periods=1).sum()
        fgc_likesperuser_plus1y = df_all.groupby('user_id')['fgc_like'].rolling('365d',min_periods=1).sum()
        fgc_likesperuser_plus1q = df_all.groupby('user_id')['fgc_like'].rolling('90d',min_periods=1).sum()
        fgc_likesperuser_plus1m = df_all.groupby('user_id')['fgc_like'].rolling('30d',min_periods=1).sum()
        fgc_likesperuser_plus1w = df_all.groupby('user_id')['fgc_like'].rolling('7d',min_periods=1).sum()
        
        ugc_likesperuser_plustotal = likesperuser_plustotal - fgc_likesperuser_plustotal
        ugc_likesperuser_plus1y = likesperuser_plus1y - fgc_likesperuser_plus1y
        ugc_likesperuser_plus1q = likesperuser_plus1q - fgc_likesperuser_plus1q
        ugc_likesperuser_plus1m = likesperuser_plus1m - fgc_likesperuser_plus1m
        ugc_likesperuser_plus1w = likesperuser_plus1w - fgc_likesperuser_plus1w             

        df_all.set_index('user_id',append=True, drop= False, inplace=True)
        df_all = df_all.reorder_levels(['user_id','original_post_time_unix_UTC_Zurich_neg'])
        #df_all = df_all[~df_all.index.duplicated()]   
        #df_all['likesperuser_plustotal'] = likesperuser_plustotal[~likesperuser_plustotal.index.duplicated()]
        #print(likesperuser_plustotal[likesperuser_plustotal.index.duplicated()])
        df_all['likesperuser_plustotal'] = likesperuser_plustotal
        df_all['likesperuser_plus1y'] = likesperuser_plus1y
        df_all['likesperuser_plus1q'] = likesperuser_plus1q
        df_all['likesperuser_plus1m'] = likesperuser_plus1m
        df_all['likesperuser_plus1w'] = likesperuser_plus1w
        
        df_all['fgc_likesperuser_plustotal'] = fgc_likesperuser_plustotal
        df_all['fgc_likesperuser_plus1y'] = fgc_likesperuser_plus1y
        df_all['fgc_likesperuser_plus1q'] = fgc_likesperuser_plus1q
        df_all['fgc_likesperuser_plus1m'] = fgc_likesperuser_plus1m
        df_all['fgc_likesperuser_plus1w'] = fgc_likesperuser_plus1w
        
        df_all['ugc_likesperuser_plustotal'] = ugc_likesperuser_plustotal
        df_all['ugc_likesperuser_plus1y'] = ugc_likesperuser_plus1y
        df_all['ugc_likesperuser_plus1q'] = ugc_likesperuser_plus1q
        df_all['ugc_likesperuser_plus1m'] = ugc_likesperuser_plus1m
        df_all['ugc_likesperuser_plus1w'] = ugc_likesperuser_plus1w
                
        #then do backward windowing with correct time index
        print("starting to generate Liker-level indicators of file (backward windowing) "+filename_raw+" at "+str(datetime.datetime.now()) )

        df_all.set_index('original_post_time_unix_UTC_Zurich',append=False, drop=False, inplace=True)
        df_all.sort_index(inplace=True)    

        likesperuser_minustotal = df_all.groupby('user_id')['fgc_like'].rolling('3650d',min_periods=1).count()
        likesperuser_minus1y = df_all.groupby('user_id')['fgc_like'].rolling('365d',min_periods=1).count()
        likesperuser_minus1q = df_all.groupby('user_id')['fgc_like'].rolling('90d',min_periods=1).count()
        likesperuser_minus1m = df_all.groupby('user_id')['fgc_like'].rolling('30d',min_periods=1).count()
        likesperuser_minus1w = df_all.groupby('user_id')['fgc_like'].rolling('7d',min_periods=1).count()
        
        fgc_likesperuser_minustotal = df_all.groupby('user_id')['fgc_like'].rolling('3650d',min_periods=1).sum()
        fgc_likesperuser_minus1y = df_all.groupby('user_id')['fgc_like'].rolling('365d',min_periods=1).sum()
        fgc_likesperuser_minus1q = df_all.groupby('user_id')['fgc_like'].rolling('90d',min_periods=1).sum()
        fgc_likesperuser_minus1m = df_all.groupby('user_id')['fgc_like'].rolling('30d',min_periods=1).sum()
        fgc_likesperuser_minus1w = df_all.groupby('user_id')['fgc_like'].rolling('7d',min_periods=1).sum()
        
        ugc_likesperuser_minustotal = likesperuser_minustotal - fgc_likesperuser_minustotal
        ugc_likesperuser_minus1y = likesperuser_minus1y - fgc_likesperuser_minus1y
        ugc_likesperuser_minus1q = likesperuser_minus1q - fgc_likesperuser_minus1q
        ugc_likesperuser_minus1m = likesperuser_minus1m - fgc_likesperuser_minus1m
        ugc_likesperuser_minus1w = likesperuser_minus1w - fgc_likesperuser_minus1w
       
        
        #mytestseries = df_all.apply(lambda x: testseries[(testseries.index > x.index) & (testseries.index < (x.index +  datetime.timedelta(days=365)))].mean())
        df_all.set_index('user_id',append=True, inplace=True)
        df_all = df_all.reorder_levels(['user_id','original_post_time_unix_UTC_Zurich'])

        df_all['likesperuser_minustotal'] = likesperuser_minustotal
        df_all['likesperuser_minus1y'] = likesperuser_minus1y
        df_all['likesperuser_minus1q'] = likesperuser_minus1q
        df_all['likesperuser_minus1m'] = likesperuser_minus1m
        df_all['likesperuser_minus1w'] = likesperuser_minus1w
        df_all['fgc_likesperuser_minustotal'] = fgc_likesperuser_minustotal
        df_all['ugc_likesperuser_minustotal'] = ugc_likesperuser_minustotal
      
#         print('start groupbys')
#         df_all['likesperuser_plustotal'] = df_all['fgc_like'].groupby(level=0).apply(lambda df_grouped: df_grouped.groupby(level=1).apply(lambda mycol: df_grouped[(df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') > mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0]) & (df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') <= (mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0] +  datetime.timedelta(days=3650)))].count()))    
# 
#         print(df_all)
#         
#         df_all['likesperuser_plus1y'] = df_all['fgc_like'].groupby(level=0).apply(lambda df_grouped: df_grouped.groupby(level=1).apply(lambda mycol: df_grouped[(df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') > mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0]) & (df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') <= (mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0] +  datetime.timedelta(days=365)))].count()))
#         df_all['likesperuser_plus1q'] = df_all['fgc_like'].groupby(level=0).apply(lambda df_grouped: df_grouped.groupby(level=1).apply(lambda mycol: df_grouped[(df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') > mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0]) & (df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') <= (mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0] +  datetime.timedelta(days=90)))].count()))
#         df_all['likesperuser_plus1m'] = df_all['fgc_like'].groupby(level=0).apply(lambda df_grouped: df_grouped.groupby(level=1).apply(lambda mycol: df_grouped[(df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') > mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0]) & (df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') <= (mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0] +  datetime.timedelta(days=30)))].count()))
#         df_all['likesperuser_plus1w'] = df_all['fgc_like'].groupby(level=0).apply(lambda df_grouped: df_grouped.groupby(level=1).apply(lambda mycol: df_grouped[(df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') > mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0]) & (df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') <= (mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0] +  datetime.timedelta(days=7)))].count()))
#     
#         df_all['fgc_likesperuser_plustotal'] = df_all['fgc_like'].groupby(level=0).apply(lambda df_grouped: df_grouped.groupby(level=1).apply(lambda mycol: df_grouped[(df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') > mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0]) & (df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') <= (mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0] +  datetime.timedelta(days=3650)))].sum()))    
#         df_all['fgc_likesperuser_plus1y'] = df_all['fgc_like'].groupby(level=0).apply(lambda df_grouped: df_grouped.groupby(level=1).apply(lambda mycol: df_grouped[(df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') > mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0]) & (df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') <= (mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0] +  datetime.timedelta(days=365)))].sum()))
#         df_all['fgc_likesperuser_plus1q'] = df_all['fgc_like'].groupby(level=0).apply(lambda df_grouped: df_grouped.groupby(level=1).apply(lambda mycol: df_grouped[(df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') > mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0]) & (df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') <= (mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0] +  datetime.timedelta(days=90)))].sum()))
#         df_all['fgc_likesperuser_plus1m'] = df_all['fgc_like'].groupby(level=0).apply(lambda df_grouped: df_grouped.groupby(level=1).apply(lambda mycol: df_grouped[(df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') > mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0]) & (df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') <= (mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0] +  datetime.timedelta(days=30)))].sum()))
#         df_all['fgc_likesperuser_plus1w'] = df_all['fgc_like'].groupby(level=0).apply(lambda df_grouped: df_grouped.groupby(level=1).apply(lambda mycol: df_grouped[(df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') > mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0]) & (df_grouped.index.get_level_values('original_post_time_unix_UTC_Zurich') <= (mycol.index.get_level_values('original_post_time_unix_UTC_Zurich')[0] +  datetime.timedelta(days=7)))].sum()))
#         print('end groupbys')
#         
#         df_all['ugc_likesperuser_plustotal'] = df_all['likesperuser_plustotal'] - df_all['fgc_likesperuser_plustotal']
#         df_all['ugc_likesperuser_plus1y'] = df_all['likesperuser_plus1y'] - df_all['fgc_likesperuser_plus1y'] 
#         df_all['ugc_likesperuser_plus1q'] = df_all['likesperuser_plus1q'] - df_all['fgc_likesperuser_plus1q']
#         df_all['ugc_likesperuser_plus1m'] = df_all['likesperuser_plus1m'] - df_all['fgc_likesperuser_plus1m']
#         df_all['ugc_likesperuser_plus1w'] = df_all['likesperuser_plus1w'] - df_all['fgc_likesperuser_plus1w']
    
           
        df_all['likesperuser_change1y'] = (df_all['likesperuser_plus1y'] - df_all['likesperuser_minus1y']) / 365
        df_all['likesperuser_change1q'] = (df_all['likesperuser_plus1q'] - df_all['likesperuser_minus1q']) / 90
        df_all['likesperuser_change1m'] = (df_all['likesperuser_plus1m'] - df_all['likesperuser_minus1m']) / 30
        df_all['likesperuser_change1w'] = (df_all['likesperuser_plus1w'] - df_all['likesperuser_minus1w']) / 7 
        df_all['likesperuser_attraction'] = (df_all['likesperuser_minustotal'] == 0).astype(int)
        df_all['likesperuser_aversion'] = (df_all['likesperuser_plustotal'] == 0).astype(int)   
        
        df_all['fgc_likesperuser_change1y'] = (df_all['fgc_likesperuser_plus1y'] - fgc_likesperuser_minus1y) / 365
        df_all['fgc_likesperuser_change1q'] = (df_all['fgc_likesperuser_plus1q'] - fgc_likesperuser_minus1q) / 90
        df_all['fgc_likesperuser_change1m'] = (df_all['fgc_likesperuser_plus1m'] - fgc_likesperuser_minus1m) / 30
        df_all['fgc_likesperuser_change1w'] = (df_all['fgc_likesperuser_plus1w'] - fgc_likesperuser_minus1w) / 7 
        df_all['fgc_likesperuser_attraction'] = (df_all['fgc_likesperuser_minustotal'] == 0).astype(int)
        df_all['fgc_likesperuser_aversion'] = (df_all['fgc_likesperuser_plustotal'] == 0).astype(int)  
        
        df_all['ugc_likesperuser_change1y'] = (df_all['ugc_likesperuser_plus1y'] - ugc_likesperuser_minus1y) / 365
        df_all['ugc_likesperuser_change1q'] = (df_all['ugc_likesperuser_plus1q'] - ugc_likesperuser_minus1q) / 90
        df_all['ugc_likesperuser_change1m'] = (df_all['ugc_likesperuser_plus1m'] - ugc_likesperuser_minus1m) / 30
        df_all['ugc_likesperuser_change1w'] = (df_all['ugc_likesperuser_plus1w'] - ugc_likesperuser_minus1w) / 7 
        df_all['ugc_likesperuser_attraction'] = (df_all['ugc_likesperuser_minustotal'] == 0).astype(int)
        df_all['ugc_likesperuser_aversion'] = (df_all['ugc_likesperuser_plustotal'] == 0).astype(int)
        

        
        df_status['num_likes'] = df_all[['fgc_like','referring_post_or_comment_id']].groupby('referring_post_or_comment_id').count()
        df_status['num_likes'].fillna(0,inplace=True)
        df_status['num_fgc_likes'] = df_status['num_likes'] * df_status['post_by_pageID']
        df_status['num_ugc_likes'] = df_status['num_likes'] - df_status['num_fgc_likes']

        
        #LIKER LEVEL INDICATION GENERATION: Aggregate to Post level      
        print("starting to aggregate Liker-level indicators to post level at file "+filename_raw+" at "+str(datetime.datetime.now()) )               
        #TODO speedup: alle mit apply zusammenfassen
        df_status['likes_change1y'] = df_all.groupby('referring_post_or_comment_id')['likesperuser_change1y'].mean()
        df_status['likes_change1q'] = df_all.groupby('referring_post_or_comment_id')['likesperuser_change1q'].mean()
        df_status['likes_change1m'] = df_all.groupby('referring_post_or_comment_id')['likesperuser_change1m'].mean()
        df_status['likes_change1w'] = df_all.groupby('referring_post_or_comment_id')['likesperuser_change1w'].mean()
        df_status['likes_attraction'] = df_all.groupby('referring_post_or_comment_id')['likesperuser_attraction'].sum()
        df_status['likes_aversion'] = df_all.groupby('referring_post_or_comment_id')['likesperuser_aversion'].sum()
        df_status['likes_acquisition'] = df_status['likes_attraction'] - df_status['likes_aversion']
        
        df_status['fgc_likes_change1y'] = df_all.groupby('referring_post_or_comment_id')['fgc_likesperuser_change1y'].mean()
        df_status['fgc_likes_change1q'] = df_all.groupby('referring_post_or_comment_id')['fgc_likesperuser_change1q'].mean()
        df_status['fgc_likes_change1m'] = df_all.groupby('referring_post_or_comment_id')['fgc_likesperuser_change1m'].mean()
        df_status['fgc_likes_change1w'] = df_all.groupby('referring_post_or_comment_id')['fgc_likesperuser_change1w'].mean()
        df_status['fgc_likes_attraction'] = df_all.groupby('referring_post_or_comment_id')['fgc_likesperuser_attraction'].sum()
        df_status['fgc_likes_aversion'] = df_all.groupby('referring_post_or_comment_id')['fgc_likesperuser_aversion'].sum()
        df_status['fgc_likes_acquisition'] = df_status['fgc_likes_attraction'] - df_status['fgc_likes_aversion']
        
        df_status['ugc_likes_change1y'] = df_all.groupby('referring_post_or_comment_id')['ugc_likesperuser_change1y'].mean()
        df_status['ugc_likes_change1q'] = df_all.groupby('referring_post_or_comment_id')['ugc_likesperuser_change1q'].mean()
        df_status['ugc_likes_change1m'] = df_all.groupby('referring_post_or_comment_id')['ugc_likesperuser_change1m'].mean()
        df_status['ugc_likes_change1w'] = df_all.groupby('referring_post_or_comment_id')['ugc_likesperuser_change1w'].mean()
        df_status['ugc_likes_attraction'] = df_all.groupby('referring_post_or_comment_id')['ugc_likesperuser_attraction'].sum()
        df_status['ugc_likes_aversion'] = df_all.groupby('referring_post_or_comment_id')['ugc_likesperuser_aversion'].sum()
        df_status['ugc_likes_acquisition'] = df_status['ugc_likes_attraction'] - df_status['ugc_likes_aversion']
        
        #now the indicators for the overall impact
        #START OF OVERALL LEVEL INDICATOR GENERATION    
        print("starting to generate overall-level indicators of file "+filename_raw+" at "+str(datetime.datetime.now()) )
        
        #changed 20170108
        df_status['post_id'] = df_status.index
        
        df_status.set_index('status_published_unix_UTC_Zurich_neg',inplace=True,drop=False)
        df_status.sort_index(inplace=True)
        df_status['overall_likes_plus1y'] = df_status['num_likes'].rolling('365d',min_periods=1).sum()
        df_status['overall_likes_plus1q'] = df_status['num_likes'].rolling('90d',min_periods=1).sum()
        df_status['overall_likes_plus1m'] = df_status['num_likes'].rolling('30d',min_periods=1).sum()
        df_status['overall_likes_plus1w'] = df_status['num_likes'].rolling('7d',min_periods=1).sum()
        
        df_status['overall_fgc_likes_plus1y'] = df_status['num_fgc_likes'].rolling('365d',min_periods=1).sum()
        df_status['overall_fgc_likes_plus1q'] = df_status['num_fgc_likes'].rolling('90d',min_periods=1).sum()
        df_status['overall_fgc_likes_plus1m'] = df_status['num_fgc_likes'].rolling('30d',min_periods=1).sum()
        df_status['overall_fgc_likes_plus1w'] = df_status['num_fgc_likes'].rolling('7d',min_periods=1).sum()
        
        df_status['overall_ugc_likes_plus1y'] = df_status['num_fgc_likes'].rolling('365d',min_periods=1).sum()
        df_status['overall_ugc_likes_plus1q'] = df_status['num_fgc_likes'].rolling('90d',min_periods=1).sum()
        df_status['overall_ugc_likes_plus1m'] = df_status['num_fgc_likes'].rolling('30d',min_periods=1).sum()
        df_status['overall_ugc_likes_plus1w'] = df_status['num_fgc_likes'].rolling('7d',min_periods=1).sum()        
        
        df_status.set_index('status_published_unix_UTC_Zurich',inplace=True,drop=False)
        df_status.sort_index(inplace=True) 
        df_status['overall_likes_minus1y'] = df_status['num_likes'].rolling('365d',min_periods=1).sum()
        df_status['overall_likes_minus1q'] = df_status['num_likes'].rolling('90d',min_periods=1).sum()
        df_status['overall_likes_minus1m'] = df_status['num_likes'].rolling('30d',min_periods=1).sum()
        df_status['overall_likes_minus1w'] = df_status['num_likes'].rolling('7d',min_periods=1).sum()
        
        df_status['overall_fgc_likes_minus1y'] = df_status['num_fgc_likes'].rolling('365d',min_periods=1).sum()
        df_status['overall_fgc_likes_minus1q'] = df_status['num_fgc_likes'].rolling('90d',min_periods=1).sum()
        df_status['overall_fgc_likes_minus1m'] = df_status['num_fgc_likes'].rolling('30d',min_periods=1).sum()
        df_status['overall_fgc_likes_minus1w'] = df_status['num_fgc_likes'].rolling('7d',min_periods=1).sum()
        
        df_status['overall_ugc_likes_minus1y'] = df_status['num_fgc_likes'].rolling('365d',min_periods=1).sum()
        df_status['overall_ugc_likes_minus1q'] = df_status['num_fgc_likes'].rolling('90d',min_periods=1).sum()
        df_status['overall_ugc_likes_minus1m'] = df_status['num_fgc_likes'].rolling('30d',min_periods=1).sum()
        df_status['overall_ugc_likes_minus1w'] = df_status['num_fgc_likes'].rolling('7d',min_periods=1).sum()
        
#         
#         df_status['overall_likes_minus1y'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (timest - datetime.timedelta(days=365))) ].count())
#         df_status['overall_likes_minus1q'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (timest - datetime.timedelta(days=90))) ].count())
#         df_status['overall_likes_minus1m'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (timest - datetime.timedelta(days=30))) ].count())
#         df_status['overall_likes_minus1w'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (timest - datetime.timedelta(days=7))) ].count())
# #     
#         df_status['overall_likes_plus1y'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (timest + datetime.timedelta(days=365))) ].count())
#         df_status['overall_likes_plus1q'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (timest + datetime.timedelta(days=90))) ].count())
#         df_status['overall_likes_plus1m'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (timest + datetime.timedelta(days=30))) ].count())
#         df_status['overall_likes_plus1w'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (timest + datetime.timedelta(days=7))) ].count())      
#         
        df_status['overall_likes_change1y'] = (df_status['overall_likes_plus1y'] - df_status['overall_likes_minus1y']) / 365
        df_status['overall_likes_change1q'] = (df_status['overall_likes_plus1q'] - df_status['overall_likes_minus1q']) / 90
        df_status['overall_likes_change1m'] = (df_status['overall_likes_plus1m'] - df_status['overall_likes_minus1m']) / 30
        df_status['overall_likes_change1w'] = (df_status['overall_likes_plus1w'] - df_status['overall_likes_minus1w']) / 7
#         
#         df_status['overall_fgc_likes_minus1y'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (timest - datetime.timedelta(days=365))) ].sum())
#         df_status['overall_fgc_likes_minus1q'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (timest - datetime.timedelta(days=90))) ].sum())
#         df_status['overall_fgc_likes_minus1m'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (timest - datetime.timedelta(days=30))) ].sum())
#         df_status['overall_fgc_likes_minus1w'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (timest - datetime.timedelta(days=7))) ].sum())
#     
#         df_status['overall_fgc_likes_plus1y'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (timest + datetime.timedelta(days=365))) ].sum())
#         df_status['overall_fgc_likes_plus1q'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (timest + datetime.timedelta(days=90))) ].sum())
#         df_status['overall_fgc_likes_plus1m'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (timest + datetime.timedelta(days=30))) ].sum())
#         df_status['overall_fgc_likes_plus1w'] = df_status['status_published_unix_UTC_Zurich'].apply(lambda timest: df_all['fgc_like'][(df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  timest) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (timest + datetime.timedelta(days=7))) ].sum())      
#         
        df_status['overall_fgc_likes_change1y'] = (df_status['overall_fgc_likes_plus1y'] - df_status['overall_fgc_likes_minus1y']) / 365
        df_status['overall_fgc_likes_change1q'] = (df_status['overall_fgc_likes_plus1q'] - df_status['overall_fgc_likes_minus1q']) / 90
        df_status['overall_fgc_likes_change1m'] = (df_status['overall_fgc_likes_plus1m'] - df_status['overall_fgc_likes_minus1m']) / 30
        df_status['overall_fgc_likes_change1w'] = (df_status['overall_fgc_likes_plus1w'] - df_status['overall_fgc_likes_minus1w']) / 7    
#         
#         df_status['overall_ugc_likes_minus1y'] = df_status['overall_likes_minus1y'] - df_status['overall_fgc_likes_minus1y']
#         df_status['overall_ugc_likes_minus1q'] = df_status['overall_likes_minus1q'] - df_status['overall_fgc_likes_minus1q']
#         df_status['overall_ugc_likes_minus1m'] = df_status['overall_likes_minus1m'] - df_status['overall_fgc_likes_minus1m']
#         df_status['overall_ugc_likes_minus1w'] = df_status['overall_likes_minus1w'] - df_status['overall_fgc_likes_minus1w']
#         
#         df_status['overall_ugc_likes_plus1y'] = df_status['overall_likes_plus1y'] - df_status['overall_fgc_likes_plus1y']
#         df_status['overall_ugc_likes_plus1q'] = df_status['overall_likes_plus1q'] - df_status['overall_fgc_likes_plus1q']
#         df_status['overall_ugc_likes_plus1m'] = df_status['overall_likes_plus1m'] - df_status['overall_fgc_likes_plus1m']
#         df_status['overall_ugc_likes_plus1w'] = df_status['overall_likes_plus1w'] - df_status['overall_fgc_likes_plus1w']
#         
        df_status['overall_ugc_likes_change1y'] = (df_status['overall_ugc_likes_plus1y'] - df_status['overall_ugc_likes_minus1y']) / 365
        df_status['overall_ugc_likes_change1q'] = (df_status['overall_ugc_likes_plus1q'] - df_status['overall_ugc_likes_minus1q']) / 90
        df_status['overall_ugc_likes_change1m'] = (df_status['overall_ugc_likes_plus1m'] - df_status['overall_ugc_likes_minus1m']) / 30
        df_status['overall_ugc_likes_change1w'] = (df_status['overall_ugc_likes_plus1w'] - df_status['overall_ugc_likes_minus1w']) / 7       
      
        
        #generate indicators for the post issuer impact
        #START OF ISSUer LEVEL INDICATOR GENERATION    
        print("starting to generate issuer-level indicators of file "+filename_raw+" at "+str(datetime.datetime.now()) )
        #print(df_all)
        allkeys = df_all.index.get_level_values('user_id')
        #df_status['thetest'] = df_status[['user_id','status_published_unix_UTC_Zurich']].apply(lambda ser: df_all.xs(ser['user_id'],level='user_id').index.searchsorted(ser['status_published_unix_UTC_Zurich']) if ser['user_id'] in allkeys else None, axis=1)
        #df_status[['issuer_likes_minustotal','issuer_likes_minus1w']] = df_status[['user_id','status_published_unix_UTC_Zurich']].apply(lambda ser: df_all.xs(ser['user_id'],level='user_id').ix[df_all.xs(ser['user_id'],level='user_id').index.searchsorted(ser['status_published_unix_UTC_Zurich'])-1][['likesperuser_minustotal','likesperuser_minus1w']] if ser['user_id'] in allkeys else pd.Series([None, None], index=['likesperuser_minustotal','likesperuser_minus1w']), axis=1)
        #df_status['issuer_likes_plustotal'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda ser: df_all.xs(ser['user_id'],level='user_id').ix[min([df_all.xs(ser['user_id'],level='user_id').index.searchsorted(ser['status_published_unix_UTC_Zurich'], side='right'),df_all.xs(ser['user_id'],level='user_id').index.get_loc(df_all.xs(ser['user_id'],level='user_id').index.max())]) : max([df_all.xs(ser['user_id'],level='user_id').index.searchsorted((ser['status_published_unix_UTC_Zurich'] + datetime.timedelta(days=3650)), side='left'),df_all.xs(ser['user_id'],level='user_id').index.get_loc(df_all.xs(ser['user_id'],level='user_id').index.min())])].count() if ser['user_id'] in allkeys else None, axis=1)
        #df_status['issuer_likes_plustotal'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda ser: df_all.xs(ser['user_id'],level='user_id').ix[min([df_all.xs(ser['user_id'],level='user_id').index.searchsorted(ser['status_published_unix_UTC_Zurich'], side='right')+1,df_all.xs(ser['user_id'],level='user_id').index.get_loc(df_all.xs(ser['user_id'],level='user_id').index.max())]) : max([df_all.xs(ser['user_id'],level='user_id').index.searchsorted((ser['status_published_unix_UTC_Zurich'] + datetime.timedelta(days=3650)), side='left')-1,df_all.xs(ser['user_id'],level='user_id').index.get_loc(df_all.xs(ser['user_id'],level='user_id').index.min())])].count() if ser['user_id'] in allkeys else None, axis=1)
        #mytest = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda ser: df_all.xs(ser['user_id'],level='user_id').index.get_loc(df_all.xs(ser['user_id'],level='user_id').index.max()) if ser['user_id'] in allkeys else None, axis=1)        
        
        def sumcount(mydf):
            return pd.Series([mydf.count(),mydf.sum()])
        
        def issuer_analysis(theser):
            user_df1 = df_all.xs(theser['user_id'],level='user_id')
            user_df=user_df1['fgc_like']
            plustotal = sumcount(user_df[(user_df.index > theser.status_published_unix_UTC_Zurich) & (user_df.index <=  (theser.status_published_unix_UTC_Zurich + datetime.timedelta(days=3650)))])
            plusy = sumcount(user_df[(user_df.index > theser.status_published_unix_UTC_Zurich) & (user_df.index <=  (theser.status_published_unix_UTC_Zurich + datetime.timedelta(days=365)))])
            plusq = sumcount(user_df[(user_df.index > theser.status_published_unix_UTC_Zurich) & (user_df.index <=  (theser.status_published_unix_UTC_Zurich + datetime.timedelta(days=90)))])
            plusm = sumcount(user_df[(user_df.index > theser.status_published_unix_UTC_Zurich) & (user_df.index <=  (theser.status_published_unix_UTC_Zurich + datetime.timedelta(days=30)))])
            plusw = sumcount(user_df[(user_df.index > theser.status_published_unix_UTC_Zurich) & (user_df.index <=  (theser.status_published_unix_UTC_Zurich + datetime.timedelta(days=7)))])
            
            minustotal = sumcount(user_df[(user_df.index >=  (theser.status_published_unix_UTC_Zurich - datetime.timedelta(days=3650))) & (user_df.index < theser.status_published_unix_UTC_Zurich)])
            minusy = sumcount(user_df[(user_df.index >=  (theser.status_published_unix_UTC_Zurich - datetime.timedelta(days=365))) & (user_df.index < theser.status_published_unix_UTC_Zurich)])
            minusq = sumcount(user_df[(user_df.index >=  (theser.status_published_unix_UTC_Zurich - datetime.timedelta(days=90))) & (user_df.index < theser.status_published_unix_UTC_Zurich)])
            minusm = sumcount(user_df[(user_df.index >=  (theser.status_published_unix_UTC_Zurich - datetime.timedelta(days=30))) & (user_df.index < theser.status_published_unix_UTC_Zurich)])
            minusw = sumcount(user_df[(user_df.index >=  (theser.status_published_unix_UTC_Zurich - datetime.timedelta(days=7))) & (user_df.index < theser.status_published_unix_UTC_Zurich)])
                        
            return pd.concat([plustotal,plusy,plusq,plusm,plusw,minustotal,minusy,minusq,minusm,minusw],ignore_index=True)

        df_status[['issuer_likes_plustotal', 'issuer_fgc_likes_plustotal', 'issuer_likes_plus1y', 'issuer_fgc_likes_plus1y', 'issuer_likes_plus1q', 'issuer_fgc_likes_plus1q', 'issuer_likes_plus1m', 'issuer_fgc_likes_plus1m', 'issuer_likes_plus1w', 'issuer_fgc_likes_plus1w', 'issuer_likes_minustotal', 'issuer_fgc_likes_minustotal', 'issuer_likes_minus1y', 'issuer_fgc_likes_minus1y', 'issuer_likes_minus1q', 'issuer_fgc_likes_minus1q', 'issuer_likes_minus1m', 'issuer_fgc_likes_minus1m', 'issuer_likes_minus1w', 'issuer_fgc_likes_minus1w']] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: issuer_analysis(theser) if theser['user_id'] in allkeys else pd.Series([0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0]), axis=1)
        
#         df_status['issuer_likes_minustotal'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (theser.status_published_unix_UTC_Zurich - datetime.timedelta(days=3650))) ].count(), axis=1)
#         df_status['issuer_likes_minus1w'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (theser.status_published_unix_UTC_Zurich - datetime.timedelta(days=7))) ].count(), axis=1)
#         df_status['issuer_likes_minus1y'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (theser.status_published_unix_UTC_Zurich - datetime.timedelta(days=365))) ].count(), axis=1)
#         df_status['issuer_likes_minus1q'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (theser.status_published_unix_UTC_Zurich - datetime.timedelta(days=90))) ].count(), axis=1)
#         df_status['issuer_likes_minus1m'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (theser.status_published_unix_UTC_Zurich - datetime.timedelta(days=30))) ].count(), axis=1)
#         df_status['issuer_likes_minus1w2'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (theser.status_published_unix_UTC_Zurich - datetime.timedelta(days=7))) ].count(), axis=1)
#     
#         df_status['issuer_likes_plustotal'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (theser.status_published_unix_UTC_Zurich + datetime.timedelta(days=3650))) ].count(), axis=1)
#         df_status['issuer_likes_plus1y'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (theser.status_published_unix_UTC_Zurich + datetime.timedelta(days=365))) ].count(), axis=1)
#         df_status['issuer_likes_plus1q'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (theser.status_published_unix_UTC_Zurich + datetime.timedelta(days=90))) ].count(), axis=1)
#         df_status['issuer_likes_plus1m'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (theser.status_published_unix_UTC_Zurich + datetime.timedelta(days=30))) ].count(), axis=1)
#         df_status['issuer_likes_plus1w'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (theser.status_published_unix_UTC_Zurich + datetime.timedelta(days=7))) ].count(), axis=1)
#     
        df_status['issuer_likes_change1y'] = (df_status['issuer_likes_plus1y'] - df_status['issuer_likes_minus1y']) / 365
        df_status['issuer_likes_change1q'] = (df_status['issuer_likes_plus1q'] - df_status['issuer_likes_minus1q']) / 90
        df_status['issuer_likes_change1m'] = (df_status['issuer_likes_plus1m'] - df_status['issuer_likes_minus1m']) / 30
        df_status['issuer_likes_change1w'] = (df_status['issuer_likes_plus1w'] - df_status['issuer_likes_minus1w']) / 7 
        
        df_status['issuer_likes_attraction'] = (df_status['issuer_likes_minustotal'] == 0).astype(int)
        df_status['issuer_likes_aversion'] = (df_status['issuer_likes_plustotal'] == 0).astype(int)   
        
        
#         df_status['issuer_fgc_likes_minus1y'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (theser.status_published_unix_UTC_Zurich - datetime.timedelta(days=365))) ].sum(), axis=1)
#         df_status['issuer_fgc_likes_minus1q'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (theser.status_published_unix_UTC_Zurich - datetime.timedelta(days=90))) ].sum(), axis=1)
#         df_status['issuer_fgc_likes_minus1m'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (theser.status_published_unix_UTC_Zurich - datetime.timedelta(days=30))) ].sum(), axis=1)
#         df_status['issuer_fgc_likes_minus1w'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >=  (theser.status_published_unix_UTC_Zurich - datetime.timedelta(days=7))) ].sum(), axis=1)
#     
#         df_status['issuer_fgc_likes_plus1y'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (theser.status_published_unix_UTC_Zurich + datetime.timedelta(days=365))) ].sum(), axis=1)
#         df_status['issuer_fgc_likes_plus1q'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (theser.status_published_unix_UTC_Zurich + datetime.timedelta(days=90))) ].sum(), axis=1)
#         df_status['issuer_fgc_likes_plus1m'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (theser.status_published_unix_UTC_Zurich + datetime.timedelta(days=30))) ].sum(), axis=1)
#         df_status['issuer_fgc_likes_plus1w'] = df_status[['status_published_unix_UTC_Zurich','user_id']].apply(lambda theser: df_all['fgc_like'][(df_all.index.get_level_values('user_id') == theser['user_id']) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') >  theser.status_published_unix_UTC_Zurich) & (df_all.index.get_level_values('original_post_time_unix_UTC_Zurich') <=  (theser.status_published_unix_UTC_Zurich + datetime.timedelta(days=7))) ].sum(), axis=1)
#     
        df_status['issuer_fgc_likes_change1y'] = (df_status['issuer_fgc_likes_plus1y'] - df_status['issuer_fgc_likes_minus1y']) / 365
        df_status['issuer_fgc_likes_change1q'] = (df_status['issuer_fgc_likes_plus1q'] - df_status['issuer_fgc_likes_minus1q']) / 90
        df_status['issuer_fgc_likes_change1m'] = (df_status['issuer_fgc_likes_plus1m'] - df_status['issuer_fgc_likes_minus1m']) / 30
        df_status['issuer_fgc_likes_change1w'] = (df_status['issuer_fgc_likes_plus1w'] - df_status['issuer_fgc_likes_minus1w']) / 7    
        
        df_status['issuer_ugc_likes_minus1y'] = df_status['issuer_likes_minus1y'] - df_status['issuer_fgc_likes_minus1y']
        df_status['issuer_ugc_likes_minus1q'] = df_status['issuer_likes_minus1q'] - df_status['issuer_fgc_likes_minus1q']
        df_status['issuer_ugc_likes_minus1m'] = df_status['issuer_likes_minus1m'] - df_status['issuer_fgc_likes_minus1m']
        df_status['issuer_ugc_likes_minus1w'] = df_status['issuer_likes_minus1w'] - df_status['issuer_fgc_likes_minus1w']
        
        df_status['issuer_ugc_likes_plus1y'] = df_status['issuer_likes_plus1y'] - df_status['issuer_fgc_likes_plus1y']
        df_status['issuer_ugc_likes_plus1q'] = df_status['issuer_likes_plus1q'] - df_status['issuer_fgc_likes_plus1q']
        df_status['issuer_ugc_likes_plus1m'] = df_status['issuer_likes_plus1m'] - df_status['issuer_fgc_likes_plus1m']
        df_status['issuer_ugc_likes_plus1w'] = df_status['issuer_likes_plus1w'] - df_status['issuer_fgc_likes_plus1w']   
     
        df_status['issuer_ugc_likes_change1y'] = (df_status['issuer_ugc_likes_plus1y'] - df_status['issuer_ugc_likes_minus1y']) / 365
        df_status['issuer_ugc_likes_change1q'] = (df_status['issuer_ugc_likes_plus1q'] - df_status['issuer_ugc_likes_minus1q']) / 90
        df_status['issuer_ugc_likes_change1m'] = (df_status['issuer_ugc_likes_plus1m'] - df_status['issuer_ugc_likes_minus1m']) / 30
        df_status['issuer_ugc_likes_change1w'] = (df_status['issuer_ugc_likes_plus1w'] - df_status['issuer_ugc_likes_minus1w']) / 7      
        
        #df_all.sort_index(inplace=True)
        
        finalfilename='./likefeatures_final/'+filename_out+'_likefeatures.csv'
        df_status.to_csv(finalfilename, sep='\t',index=True,encoding='utf-8')
#         del df_all
#         del df_status
        print("finished to process file "+filename_raw+" at "+str(datetime.datetime.now()) )
        return 0

    except Exception as e:
        print('Error: '+str(e))
        print('....aborted processing file: '+filename_raw) 
        return 1
            

###################################################################################################
####entry point which organizes multiprocessing
####################################################################################################
if __name__ == '__main__':
    if not os.path.exists("likefeatures_final"):
        os.makedirs("likefeatures_final")
    files_done = glob.glob('./likefeatures_final/*.csv')
    
    if os.name == 'nt':
        files_done = [filedone.split("\\")[1].split("_")[0] for filedone in files_done] #this windows
    else:
        files_done = [filedone.split("/")[2].split("_")[0] for filedone in files_done] #this linux
    
    
    
    #read csv file
    files = glob.glob('./*_feed_*.csv')

    files_formated = dict()
    if os.name == 'nt':
        for filename_raw in files:
            files_formated[filename_raw] = filename_raw.split("\\")[1].split("_")[0] #this windows
    else:
        for filename_raw in files:
            files_formated[filename_raw] = filename_raw.split("/")[1].split("_")[0] #this linux

    #kick out the ones already done
    files_selected = [rawname for rawname,thefile in files_formated.iteritems() if (thefile not in files_done) ]

    pool = Pool(9)
    dropcntsum = 0
    filecnt = 0
    filetotal = len(files_selected)
    for dropcnt in pool.imap_unordered(process_likes, files_selected):
        filecnt += 1
        dropcntsum += dropcnt
        print("%d of %d files processed at time %s" % (filecnt,filetotal,str(datetime.datetime.now()))) 
    print("finished with all  at "+str(datetime.datetime.now()))
    print("overall number of aborted files: ")
    print(dropcntsum)          
