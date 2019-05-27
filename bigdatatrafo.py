
'''
Created on 11.10.2016

@author: jwulf
'''
'''
REMEMBER TO SET THE LANGUAGE
SPECIFY FILTER



TODO dtypes and typelists based on language
TODO timezones


CHANGES:
23.12.2016 added "UserOriginalPost_comments"
23.12.2016 added "FGC_comments_likes_sum":float    Summe der Likes auf alle FGC_comments; "FGC_comments_likes_mean":float    Durchschnitt der Likes pro  FGC_comment
25.12.2016 added posemo_before':float    negemo_before':float    posemo_3meanback':float    posemo_3meanforward':float    posemo_3meandiff':float    negemo_3meanback':float    negemo_3meanforward':float    negemo_3meandiff':float    posemo-negemo_3meandiff':float
9.1. and before join with like_features and set strategies to None if not Situation Selection
TODO firmtext komplett einfuegen
11.03.2018 Likes Infos rausgenommen, filter only in the relevant time range
18.04 an die CUTOFFS denken!!!!
19.04. CUTOFFS wieder raus, ebenso evolved
'''

import glob, os, re
from datetime import datetime
import pytz
#import dask.dataframe as dd
import pandas as pd
from collections import OrderedDict
import numpy as np
from multiprocessing import Pool

languageset = "en"
#languageset = "de"


###############################################################################################################
#####Function Definitions
###############################################################################################################
def getsinglepostfeatures(pid_series, pid_childs_df, imfout, user_posemo_sum, user_posemo_mean, user_negemo_sum, user_negemo_mean, indicator_dic):
    try:
        
        pid_userid=pid_series['user_id']

        
        #this needs to be ordered for doing index-based reasoning, so lets do this now
        pid_childs_df.sort_values(['status_published_unix_UTC_Zurich'], ascending=[True], inplace=True)
        
        
        if not pid_childs_df.empty:
            TOTAL_likes_sub = pid_childs_df['num_likes'].sum()
            TOTAL_comments_sub = pid_childs_df['num_comments'].sum()
            TOTAL_shares_sub = pid_childs_df['num_shares'].sum()
            TOTAL_likes = TOTAL_likes_sub + pid_series['num_likes']
            TOTAL_comments = TOTAL_comments_sub + pid_series['num_comments']
            #comments have no shares...
            if pid_series['num_shares'] and TOTAL_shares_sub:
                TOTAL_shares = TOTAL_shares_sub + pid_series['num_shares']
            elif pid_series['num_shares'] and not TOTAL_shares_sub:
                TOTAL_shares = pid_series['num_shares']
            elif not pid_series['num_shares'] and TOTAL_shares_sub:
                TOTAL_shares = TOTAL_shares_sub
            else:
                TOTAL_shares = None
         
            FGC_comments  = pid_childs_df["post_by_pageID"].sum()
            if FGC_comments>0:
                FGC_comments_likes_sum = pid_childs_df[pid_childs_df["post_by_pageID"]==1]['num_likes'].sum()
                FGC_comments_likes_mean = FGC_comments_likes_sum / FGC_comments
            else:
                FGC_comments_likes_sum = None
                FGC_comments_likes_mean = None
                
            UGC_comments = pid_childs_df[pid_childs_df['user_id']!=pid_userid]['OP_UGC'].sum()
            UserOriginalPost_comments = pid_childs_df[pid_childs_df['user_id']==pid_userid]['OP_UGC'].sum()
            T_Duration = (pid_childs_df["status_published_unix_UTC_Zurich"].max()-pid_series['status_published_unix_UTC_Zurich'])/60
            T_Reaction = (pid_childs_df[pid_childs_df['post_by_pageID']==1]["status_published_unix_UTC_Zurich"].min()-pid_series['status_published_unix_UTC_Zurich'])/60
             
            if languageset == "de":
                pid_childs_sub_df = pid_childs_df[((pid_childs_df['language'] == "de") | (pid_childs_df['language'] == "en")) & (pid_childs_df['Dic'] > 0)]
            else:
                pid_childs_sub_df = pid_childs_df[(pid_childs_df['language'] == "en") & (pid_childs_df['Dic'] > 0)]
                
            sum_OP_UGC = pid_childs_sub_df['OP_UGC'].sum()
            if sum_OP_UGC>0:
                Post_UGC_Affect_sum = (pid_childs_sub_df['affect']*pid_childs_sub_df['OP_UGC']).sum()
                Post_UGC_negemo_sum = (pid_childs_sub_df['negemo']*pid_childs_sub_df['OP_UGC']).sum()
                Post_UGC_posemo_sum = (pid_childs_sub_df['posemo']*pid_childs_sub_df['OP_UGC']).sum()
                Post_UGC_Affect_mean = Post_UGC_Affect_sum / sum_OP_UGC
                Post_UGC_negemo_mean = Post_UGC_negemo_sum / sum_OP_UGC
                Post_UGC_posemo_mean = Post_UGC_posemo_sum / sum_OP_UGC
                UGC_Cogproc_mean = (pid_childs_sub_df['cogproc']*pid_childs_sub_df['OP_UGC']).sum()/ sum_OP_UGC
            else:
                Post_UGC_Affect_sum = None
                Post_UGC_negemo_sum = None
                Post_UGC_posemo_sum = None
                Post_UGC_Affect_mean = None
                Post_UGC_negemo_mean = None
                Post_UGC_posemo_mean = None
                UGC_Cogproc_mean = None
            OP_sum_FGC = pid_childs_sub_df['post_by_pageID'].sum()
            IS_SituationSelection = int(OP_sum_FGC>0)
    
            if OP_sum_FGC>0:

                
                
                #201801417 here we rename same indicators because we introduce new regulation indicators
                OP_Modification_sum = (pid_childs_sub_df['new_modification']*pid_childs_sub_df['post_by_pageID']).sum()
                IS_SituationModification_sum = int(OP_Modification_sum>0)
                OP_apology_sum = (pid_childs_sub_df['new_apology']*pid_childs_sub_df['post_by_pageID']).sum()
                IS_apology_sum = int(OP_apology_sum>0)
                IS_SituationModificationApology_sum = IS_SituationModification_sum*IS_apology_sum
                OP_AttentionalDeployment_sum = (pid_childs_sub_df['posemo']*pid_childs_sub_df['post_by_pageID']).sum()
                IS_AttentionalDeployment_sum = int(OP_AttentionalDeployment_sum>0)
                OP_CognitiveChange_sum = (pid_childs_sub_df['cogproc']*pid_childs_sub_df['post_by_pageID']).sum()
                IS_CognitiveChange_sum = int(OP_CognitiveChange_sum>0)
                OP_insight_sum = (pid_childs_sub_df['insight']*pid_childs_sub_df['post_by_pageID']).sum()
                OP_cause_sum = (pid_childs_sub_df['cause']*pid_childs_sub_df['post_by_pageID']).sum()
                OP_AttentionalDeployment_mean = OP_AttentionalDeployment_sum / OP_sum_FGC
                OP_CognitiveChange_mean = OP_CognitiveChange_sum / OP_sum_FGC
                
                #201801417 get all firm replies
                pid_childs_sub_fgc_df = pid_childs_sub_df[pid_childs_sub_df['post_by_pageID'] == 1]
                ##just to be sure, should be in right order already
                pid_childs_sub_fgc_df.sort_values(['status_published_unix_UTC_Zurich'], ascending=[True], inplace=True)
                
                #this is new:
                firmtext1 = pid_childs_sub_fgc_df['status_message'].iloc[0]
               
                if pid_childs_sub_fgc_df.shape[0]>1:
                    firmtext2 =  pid_childs_sub_fgc_df['status_message'].iloc[1]
                else:
                    firmtext2 = None
                    
                if pid_childs_sub_fgc_df.shape[0]>2:
                    firmtext3 =  pid_childs_sub_fgc_df['status_message'].iloc[2]
                else:
                    firmtext3 = None  
                
                if pid_childs_sub_fgc_df.shape[0]>3:
                    firmtext4 =  pid_childs_sub_fgc_df['status_message'].iloc[3]
                else:
                    firmtext4 = None  
                    
                if pid_childs_sub_fgc_df.shape[0]>4:
                    firmtext5 =  pid_childs_sub_fgc_df['status_message'].iloc[4]
                else:
                    firmtext5 = None                  
                
                
                ##now get first row and define the indicators accordingly
                first_fgc_series = pid_childs_sub_fgc_df.iloc[0]
                OP_Modification_first = first_fgc_series['new_modification']
                IS_SituationModification_first = int(OP_Modification_first>0)
                OP_apology_first = first_fgc_series['new_apology']
                IS_apology_first = int(OP_apology_first>0)
                IS_SituationModificationApology_first = IS_SituationModification_first*IS_apology_first
                OP_AttentionalDeployment_first = first_fgc_series['posemo']
                IS_AttentionalDeployment_first = int(OP_AttentionalDeployment_first>0)
                OP_CognitiveChange_first = first_fgc_series['cogproc']
                IS_CognitiveChange_first = int(OP_CognitiveChange_first>0)
                OP_insight_first = first_fgc_series['insight']
                OP_cause_first = first_fgc_series['cause']
                OP_response_modulation_first = first_fgc_series['new_resp_mod']
                IS_response_modulation_first = int(OP_response_modulation_first>0)
                
                ###20180417 now do all but first
                if OP_sum_FGC>1: #more than one firm comment
                    ##new 20180429 read out timestamp of second comment
                    second_fgc_series = pid_childs_sub_fgc_df.iloc[1]
                    firm_reaction_time_2nd = second_fgc_series['status_published_unix_UTC_Zurich']
                    
                    #drop the first comment
                    pid_childs_sub_fgc_df.drop(pid_childs_sub_fgc_df.index[:1], inplace=True)
                    
                    OP_Modification_allbutfirst = pid_childs_sub_fgc_df['new_modification'].sum()
                    IS_SituationModification_allbutfirst = int(OP_Modification_allbutfirst>0)
                    OP_apology_allbutfirst = pid_childs_sub_fgc_df['new_apology'].sum()
                    IS_apology_allbutfirst = int(OP_apology_allbutfirst>0)
                    IS_SituationModificationApology_allbutfirst = IS_SituationModification_allbutfirst*IS_apology_allbutfirst
                    OP_AttentionalDeployment_allbutfirst = pid_childs_sub_fgc_df['posemo'].sum()
                    IS_AttentionalDeployment_allbutfirst = int(OP_AttentionalDeployment_allbutfirst>0)
                    OP_CognitiveChange_allbutfirst = pid_childs_sub_fgc_df['cogproc'].sum()
                    IS_CognitiveChange_allbutfirst = int(OP_CognitiveChange_allbutfirst>0)
                    OP_insight_allbutfirst = pid_childs_sub_fgc_df['insight'].sum()
                    OP_cause_allbutfirst = pid_childs_sub_fgc_df['cause'].sum()
                    OP_response_modulation_allbutfirst = pid_childs_sub_fgc_df['new_resp_mod'].sum()
                    IS_response_modulation_allbutfirst = int(OP_response_modulation_allbutfirst>0)
                    del second_fgc_series
                    
                else: #no second firm comment
                    #set to none  
                    firm_reaction_time_2nd = None
                    OP_Modification_allbutfirst = None
                    IS_SituationModification_allbutfirst = None
                    OP_apology_allbutfirst = None
                    IS_apology_allbutfirst = None
                    IS_SituationModificationApology_allbutfirst = None
                    OP_AttentionalDeployment_allbutfirst = None
                    IS_AttentionalDeployment_allbutfirst = None
                    OP_CognitiveChange_allbutfirst = None
                    IS_CognitiveChange_allbutfirst = None
                    OP_insight_allbutfirst = None
                    OP_cause_allbutfirst = None
                    OP_response_modulation_allbutfirst = None
                    IS_response_modulation_allbutfirst = None
                
                del pid_childs_sub_fgc_df
                del first_fgc_series
                
                
                #calculate an FGC's influence on the discussion
                #get the list of timestamps of all FGC post_ids, sort this list by timestamp
                timestamp_list_unsorted = pid_childs_sub_df[pid_childs_sub_df['post_by_pageID'] == 1]['status_published_unix_UTC_Zurich'].tolist()
                timestamp_list = sorted(timestamp_list_unsorted)
                posemo_mean_diff_list = list()
                negemo_mean_diff_list = list()
                cogproc_mean_diff_list = list()
                posemo_mean_diff_abs_list = list()
                negemo_mean_diff_abs_list = list()
                cogproc_mean_diff_abs_list = list()
                posemo_mean3_diff_list = list()
                negemo_mean3_diff_list = list()
                cogproc_mean3_diff_list = list()
                posemo_mean3_diff_abs_list = list()
                negemo_mean3_diff_abs_list = list()
                cogproc_mean3_diff_abs_list = list()
                
                #initialize timestamp1 with first timestamp of pid_series
                if pid_series['status_published_unix_UTC_Zurich']:
                    timestamp1 = pid_series['status_published_unix_UTC_Zurich']
                else:
                    timestamp1 = 0
                    
                #get first timestamp and set timestamp2 to it
                timestamp2 = timestamp_list[0]
                #get first dataframe between 1 and 2
                #preFGCdf = pid_childs_sub_df[['posemo','negemo','cogproc','status_published_unix_UTC_Zurich']][pid_childs_sub_df['status_published_unix_UTC_Zurich'] > timestamp1][pid_childs_sub_df['status_published_unix_UTC_Zurich'] < timestamp2]
                preFGCdf = pid_childs_sub_df[['posemo','negemo','cogproc','status_published_unix_UTC_Zurich']][pid_childs_sub_df['status_published_unix_UTC_Zurich'].between(timestamp1,timestamp2,inclusive=False)][pid_childs_sub_df['OP_UGC']==1]
    
                #append the org post information here since we are interested in change influence of FGC
                pre_posemo_series = preFGCdf['posemo'].append(pd.Series(pid_series['posemo']))
                pre_posemo_mean = pre_posemo_series.mean()
                pre3_posemo_mean = pre_posemo_series.tail(n=3).mean()
                pre_negemo_series = preFGCdf['negemo'].append(pd.Series(pid_series['negemo']))
                pre_negemo_mean = pre_negemo_series.mean()
                pre3_negemo_mean = pre_negemo_series.tail(n=3).mean()
                pre_cogproc_series = preFGCdf['cogproc'].append(pd.Series(pid_series['cogproc']))
                pre_cogproc_mean = pre_cogproc_series.mean()
                pre3_cogproc_mean = pre_cogproc_series.tail(n=3).mean()
                
                timestampit = 1
                while (timestampit < len(timestamp_list)):
                    timestamp3 = timestamp_list[timestampit]
                    #postFGCdf = pid_childs_sub_df[['posemo','negemo','cogproc','status_published_unix_UTC_Zurich']][pid_childs_sub_df['status_published_unix_UTC_Zurich'] > timestamp2][pid_childs_sub_df['status_published_unix_UTC_Zurich'] < timestamp3]
                    postFGCdf = pid_childs_sub_df[['posemo','negemo','cogproc','status_published_unix_UTC_Zurich']][pid_childs_sub_df['status_published_unix_UTC_Zurich'] > timestamp2][pid_childs_sub_df['status_published_unix_UTC_Zurich'] < timestamp3][pid_childs_sub_df['OP_UGC']==1]
    
                    post_posemo_mean = postFGCdf['posemo'].mean()
                    post_negemo_mean = postFGCdf['negemo'].mean()
                    post_cogproc_mean = postFGCdf['cogproc'].mean()
                    post3_posemo_mean = postFGCdf['posemo'].head(n=3).mean()
                    post3_negemo_mean = postFGCdf['negemo'].head(n=3).mean()
                    post3_cogproc_mean = postFGCdf['cogproc'].head(n=3).mean()
                    posemo_mean_diff_list.append((post_posemo_mean-pre_posemo_mean))
                    negemo_mean_diff_list.append((post_negemo_mean-pre_negemo_mean))
                    cogproc_mean_diff_list.append((post_cogproc_mean-pre_cogproc_mean))
                    posemo_mean_diff_abs_list.append(abs(post_posemo_mean-pre_posemo_mean))
                    negemo_mean_diff_abs_list.append(abs(post_negemo_mean-pre_negemo_mean))
                    cogproc_mean_diff_abs_list.append(abs(post_cogproc_mean-pre_cogproc_mean))
                    posemo_mean3_diff_list.append((post3_posemo_mean-pre3_posemo_mean))
                    negemo_mean3_diff_list.append((post3_negemo_mean-pre3_negemo_mean))
                    cogproc_mean3_diff_list.append((post3_cogproc_mean-pre3_cogproc_mean))
                    posemo_mean3_diff_abs_list.append(abs(post3_posemo_mean-pre3_posemo_mean))
                    negemo_mean3_diff_abs_list.append(abs(post3_negemo_mean-pre3_negemo_mean))
                    cogproc_mean3_diff_abs_list.append(abs(post3_cogproc_mean-pre3_cogproc_mean))
                    preFGCdf = postFGCdf
                    pre_posemo_mean = post_posemo_mean
                    pre_negemo_mean = post_negemo_mean
                    pre_cogproc_mean = post_cogproc_mean
                    pre3_posemo_mean = post3_posemo_mean
                    pre3_negemo_mean = post3_negemo_mean
                    pre3_cogproc_mean = post3_cogproc_mean
                    timestamp2 = timestamp3
                    timestampit+=1
                
                postFGCdf = pid_childs_sub_df[pid_childs_sub_df['status_published_unix_UTC_Zurich'] > timestamp2][pid_childs_sub_df['OP_UGC']==1][['posemo','negemo','cogproc']]
                post_posemo_mean = postFGCdf['posemo'].mean()
                post_negemo_mean = postFGCdf['negemo'].mean()
                post_cogproc_mean = postFGCdf['cogproc'].mean()
                post3_posemo_mean = postFGCdf['posemo'].head(n=3).mean()
                post3_negemo_mean = postFGCdf['negemo'].head(n=3).mean()
                post3_cogproc_mean = postFGCdf['cogproc'].head(n=3).mean()
                posemo_mean_diff_list.append((post_posemo_mean-pre_posemo_mean))
                negemo_mean_diff_list.append((post_negemo_mean-pre_negemo_mean))
                cogproc_mean_diff_list.append((post_cogproc_mean-pre_cogproc_mean))
                posemo_mean_diff_abs_list.append(abs(post_posemo_mean-pre_posemo_mean))
                negemo_mean_diff_abs_list.append(abs(post_negemo_mean-pre_negemo_mean))
                cogproc_mean_diff_abs_list.append(abs(post_cogproc_mean-pre_cogproc_mean))
                posemo_mean3_diff_list.append((post3_posemo_mean-pre3_posemo_mean))
                negemo_mean3_diff_list.append((post3_negemo_mean-pre3_negemo_mean))
                cogproc_mean3_diff_list.append((post3_cogproc_mean-pre3_cogproc_mean))
                posemo_mean3_diff_abs_list.append(abs(post3_posemo_mean-pre3_posemo_mean))
                negemo_mean3_diff_abs_list.append(abs(post3_negemo_mean-pre3_negemo_mean))
                cogproc_mean3_diff_abs_list.append(abs(post3_cogproc_mean-pre3_cogproc_mean))
                
                OP_FGC_posemo_mean_diff_mean = np.nanmean(posemo_mean_diff_list)
                OP_FGC_posemo_mean_diff_abs_mean =np.nanmean(posemo_mean_diff_abs_list)
                OP_FGC_negemo_mean_diff_mean = np.nanmean(negemo_mean_diff_list)
                OP_FGC_negemo_mean_diff_abs_mean =np.nanmean(negemo_mean_diff_abs_list)
                OP_FGC_cogproc_mean_diff_mean = np.nanmean(cogproc_mean_diff_list)
                OP_FGC_cogproc_mean_diff_abs_mean =np.nanmean(cogproc_mean_diff_abs_list)
                OP_FGC_posemo_mean3_diff_mean = np.nanmean(posemo_mean3_diff_list)
                OP_FGC_posemo_mean3_diff_abs_mean =np.nanmean(posemo_mean3_diff_abs_list)
                OP_FGC_negemo_mean3_diff_mean = np.nanmean(negemo_mean3_diff_list)
                OP_FGC_negemo_mean3_diff_abs_mean =np.nanmean(negemo_mean3_diff_abs_list)
                OP_FGC_cogproc_mean3_diff_mean = np.nanmean(cogproc_mean3_diff_list)
                OP_FGC_cogproc_mean3_diff_abs_mean =np.nanmean(cogproc_mean3_diff_abs_list)
                
                
            else:
                firmtext1 = None
                firmtext2 = None
                firmtext3 = None
                firmtext4 = None
                firmtext5 = None
                OP_Modification_sum = None
                IS_SituationModification_sum = None
                OP_apology_sum = None
                IS_apology_sum = None
                IS_SituationModificationApology_sum = None
                OP_AttentionalDeployment_sum = None
                IS_AttentionalDeployment_sum = None
                OP_CognitiveChange_sum = None
                IS_CognitiveChange_sum = None
                OP_insight_sum = None
                OP_cause_sum = None
                OP_AttentionalDeployment_mean = None
                OP_CognitiveChange_mean = None
                OP_Modification_first = None
                IS_SituationModification_first = None
                OP_apology_first = None
                IS_apology_first = None
                IS_SituationModificationApology_first = None
                OP_AttentionalDeployment_first = None
                IS_AttentionalDeployment_first = None
                OP_CognitiveChange_first = None
                IS_CognitiveChange_first = None
                OP_insight_first = None
                OP_cause_first = None
                OP_response_modulation_first = None
                IS_response_modulation_first = None  
                
                firm_reaction_time_2nd = None
                OP_Modification_allbutfirst = None
                IS_SituationModification_allbutfirst = None
                OP_apology_allbutfirst = None
                IS_apology_allbutfirst = None
                IS_SituationModificationApology_allbutfirst = None
                OP_AttentionalDeployment_allbutfirst = None
                IS_AttentionalDeployment_allbutfirst = None
                OP_CognitiveChange_allbutfirst = None
                IS_CognitiveChange_allbutfirst = None
                OP_insight_allbutfirst = None
                OP_cause_allbutfirst = None
                OP_response_modulation_allbutfirst = None
                IS_response_modulation_allbutfirst = None              
                
                OP_FGC_posemo_mean_diff_mean= None
                OP_FGC_posemo_mean_diff_abs_mean= None
                OP_FGC_negemo_mean_diff_mean= None
                OP_FGC_negemo_mean_diff_abs_mean= None
                OP_FGC_cogproc_mean_diff_mean= None
                OP_FGC_cogproc_mean_diff_abs_mean= None
                OP_FGC_posemo_mean3_diff_mean= None
                OP_FGC_posemo_mean3_diff_abs_mean= None
                OP_FGC_negemo_mean3_diff_mean= None
                OP_FGC_negemo_mean3_diff_abs_mean= None
                OP_FGC_cogproc_mean3_diff_mean= None
                OP_FGC_cogproc_mean3_diff_abs_mean= None
                
                
            if OP_AttentionalDeployment_mean and Post_UGC_posemo_mean:
                OP_AttentionalDeployment_mean_diff = OP_AttentionalDeployment_mean - Post_UGC_posemo_mean
            else:
                OP_AttentionalDeployment_mean_diff = None
            if OP_CognitiveChange_mean and UGC_Cogproc_mean:
                OP_CognitiveChange_mean_diff = OP_CognitiveChange_mean - UGC_Cogproc_mean
            else:
                OP_CognitiveChange_mean_diff = None
            
            if OP_sum_FGC>0:
                OP_response_modulation_sum = (pid_childs_sub_df['new_resp_mod']*pid_childs_sub_df['post_by_pageID']).sum()
                IS_response_modulation_sum = int(OP_response_modulation_sum>0)
            else:
                OP_response_modulation_sum = None
                IS_response_modulation_sum = None
    
        else: #we have post without comments
            TOTAL_likes_sub = None
            TOTAL_comments_sub = None
            TOTAL_shares_sub = None
            TOTAL_likes = pid_series['num_likes']
            TOTAL_comments = pid_series['num_comments']
            TOTAL_shares = pid_series['num_shares']     
            FGC_comments  = 0
            FGC_comments_likes_sum = None
            FGC_comments_likes_mean = None
            UGC_comments = 0
            UserOriginalPost_comments = 0
            T_Duration = None
            T_Reaction = None
    
            Post_UGC_Affect_sum = None
            Post_UGC_negemo_sum = None
            Post_UGC_posemo_sum = None
            Post_UGC_Affect_mean = None
            Post_UGC_negemo_mean = None
            Post_UGC_posemo_mean = None
            UGC_Cogproc_mean = None
            IS_SituationSelection = 0
            firmtext1 = None
            firmtext2 = None
            firmtext3 = None
            firmtext4 = None
            firmtext5 = None
            OP_Modification_sum = None
            IS_SituationModification_sum = 0
            OP_apology_sum = None
            IS_apology_sum = 0
            IS_SituationModificationApology_sum = IS_SituationModification_sum*IS_apology_sum
            OP_AttentionalDeployment_sum = None
            IS_AttentionalDeployment_sum = None
            OP_CognitiveChange_sum = None
            IS_CognitiveChange_sum = None
            OP_insight_sum = None
            OP_cause_sum = None
            OP_AttentionalDeployment_mean = None
            OP_CognitiveChange_mean = None
            OP_AttentionalDeployment_mean_diff = None
            OP_CognitiveChange_mean_diff = None
            OP_response_modulation_sum = None
            IS_response_modulation_sum = 0
            OP_sum_FGC = 0
            OP_Modification_first = None
            IS_SituationModification_first = None
            OP_apology_first = None
            IS_apology_first = None
            IS_SituationModificationApology_first = None
            OP_AttentionalDeployment_first = None
            IS_AttentionalDeployment_first = None
            OP_CognitiveChange_first = None
            IS_CognitiveChange_first = None
            OP_insight_first = None
            OP_cause_first = None
            OP_response_modulation_first = None
            IS_response_modulation_first = None   
            
            firm_reaction_time_2nd = None
            OP_Modification_allbutfirst = None
            IS_SituationModification_allbutfirst = None
            OP_apology_allbutfirst = None
            IS_apology_allbutfirst = None
            IS_SituationModificationApology_allbutfirst = None
            OP_AttentionalDeployment_allbutfirst = None
            IS_AttentionalDeployment_allbutfirst = None
            OP_CognitiveChange_allbutfirst = None
            IS_CognitiveChange_allbutfirst = None
            OP_insight_allbutfirst = None
            OP_cause_allbutfirst = None
            OP_response_modulation_allbutfirst = None
            IS_response_modulation_allbutfirst = None 
            
            OP_FGC_posemo_mean_diff_mean= None
            OP_FGC_posemo_mean_diff_abs_mean= None
            OP_FGC_negemo_mean_diff_mean= None
            OP_FGC_negemo_mean_diff_abs_mean= None
            OP_FGC_cogproc_mean_diff_mean= None
            OP_FGC_cogproc_mean_diff_abs_mean= None
            OP_FGC_posemo_mean3_diff_mean= None
            OP_FGC_posemo_mean3_diff_abs_mean= None
            OP_FGC_negemo_mean3_diff_mean= None
            OP_FGC_negemo_mean3_diff_abs_mean= None
            OP_FGC_cogproc_mean3_diff_mean= None
            OP_FGC_cogproc_mean3_diff_abs_mean= None
            
                  
    
    # #  #do the line level additions
    
        OP_Posemo_Adjusted = (pid_series['posemo'] - indicator_dic["posemo_meanOP"])/100 + 1
        OP_Posemo_Adjusted_all = (pid_series['posemo'] - indicator_dic["posemo_mean_all"])/100 + 1    
        OP_Negemo_Adjusted = (pid_series['negemo'] - indicator_dic["negemo_meanOP"])/100 + 1
        OP_Negemo_Adjusted_all = (pid_series['negemo'] - indicator_dic["negemo_mean_all"])/100 + 1
        
        CON_Negativity_OP = (pid_series['posemo'] - pid_series['negemo'])/100 + 1
        CON_Negativity_Adjusted = (OP_Negemo_Adjusted - OP_Posemo_Adjusted) + 2
        CON_Negativity_Adjusted_all = (OP_Negemo_Adjusted_all - OP_Posemo_Adjusted_all) + 2
     
        LSM_Preps  = 1 - (abs(pid_series['prep']- indicator_dic["prep_meanOP"])/ (pid_series['prep'] + indicator_dic["prep_meanOP"] + 0.0001))
        LSM_Preps_all  = 1 - (abs(pid_series['prep']- indicator_dic["prep_mean_all"])/ (pid_series['prep'] + indicator_dic["prep_mean_all"] + 0.0001)) 
        LSM_Pronoun = 1 - (abs(pid_series['pronoun'] - indicator_dic["pronoun_meanOP"])/ (pid_series['pronoun'] + indicator_dic["pronoun_meanOP"] + 0.0001))
        LSM_Pronoun_all = 1 - (abs(pid_series['pronoun'] - indicator_dic["pronoun_mean_all"])/ (pid_series['pronoun'] + indicator_dic["pronoun_mean_all"] + 0.0001))
        LSM_Negate = 1 - (abs(pid_series['negate'] - indicator_dic["negate_meanOP"])/ (pid_series['negate'] + indicator_dic["negate_meanOP"] + 0.0001))
        LSM_Negate_all = 1 - (abs(pid_series['negate'] - indicator_dic["negate_mean_all"])/ (pid_series['negate'] + indicator_dic["negate_mean_all"] + 0.0001))
        LSM_Article = 1 - (abs(pid_series['article'] - indicator_dic["article_meanOP"])/ (pid_series['article'] + indicator_dic["article_meanOP"] + 0.0001))
        LSM_Article_all = 1 - (abs(pid_series['article'] - indicator_dic["article_mean_all"])/ (pid_series['article'] + indicator_dic["article_mean_all"] + 0.0001))
        LSM_Netspeak = 1 - (abs(pid_series['netspeak'] - indicator_dic["netspeak_meanOP"])/ (pid_series['netspeak'] + indicator_dic["netspeak_meanOP"] + 0.0001))
        LSM_Netspeak_all = 1 - (abs(pid_series['netspeak'] - indicator_dic["netspeak_mean_all"])/ (pid_series['netspeak'] + indicator_dic["netspeak_mean_all"] + 0.0001))
    #             
        CON_LSM_OP = (LSM_Preps + LSM_Pronoun + LSM_Negate + LSM_Article)/4
        CON_LSM_OP_Netspeak = (LSM_Preps + LSM_Pronoun + LSM_Negate + LSM_Article+LSM_Netspeak)/5
        CON_LSM_OP_all = (LSM_Preps_all + LSM_Pronoun_all + LSM_Negate_all + LSM_Article_all)/4
        CON_LSM_OP_Netspeak_all = (LSM_Preps_all + LSM_Pronoun_all + LSM_Negate_all + LSM_Article_all+LSM_Netspeak_all)/5
        
        #now get the rolling values with 3 months
        #current_year = pid_series['OP_year']
        #current_month = pid_series['OP_month']
        current_yearmonth = str(pid_series['OP_yearmonth'])
    
        LSM_Preps_3Roll  = 1 - (abs(pid_series['prep']- indicator_dic["series_prep_3month_rollingmean_shifted"][current_yearmonth])/ (pid_series['prep'] + indicator_dic["series_prep_3month_rollingmean_shifted"][current_yearmonth] + 0.0001))
        LSM_Pronoun_3Roll = 1 - (abs(pid_series['pronoun'] - indicator_dic["series_pronoun_3month_rollingmean_shifted"][current_yearmonth])/ (pid_series['pronoun'] + indicator_dic["series_pronoun_3month_rollingmean_shifted"][current_yearmonth] + 0.0001))
        LSM_Negate_3Roll = 1 - (abs(pid_series['negate'] - indicator_dic["series_negate_3month_rollingmean_shifted"][current_yearmonth])/ (pid_series['negate'] + indicator_dic["series_negate_3month_rollingmean_shifted"][current_yearmonth] + 0.0001))
        LSM_Article_3Roll = 1 - (abs(pid_series['article'] - indicator_dic["series_article_3month_rollingmean_shifted"][current_yearmonth])/ (pid_series['article'] + indicator_dic["series_article_3month_rollingmean_shifted"][current_yearmonth] + 0.0001))
        LSM_Netspeak_3Roll = 1 - (abs(pid_series['netspeak'] - indicator_dic["series_netspeak_3month_rollingmean_shifted"][current_yearmonth])/ (pid_series['netspeak'] + indicator_dic["series_netspeak_3month_rollingmean_shifted"][current_yearmonth] + 0.0001))    
        CON_LSM_OP_3Roll = (LSM_Preps_3Roll + LSM_Pronoun_3Roll + LSM_Negate_3Roll + LSM_Article_3Roll)/4
        CON_LSM_OP_Netspeak_3Roll = (LSM_Preps_3Roll + LSM_Pronoun_3Roll + LSM_Negate_3Roll + LSM_Article_3Roll+LSM_Netspeak_3Roll)/5
    
         
        OP_Directedness_Adjusted = (pid_series['you']- indicator_dic["you_meanOP"])/100 + 1
        OP_Directedness_Adjusted_all = (pid_series['you']- indicator_dic["you_mean_all"])/100 + 1
        OP_Causality_Adjusted = (pid_series['cogproc'] - indicator_dic["cogproc_meanOP"])/100 + 1
        OP_Causality_Adjusted_all = (pid_series['cogproc'] - indicator_dic["cogproc_mean_all"])/100 + 1
        
        #write lines
        #append series with keys and values
        new_features = [user_posemo_sum, user_posemo_mean, user_negemo_sum, user_negemo_mean, TOTAL_likes_sub , TOTAL_comments_sub , TOTAL_shares_sub , TOTAL_likes , TOTAL_comments , TOTAL_shares , FGC_comments , FGC_comments_likes_sum, FGC_comments_likes_mean, UGC_comments , UserOriginalPost_comments, T_Duration , Post_UGC_Affect_sum , Post_UGC_Affect_mean , Post_UGC_negemo_sum , Post_UGC_negemo_mean , Post_UGC_posemo_sum , Post_UGC_posemo_mean , UGC_Cogproc_mean , OP_sum_FGC , IS_SituationSelection , OP_Modification_sum , IS_SituationModification_sum , OP_apology_sum , IS_apology_sum , IS_SituationModificationApology_sum , OP_AttentionalDeployment_sum , IS_AttentionalDeployment_sum , OP_CognitiveChange_sum , IS_CognitiveChange_sum , OP_insight_sum, OP_cause_sum, OP_AttentionalDeployment_mean , OP_CognitiveChange_mean , OP_AttentionalDeployment_mean_diff , OP_CognitiveChange_mean_diff , OP_response_modulation_sum , IS_response_modulation_sum , OP_Modification_first , IS_SituationModification_first , OP_apology_first , IS_apology_first , IS_SituationModificationApology_first , OP_AttentionalDeployment_first , IS_AttentionalDeployment_first , OP_CognitiveChange_first , IS_CognitiveChange_first , OP_insight_first , OP_cause_first , OP_response_modulation_first , IS_response_modulation_first , firm_reaction_time_2nd, OP_Modification_allbutfirst , IS_SituationModification_allbutfirst , OP_apology_allbutfirst , IS_apology_allbutfirst , IS_SituationModificationApology_allbutfirst , OP_AttentionalDeployment_allbutfirst , IS_AttentionalDeployment_allbutfirst , OP_CognitiveChange_allbutfirst , IS_CognitiveChange_allbutfirst , OP_insight_allbutfirst , OP_cause_allbutfirst , OP_response_modulation_allbutfirst , IS_response_modulation_allbutfirst , OP_Posemo_Adjusted , OP_Negemo_Adjusted , CON_Negativity_OP , CON_Negativity_Adjusted , CON_LSM_OP , CON_LSM_OP_Netspeak , OP_Directedness_Adjusted , OP_Causality_Adjusted , OP_FGC_posemo_mean_diff_mean , OP_FGC_posemo_mean_diff_abs_mean , OP_FGC_negemo_mean_diff_mean , OP_FGC_negemo_mean_diff_abs_mean , OP_FGC_cogproc_mean_diff_mean , OP_FGC_cogproc_mean_diff_abs_mean ,  OP_FGC_posemo_mean3_diff_mean , OP_FGC_posemo_mean3_diff_abs_mean , OP_FGC_negemo_mean3_diff_mean , OP_FGC_negemo_mean3_diff_abs_mean , OP_FGC_cogproc_mean3_diff_mean , OP_FGC_cogproc_mean3_diff_abs_mean , T_Reaction , CON_LSM_OP_3Roll , CON_LSM_OP_Netspeak_3Roll   , OP_Posemo_Adjusted_all , OP_Negemo_Adjusted_all , CON_Negativity_Adjusted_all , CON_LSM_OP_all , CON_LSM_OP_Netspeak_all , OP_Directedness_Adjusted_all , OP_Causality_Adjusted_all, firmtext1, firmtext2, firmtext3, firmtext4, firmtext5]
        #This could be handled more efficiently without tostring conversion and write csv
        #result_line = [str(itm) for itm in pid_series.tolist()] #this is unordered, better to iterate over list and retrieve elements
        result_line=list()
        for resobj in imcolumnames_raw[:-1]:
            result_line.append(str(pid_series[resobj]))
        result_line.extend([str(nitm) for nitm in new_features])
        result_line.append('\r\n')
        result_line_out = '\t'.join(result_line)
    #     feat_collector=OrderedDict()
    #     for feat_cnt in range(0,(len(new_features)-1)):
    #         feat_collector.update({new_features_head[feat_cnt]:new_features[feat_cnt]})
    #     pid_series.append(pd.Series(feat_collector))
             
        #write series to csv
        imfout.write(result_line_out)
        del result_line_out
        del result_line
        del pid_series
        del pid_childs_df

    
    except Exception as e:
        print("caught exception: %s at userid: %s") % (e,pid_userid)
               

def getallpostfeatures(collectordf,imfout,indicator_dic):
    #20180418 for now we just want posts noch comments as originators
    #id_list1 = collectordf[collectordf['type'] == 'post']['post_id'].unique().tolist()
    #id_list2 = collectordf[collectordf['type_details'] == "comment"]['post_id'].unique().tolist()
    #id_list = id_list1 + id_list2
    
    id_list = collectordf[collectordf['type'] == 'post']['post_id'].unique().tolist()

    for pid in id_list:
        if pid != '':
        #get the pid line as a df
            pid_line_series =  collectordf[collectordf['post_id']==pid].iloc[0,]
            #kick out firm comments/posts and those without dicwords and English language recognition
            if languageset == "de":
                haslanguage = (pid_line_series["language"] in ["en","de"])
            else:
                haslanguage = (pid_line_series["language"] == "en")
                
            
            if not pid_line_series.empty and pid_line_series["post_by_pageID"]!=1 and pid_line_series["Dic"]>0 and haslanguage:
                
                #check if we have FGC comments to the UGC and analyze it
                firm_childs = collectordf[collectordf['referring_post_or_comment_id']==pid][collectordf["post_by_pageID"]==1]['post_id'].unique().tolist()

                user_posemo_sum=0
                user_posemo_num=0
                user_negemo_sum=0
                user_negemo_num=0
                
                for fgcid in firm_childs:
                    #get the user comments to this firmreaction
                    user_childs = collectordf[collectordf['referring_post_or_comment_id']==fgcid][collectordf["post_by_pageID"]!=1]
                    if not user_childs.empty:
                        #read out posemo and negemo info and add it to overall
                        user_posemo_sum = user_posemo_sum + user_childs['posemo'].sum()
                        user_posemo_num = user_posemo_num + user_childs['posemo'].count()
                        user_negemo_sum = user_negemo_sum + user_childs['negemo'].sum()
                        user_negemo_num = user_negemo_num + user_childs['negemo'].count()
                #generate posemo and negemo means
                if user_posemo_num == 0:
                    user_posemo_sum = None
                    user_posemo_mean = None
                else:
                    user_posemo_mean = user_posemo_sum / user_posemo_num
                
                if user_negemo_num == 0:
                    user_negemo_sum = None
                    user_negemo_mean = None
                else:
                    user_negemo_mean = user_negemo_sum / user_negemo_num                    
                
                        
                #get the subsumed comments
                pid_childs = collectordf[collectordf['referring_post_or_comment_id']==pid]
                #get the features for the pid (if any)
                getsinglepostfeatures(pid_line_series, pid_childs, imfout, user_posemo_sum, user_posemo_mean, user_negemo_sum, user_negemo_mean, indicator_dic)
                del pid_childs
            del pid_line_series
    del id_list
    del collectordf

################################################################################################################################
########process_file main function
##################################################################################################################################


def process_file(filename_raw):    
    try:
        global dtypes
        global dtypelist
        global imtypelist    
        #dtypes = {"pageID": str,"page_name": str,"scrape_time": str,"type": str,"type_details": str,"original_post_id": str,"post_id": str,"pre_post_id": str,"post_id_summary": str,"from": str,"user_id": str,"status_message": str,"link_name": str,"status_type": str,"link": str,"status_published_UTC_Zurich": str,"status_published_unix_UTC_Zurich": str,"status_updated_UTC_Zurich": str,"status_updated_unix_UTC_Zurich": str,"num_likes": float,"num_comments": float,"num_shares": float,"post_by_pageID": float,"original_post_time_UTC_Zurich": str,"original_post_time_unix_UTC_Zurich": str,"referring_post_or_comment_id": str,"WC": float,"WPS": float,"Sixltr": float,"Dic": float,"Numerals": float,"function": float,"pronoun": float,"ppron": float,"i": float,"we": float,"you": float,"shehe": float,"they": float,"ipron": float,"article": float,"prep": float,"auxverb": float,"adverb": float,"conj": float,"negate": float,"verb": float,"adj": float,"compare": float,"floaterrog": float,"number": float,"quant": float,"affect": float,"posemo": float,"negemo": float,"anx": float,"anger": float,"sad": float,"social": float,"family": float,"friend": float,"female": float,"male": float,"cogproc": float,"insight": float,"cause": float,"discrep": float,"tentat": float,"certain": float,"differ": float,"percept": float,"see": float,"hear": float,"feel": float,"bio": float,"body": float,"health": float,"sexual": float,"ingest": float,"drives": float,"affiliation": float,"achiev": float,"power": float,"reward": float,"risk": float,"focuspast": float,"focuspresent": float,"focusfuture": float,"relativ": float,"motion": float,"space": float,"time": float,"work": float,"leisure": float,"home": float,"money": float,"relig": float,"death": float,"informal": float,"swear": float,"netspeak": float,"assent": float,"nonflu": float,"filler": float,"new_apology": float,"new_resp_mod": float,"new_modification": float,"Period": float,"Comma": float,"Colon": float,"SemiC": float,"QMark": float,"Exclam": float,"Dash": float,"Quote": float,"Apostro": float,"Parenth": float,"OtherP": float,"AllPct": float,"language": str,"lang_prob": float} 
        dtypes = {"WC": float,"WPS": float,"Sixltr": float,"Dic": float,"Numerals": float,"function": float,"pronoun": float,"ppron": float,"i": float,"we": float,"you": float,"shehe": float,"they": float,"ipron": float,"article": float,"prep": float,"auxverb": float,"adverb": float,"conj": float,"negate": float,"verb": float,"adj": float,"compare": float,"floaterrog": float,"number": float,"quant": float,"affect": float,"posemo": float,"negemo": float,"anx": float,"anger": float,"sad": float,"social": float,"family": float,"friend": float,"female": float,"male": float,"cogproc": float,"insight": float,"cause": float,"discrep": float,"tentat": float,"certain": float,"differ": float,"percept": float,"see": float,"hear": float,"feel": float,"bio": float,"body": float,"health": float,"sexual": float,"ingest": float,"drives": float,"affiliation": float,"achiev": float,"power": float,"reward": float,"risk": float,"focuspast": float,"focuspresent": float,"focusfuture": float,"relativ": float,"motion": float,"space": float,"time": float,"work": float,"leisure": float,"home": float,"money": float,"relig": float,"death": float,"informal": float,"swear": float,"netspeak": float,"assent": float,"nonflu": float,"filler": float,"new_apology": float,"new_resp_mod": float,"new_modification": float,"Period": float,"Comma": float,"Colon": float,"SemiC": float,"QMark": float,"Exclam": float,"Dash": float,"Quote": float,"Apostro": float,"Parenth": float,"OtherP": float,"AllPct": float,"language": str,"lang_prob": float, "has_mail": str, 'OP_yearmonth': str, 'status_published_unix_UTC_Zurich': float} 
        #dtypes = {'pageID':str,'page_name':str,'scrape_time':str,'type':str,'type_details':str,'original_post_id':str,'post_id':str,'pre_post_id':str,'post_id_summary':str,'from':str,'user_id':str,'status_message':str,'link_name':str,'status_type':str,'link':str,'status_published_UTC_Zurich':str,'status_published_unix_UTC_Zurich':float,'status_updated_UTC_Zurich':str,'status_updated_unix_UTC_Zurich':float,'num_likes':float,'num_comments':float,'num_shares':float,'post_by_pageID':float,'original_post_time_UTC_Zurich':str,'original_post_time_unix_UTC_Zurich':str,'referring_post_or_comment_id':str,'WC':float,'WPS':float,'Sixltr':float,'Dic':float,'Numerals':float,'function':float,'pronoun':float,'ppron':float,'i':float,'we':float,'you':float,'shehe':float,'they':float,'ipron':float,'article':float,'prep':float,'auxverb':float,'adverb':float,'conj':float,'negate':float,'verb':float,'adj':float,'compare':float,'interrog':float,'number':float,'quant':float,'affect':float,'posemo':float,'negemo':float,'anx':float,'anger':float,'sad':float,'social':float,'family':float,'friend':float,'female':float,'male':float,'cogproc':float,'insight':float,'cause':float,'discrep':float,'tentat':float,'certain':float,'differ':float,'percept':float,'see':float,'hear':float,'feel':float,'bio':float,'body':float,'health':float,'sexual':float,'ingest':float,'drives':float,'affiliation':float,'achiev':float,'power':float,'reward':float,'risk':float,'focuspast':float,'focuspresent':float,'focusfuture':float,'relativ':float,'motion':float,'space':float,'time':float,'work':float,'leisure':float,'home':float,'money':float,'relig':float,'death':float,'informal':float,'swear':float,'netspeak':float,'assent':float,'nonflu':float,'filler':float,'new_disgust':float,'new_apology':float,'new_resp_mod':float,'new_modification':float,'posemo_bi':float,'negemo_bi':float,'anger_bi':float,'anx_bi':float,'sad_bi':float,'new_disgust_bi':float,'negemo_bi_rev':float,'posemo_bi_rev':float,'negemo_sen':float,'posemo_sen':float,'anger_sen':float,'anx_sen':float,'sad_sen':float,'new_disgust_sen':float,'posemo_sen_rev':float,'negemo_sen_rev':float,'negate_bi':float,'Period':float,'Comma':float,'Colon':float,'SemiC':float,'QMark':float,'Exclam':float,'Dash':float,'Quote':float,'Apostro':float,'Parenth':float,'OtherP':float,'AllPct':float,'language':str,'lang_prob':float,'has_mail':str,'OP_UGC':int,'CON_Negativity_OP':float,'OP_Picture':int,'OP_Video':int,'OP_Event':int,'OP_Link':int,'OP_Status':int,'OP_weekend':int,'OP_morning':int,'OP_afternoon':int,'OP_evening':int,'OP_night':int,'OP_yearmonth':int,'OP_firmlikes_total':float,'OP_firmlikes_minusone':float,'OP_firmlikes_minustwo':float,'OP_firmlikes_plusone':float,'OP_firmlikes_change_daylevel':float,'OP_firmlikes_plusseven':float,'OP_firmlikes_minuseight':float,'OP_firmlikes_change_weeklevel':float,'OP_firmlikes_plusthirty':float,'OP_firmlikes_minusthirtyone':float,'OP_firmlikes_change_monthlevel':float,'OP_firmlikes_plushundredeighty':float,'OP_firmlikes_minushundredeightyone':float,'OP_firmlikes_change_sixmonthlevel':float}
        
        #typelist = ["str","str","str","str","str","str","str","str","str","str","str","str","str","str","str","str","float","str","float","float","float","float","float","str","str","str","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","str","float","str","str"]
        #typelist = ["str","str","str","str","str","str","str","str","str","str","str","str","str","str","str","str","float","str","float","float","float","float","float","str","str","str","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","str","float","str","int","float","int","int","int","int","int","int","int","int","int","int","int","float","float","float","float","float","float","float","float","float","float","float","float","float","float","str"]
        typelist = ['str','str','str','str','str','str','str','str','str','str','str','str','str','str','str','str','float','str','float','float','float','float','float','str','str','str','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','str','float','str','int','float','int','int','int','int','int','int','int','int','int','int','int','float','float','float','float','float','float','float','float','float','float','float','float','float','float','str']
        
        #imtypelist = ["str","str","str","str","str","str","str","str","str","str","str","str","str","str","str","str","float","str","float","float","float","float","float","str","str","str","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","str","float","str",'int','float','int','int','int','int','int','int','int','int','int','int','int','int','float','float','float','float','float','float','float','float','float','float','float',"str"]
        #imtypelist = ["str","str","str","str","str","str","str","str","str","str","str","str","str","str","str","str","float","str","float","float","float","float","float","str","str","str","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","str","float","str",'int','float','int','int','int','int','int','int','int','int','int','int','int',"str"]
        #imtypelist = ["str","str","str","str","str","str","str","str","str","str","str","str","str","str","str","str","float","str","float","float","float","float","float","str","str","str","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","float","str","float","str","int","float","int","int","int","int","int","int","int","int","int","int","int","float","float","float","float","float","float","float","float","float","float","float","float","float","float","str"]
        imtypelist = ['str','str','str','str','str','str','str','str','str','str','str','str','str','str','str','str','float','str','float','float','float','float','float','str','str','str','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','float','str','float','str','int','float','int','int','int','int','int','int','int','int','int','int','int','float','float','float','float','float','float','float','float','float','float','float','float','float','float','str']
    
        dropcnt = 0
            #filecount+=1
            #print("starting to process file "+filename_raw+", number "+str(filecount)+" of "+str(totalfiles) +" at "+str(datetime.now()) )
        print("starting to process file "+filename_raw+" at "+str(datetime.now()) )
           
        #for file in directory
        #filename_raw = 'C:/Users/jwulf/workspace/FBDataTrafo/FBDataTrafo/yahoo_facebook_feed_2016-09-21-211651_nolikes_merged_liwc.csv'
        #filename_raw = 'C:/Users/jwulf/workspace/FBDataTrafo/FBDataTrafo/Fiserv_facebook_feed_2016-09-21-211651_nolikes_merged_liwc.csv'
        filename = filename_raw.replace(".\\",'')
        filename_out = filename.replace(".csv",'')
        filelines=open(filename,"rb")
        
        #20180313 name confusion
        #likefilename = filename.split('_nolikes')[0].replace("feed","page_likes")
        likefilename = filename.split('_merged')[0].replace("feed","page_likes")
        firmname = filename.split('_facebook')[0]
    
        #likefilename = filename.split('132557')[0].replace("feed","page_likes")+'132558'
        df_likes = pd.read_csv("./page_likes/"+likefilename+".csv", index_col=False, sep="\t",header='infer', usecols=['date', 'total'], dtype={'date':str, 'total':float})
        df_likes['date_formated'] = df_likes['date'].apply(lambda x: x.split('T')[0].replace("-",""))
        df_likes.sort_values('date_formated', inplace=True)
        df_likes['firmlikes_minusone']=df_likes['total'].shift(1)
        df_likes['firmlikes_minustwo']=df_likes['total'].shift(2)
        df_likes['firmlikes_plusone']=df_likes['total'].shift(-1)
        df_likes['firmlikes_change_daylevel'] = np.where(df_likes['firmlikes_plusone']>0, ((df_likes['firmlikes_plusone']-df_likes['total'])-(df_likes['firmlikes_minusone']-df_likes['firmlikes_minustwo']))/(df_likes['firmlikes_plusone']),0) 
        df_likes['firmlikes_plusseven']=df_likes['total'].shift(-7)
        df_likes['firmlikes_minuseight']=df_likes['total'].shift(8)
        df_likes['firmlikes_change_weeklevel'] = np.where(df_likes['firmlikes_plusseven']>0, ((df_likes['firmlikes_plusseven']-df_likes['total'])-(df_likes['firmlikes_minusone']-df_likes['firmlikes_minuseight']))/(df_likes['firmlikes_plusseven']),0) 
        df_likes['firmlikes_plusthirty']=df_likes['total'].shift(-30)
        df_likes['firmlikes_minusthirtyone']=df_likes['total'].shift(31)
        df_likes['firmlikes_change_monthlevel'] = np.where(df_likes['firmlikes_plusthirty']>0, ((df_likes['firmlikes_plusthirty']-df_likes['total'])-(df_likes['firmlikes_minusone']-df_likes['firmlikes_minusthirtyone']))/(df_likes['firmlikes_plusthirty']),0) 
        df_likes['firmlikes_plushundredeighty']=df_likes['total'].shift(-180)
        df_likes['firmlikes_minushundredeightyone']=df_likes['total'].shift(181)
        df_likes['firmlikes_change_sixmonthlevel'] = np.where(df_likes['firmlikes_plushundredeighty']>0, ((df_likes['firmlikes_plushundredeighty']-df_likes['total'])-(df_likes['firmlikes_minusone']-df_likes['firmlikes_minushundredeightyone']))/(df_likes['firmlikes_plushundredeighty']),0) 
    
    #fout=open("C:/Users/jwulf/workspace/FBDataTrafo/FBDataTrafo/Fiserv_facebook_feed_2016-09-21-211651_nolikes_merged_liwc_features.csv","wb") 
        fout=open("./features_new/"+filename_out+"_features.csv","wb") 
        header=filelines.next()
        columnames_raw=re.split(r'\t',header)
        columnames = columnames_raw[:-1]
        headersize=len(columnames)
        columnames.extend(['OP_UGC','CON_Negativity_OP','OP_Picture','OP_Video','OP_Event','OP_Link','OP_Status','OP_weekend','OP_morning','OP_afternoon','OP_evening','OP_night','OP_yearmonth', 'OP_firmlikes_total', 'OP_firmlikes_minusone' , 'OP_firmlikes_minustwo' , 'OP_firmlikes_plusone' , 'OP_firmlikes_change_daylevel' , 'OP_firmlikes_plusseven' , 'OP_firmlikes_minuseight' , 'OP_firmlikes_change_weeklevel' , 'OP_firmlikes_plusthirty' , 'OP_firmlikes_minusthirtyone' , 'OP_firmlikes_change_monthlevel' , 'OP_firmlikes_plushundredeighty' , 'OP_firmlikes_minushundredeightyone' , 'OP_firmlikes_change_sixmonthlevel','\r\n'])
        header_out = '\t'.join(columnames)
        fout.write(header_out)
        
        #create indexes
        colindex=dict()
        for item in columnames:
            colindex[item]=columnames.index(item)
        
        linecnt=0
        #read in file line by line
        for myline in filelines:
            linecnt +=1
        #for line in file    
            filelinesplit=re.split(r'\t',myline)
            
            #check if language info correct
            try:
                languagecheck = filelinesplit[(headersize-3)]
            except Exception as e:
                print("languagecheck not possible, line is too short :"+str(filelinesplit))
                print(filename) 
                dropcnt+=1
                continue
            check = re.findall('[a-z][a-z]',languagecheck)
            if len(check)==0:
                print("problem occured with formatting (no lang info) in line :"+myline)
                dropcnt+=1
                continue
        
        #cast entries and catch errors if error log and ignore
        
            objcounter=0
            for obj in filelinesplit:
                objtype=typelist[objcounter]
                if objtype=="float":
                    castobj = filelinesplit[objcounter]
                    if castobj !="":
                        try:
                            filelinesplit[objcounter]=float(obj)
                        except Exception as e:
                            print("cast error str to float in line "+str(filelinesplit))
        
                objcounter+=1
            
        
        #produce line-level indicators
        #filelinesplit[]
            OP_UGC = int(1 - filelinesplit[colindex["post_by_pageID"]])
            CON_Negativity_OP= (filelinesplit[colindex["posemo"]]-filelinesplit[colindex["negemo"]])/100 + 1
            
            #vividness
            OP_Picture = int(filelinesplit[colindex["status_type"]] == "photo")
            OP_Video = int(filelinesplit[colindex["status_type"]] == "video")
            OP_Event = int(filelinesplit[colindex["status_type"]] == "event")
            OP_Link = int(filelinesplit[colindex["status_type"]] == "link")
            OP_Status = int(filelinesplit[colindex["status_type"]] == "status")
            
                     
            try:
                date = datetime.fromtimestamp(filelinesplit[colindex['status_published_unix_UTC_Zurich']],pytz.timezone("US/Central"))
            except Exception as e:
                try:
                    date = datetime.fromtimestamp(filelinesplit[colindex['status_updated_unix_UTC_Zurich']],pytz.timezone("US/Central"))
                except Exception as e:
                    print("dateconversion error:")
                    print(filelinesplit[colindex['status_published_unix_UTC_Zurich']])
                    print(filelinesplit[colindex['post_id']])
                print("dateconversion error:")
                print(filelinesplit[colindex['status_published_unix_UTC_Zurich']])
                print(filelinesplit[colindex['post_id']])
                
            
            OP_weekend = int(date.weekday() > 4)
            hourofday = date.hour
            OP_morning =int((hourofday < 12) & (hourofday > 5))
            OP_afternoon = int((hourofday < 18) & (hourofday > 11))
            OP_evening =int(hourofday > 17)
            OP_night = int(hourofday < 6)
            
            OP_year = str(date.year)
            OP_month = str(date.month).zfill(2)
            OP_yearmonth=OP_year+OP_month
            
            OP_day = str(date.day).zfill(2)
            OP_yearmonthday=OP_year+OP_month+OP_day
            likes_series = df_likes[df_likes['date_formated']==OP_yearmonthday]
            if len(likes_series)>0:
                OP_firmlikes_total = float(likes_series['total'])
                OP_firmlikes_minusone = float(likes_series['firmlikes_minusone'])
                OP_firmlikes_minustwo = float(likes_series['firmlikes_minustwo'])
                OP_firmlikes_plusone =  float(likes_series['firmlikes_plusone'])
                OP_firmlikes_change_daylevel =  float(likes_series['firmlikes_change_daylevel'])
                OP_firmlikes_plusseven =  float(likes_series['firmlikes_plusseven'])
                OP_firmlikes_minuseight =  float(likes_series['firmlikes_minuseight'])
                OP_firmlikes_change_weeklevel =  float(likes_series['firmlikes_change_weeklevel'])
                OP_firmlikes_plusthirty =  float(likes_series['firmlikes_plusthirty'])
                OP_firmlikes_minusthirtyone =  float(likes_series['firmlikes_minusthirtyone'])
                OP_firmlikes_change_monthlevel =  float(likes_series['firmlikes_change_monthlevel'])
                OP_firmlikes_plushundredeighty =  float(likes_series['firmlikes_plushundredeighty'])
                OP_firmlikes_minushundredeightyone =  float(likes_series['firmlikes_minushundredeightyone'])
                OP_firmlikes_change_sixmonthlevel =  float(likes_series['firmlikes_change_sixmonthlevel'])
            else:
                OP_firmlikes_total = None
                OP_firmlikes_minusone = None
                OP_firmlikes_minustwo = None
                OP_firmlikes_plusone =  None
                OP_firmlikes_change_daylevel =  None
                OP_firmlikes_plusseven =  None
                OP_firmlikes_minuseight =  None
                OP_firmlikes_change_weeklevel =  None
                OP_firmlikes_plusthirty =  None
                OP_firmlikes_minusthirtyone =  None
                OP_firmlikes_change_monthlevel =  None
                OP_firmlikes_plushundredeighty =  None
                OP_firmlikes_minushundredeightyone =  None
                OP_firmlikes_change_sixmonthlevel =  None
        #writeeverything                
            lineoutlist = filelinesplit[:-1]
            lineoutlist.extend([OP_UGC,CON_Negativity_OP,OP_Picture,OP_Video,OP_Event,OP_Link,OP_Status,OP_weekend,OP_morning,OP_afternoon,OP_evening,OP_night,OP_yearmonth,OP_firmlikes_total,OP_firmlikes_minusone , OP_firmlikes_minustwo , OP_firmlikes_plusone , OP_firmlikes_change_daylevel , OP_firmlikes_plusseven , OP_firmlikes_minuseight , OP_firmlikes_change_weeklevel , OP_firmlikes_plusthirty , OP_firmlikes_minusthirtyone , OP_firmlikes_change_monthlevel , OP_firmlikes_plushundredeighty , OP_firmlikes_minushundredeightyone , OP_firmlikes_change_sixmonthlevel,'\r\n'])
            lineoutlist_str=[str(i) for i in lineoutlist]
            line_out = '\t'.join(lineoutlist_str)
            
            #20180313 write out ONLY if in our timeframe
            if (int(OP_yearmonth) > 201109) and (int(OP_yearmonth) < 201602):
                fout.write(line_out)
            del lineoutlist
            del lineoutlist_str
            del line_out
        fout.close()
        if linecnt==0: #check if we have data otherwise delete file and jump to the next right away
            print("empty file: "+filename)
            os.remove("./features_new/"+filename_out+"_features.csv")
            return 0
        # #write to efficient storage format
    ##############################################################################################################
    ###########do the line level dask mean computations
    ################################################################################################################33
    
        # #read in resultfile into daks dataframe subset by dic, language and UGC
    
        #analysiscolumns = ['type','type_details','post_by_pageID','language','WC','WPS','Sixltr','Dic','Numerals','function','pronoun','ppron','i','we','you','shehe','they','ipron','article','prep','auxverb','adverb','conj','negate','verb','adj','compare','interrog','number','quant','affect','posemo','negemo','anx','anger','sad','social','family','friend','female','male','cogproc','insight','cause','discrep','tentat','certain','differ','percept','see','hear','feel','bio','body','health','sexual','ingest','drives','affiliation','achiev','power','reward','risk','focuspast','focuspresent','focusfuture','relativ','motion','space','time','work','leisure','home','money','relig','death','informal','swear','netspeak','assent','nonflu','filler','new_apology','new_resp_mod','new_modification','Period','Comma','Colon','SemiC','QMark','Exclam','Dash','Quote','Apostro','Parenth','OtherP','AllPct','OP_yearmonth','status_published_unix_UTC_Zurich']
        analysiscolumns = ['type','type_details','post_by_pageID','language','WC','WPS','Sixltr','Dic','Numerals','function','pronoun','you','article','prep','negate','verb','adj','affect','posemo','negemo','cogproc','netspeak','new_apology','new_resp_mod','new_modification','OP_yearmonth','status_published_unix_UTC_Zurich']
    
        #df_raw = dd.read_csv("./features_new/"+filename_out+"_features.csv",sep="\t",low_memory=False,quoting=3, header='infer')
    
        #df_raw = pd.read_csv('Fiserv_facebook_feed_2016-09-21-211651_nolikes_merged_liwc_features.csv',sep="\t",low_memory=False,quoting=3, header='infer', usecols=analysiscolumns)
        df_raw = pd.read_csv("./features_new/"+filename_out+"_features.csv",sep="\t",low_memory=False,quoting=3, header='infer', dtype=dtypes, usecols=analysiscolumns)
        #df_raw = dd.read_csv('Fiserv_facebook_feed_2016-09-21-211651_nolikes_merged_liwc_features.csv',sep="\t",low_memory=False,quoting=3, header='infer', usecols=analysiscolumns)
      
        #print("starting to calculate dask means of "+filename_raw+", number "+str(filecount)+" of "+str(totalfiles)+ " at "+str(datetime.now()))
        print("starting to calculate means of "+filename_raw+" at "+str(datetime.now()))
        
        #change this configruation to only looking at the focus posts and comments
        if languageset == "de":   
            df = df_raw[((df_raw['language'] == "en") | (df_raw['language']  ==  "de")) & (df_raw['Dic'] > 0) & (df_raw['post_by_pageID'] < 1) & ((df_raw['type_details'] == 'comment')  | (df_raw['type_details'] == 'ORIGINAL_POST'))]
        else:
            df = df_raw[(df_raw['language'] == "en") & (df_raw['post_by_pageID'] < 1) & (df_raw['Dic'] > 0) & ((df_raw['type_details'] == 'comment') | (df_raw['type_details'] == 'ORIGINAL_POST'))]
            
        
        df.reindex() #necessary if working with pandas
        
        noposts = True
        #stop the whole process if we have no qualified UGC posts or comments
        if len(df)>0:
            indicator_dic = dict()
            indicator_dic["posemo_meanOP"]=float(df['posemo'].mean())
            indicator_dic["negemo_meanOP"]=float(df['negemo'].mean())
            indicator_dic["you_meanOP"]=float(df['you'].mean())
            indicator_dic["cogproc_meanOP"]=float(df['cogproc'].mean())
            
            indicator_dic["article_meanOP"]=float(df['article'].mean())
            indicator_dic["prep_meanOP"]=float(df['prep'].mean())
            indicator_dic["negate_meanOP"]=float(df['negate'].mean())
            indicator_dic["netspeak_meanOP"]=float(df['netspeak'].mean())
            indicator_dic["pronoun_meanOP"]=float(df['pronoun'].mean())
            
            #OP_mintime = float(df[df['status_published_unix_UTC_Zurich']>0]['status_published_unix_UTC_Zurich'].min())
            OP_mintime = float(df[df['status_published_unix_UTC_Zurich']>0]['status_published_unix_UTC_Zurich'].min())
            
            if OP_mintime > 0:
                OP_mindate = datetime.fromtimestamp(OP_mintime,pytz.timezone("US/Central"))
                OP_minyear = OP_mindate.year
                OP_minmonth = OP_mindate.month
                firstdate = str(OP_minyear)+str(OP_minmonth).zfill(2)
            else:
                OP_mindate = None
                OP_minyear = None
                OP_minmonth = None
                firstdate = None
                
            #preparation for moving avg computations (test with month = 3)
            #TODO hier gleich shiften
            #http://stackoverflow.com/questions/13030245/how-to-shift-a-pandas-multiindex-series
            #http://stackoverflow.com/questions/21960446/pandas-rolling-sum-with-center-and-min-periods
            #https://pandas-docs.github.io/pandas-docs-travis/generated/pandas.Series.shift.html
            series_article_sum_monthly=df.groupby('OP_yearmonth')['article'].sum()
            series_article_count_monthly=df.groupby('OP_yearmonth')['article'].count()
            series_article_3month_rollingmean = (series_article_sum_monthly.rolling(window=3, min_periods=0).sum()/series_article_count_monthly.rolling(window=3, min_periods=0).sum())
            firstvalue_article = series_article_3month_rollingmean[firstdate]
            series_article_3month_rollingmean_shifted = series_article_3month_rollingmean.shift(1)
            #this is a workaround to avoid empty values for the first month
            series_article_3month_rollingmean_shifted[firstdate]=firstvalue_article
            indicator_dic["series_article_3month_rollingmean_shifted"] = series_article_3month_rollingmean_shifted
           
            series_prep_sum_monthly=df.groupby('OP_yearmonth')['prep'].sum()
            series_prep_count_monthly=df.groupby('OP_yearmonth')['prep'].count()
            series_prep_3month_rollingmean = (series_prep_sum_monthly.rolling(window=3, min_periods=0).sum()/series_prep_count_monthly.rolling(window=3, min_periods=0).sum())
            firstvalue_prep = series_prep_3month_rollingmean[firstdate]
            series_prep_3month_rollingmean_shifted = series_prep_3month_rollingmean.shift(1)
            #this is a workaround to avoid empty values for the first month
            series_prep_3month_rollingmean_shifted[firstdate]=firstvalue_prep
            indicator_dic["series_prep_3month_rollingmean_shifted"] = series_prep_3month_rollingmean_shifted
            
            series_negate_sum_monthly=df.groupby('OP_yearmonth')['negate'].sum()
            series_negate_count_monthly=df.groupby('OP_yearmonth')['negate'].count()
            series_negate_3month_rollingmean = (series_negate_sum_monthly.rolling(window=3, min_periods=0).sum()/series_negate_count_monthly.rolling(window=3, min_periods=0).sum())
            firstvalue_negate = series_negate_3month_rollingmean[firstdate]
            series_negate_3month_rollingmean_shifted = series_negate_3month_rollingmean.shift(1)
            #this is a workaround to avoid empty values for the first month
            series_negate_3month_rollingmean_shifted[firstdate]=firstvalue_negate
            indicator_dic["series_negate_3month_rollingmean_shifted"] = series_negate_3month_rollingmean_shifted
            
            series_netspeak_sum_monthly=df.groupby('OP_yearmonth')['netspeak'].sum()
            series_netspeak_count_monthly=df.groupby('OP_yearmonth')['netspeak'].count()
            series_netspeak_3month_rollingmean = (series_netspeak_sum_monthly.rolling(window=3, min_periods=0).sum()/series_netspeak_count_monthly.rolling(window=3, min_periods=0).sum())
            firstvalue_netspeak = series_netspeak_3month_rollingmean[firstdate]
            series_netspeak_3month_rollingmean_shifted = series_netspeak_3month_rollingmean.shift(1)
            #this is a workaround to avoid empty values for the first month
            series_netspeak_3month_rollingmean_shifted[firstdate]=firstvalue_netspeak
            indicator_dic["series_netspeak_3month_rollingmean_shifted"] = series_netspeak_3month_rollingmean_shifted  
            
            series_pronoun_sum_monthly=df.groupby('OP_yearmonth')['pronoun'].sum()
            series_pronoun_count_monthly=df.groupby('OP_yearmonth')['pronoun'].count()
            series_pronoun_3month_rollingmean = (series_pronoun_sum_monthly.rolling(window=3, min_periods=0).sum()/series_pronoun_count_monthly.rolling(window=3, min_periods=0).sum())
            firstvalue_pronoun = series_pronoun_3month_rollingmean[firstdate]
            series_pronoun_3month_rollingmean_shifted = series_pronoun_3month_rollingmean.shift(1)
            #this is a workaround to avoid empty values for the first month
            series_pronoun_3month_rollingmean_shifted[firstdate]=firstvalue_pronoun
            indicator_dic["series_pronoun_3month_rollingmean_shifted"] = series_pronoun_3month_rollingmean_shifted
    
            if languageset == "de":
                df_allUGC = df_raw[((df_raw['language'] == "en") | (df_raw['language'] == "de")) & (df_raw['post_by_pageID'] < 1)]
            else:
                df_allUGC = df_raw[(df_raw['language'] == "en") & (df_raw['post_by_pageID'] < 1)]
            indicator_dic["posemo_mean_all"]=float(df_allUGC['posemo'].mean())
            indicator_dic["negemo_mean_all"]=float(df_allUGC['negemo'].mean())
            indicator_dic["you_mean_all"]=float(df_allUGC['you'].mean())
            indicator_dic["cogproc_mean_all"]=float(df_allUGC['cogproc'].mean())
            indicator_dic["article_mean_all"]=float(df_allUGC['article'].mean())
            indicator_dic["prep_mean_all"]=float(df_allUGC['prep'].mean())
            indicator_dic["negate_mean_all"]=float(df_allUGC['negate'].mean())
            indicator_dic["netspeak_mean_all"]=float(df_allUGC['netspeak'].mean())
            indicator_dic["pronoun_mean_all"]=float(df_allUGC['pronoun'].mean())
    
            del df_raw
            del df 
            del df_allUGC
            noposts = False
        else:
            print("WARNING: no qualififed posts found in file %s" % (filename_out))
            os.remove("./features_new/"+filename_out+"_features.csv")
            del df_raw
            del df 
    
    
        
    ########################################################################################################################
    ##Post Level Aggregation
    #########################################################################################################################
    
        if not noposts:
            #open csv to save post-level results
            imfout=open("./features_immediate/"+filename_out+"_features.csv","wb")
            imfilelines=open("./features_new/"+filename_out+"_features.csv","rb")
            #print("starting to post_level aggregate "+filename_raw+", number "+str(filecount)+" of "+str(totalfiles)+" at "+str(datetime.now()))
            print("starting to post_level aggregate "+filename_raw+" at "+str(datetime.now()))
            

            
            imheader=imfilelines.next()
            global imcolumnames_raw
            imcolumnames_raw=re.split(r'\t',imheader)
            imcolumnames = imcolumnames_raw[:-1]
            imheadersize=len(imcolumnames)
            #new_features_head = ['TOTAL_likes_sub','TOTAL_comments_sub','TOTAL_shares_sub','TOTAL_likes','TOTAL_comments','TOTAL_shares','FGC_comments','UGC_comments','T_Duration','Post_UGC_Affect_sum','Post_UGC_Affect_mean','Post_UGC_negemo_sum','Post_UGC_negemo_mean','Post_UGC_posemo_sum','Post_UGC_posemo_mean','UGC_Cogproc_mean','OP_sum_FGC','IS_SituationSelection','OP_Modification','IS_SituationModification','OP_apology','IS_apology','IS_SituationModificationApology','OP_AttentionalDeployment_sum','OP_CognitiveChange_sum','OP_AttentionalDeployment_mean','OP_CognitiveChange_mean','OP_AttentionalDeployment_mean_diff','OP_CognitiveChange_mean_diff','OP_response_modulation','IS_response_modulation','OP_Posemo_Adjusted','OP_Negemo_Adjusted','CON_Negativity_OP','CON_Negativity_Adjusted','CON_LSM_OP','CON_LSM_OP_Netspeak','OP_Directedness_Adjusted','OP_Causality_Adjusted','OP_FGC_posemo_mean_diff_mean','OP_FGC_posemo_mean_diff_abs_mean','OP_FGC_negemo_mean_diff_mean','OP_FGC_negemo_mean_diff_abs_mean','OP_FGC_cogproc_mean_diff_mean','OP_FGC_cogproc_mean_diff_abs_mean', 'OP_FGC_posemo_mean3_diff_mean','OP_FGC_posemo_mean3_diff_abs_mean','OP_FGC_negemo_mean3_diff_mean','OP_FGC_negemo_mean3_diff_abs_mean','OP_FGC_cogproc_mean3_diff_mean','OP_FGC_cogproc_mean3_diff_abs_mean','T_Reaction','CON_LSM_OP_3Roll','CON_LSM_OP_Netspeak_3Roll']
            #new_features_head = ['TOTAL_likes_sub' , 'TOTAL_comments_sub' , 'TOTAL_shares_sub' , 'TOTAL_likes' , 'TOTAL_comments' , 'TOTAL_shares' , 'FGC_comments' , 'FGC_comments_likes_sum', 'FGC_comments_likes_mean', 'UGC_comments' , 'UserOriginalPost_comments', 'T_Duration' , 'Post_UGC_Affect_sum' , 'Post_UGC_Affect_mean' , 'Post_UGC_negemo_sum' , 'Post_UGC_negemo_mean' , 'Post_UGC_posemo_sum' , 'Post_UGC_posemo_mean' , 'UGC_Cogproc_mean' , 'OP_sum_FGC' , 'IS_SituationSelection' , 'OP_Modification' , 'IS_SituationModification' , 'OP_apology' , 'IS_apology' , 'IS_SituationModificationApology' , 'OP_AttentionalDeployment_sum' , 'OP_CognitiveChange_sum' , 'OP_AttentionalDeployment_mean' , 'OP_CognitiveChange_mean' , 'OP_AttentionalDeployment_mean_diff' , 'OP_CognitiveChange_mean_diff' , 'OP_response_modulation' , 'IS_response_modulation' , 'OP_Posemo_Adjusted' , 'OP_Negemo_Adjusted' , 'CON_Negativity_OP' , 'CON_Negativity_Adjusted' , 'CON_LSM_OP' , 'CON_LSM_OP_Netspeak' , 'OP_Directedness_Adjusted' , 'OP_Causality_Adjusted' , 'OP_FGC_posemo_mean_diff_mean' , 'OP_FGC_posemo_mean_diff_abs_mean' , 'OP_FGC_negemo_mean_diff_mean' , 'OP_FGC_negemo_mean_diff_abs_mean' , 'OP_FGC_cogproc_mean_diff_mean' , 'OP_FGC_cogproc_mean_diff_abs_mean' , ' OP_FGC_posemo_mean3_diff_mean' , 'OP_FGC_posemo_mean3_diff_abs_mean' , 'OP_FGC_negemo_mean3_diff_mean' , 'OP_FGC_negemo_mean3_diff_abs_mean' , 'OP_FGC_cogproc_mean3_diff_mean' , 'OP_FGC_cogproc_mean3_diff_abs_mean' , 'T_Reaction' , 'CON_LSM_OP_3Roll' , 'CON_LSM_OP_Netspeak_3Roll  ' , 'OP_Posemo_Adjusted_all' , 'OP_Negemo_Adjusted_all' , 'CON_Negativity_Adjusted_all' , 'CON_LSM_OP_all' , 'CON_LSM_OP_Netspeak_all' , 'OP_Directedness_Adjusted_all' , 'OP_Causality_Adjusted_all' , 'user_posemo_sum', 'user_posemo_mean' , 'user_negemo_sum' , 'user_negemo_mean', 'firmtext1','firmtext2']
            new_features_head = ['user_posemo_sum','user_posemo_mean','user_negemo_sum','user_negemo_mean','TOTAL_likes_sub','TOTAL_comments_sub','TOTAL_shares_sub','TOTAL_likes','TOTAL_comments','TOTAL_shares','FGC_comments','FGC_comments_likes_sum','FGC_comments_likes_mean','UGC_comments','UserOriginalPost_comments','T_Duration','Post_UGC_Affect_sum','Post_UGC_Affect_mean','Post_UGC_negemo_sum','Post_UGC_negemo_mean','Post_UGC_posemo_sum','Post_UGC_posemo_mean','UGC_Cogproc_mean','OP_sum_FGC','IS_SituationSelection','OP_Modification_sum','IS_SituationModification_sum','OP_apology_sum','IS_apology_sum','IS_SituationModificationApology_sum','OP_AttentionalDeployment_sum','IS_AttentionalDeployment_sum','OP_CognitiveChange_sum','IS_CognitiveChange_sum','OP_insight_sum','OP_cause_sum','OP_AttentionalDeployment_mean','OP_CognitiveChange_mean','OP_AttentionalDeployment_mean_diff','OP_CognitiveChange_mean_diff','OP_response_modulation_sum','IS_response_modulation_sum','OP_Modification_first','IS_SituationModification_first','OP_apology_first','IS_apology_first','IS_SituationModificationApology_first','OP_AttentionalDeployment_first','IS_AttentionalDeployment_first','OP_CognitiveChange_first','IS_CognitiveChange_first','OP_insight_first','OP_cause_first','OP_response_modulation_first','IS_response_modulation_first', 'firm_reaction_time_2nd','OP_Modification_allbutfirst','IS_SituationModification_allbutfirst','OP_apology_allbutfirst','IS_apology_allbutfirst','IS_SituationModificationApology_allbutfirst','OP_AttentionalDeployment_allbutfirst','IS_AttentionalDeployment_allbutfirst','OP_CognitiveChange_allbutfirst','IS_CognitiveChange_allbutfirst','OP_insight_allbutfirst','OP_cause_allbutfirst','OP_response_modulation_allbutfirst','IS_response_modulation_allbutfirst','OP_Posemo_Adjusted','OP_Negemo_Adjusted','CON_Negativity_OP','CON_Negativity_Adjusted','CON_LSM_OP','CON_LSM_OP_Netspeak','OP_Directedness_Adjusted','OP_Causality_Adjusted','OP_FGC_posemo_mean_diff_mean','OP_FGC_posemo_mean_diff_abs_mean','OP_FGC_negemo_mean_diff_mean','OP_FGC_negemo_mean_diff_abs_mean','OP_FGC_cogproc_mean_diff_mean','OP_FGC_cogproc_mean_diff_abs_mean',' OP_FGC_posemo_mean3_diff_mean','OP_FGC_posemo_mean3_diff_abs_mean','OP_FGC_negemo_mean3_diff_mean','OP_FGC_negemo_mean3_diff_abs_mean','OP_FGC_cogproc_mean3_diff_mean','OP_FGC_cogproc_mean3_diff_abs_mean','T_Reaction','CON_LSM_OP_3Roll','CON_LSM_OP_Netspeak_3Roll  ','OP_Posemo_Adjusted_all','OP_Negemo_Adjusted_all','CON_Negativity_Adjusted_all','CON_LSM_OP_all','CON_LSM_OP_Netspeak_all','OP_Directedness_Adjusted_all','OP_Causality_Adjusted_all','firmtext1','firmtext2','firmtext3','firmtext4','firmtext5']

            imcolumnames.extend(new_features_head)   
            imcolumnames.append('\r\n') 
            imheader_out = '\t'.join(imcolumnames)
            imfout.write(imheader_out)
        # #read in resultfile line by line
            startflag=True
            currentpostid=""
        #     postdfheaders = imcolumnames
        #     #postinidf=pd.DataFrame(columns=postdfheaders)
        #     postinidf=pd.DataFrame()
        #     postdf=postinidf
            rowcollector=[]
            for immyline in imfilelines:  
                imfilelinesplit=re.split(r'\t',immyline)
                 
                #casting
                    #cast entries and catch errors if error log and ignore
                imobjcounter=0
                diccollector=OrderedDict()
                for imobj in imfilelinesplit:
                    imobjtype=imtypelist[imobjcounter]
                    if imobjtype=="float":
                        imcastobj = imfilelinesplit[imobjcounter]
                        if imcastobj != '':
                            try:
                                imfilelinesplit[imobjcounter]=float(imobj)
                            except Exception as e:
                                print("2n round cast error str to float in line "+str(imfilelinesplit))
                    elif imobjtype=="int":
                        imcastobj = imfilelinesplit[imobjcounter]
                        if imcastobj != '':
                            try:
                                imfilelinesplit[imobjcounter]=int(imobj)
                            except Exception as e:
                                print("2nd round cast error str to float in line "+str(imfilelinesplit))
             
                    imobjcounter+=1
                #create dict out of imfilelinesplit
        #generate features
        
                    
            #return resultsdf
        
        
        # #write into dataframe until new post is reached        
                
                #check if post_id is new (what to do with last line??)
                if not startflag and imfilelinesplit[colindex["original_post_id"]]!=currentpostid:
                    #if so, hand over the collector to the analysis
                    #getpostfeatures(dataframe(rowcollector))
                    turninframe = pd.DataFrame(rowcollector)
                    getallpostfeatures(turninframe,imfout,indicator_dic)
                    del turninframe
                    #clear collector
                    del rowcollector[:]
                    #del diccollector
                    #diccollector=OrderedDict()
                    currentpostid = imfilelinesplit[colindex["original_post_id"]]
         
                #add line to the collector
                
                thingicnt=0
                for thingi in imfilelinesplit[:-1]:
                    thingikey=imcolumnames[thingicnt]
                    #if thingikey in ['post_id','original_post_id','referring_post_or_comment_id',"post_by_pageID",'user_id',"Dic","language",'num_likes','num_comments','num_shares','OP_UGC',"status_published_unix_UTC_Zurich",'affect','negemo','posemo','cogproc','new_modification','new_apology','new_resp_mod','prep','pronoun','negate','article','netspeak','you']:
                    diccollector.update({thingikey:thingi})
                    thingicnt+=1
                rowcollector.append(diccollector)
                del diccollector
                diccollector=OrderedDict()
                if startflag:
                    currentpostid = imfilelinesplit[colindex["original_post_id"]]
                    startflag=False
            #datetime.now() dumpt the last lines
            getallpostfeatures(pd.DataFrame(rowcollector),imfout,indicator_dic)
            del rowcollector[:]
            del diccollector
            
            del imfilelinesplit
            del immyline
            imfout.close()
            imfilelines.close()
            os.remove("./features_new/"+filename_out+"_features.csv")
        
        #################################################################################################################
        #######do the post level dask aggregations
        ##################################################################################################################
            #print("starting to post_level dask calculations "+filename_raw+", number "+str(filecount)+" of "+str(totalfiles)+" at "+str(datetime.now()))
            print("starting to post_level calculations "+filename_raw+" at "+str(datetime.now()))
    
    #             final_dtypes = {"pageID":str , "page_name":str , "scrape_time":str , "type":str , "type_details":str , "original_post_id":str , "post_id":str , "pre_post_id":str , "post_id_summary":str , "from":str , "user_id":str , "status_message":str , "link_name":str , "status_type":str , "link":str , "status_published_UTC_Zurich":str , "status_published_unix_UTC_Zurich":str , "status_updated_UTC_Zurich":str , "status_updated_unix_UTC_Zurich":str , "num_likes":str , "num_comments":str , "num_shares":str , "post_by_pageID":str , "original_post_time_UTC_Zurich":str , "original_post_time_unix_UTC_Zurich":str , "referring_post_or_comment_id":str , "WC":str , "WPS":str , "Sixltr":str , "Dic":str , "Numerals":str , "function":str , "pronoun":str , "ppron":str , "i":str , "we":str , "you":str , "shehe":str , "they":str , "ipron":str , "article":str , "prep":str , "auxverb":str , "adverb":str , "conj":str , "negate":str , "verb":str , "adj":str , "compare":str , "interrog":str , "number":str , "quant":str , "affect":str , "posemo":float , "negemo":float , "anx":str , "anger":str , "sad":str , "social":str , "family":str , "friend":str , "female":str , "male":str , "cogproc":str , "insight":str , "cause":str , "discrep":str , "tentat":str , "certain":str , "differ":str , "percept":str , "see":str , "hear":str , "feel":str , "bio":str , "body":str , "health":str , "sexual":str , "ingest":str , "drives":str , "affiliation":str , "achiev":str , "power":str , "reward":str , "risk":str , "focuspast":str , "focuspresent":str , "focusfuture":str , "relativ":str , "motion":str , "space":str , "time":str , "work":str , "leisure":str , "home":str , "money":str , "relig":str , "death":str , "informal":str , "swear":str , "netspeak":str , "assent":str , "nonflu":str , "filler":str , "new_apology":str , "new_resp_mod":str , "new_modification":str , "Period":str , "Comma":str , "Colon":str , "SemiC":str , "QMark":str , "Exclam":str , "Dash":str , "Quote":str , "Apostro":str , "Parenth":str , "OtherP":str , "AllPct":str , "language":str , "lang_prob":str , "has_mail":str , OP_UGC:str , "CON_Negativity_OP":str , "OP_Picture":str , "OP_Video":str , "OP_Event":str , "OP_Link":str , "OP_Status":str , "OP_weekend":str , "OP_morning":str , "OP_afternoon":str , "OP_evening":str , "OP_night":str , 'OP_year':float , 'OP_month':float , "TOTAL_likes_sub":str , "TOTAL_comments_sub":str , "TOTAL_shares_sub":str , "TOTAL_likes":float , "TOTAL_comments":float , "TOTAL_shares":float , "FGC_comments":float , "FGC_comments_likes_sum":float, "FGC_comments_likes_mean":float, "UGC_comments":float , "UserOriginalPost_comments":float, "T_Duration":float , "Post_UGC_Affect_sum":str , "Post_UGC_Affect_mean":float , "Post_UGC_negemo_sum":str , "Post_UGC_negemo_mean":float , "Post_UGC_posemo_sum":str , "Post_UGC_posemo_mean":str , "UGC_Cogproc_mean":str , "OP_sum_FGC":str , "IS_SituationSelection":str , "OP_Modification":str , "IS_SituationModification":str , "OP_apology":str , "IS_apology":str , "IS_SituationModificationApology":str , "OP_AttentionalDeployment_sum":str , "OP_CognitiveChange_sum":str , "OP_AttentionalDeployment_mean":str , "OP_CognitiveChange_mean":str , "OP_AttentionalDeployment_mean_diff":str , "OP_CognitiveChange_mean_diff":str , "OP_response_modulation":str , "IS_response_modulation":str , "OP_Posemo_Adjusted":str , "OP_Negemo_Adjusted":str , "CON_Negativity_OP":str , "CON_Negativity_Adjusted":float , "CON_LSM_OP":float , "CON_LSM_OP_Netspeak":float , "OP_Directedness_Adjusted":float , "OP_Causality_Adjusted":float , 'OP_FGC_posemo_mean_diff_mean':float , 'OP_FGC_posemo_mean_diff_abs_mean':float , 'OP_FGC_negemo_mean_diff_mean':float , 'OP_FGC_negemo_mean_diff_abs_mean':float , 'OP_FGC_cogproc_mean_diff_mean':float , 'OP_FGC_cogproc_mean_diff_abs_mean':float , 'OP_FGC_posemo_mean3_diff_mean':float , 'OP_FGC_posemo_mean3_diff_abs_mean':float , 'OP_FGC_negemo_mean3_diff_mean':float , 'OP_FGC_negemo_mean3_diff_abs_mean':float , 'OP_FGC_cogproc_mean3_diff_mean':float , 'OP_FGC_cogproc_mean3_diff_abs_mean':float , 'T_Reaction':float , 'CON_LSM_OP_3Roll':float , 'CON_LSM_OP_Netspeak_3Roll':float , 'OP_Posemo_Adjusted_all':float , 'OP_Negemo_Adjusted_all':float , 'CON_Negativity_Adjusted_all':float , 'CON_LSM_OP_all':float , 'CON_LSM_OP_Netspeak_all':float , 'OP_Directedness_Adjusted_all':float , 'OP_Causality_Adjusted_all':float, 'posemo_before':float, 'negemo_before':float, 'posemo_3meanback':float, 'posemo_3meanforward':float, 'posemo_3meandiff':float, 'negemo_3meanback':float, 'negemo_3meanforward':float, 'negemo_3meandiff':float, 'posemo-negemo_3meandiff':float, 'user_posemo_sum':float, 'user_posemo_mean':float, 'user_negemo_sum':float, 'user_negemo_mean':float, 'user_negemo_mean':float , 'OP_firmlikes_total':float , 'OP_firmlikes_minusone':float , 'OP_firmlikes_minustwo':float , 'OP_firmlikes_plusone':float , 'OP_firmlikes_change_daylevel':float , 'OP_firmlikes_plusseven':float , 'OP_firmlikes_minuseight':float , 'OP_firmlikes_change_weeklevel':float , 'OP_firmlikes_plusthirty':float , 'OP_firmlikes_minusthirtyone':float , 'OP_firmlikes_change_monthlevel':float , 'OP_firmlikes_plushundredeighty':float , 'OP_firmlikes_minushundredeightyone':float , 'OP_firmlikes_change_sixmonthlevel':float}
    #             df_final = dd.read_csv("./features_immediate/"+filename_out+"_features.csv",sep="\t",low_memory=False,quoting=3, header='infer', dtype=final_dtypes, na_values=['None','nan'])
    #              
            #final_dtypes = {"pageID":str , "page_name":str , "scrape_time":str , "type":str , "type_details":str , "original_post_id":str , "post_id":str , "pre_post_id":str , "post_id_summary":str , "from":str , "user_id":str , "status_message":object , "link_name":str , "status_type":str , "link":str , "status_published_UTC_Zurich":str , "status_published_unix_UTC_Zurich":str , "status_updated_UTC_Zurich":str , "status_updated_unix_UTC_Zurich":str , "num_likes":str , "num_comments":str , "num_shares":str , "post_by_pageID":str , "original_post_time_UTC_Zurich":str , "original_post_time_unix_UTC_Zurich":str , "referring_post_or_comment_id":str , "WC":str , "WPS":str , "Sixltr":str , "Dic":str , "Numerals":str , "function":str , "pronoun":str , "ppron":str , "i":str , "we":str , "you":str , "shehe":str , "they":str , "ipron":str , "article":str , "prep":str , "auxverb":str , "adverb":str , "conj":str , "negate":str , "verb":str , "adj":str , "compare":str , "interrog":str , "number":str , "quant":str , "affect":str , "posemo":float , "negemo":float , "anx":str , "anger":str , "sad":str , "social":str , "family":str , "friend":str , "female":str , "male":str , "cogproc":str , "insight":str , "cause":str , "discrep":str , "tentat":str , "certain":str , "differ":str , "percept":str , "see":str , "hear":str , "feel":str , "bio":str , "body":str , "health":str , "sexual":str , "ingest":str , "drives":str , "affiliation":str , "achiev":str , "power":str , "reward":str , "risk":str , "focuspast":str , "focuspresent":str , "focusfuture":str , "relativ":str , "motion":str , "space":str , "time":str , "work":str , "leisure":str , "home":str , "money":str , "relig":str , "death":str , "informal":str , "swear":str , "netspeak":str , "assent":str , "nonflu":str , "filler":str , "new_apology":str , "new_resp_mod":str , "new_modification":str , "Period":str , "Comma":str , "Colon":str , "SemiC":str , "QMark":str , "Exclam":str , "Dash":str , "Quote":str , "Apostro":str , "Parenth":str , "OtherP":str , "AllPct":str , "language":str , "lang_prob":str , "has_mail":str , OP_UGC:str , "CON_Negativity_OP":str , "OP_Picture":str , "OP_Video":str , "OP_Event":str , "OP_Link":str , "OP_Status":str , "OP_weekend":str , "OP_morning":str , "OP_afternoon":str , "OP_evening":str , "OP_night":str , 'OP_year':float , 'OP_month':float , "TOTAL_likes_sub":str , "TOTAL_comments_sub":str , "TOTAL_shares_sub":str , "TOTAL_likes":float , "TOTAL_comments":float , "TOTAL_shares":float , "FGC_comments":float , "FGC_comments_likes_sum":float, "FGC_comments_likes_mean":float, "UGC_comments":float , "UserOriginalPost_comments":float, "T_Duration":float , "Post_UGC_Affect_sum":str , "Post_UGC_Affect_mean":float , "Post_UGC_negemo_sum":str , "Post_UGC_negemo_mean":float , "Post_UGC_posemo_sum":str , "Post_UGC_posemo_mean":str , "UGC_Cogproc_mean":str , "OP_sum_FGC":str , "IS_SituationSelection":str , "OP_Modification":str , "IS_SituationModification":str , "OP_apology":str , "IS_apology":str , "IS_SituationModificationApology":str , "OP_AttentionalDeployment_sum":str , "OP_CognitiveChange_sum":str , "OP_AttentionalDeployment_mean":str , "OP_CognitiveChange_mean":str , "OP_AttentionalDeployment_mean_diff":str , "OP_CognitiveChange_mean_diff":str , "OP_response_modulation":str , "IS_response_modulation":str , "OP_Posemo_Adjusted":str , "OP_Negemo_Adjusted":str , "CON_Negativity_OP":str , "CON_Negativity_Adjusted":float , "CON_LSM_OP":float , "CON_LSM_OP_Netspeak":float , "OP_Directedness_Adjusted":float , "OP_Causality_Adjusted":float , 'OP_FGC_posemo_mean_diff_mean':float , 'OP_FGC_posemo_mean_diff_abs_mean':float , 'OP_FGC_negemo_mean_diff_mean':float , 'OP_FGC_negemo_mean_diff_abs_mean':float , 'OP_FGC_cogproc_mean_diff_mean':float , 'OP_FGC_cogproc_mean_diff_abs_mean':float , 'OP_FGC_posemo_mean3_diff_mean':float , 'OP_FGC_posemo_mean3_diff_abs_mean':float , 'OP_FGC_negemo_mean3_diff_mean':float , 'OP_FGC_negemo_mean3_diff_abs_mean':float , 'OP_FGC_cogproc_mean3_diff_mean':float , 'OP_FGC_cogproc_mean3_diff_abs_mean':float , 'T_Reaction':float , 'CON_LSM_OP_3Roll':float , 'CON_LSM_OP_Netspeak_3Roll':float , 'OP_Posemo_Adjusted_all':float , 'OP_Negemo_Adjusted_all':float , 'CON_Negativity_Adjusted_all':float , 'CON_LSM_OP_all':float , 'CON_LSM_OP_Netspeak_all':float , 'OP_Directedness_Adjusted_all':float , 'OP_Causality_Adjusted_all':float, 'posemo_before':float, 'negemo_before':float, 'posemo_3meanback':float, 'posemo_3meanforward':float, 'posemo_3meandiff':float, 'negemo_3meanback':float, 'negemo_3meanforward':float, 'negemo_3meandiff':float, 'posemo-negemo_3meandiff':float, 'user_posemo_sum':float, 'user_posemo_mean':float, 'user_negemo_sum':float, 'user_negemo_mean':float, 'firmtext1':str, 'firmtext2':str,'OP_firmlikes_total':float , 'OP_firmlikes_minusone':float , 'OP_firmlikes_minustwo':float , 'OP_firmlikes_plusone':float , 'OP_firmlikes_change_daylevel':float , 'OP_firmlikes_plusseven':float , 'OP_firmlikes_minuseight':float , 'OP_firmlikes_change_weeklevel':float , 'OP_firmlikes_plusthirty':float , 'OP_firmlikes_minusthirtyone':float , 'OP_firmlikes_change_monthlevel':float , 'OP_firmlikes_plushundredeighty':float , 'OP_firmlikes_minushundredeightyone':float , 'OP_firmlikes_change_sixmonthlevel':float}
            #final_dtypes = {'pageID':str,'page_name':str,'scrape_time':str,'type':str,'type_details':str,'original_post_id':str,'post_id':str,'pre_post_id':str,'post_id_summary':str,'from':str,'user_id':str,'status_message':str,'link_name':str,'status_type':str,'link':str,'status_published_UTC_Zurich':str,'status_published_unix_UTC_Zurich':str,'status_updated_UTC_Zurich':str,'status_updated_unix_UTC_Zurich':str,'num_likes':str,'num_comments':str,'num_shares':str,'post_by_pageID':str,'original_post_time_UTC_Zurich':str,'original_post_time_unix_UTC_Zurich':str,'referring_post_or_comment_id':str,'WC':str,'WPS':str,'Sixltr':str,'Dic':str,'Numerals':str,'function':str,'pronoun':str,'ppron':str,'i':str,'we':str,'you':str,'shehe':str,'they':str,'ipron':str,'article':str,'prep':str,'auxverb':str,'adverb':str,'conj':str,'negate':str,'verb':str,'adj':str,'compare':str,'interrog':str,'number':str,'quant':str,'affect':str,'posemo':str,'negemo':str,'anx':str,'anger':str,'sad':str,'social':str,'family':str,'friend':str,'female':str,'male':str,'cogproc':str,'insight':str,'cause':str,'discrep':str,'tentat':str,'certain':str,'differ':str,'percept':str,'see':str,'hear':str,'feel':str,'bio':str,'body':str,'health':str,'sexual':str,'ingest':str,'drives':str,'affiliation':str,'achiev':str,'power':str,'reward':str,'risk':str,'focuspast':str,'focuspresent':str,'focusfuture':str,'relativ':str,'motion':str,'space':str,'time':str,'work':str,'leisure':str,'home':str,'money':str,'relig':str,'death':str,'informal':str,'swear':str,'netspeak':str,'assent':str,'nonflu':str,'filler':str,'new_apology':str,'new_resp_mod':str,'new_modification':str,'Period':str,'Comma':str,'Colon':str,'SemiC':str,'QMark':str,'Exclam':str,'Dash':str,'Quote':str,'Apostro':str,'Parenth':str,'OtherP':str,'AllPct':str,'language':str,'lang_prob':str,'has_mail':str,'OP_UGC':str,'CON_Negativity_OP':str,'OP_Picture':str,'OP_Video':str,'OP_Event':str,'OP_Link':str,'OP_Status':str,'OP_weekend':str,'OP_morning':str,'OP_afternoon':str,'OP_evening':str,'OP_night':str,'OP_yearmonth':float,'OP_firmlikes_total':float,'OP_firmlikes_minusone':float,'OP_firmlikes_minustwo':float,'OP_firmlikes_plusone':float,'OP_firmlikes_change_daylevel':float,'OP_firmlikes_plusseven':float,'OP_firmlikes_minuseight':float,'OP_firmlikes_change_weeklevel':float,'OP_firmlikes_plusthirty':float,'OP_firmlikes_minusthirtyone':float,'OP_firmlikes_change_monthlevel':float,'OP_firmlikes_plushundredeighty':float,'OP_firmlikes_minushundredeightyone':float,'OP_firmlikes_change_sixmonthlevel':float,'user_posemo_sum':float,'user_posemo_mean':float,'user_negemo_sum':float,'user_negemo_mean':float,'TOTAL_likes_sub':str,'TOTAL_comments_sub':str,'TOTAL_shares_sub':str,'TOTAL_likes':float,'TOTAL_comments':float,'TOTAL_shares':float,'FGC_comments':float,'FGC_comments_likes_sum':float,'FGC_comments_likes_mean':float,'UGC_comments':float,'UserOriginalPost_comments':float,'T_Duration':float,'Post_UGC_Affect_sum':str,'Post_UGC_Affect_mean':float,'Post_UGC_negemo_sum':str,'Post_UGC_negemo_mean':float,'Post_UGC_posemo_sum':str,'Post_UGC_posemo_mean':str,'UGC_Cogproc_mean':str,'OP_sum_FGC':str,'IS_SituationSelection':str,'OP_Modification':str,'IS_SituationModification':str,'OP_apology':str,'IS_apology':str,'IS_SituationModificationApology':str,'OP_AttentionalDeployment_sum':str,'IS_AttentionalDeployment_sum':str,'OP_CognitiveChange_sum':str,'IS_CognitiveChange_sum':str,'OP_insight_sum':str,'OP_cause_sum':str,'OP_AttentionalDeployment_mean':str,'OP_CognitiveChange_mean':str,'OP_AttentionalDeployment_mean_diff':str,'OP_CognitiveChange_mean_diff':str,'OP_response_modulation':str,'IS_response_modulation':str,'OP_Modification_first':str,'IS_SituationModification_first':str,'OP_apology_first':str,'IS_apology_first':str,'IS_SituationModificationApology_first':str,'OP_AttentionalDeployment_first':str,'IS_AttentionalDeployment_first':str,'OP_CognitiveChange_first':str,'IS_CognitiveChange_first':str,'OP_insight_first':str,'OP_cause_first':str,'OP_response_modulation_first':str,'IS_response_modulation_first':str,'OP_Modification_allbutfirst':str,'IS_SituationModification_allbutfirst':str,'OP_apology_allbutfirst':str,'IS_apology_allbutfirst':str,'IS_SituationModificationApology_allbutfirst':str,'OP_AttentionalDeployment_allbutfirst':str,'IS_AttentionalDeployment_allbutfirst':str,'OP_CognitiveChange_allbutfirst':str,'IS_CognitiveChange_allbutfirst':str,'OP_insight_allbutfirst':str,'OP_cause_allbutfirst':str,'OP_response_modulation_allbutfirst':str,'IS_response_modulation_allbutfirst':str,'OP_Posemo_Adjusted':str,'OP_Negemo_Adjusted':str,'CON_Negativity_OP':str,'CON_Negativity_Adjusted':float,'CON_LSM_OP':float,'CON_LSM_OP_Netspeak':float,'OP_Directedness_Adjusted':float,'OP_Causality_Adjusted':float,'OP_FGC_posemo_mean_diff_mean':float,'OP_FGC_posemo_mean_diff_abs_mean':float,'OP_FGC_negemo_mean_diff_mean':float,'OP_FGC_negemo_mean_diff_abs_mean':float,'OP_FGC_cogproc_mean_diff_mean':float,'OP_FGC_cogproc_mean_diff_abs_mean':float,'OP_FGC_posemo_mean3_diff_mean':float,'OP_FGC_posemo_mean3_diff_abs_mean':float,'OP_FGC_negemo_mean3_diff_mean':float,'OP_FGC_negemo_mean3_diff_abs_mean':float,'OP_FGC_cogproc_mean3_diff_mean':float,'OP_FGC_cogproc_mean3_diff_abs_mean':float,'T_Reaction':float,'CON_LSM_OP_3Roll':float,'CON_LSM_OP_Netspeak_3Roll':float,'OP_Posemo_Adjusted_all':float,'OP_Negemo_Adjusted_all':float,'CON_Negativity_Adjusted_all':float,'CON_LSM_OP_all':float,'CON_LSM_OP_Netspeak_all':float,'OP_Directedness_Adjusted_all':float,'OP_Causality_Adjusted_all':float,'firmtext1':str,'firmtext2':str,'posemo_before':float,'negemo_before':float,'posemo_3meanback':float,'posemo_3meanforward':float,'posemo_3meandiff':float,'negemo_3meanback':float,'negemo_3meanforward':float,'negemo_3meandiff':float,'posemo-negemo_3meandiff':float}
            final_dtypes = {'pageID':str,'page_name':str,'scrape_time':str,'type':str,'type_details':str,'original_post_id':str,'post_id':str,'pre_post_id':str,'post_id_summary':str,'from':str,'user_id':str,'status_message':str,'link_name':str,'status_type':str,'link':str,'status_published_UTC_Zurich':str,'status_published_unix_UTC_Zurich':str,'status_updated_UTC_Zurich':str,'status_updated_unix_UTC_Zurich':str,'num_likes':str,'num_comments':str,'num_shares':str,'post_by_pageID':str,'original_post_time_UTC_Zurich':str,'original_post_time_unix_UTC_Zurich':str,'referring_post_or_comment_id':str,'WC':str,'WPS':str,'Sixltr':str,'Dic':str,'Numerals':str,'function':str,'pronoun':str,'ppron':str,'i':str,'we':str,'you':str,'shehe':str,'they':str,'ipron':str,'article':str,'prep':str,'auxverb':str,'adverb':str,'conj':str,'negate':str,'verb':str,'adj':str,'compare':str,'interrog':str,'number':str,'quant':str,'affect':str,'posemo':str,'negemo':str,'anx':str,'anger':str,'sad':str,'social':str,'family':str,'friend':str,'female':str,'male':str,'cogproc':str,'insight':str,'cause':str,'discrep':str,'tentat':str,'certain':str,'differ':str,'percept':str,'see':str,'hear':str,'feel':str,'bio':str,'body':str,'health':str,'sexual':str,'ingest':str,'drives':str,'affiliation':str,'achiev':str,'power':str,'reward':str,'risk':str,'focuspast':str,'focuspresent':str,'focusfuture':str,'relativ':str,'motion':str,'space':str,'time':str,'work':str,'leisure':str,'home':str,'money':str,'relig':str,'death':str,'informal':str,'swear':str,'netspeak':str,'assent':str,'nonflu':str,'filler':str,'new_apology':str,'new_resp_mod':str,'new_modification':str,'Period':str,'Comma':str,'Colon':str,'SemiC':str,'QMark':str,'Exclam':str,'Dash':str,'Quote':str,'Apostro':str,'Parenth':str,'OtherP':str,'AllPct':str,'language':str,'lang_prob':str,'has_mail':str,'OP_UGC':str,'CON_Negativity_OP':str,'OP_Picture':str,'OP_Video':str,'OP_Event':str,'OP_Link':str,'OP_Status':str,'OP_weekend':str,'OP_morning':str,'OP_afternoon':str,'OP_evening':str,'OP_night':str,'OP_yearmonth':float,'OP_firmlikes_total':float,'OP_firmlikes_minusone':float,'OP_firmlikes_minustwo':float,'OP_firmlikes_plusone':float,'OP_firmlikes_change_daylevel':float,'OP_firmlikes_plusseven':float,'OP_firmlikes_minuseight':float,'OP_firmlikes_change_weeklevel':float,'OP_firmlikes_plusthirty':float,'OP_firmlikes_minusthirtyone':float,'OP_firmlikes_change_monthlevel':float,'OP_firmlikes_plushundredeighty':float,'OP_firmlikes_minushundredeightyone':float,'OP_firmlikes_change_sixmonthlevel':float,'user_posemo_sum':float,'user_posemo_mean':float,'user_negemo_sum':float,'user_negemo_mean':float,'TOTAL_likes_sub':str,'TOTAL_comments_sub':str,'TOTAL_shares_sub':str,'TOTAL_likes':float,'TOTAL_comments':float,'TOTAL_shares':float,'FGC_comments':float,'FGC_comments_likes_sum':float,'FGC_comments_likes_mean':float,'UGC_comments':float,'UserOriginalPost_comments':float,'T_Duration':float,'Post_UGC_Affect_sum':str,'Post_UGC_Affect_mean':float,'Post_UGC_negemo_sum':str,'Post_UGC_negemo_mean':float,'Post_UGC_posemo_sum':str,'Post_UGC_posemo_mean':str,'UGC_Cogproc_mean':str,'OP_sum_FGC':str,'IS_SituationSelection':str,'OP_Modification':str,'IS_SituationModification':str,'OP_apology':str,'IS_apology':str,'IS_SituationModificationApology':str,'OP_AttentionalDeployment_sum':str,'IS_AttentionalDeployment_sum':str,'OP_CognitiveChange_sum':str,'IS_CognitiveChange_sum':str,'OP_insight_sum':str,'OP_cause_sum':str,'OP_AttentionalDeployment_mean':str,'OP_CognitiveChange_mean':str,'OP_AttentionalDeployment_mean_diff':str,'OP_CognitiveChange_mean_diff':str,'OP_response_modulation':str,'IS_response_modulation':str,'OP_Modification_first':str,'IS_SituationModification_first':str,'OP_apology_first':str,'IS_apology_first':str,'IS_SituationModificationApology_first':str,'OP_AttentionalDeployment_first':str,'IS_AttentionalDeployment_first':str,'OP_CognitiveChange_first':str,'IS_CognitiveChange_first':str,'OP_insight_first':str,'OP_cause_first':str,'OP_response_modulation_first':str,'IS_response_modulation_first':str, 'firm_reaction_time_2nd':str,'OP_Modification_allbutfirst':str,'IS_SituationModification_allbutfirst':str,'OP_apology_allbutfirst':str,'IS_apology_allbutfirst':str,'IS_SituationModificationApology_allbutfirst':str,'OP_AttentionalDeployment_allbutfirst':str,'IS_AttentionalDeployment_allbutfirst':str,'OP_CognitiveChange_allbutfirst':str,'IS_CognitiveChange_allbutfirst':str,'OP_insight_allbutfirst':str,'OP_cause_allbutfirst':str,'OP_response_modulation_allbutfirst':str,'IS_response_modulation_allbutfirst':str,'OP_Posemo_Adjusted':str,'OP_Negemo_Adjusted':str,'CON_Negativity_OP':str,'CON_Negativity_Adjusted':float,'CON_LSM_OP':float,'CON_LSM_OP_Netspeak':float,'OP_Directedness_Adjusted':float,'OP_Causality_Adjusted':float,'OP_FGC_posemo_mean_diff_mean':float,'OP_FGC_posemo_mean_diff_abs_mean':float,'OP_FGC_negemo_mean_diff_mean':float,'OP_FGC_negemo_mean_diff_abs_mean':float,'OP_FGC_cogproc_mean_diff_mean':float,'OP_FGC_cogproc_mean_diff_abs_mean':float,'OP_FGC_posemo_mean3_diff_mean':float,'OP_FGC_posemo_mean3_diff_abs_mean':float,'OP_FGC_negemo_mean3_diff_mean':float,'OP_FGC_negemo_mean3_diff_abs_mean':float,'OP_FGC_cogproc_mean3_diff_mean':float,'OP_FGC_cogproc_mean3_diff_abs_mean':float,'T_Reaction':float,'CON_LSM_OP_3Roll':float,'CON_LSM_OP_Netspeak_3Roll':float,'OP_Posemo_Adjusted_all':float,'OP_Negemo_Adjusted_all':float,'CON_Negativity_Adjusted_all':float,'CON_LSM_OP_all':float,'CON_LSM_OP_Netspeak_all':float,'OP_Directedness_Adjusted_all':float,'OP_Causality_Adjusted_all':float,'firmtext1':str,'firmtext2':str,'firmtext3':str,'firmtext4':str,'firmtext5':str,'posemo_before':float,'negemo_before':float,'posemo_3meanback':float,'posemo_3meanforward':float,'posemo_3meandiff':float,'negemo_3meanback':float,'negemo_3meanforward':float,'negemo_3meandiff':float,'posemo-negemo_3meandiff':float}
            
            df_final = pd.read_csv("./features_immediate/"+filename_out+"_features.csv",sep="\t",low_memory=False,quoting=3, header='infer', dtype=final_dtypes, na_values=['None','nan'])
            df_final['status_published_unix_UTC_Zurich_float']=df_final['status_published_unix_UTC_Zurich'].astype(float)
    
            df_final.set_index('status_published_unix_UTC_Zurich_float', drop=True, inplace=True)
            df_final.sort_index(inplace=True)
            if len(df_final) !=0:     
                CON_Negativity_Adjusted_mean  = float(df_final["CON_Negativity_Adjusted"].mean())
                CON_LSM_OP_mean  = float(df_final["CON_LSM_OP"].mean())
                CON_LSM_OP_Netspeak_mean  = float(df_final["CON_LSM_OP_Netspeak"].mean())
                OP_Directedness_Adjusted_mean = float(df_final["OP_Directedness_Adjusted"].mean())
                OP_Causality_Adjusted_mean  = float(df_final["OP_Causality_Adjusted"].mean())
            
                df_final["c_CON_Negativity_Adjusted"] = df_final["CON_Negativity_Adjusted"] - CON_Negativity_Adjusted_mean
                df_final["c_CON_LSM_OP"] = df_final["CON_LSM_OP"] - CON_LSM_OP_mean
                df_final["c_CON_LSM_OP_Netspeak"] = df_final["CON_LSM_OP_Netspeak"] - CON_LSM_OP_Netspeak_mean
                df_final["c_OP_Directedness_Adjusted"] = df_final["OP_Directedness_Adjusted"] - OP_Directedness_Adjusted_mean
                df_final['c_OP_Causality_Adjusted'] = df_final["OP_Causality_Adjusted"] - OP_Causality_Adjusted_mean
                         
                df_final['CON_Negativity_Squared'] = (df_final.c_CON_Negativity_Adjusted)**2
                df_final['CON_LSM_OP_Squared'] = (df_final.c_CON_LSM_OP)**2
                df_final['CON_LSM_OP_Netspeak_Squared'] = (df_final.c_CON_LSM_OP_Netspeak)**2
                df_final['CON_OP_Directedness_Squared'] = (df_final.c_OP_Directedness_Adjusted)**2
                df_final['CON_OP_Causality_Squared'] = (df_final.c_OP_Causality_Adjusted)**2
                         
                df_final['X_Neg_Direct'] = df_final.c_CON_Negativity_Adjusted  * df_final.c_OP_Directedness_Adjusted
                df_final['X_Neg_Causal'] = df_final.c_CON_Negativity_Adjusted * df_final.c_OP_Causality_Adjusted
                df_final['X_LSM_Direct'] = df_final.c_CON_LSM_OP  * df_final.c_OP_Directedness_Adjusted
                df_final['X_LSM_Causal'] = df_final.c_CON_LSM_OP * df_final.c_OP_Causality_Adjusted
                df_final['X_LSM_Netspeak_Direct'] = df_final.c_CON_LSM_OP_Netspeak  * df_final.c_OP_Directedness_Adjusted
                df_final['X_LSM_Netspeak_Causal'] = df_final.c_CON_LSM_OP_Netspeak * df_final.c_OP_Causality_Adjusted   
                       
                CON_Negativity_Squared_mean = float(df_final["CON_Negativity_Squared"].mean())
                CON_LSM_OP_Squared_mean = float(df_final["CON_LSM_OP_Squared"].mean())
                CON_LSM_OP_Netspeak_Squared_mean = float(df_final["CON_LSM_OP_Netspeak_Squared"].mean())
                        
                df_final['c_CON_Negativity_Squared'] = df_final.CON_Negativity_Squared - CON_Negativity_Squared_mean
                df_final['c_CON_LSM_OP_Squared'] = df_final.CON_LSM_OP_Squared - CON_LSM_OP_Squared_mean
                df_final['c_CON_LSM_OP_Netspeak_Squared'] = df_final.CON_LSM_OP_Netspeak_Squared - CON_LSM_OP_Netspeak_Squared_mean
                         
                df_final['X_Neg2_Direct'] = df_final['c_CON_Negativity_Squared'] * df_final['c_OP_Directedness_Adjusted']
                df_final['X_Neg2_Causal'] = df_final['c_CON_Negativity_Squared'] * df_final['c_OP_Causality_Adjusted']
                df_final['X_LSM2_Direct'] = df_final['c_CON_LSM_OP_Squared'] * df_final['c_OP_Directedness_Adjusted']
                df_final['X_LSM2_Causal'] = df_final['c_CON_LSM_OP_Squared'] * df_final['c_OP_Causality_Adjusted']
                df_final['X_LSM_Netspeak2_Direct'] = df_final['c_CON_LSM_OP_Netspeak_Squared'] * df_final['c_OP_Directedness_Adjusted']
                df_final['X_LSM_Netspeak2_Causal'] = df_final['c_CON_LSM_OP_Netspeak_Squared'] * df_final['c_OP_Causality_Adjusted']
                           
                #             #drop intermediary columns
                #             df.drop(df[['CON_Negativity_Adjusted_mean','CON_LSM_OP_mean', 'OP_Directedness_Adjusted_mean', 'OP_Causality_Adjusted_mean', 'CON_Negativity_Squared_mean', 'CON_LSM_OP_Squared_mean']], axis=1, inplace=True)
                TOTAL_likes_mean = float(df_final['TOTAL_likes'].mean())
                df_final['TOTAL_likes_adjusted'] = df_final.TOTAL_likes/TOTAL_likes_mean
                TOTAL_comments_mean = float(df_final['TOTAL_comments'].mean())
                df_final['TOTAL_comments_adjusted'] = df_final.TOTAL_comments/TOTAL_comments_mean
                TOTAL_shares_mean = float(df_final['TOTAL_shares'].mean())
                df_final['TOTAL_shares_adjusted'] = df_final.TOTAL_shares/TOTAL_shares_mean
                FGC_comments_mean = float(df_final['FGC_comments'].mean())
                df_final['FGC_comments_adjusted'] = df_final.FGC_comments/FGC_comments_mean
                UGC_comments_mean = float(df_final['UGC_comments'].mean()) 
                df_final['UGC_comments_adjusted'] = df_final.UGC_comments/UGC_comments_mean
                T_Duration_mean = float(df_final['T_Duration'].mean())
                df_final['T_Duration_adjusted'] = df_final.T_Duration/T_Duration_mean
                post_UGC_Affect_mean_mean = float(df_final['Post_UGC_Affect_mean'].mean())
                df_final['Post_UGC_Affect_mean_adjusted']=df_final['Post_UGC_Affect_mean']/post_UGC_Affect_mean_mean
                post_UGC_negemo_mean_mean = float(df_final['Post_UGC_negemo_mean'].mean())
                df_final['Post_UGC_negemo_mean_adjusted']=df_final['Post_UGC_negemo_mean']/post_UGC_negemo_mean_mean
                user_numbers=df_final.groupby('user_id')['post_id_summary'].count()
                df_final['Frequency_USER'] = df_final.apply(lambda row: user_numbers[row['user_id']] if not (row['user_id'] != row['user_id']) else "", axis=1)
    
                T_Reaction_mean = float(df_final['T_Reaction'].mean())
                df_final['T_Reaction_adjusted'] = df_final.T_Reaction/T_Reaction_mean
    
                df_final['posemo_before']=(df_final['posemo'].rolling(window=2, min_periods=0).sum())-df_final['posemo'].astype(float)
                df_final['negemo_before']=(df_final['negemo'].rolling(window=2, min_periods=0).sum())-df_final['negemo'].astype(float)
                df_final['posemo_3meanback']=((df_final['posemo'].rolling(window=4, min_periods=0).sum())-df_final['posemo'].astype(float))/3
                df_final['posemo_3meanforward']=((df_final['posemo'].rolling(window=7, min_periods=0,center=True).sum())-(df_final['posemo_3meanback']*3)-df_final['posemo'].astype(float))/3
                df_final['posemo_3meandiff']=df_final['posemo_3meanforward']-df_final['posemo_3meanback']
                df_final['negemo_3meanback']=((df_final['negemo'].rolling(window=4, min_periods=0).sum())-df_final['negemo'].astype(float))/3
                df_final['negemo_3meanforward']=((df_final['negemo'].rolling(window=7, min_periods=0,center=True).sum())-(df_final['negemo_3meanback']*3)-df_final['negemo'].astype(float))/3
                df_final['negemo_3meandiff']=df_final['negemo_3meanforward']-df_final['negemo_3meanback']
                df_final['posemo-negemo_3meandiff']=df_final['posemo_3meandiff']-df_final['negemo_3meandiff']
               
                ######Join with like_features 
                #Read in file and select relevant columns
                #!!Uebergangsloesuing
    #                 likefeaturesfilename = filename.split('_merged')[0]
    #                 lfcols = ['status_published_unix_UTC_Zurich', 'user_id', 'likes_change1y', 'likes_change1q', 'likes_change1m', 'likes_change1w', 'likes_attraction', 'likes_aversion', 'likes_acquisition', 'fgc_likes_change1y', 'fgc_likes_change1q', 'fgc_likes_change1m', 'fgc_likes_change1w', 'fgc_likes_attraction', 'fgc_likes_aversion', 'fgc_likes_acquisition', 'ugc_likes_change1y', 'ugc_likes_change1q', 'ugc_likes_change1m', 'ugc_likes_change1w', 'ugc_likes_attraction', 'ugc_likes_aversion', 'ugc_likes_acquisition', 'overall_likes_change1y', 'overall_likes_change1q', 'overall_likes_change1m', 'overall_likes_change1w', 'overall_fgc_likes_change1y', 'overall_fgc_likes_change1q', 'overall_fgc_likes_change1m', 'overall_fgc_likes_change1w', 'overall_ugc_likes_change1y', 'overall_ugc_likes_change1q', 'overall_ugc_likes_change1m', 'overall_ugc_likes_change1w', 'issuer_likes_change1y', 'issuer_likes_change1q', 'issuer_likes_change1m', 'issuer_likes_change1w', 'issuer_likes_attraction', 'issuer_likes_aversion', 'issuer_fgc_likes_change1y', 'issuer_fgc_likes_change1q', 'issuer_fgc_likes_change1m', 'issuer_fgc_likes_change1w', 'issuer_ugc_likes_change1y', 'issuer_ugc_likes_change1q', 'issuer_ugc_likes_change1m', 'issuer_ugc_likes_change1w']
    #                 lfdefs = {'status_published_unix_UTC_Zurich': str, 'user_id': str, 'likes_change1y': float, 'likes_change1q': float, 'likes_change1m': float, 'likes_change1w': float, 'likes_attraction': float, 'likes_aversion': float, 'likes_acquisition': float, 'fgc_likes_change1y': float, 'fgc_likes_change1q': float, 'fgc_likes_change1m': float, 'fgc_likes_change1w': float, 'fgc_likes_attraction': float, 'fgc_likes_aversion': float, 'fgc_likes_acquisition': float, 'ugc_likes_change1y': float, 'ugc_likes_change1q': float, 'ugc_likes_change1m': float, 'ugc_likes_change1w': float, 'ugc_likes_attraction': float, 'ugc_likes_aversion': float, 'ugc_likes_acquisition': float, 'overall_likes_change1y': float, 'overall_likes_change1q': float, 'overall_likes_change1m': float, 'overall_likes_change1w': float, 'overall_fgc_likes_change1y': float, 'overall_fgc_likes_change1q': float, 'overall_fgc_likes_change1m': float, 'overall_fgc_likes_change1w': float, 'overall_ugc_likes_change1y': float, 'overall_ugc_likes_change1q': float, 'overall_ugc_likes_change1m': float, 'overall_ugc_likes_change1w': float, 'issuer_likes_change1y': float, 'issuer_likes_change1q': float, 'issuer_likes_change1m': float, 'issuer_likes_change1w': float, 'issuer_likes_attraction': float, 'issuer_likes_aversion': float, 'issuer_fgc_likes_change1y': float, 'issuer_fgc_likes_change1q': float, 'issuer_fgc_likes_change1m': float, 'issuer_fgc_likes_change1w': float, 'issuer_ugc_likes_change1y': float, 'issuer_ugc_likes_change1q': float, 'issuer_ugc_likes_change1m': float, 'issuer_ugc_likes_change1w': float}
    #                 parse_dates = ['status_published_unix_UTC_Zurich']
    #                 df_likefeatures = pd.read_csv('./like_features/'+likefeaturesfilename+'_merged_likefeatures.csv', parse_dates = parse_dates, sep="\t",header='infer', usecols=lfcols,dtype=lfdefs)
    #                 df_likefeatures['index'] = pd.DatetimeIndex(df_likefeatures['status_published_unix_UTC_Zurich'])
    #                 df_likefeatures['index_int']= df_likefeatures['index'].astype(np.int64) // 10**9
    #                 df_likefeatures['index_string']=df_likefeatures['index_int'].astype(float).round(decimals=1).astype(str)
    #                 df_likefeatures['post_id_new']=df_likefeatures['index_string']+df_likefeatures['user_id']
    #                 
    #                 df_likefeatures.set_index(['post_id_new'], drop=True, inplace=True, append=False)
    #                 df_likefeatures.drop(['index','index_int','index_string','status_published_unix_UTC_Zurich','user_id'], inplace=True, axis=1)
    #                 
    #                 
    #                 df_final['new_id'] = df_final['status_published_unix_UTC_Zurich'].astype(str)+df_final['user_id']
    #                 
    #                 
    #                 df_final = df_final.join(df_likefeatures, on='new_id', how='left')
                
                #!! Bessere Loesung #11.03.2018 likefeatures removed
    #             likefeaturesfilename = filename.split('_nolikes')[0]
    #             
    #             #old version
    #             lfcols = ['post_id', 'likes_change1y', 'likes_change1q', 'likes_change1m', 'likes_change1w', 'likes_attraction', 'likes_aversion', 'likes_acquisition', 'fgc_likes_change1y', 'fgc_likes_change1q', 'fgc_likes_change1m', 'fgc_likes_change1w', 'fgc_likes_attraction', 'fgc_likes_aversion', 'fgc_likes_acquisition', 'ugc_likes_change1y', 'ugc_likes_change1q', 'ugc_likes_change1m', 'ugc_likes_change1w', 'ugc_likes_attraction', 'ugc_likes_aversion', 'ugc_likes_acquisition', 'overall_likes_change1y', 'overall_likes_change1q', 'overall_likes_change1m', 'overall_likes_change1w', 'overall_fgc_likes_change1y', 'overall_fgc_likes_change1q', 'overall_fgc_likes_change1m', 'overall_fgc_likes_change1w', 'overall_ugc_likes_change1y', 'overall_ugc_likes_change1q', 'overall_ugc_likes_change1m', 'overall_ugc_likes_change1w', 'issuer_likes_change1y', 'issuer_likes_change1q', 'issuer_likes_change1m', 'issuer_likes_change1w', 'issuer_likes_attraction', 'issuer_likes_aversion', 'issuer_fgc_likes_change1y', 'issuer_fgc_likes_change1q', 'issuer_fgc_likes_change1m', 'issuer_fgc_likes_change1w', 'issuer_ugc_likes_change1y', 'issuer_ugc_likes_change1q', 'issuer_ugc_likes_change1m', 'issuer_ugc_likes_change1w']
    #             lfdefs = {'post_id': str, 'likes_change1y': float, 'likes_change1q': float, 'likes_change1m': float, 'likes_change1w': float, 'likes_attraction': float, 'likes_aversion': float, 'likes_acquisition': float, 'fgc_likes_change1y': float, 'fgc_likes_change1q': float, 'fgc_likes_change1m': float, 'fgc_likes_change1w': float, 'fgc_likes_attraction': float, 'fgc_likes_aversion': float, 'fgc_likes_acquisition': float, 'ugc_likes_change1y': float, 'ugc_likes_change1q': float, 'ugc_likes_change1m': float, 'ugc_likes_change1w': float, 'ugc_likes_attraction': float, 'ugc_likes_aversion': float, 'ugc_likes_acquisition': float, 'overall_likes_change1y': float, 'overall_likes_change1q': float, 'overall_likes_change1m': float, 'overall_likes_change1w': float, 'overall_fgc_likes_change1y': float, 'overall_fgc_likes_change1q': float, 'overall_fgc_likes_change1m': float, 'overall_fgc_likes_change1w': float, 'overall_ugc_likes_change1y': float, 'overall_ugc_likes_change1q': float, 'overall_ugc_likes_change1m': float, 'overall_ugc_likes_change1w': float, 'issuer_likes_change1y': float, 'issuer_likes_change1q': float, 'issuer_likes_change1m': float, 'issuer_likes_change1w': float, 'issuer_likes_attraction': float, 'issuer_likes_aversion': float, 'issuer_fgc_likes_change1y': float, 'issuer_fgc_likes_change1q': float, 'issuer_fgc_likes_change1m': float, 'issuer_fgc_likes_change1w': float, 'issuer_ugc_likes_change1y': float, 'issuer_ugc_likes_change1q': float, 'issuer_ugc_likes_change1m': float, 'issuer_ugc_likes_change1w': float}
    #             
                #new version
                #lfcols = ['post_id', 'likes_change1y', 'likes_change1q', 'likes_change1m', 'likes_change1w', 'likes_attraction', 'likes_aversion', 'likes_acquisition', 'fgc_likes_change1y', 'fgc_likes_change1q', 'fgc_likes_change1m', 'fgc_likes_change1w', 'fgc_likes_attraction', 'fgc_likes_aversion', 'fgc_likes_acquisition', 'ugc_likes_change1y', 'ugc_likes_change1q', 'ugc_likes_change1m', 'ugc_likes_change1w', 'ugc_likes_attraction', 'ugc_likes_aversion', 'ugc_likes_acquisition', 'overall_likes_change1y', 'overall_likes_change1q', 'overall_likes_change1m', 'overall_likes_change1w', 'overall_fgc_likes_change1y', 'overall_fgc_likes_change1q', 'overall_fgc_likes_change1m', 'overall_fgc_likes_change1w', 'overall_ugc_likes_change1y', 'overall_ugc_likes_change1q', 'overall_ugc_likes_change1m', 'overall_ugc_likes_change1w', 'issuer_likes_change1y', 'issuer_likes_change1q', 'issuer_likes_change1m', 'issuer_likes_change1w', 'issuer_likes_attraction', 'issuer_likes_aversion', 'issuer_fgc_likes_change1y', 'issuer_fgc_likes_change1q', 'issuer_fgc_likes_change1m', 'issuer_fgc_likes_change1w', 'issuer_ugc_likes_change1y', 'issuer_ugc_likes_change1q', 'issuer_ugc_likes_change1m', 'issuer_ugc_likes_change1w',  'overall_ugc_comments_change1y' , 'overall_ugc_comments_change1q' , 'overall_ugc_comments_change1m' , 'overall_ugc_comments_change1w' , 'overall_likes_change_standard1y' , 'overall_likes_change_standard1q' , 'overall_likes_change_standard1m' , 'overall_likes_change_standard1w' , 'overall_fgc_likes_change_standard1y' , 'overall_fgc_likes_change_standard1q' , 'overall_fgc_likes_change_standard1m' , 'overall_fgc_likes_change_standard1w' , 'overall_ugc_likes_change_standard1y' , 'overall_ugc_likes_change_standard1q' , 'overall_ugc_likes_change_standard1m' , 'overall_ugc_likes_change_standard1w', 'overall_ugc_comments_change_standard1y' , 'overall_ugc_comments_change_standard1q' , 'overall_ugc_comments_change_standard1m' , 'overall_ugc_comments_change_standard1w']
                #lfdefs = {'post_id': str, 'likes_change1y': float, 'likes_change1q': float, 'likes_change1m': float, 'likes_change1w': float, 'likes_attraction': float, 'likes_aversion': float, 'likes_acquisition': float, 'fgc_likes_change1y': float, 'fgc_likes_change1q': float, 'fgc_likes_change1m': float, 'fgc_likes_change1w': float, 'fgc_likes_attraction': float, 'fgc_likes_aversion': float, 'fgc_likes_acquisition': float, 'ugc_likes_change1y': float, 'ugc_likes_change1q': float, 'ugc_likes_change1m': float, 'ugc_likes_change1w': float, 'ugc_likes_attraction': float, 'ugc_likes_aversion': float, 'ugc_likes_acquisition': float, 'overall_likes_change1y': float, 'overall_likes_change1q': float, 'overall_likes_change1m': float, 'overall_likes_change1w': float, 'overall_fgc_likes_change1y': float, 'overall_fgc_likes_change1q': float, 'overall_fgc_likes_change1m': float, 'overall_fgc_likes_change1w': float, 'overall_ugc_likes_change1y': float, 'overall_ugc_likes_change1q': float, 'overall_ugc_likes_change1m': float, 'overall_ugc_likes_change1w': float, 'issuer_likes_change1y': float, 'issuer_likes_change1q': float, 'issuer_likes_change1m': float, 'issuer_likes_change1w': float, 'issuer_likes_attraction': float, 'issuer_likes_aversion': float, 'issuer_fgc_likes_change1y': float, 'issuer_fgc_likes_change1q': float, 'issuer_fgc_likes_change1m': float, 'issuer_fgc_likes_change1w': float, 'issuer_ugc_likes_change1y': float, 'issuer_ugc_likes_change1q': float, 'issuer_ugc_likes_change1m': float, 'issuer_ugc_likes_change1w': float, 'overall_ugc_comments_change1y' : float,  'overall_ugc_comments_change1q' : float,  'overall_ugc_comments_change1m' : float,  'overall_ugc_comments_change1w' : float,  'overall_likes_change_standard1y' : float,  'overall_likes_change_standard1q' : float,  'overall_likes_change_standard1m' : float,  'overall_likes_change_standard1w' : float,  'overall_fgc_likes_change_standard1y' : float,  'overall_fgc_likes_change_standard1q' : float,  'overall_fgc_likes_change_standard1m' : float,  'overall_fgc_likes_change_standard1w' : float,  'overall_ugc_likes_change_standard1y' : float,  'overall_ugc_likes_change_standard1q' : float,  'overall_ugc_likes_change_standard1m' : float,  'overall_ugc_likes_change_standard1w': float,  'overall_ugc_comments_change_standard1y' : float,  'overall_ugc_comments_change_standard1q' : float,  'overall_ugc_comments_change_standard1m' : float,  'overall_ugc_comments_change_standard1w' : float}
                
                
                ##11.03.2018 Likeinfos entfernt
                #df_likefeatures = pd.read_csv('./like_features/'+likefeaturesfilename+'_merged_likefeatures.csv', sep="\t",header='infer', usecols=lfcols,dtype=lfdefs, index_col='post_id')
                
    
                #df_final = df_final.join(df_likefeatures, on='post_id', how='left') 
                ##TODO ggf mit dask testen               
                
                
          
                
                #left join on post_id
                #respectively for now on 
    
                finalfilename='./features_final/'+filename_out+'_features.csv'
                #old version
                #new_final_columns = ["c_CON_Negativity_Adjusted","c_CON_LSM_OP","c_CON_LSM_OP_Netspeak","c_OP_Directedness_Adjusted",'c_OP_Causality_Adjusted','X_Neg_Direct','X_Neg_Causal','X_LSM_Direct','X_LSM_Causal','X_LSM_Netspeak_Direct','X_LSM_Netspeak_Causal','c_CON_Negativity_Squared','c_CON_LSM_OP_Squared','c_CON_LSM_OP_Netspeak_Squared','X_Neg2_Direct','X_Neg2_Causal','X_LSM2_Direct','X_LSM2_Causal','X_LSM_Netspeak2_Direct','X_LSM_Netspeak2_Causal','TOTAL_likes_adjusted','TOTAL_comments_adjusted','TOTAL_shares_adjusted','FGC_comments_adjusted','UGC_comments_adjusted','T_Duration_adjusted','Post_UGC_Affect_mean_adjusted','Post_UGC_negemo_mean_adjusted','Frequency_USER','T_Reaction_adjusted','posemo_before', 'negemo_before', 'posemo_3meanback', 'posemo_3meanforward', 'posemo_3meandiff', 'negemo_3meanback', 'negemo_3meanforward', 'negemo_3meandiff', 'posemo-negemo_3meandiff' , 'user_posemo_sum' , 'user_posemo_mean' , 'user_negemo_sum' , 'user_negemo_mean', "likes_change1y", "likes_change1q", "likes_change1m", "likes_change1w", "likes_attraction", "likes_aversion", "likes_acquisition", "fgc_likes_change1y", "fgc_likes_change1q", "fgc_likes_change1m", "fgc_likes_change1w", "fgc_likes_attraction", "fgc_likes_aversion", "fgc_likes_acquisition", "ugc_likes_change1y", "ugc_likes_change1q", "ugc_likes_change1m", "ugc_likes_change1w", "ugc_likes_attraction", "ugc_likes_aversion", "ugc_likes_acquisition", "overall_likes_change1y", "overall_likes_change1q", "overall_likes_change1m", "overall_likes_change1w", "overall_fgc_likes_change1y", "overall_fgc_likes_change1q", "overall_fgc_likes_change1m", "overall_fgc_likes_change1w", "overall_ugc_likes_change1y", "overall_ugc_likes_change1q", "overall_ugc_likes_change1m", "overall_ugc_likes_change1w", "issuer_likes_change1y", "issuer_likes_change1q", "issuer_likes_change1m", "issuer_likes_change1w", "issuer_likes_attraction", "issuer_likes_aversion", "issuer_fgc_likes_change1y", "issuer_fgc_likes_change1q", "issuer_fgc_likes_change1m", "issuer_fgc_likes_change1w", "issuer_ugc_likes_change1y", "issuer_ugc_likes_change1q", "issuer_ugc_likes_change1m", "issuer_ugc_likes_change1w"]
                
                #new version
                #new_final_columns = ["c_CON_Negativity_Adjusted","c_CON_LSM_OP","c_CON_LSM_OP_Netspeak","c_OP_Directedness_Adjusted",'c_OP_Causality_Adjusted','X_Neg_Direct','X_Neg_Causal','X_LSM_Direct','X_LSM_Causal','X_LSM_Netspeak_Direct','X_LSM_Netspeak_Causal','c_CON_Negativity_Squared','c_CON_LSM_OP_Squared','c_CON_LSM_OP_Netspeak_Squared','X_Neg2_Direct','X_Neg2_Causal','X_LSM2_Direct','X_LSM2_Causal','X_LSM_Netspeak2_Direct','X_LSM_Netspeak2_Causal','TOTAL_likes_adjusted','TOTAL_comments_adjusted','TOTAL_shares_adjusted','FGC_comments_adjusted','UGC_comments_adjusted','T_Duration_adjusted','Post_UGC_Affect_mean_adjusted','Post_UGC_negemo_mean_adjusted','Frequency_USER','T_Reaction_adjusted','posemo_before', 'negemo_before', 'posemo_3meanback', 'posemo_3meanforward', 'posemo_3meandiff', 'negemo_3meanback', 'negemo_3meanforward', 'negemo_3meandiff', 'posemo-negemo_3meandiff' , 'user_posemo_sum' , 'user_posemo_mean' , 'user_negemo_sum' , 'user_negemo_mean', "likes_change1y", "likes_change1q", "likes_change1m", "likes_change1w", "likes_attraction", "likes_aversion", "likes_acquisition", "fgc_likes_change1y", "fgc_likes_change1q", "fgc_likes_change1m", "fgc_likes_change1w", "fgc_likes_attraction", "fgc_likes_aversion", "fgc_likes_acquisition", "ugc_likes_change1y", "ugc_likes_change1q", "ugc_likes_change1m", "ugc_likes_change1w", "ugc_likes_attraction", "ugc_likes_aversion", "ugc_likes_acquisition", "overall_likes_change1y", "overall_likes_change1q", "overall_likes_change1m", "overall_likes_change1w", "overall_fgc_likes_change1y", "overall_fgc_likes_change1q", "overall_fgc_likes_change1m", "overall_fgc_likes_change1w", "overall_ugc_likes_change1y", "overall_ugc_likes_change1q", "overall_ugc_likes_change1m", "overall_ugc_likes_change1w", "issuer_likes_change1y", "issuer_likes_change1q", "issuer_likes_change1m", "issuer_likes_change1w", "issuer_likes_attraction", "issuer_likes_aversion", "issuer_fgc_likes_change1y", "issuer_fgc_likes_change1q", "issuer_fgc_likes_change1m", "issuer_fgc_likes_change1w", "issuer_ugc_likes_change1y", "issuer_ugc_likes_change1q", "issuer_ugc_likes_change1m", "issuer_ugc_likes_change1w",  'overall_ugc_comments_change1y' , 'overall_ugc_comments_change1q' , 'overall_ugc_comments_change1m' , 'overall_ugc_comments_change1w' , 'overall_likes_change_standard1y' , 'overall_likes_change_standard1q' , 'overall_likes_change_standard1m' , 'overall_likes_change_standard1w' , 'overall_fgc_likes_change_standard1y' , 'overall_fgc_likes_change_standard1q' , 'overall_fgc_likes_change_standard1m' , 'overall_fgc_likes_change_standard1w' , 'overall_ugc_likes_change_standard1y' , 'overall_ugc_likes_change_standard1q' , 'overall_ugc_likes_change_standard1m' , 'overall_ugc_likes_change_standard1w', 'overall_ugc_comments_change_standard1y' , 'overall_ugc_comments_change_standard1q' , 'overall_ugc_comments_change_standard1m' , 'overall_ugc_comments_change_standard1w']
                #11.03.2018 likeinfos removes
                new_final_columns = ["c_CON_Negativity_Adjusted","c_CON_LSM_OP","c_CON_LSM_OP_Netspeak","c_OP_Directedness_Adjusted",'c_OP_Causality_Adjusted','X_Neg_Direct','X_Neg_Causal','X_LSM_Direct','X_LSM_Causal','X_LSM_Netspeak_Direct','X_LSM_Netspeak_Causal','c_CON_Negativity_Squared','c_CON_LSM_OP_Squared','c_CON_LSM_OP_Netspeak_Squared','X_Neg2_Direct','X_Neg2_Causal','X_LSM2_Direct','X_LSM2_Causal','X_LSM_Netspeak2_Direct','X_LSM_Netspeak2_Causal','TOTAL_likes_adjusted','TOTAL_comments_adjusted','TOTAL_shares_adjusted','FGC_comments_adjusted','UGC_comments_adjusted','T_Duration_adjusted','Post_UGC_Affect_mean_adjusted','Post_UGC_negemo_mean_adjusted','Frequency_USER','T_Reaction_adjusted','posemo_before', 'negemo_before', 'posemo_3meanback', 'posemo_3meanforward', 'posemo_3meandiff', 'negemo_3meanback', 'negemo_3meanforward', 'negemo_3meandiff', 'posemo-negemo_3meandiff' , 'user_posemo_sum' , 'user_posemo_mean' , 'user_negemo_sum' , 'user_negemo_mean']
    
                final_columns = imcolumnames[:-1]
                final_columns.extend(new_final_columns)
                df_final.drop_duplicates().to_csv(finalfilename, sep='\t',index=False,encoding='utf-8',columns=final_columns)
                os.remove("./features_immediate/"+filename_out+"_features.csv")
                #del user_numbers
                del df_final           
    #             if len(df_final) !=0:     
    #                 CON_Negativity_Adjusted_mean  = float(df_final["CON_Negativity_Adjusted"].mean().compute())
    #                 CON_LSM_OP_mean  = float(df_final["CON_LSM_OP"].mean().compute())
    #                 CON_LSM_OP_Netspeak_mean  = float(df_final["CON_LSM_OP_Netspeak"].mean().compute())
    #                 OP_Directedness_Adjusted_mean = float(df_final["OP_Directedness_Adjusted"].mean().compute())
    #                 OP_Causality_Adjusted_mean  = float(df_final["OP_Causality_Adjusted"].mean().compute())
    #             
    #                 df_final["c_CON_Negativity_Adjusted"] = df_final["CON_Negativity_Adjusted"] - CON_Negativity_Adjusted_mean
    #                 df_final["c_CON_LSM_OP"] = df_final["CON_LSM_OP"] - CON_LSM_OP_mean
    #                 df_final["c_CON_LSM_OP_Netspeak"] = df_final["CON_LSM_OP_Netspeak"] - CON_LSM_OP_Netspeak_mean
    #                 df_final["c_OP_Directedness_Adjusted"] = df_final["OP_Directedness_Adjusted"] - OP_Directedness_Adjusted_mean
    #                 df_final['c_OP_Causality_Adjusted'] = df_final["OP_Causality_Adjusted"] - OP_Causality_Adjusted_mean
    #                          
    #                 df_final['CON_Negativity_Squared'] = (df_final.c_CON_Negativity_Adjusted)**2
    #                 df_final['CON_LSM_OP_Squared'] = (df_final.c_CON_LSM_OP)**2
    #                 df_final['CON_LSM_OP_Netspeak_Squared'] = (df_final.c_CON_LSM_OP_Netspeak)**2
    #                 df_final['CON_OP_Directedness_Squared'] = (df_final.c_OP_Directedness_Adjusted)**2
    #                 df_final['CON_OP_Causality_Squared'] = (df_final.c_OP_Causality_Adjusted)**2
    #                          
    #                 df_final['X_Neg_Direct'] = df_final.c_CON_Negativity_Adjusted  * df_final.c_OP_Directedness_Adjusted
    #                 df_final['X_Neg_Causal'] = df_final.c_CON_Negativity_Adjusted * df_final.c_OP_Causality_Adjusted
    #                 df_final['X_LSM_Direct'] = df_final.c_CON_LSM_OP  * df_final.c_OP_Directedness_Adjusted
    #                 df_final['X_LSM_Causal'] = df_final.c_CON_LSM_OP * df_final.c_OP_Causality_Adjusted
    #                 df_final['X_LSM_Netspeak_Direct'] = df_final.c_CON_LSM_OP_Netspeak  * df_final.c_OP_Directedness_Adjusted
    #                 df_final['X_LSM_Netspeak_Causal'] = df_final.c_CON_LSM_OP_Netspeak * df_final.c_OP_Causality_Adjusted   
    #                        
    #                 CON_Negativity_Squared_mean = float(df_final["CON_Negativity_Squared"].mean().compute())
    #                 CON_LSM_OP_Squared_mean = float(df_final["CON_LSM_OP_Squared"].mean().compute())
    #                 CON_LSM_OP_Netspeak_Squared_mean = float(df_final["CON_LSM_OP_Netspeak_Squared"].mean().compute())
    #                         
    #                 df_final['c_CON_Negativity_Squared'] = df_final.CON_Negativity_Squared - CON_Negativity_Squared_mean
    #                 df_final['c_CON_LSM_OP_Squared'] = df_final.CON_LSM_OP_Squared - CON_LSM_OP_Squared_mean
    #                 df_final['c_CON_LSM_OP_Netspeak_Squared'] = df_final.CON_LSM_OP_Netspeak_Squared - CON_LSM_OP_Netspeak_Squared_mean
    #                          
    #                 df_final['X_Neg2_Direct'] = df_final['c_CON_Negativity_Squared'] * df_final['c_OP_Directedness_Adjusted']
    #                 df_final['X_Neg2_Causal'] = df_final['c_CON_Negativity_Squared'] * df_final['c_OP_Causality_Adjusted']
    #                 df_final['X_LSM2_Direct'] = df_final['c_CON_LSM_OP_Squared'] * df_final['c_OP_Directedness_Adjusted']
    #                 df_final['X_LSM2_Causal'] = df_final['c_CON_LSM_OP_Squared'] * df_final['c_OP_Causality_Adjusted']
    #                 df_final['X_LSM_Netspeak2_Direct'] = df_final['c_CON_LSM_OP_Netspeak_Squared'] * df_final['c_OP_Directedness_Adjusted']
    #                 df_final['X_LSM_Netspeak2_Causal'] = df_final['c_CON_LSM_OP_Netspeak_Squared'] * df_final['c_OP_Causality_Adjusted']
    #                            
    #                 #             #drop intermediary columns
    #                 #             df.drop(df[['CON_Negativity_Adjusted_mean','CON_LSM_OP_mean', 'OP_Directedness_Adjusted_mean', 'OP_Causality_Adjusted_mean', 'CON_Negativity_Squared_mean', 'CON_LSM_OP_Squared_mean']], axis=1, inplace=True)
    #                 TOTAL_likes_mean = float(df_final['TOTAL_likes'].mean().compute())
    #                 df_final['TOTAL_likes_adjusted'] = df_final.TOTAL_likes/TOTAL_likes_mean
    #                 TOTAL_comments_mean = float(df_final['TOTAL_comments'].mean().compute())
    #                 df_final['TOTAL_comments_adjusted'] = df_final.TOTAL_comments/TOTAL_comments_mean
    #                 TOTAL_shares_mean = float(df_final['TOTAL_shares'].mean().compute())
    #                 df_final['TOTAL_shares_adjusted'] = df_final.TOTAL_shares/TOTAL_shares_mean
    #                 FGC_comments_mean = float(df_final['FGC_comments'].mean().compute())
    #                 df_final['FGC_comments_adjusted'] = df_final.FGC_comments/FGC_comments_mean
    #                 UGC_comments_mean = float(df_final['UGC_comments'].mean().compute()) 
    #                 df_final['UGC_comments_adjusted'] = df_final.UGC_comments/UGC_comments_mean
    #                 T_Duration_mean = float(df_final['T_Duration'].mean().compute())
    #                 df_final['T_Duration_adjusted'] = df_final.T_Duration/T_Duration_mean
    #                 post_UGC_Affect_mean_mean = float(df_final['Post_UGC_Affect_mean'].mean().compute())
    #                 df_final['Post_UGC_Affect_mean_adjusted']=df_final['Post_UGC_Affect_mean']/post_UGC_Affect_mean_mean
    #                 post_UGC_negemo_mean_mean = float(df_final['Post_UGC_negemo_mean'].mean().compute())
    #                 df_final['Post_UGC_negemo_mean_adjusted']=df_final['Post_UGC_negemo_mean']/post_UGC_negemo_mean_mean
    #                 user_numbers=df_final.groupby('user_id')['post_id_summary'].count().compute()
    #                 df_final['Frequency_USER'] = df_final.apply(lambda row: user_numbers[row['user_id']] if not (row['user_id'] != row['user_id']) else "", axis=1, meta=('x', 'f8'))
    #     
    #                 T_Reaction_mean = float(df_final['T_Reaction'].mean().compute())
    #                 df_final['T_Reaction_adjusted'] = df_final.T_Reaction/T_Reaction_mean
    # 
    #                 df_final['posemo_before']=(df_final['posemo'].rolling(window=2, min_periods=0).sum())-df_final['posemo'].astype(float)
    #                 df_final['negemo_before']=(df_final['negemo'].rolling(window=2, min_periods=0).sum())-df_final['negemo'].astype(float)
    #                 df_final['posemo_3meanback']=((df_final['posemo'].rolling(window=4, min_periods=0).sum())-df_final['posemo'].astype(float))/3
    #                 df_final['posemo_3meanforward']=((df_final['posemo'].rolling(window=7, min_periods=0,center=True).sum())-(df_final['posemo_3meanback']*3)-df_final['posemo'].astype(float))/3
    #                 df_final['posemo_3meandiff']=df_final['posemo_3meanforward']-df_final['posemo_3meanback']
    #                 df_final['negemo_3meanback']=((df_final['negemo'].rolling(window=4, min_periods=0).sum())-df_final['negemo'].astype(float))/3
    #                 df_final['negemo_3meanforward']=((df_final['negemo'].rolling(window=7, min_periods=0,center=True).sum())-(df_final['negemo_3meanback']*3)-df_final['negemo'].astype(float))/3
    #                 df_final['negemo_3meandiff']=df_final['negemo_3meanforward']-df_final['negemo_3meanback']
    #                 df_final['posemo-negemo_3meandiff']=df_final['posemo_3meandiff']-df_final['negemo_3meandiff']
    #                
    #                 ######Join with like_features 
    #                 #Read in file and select relevant columns
    #                 #Uebergangsloesuing
    #                 likefeaturesfilename = filename.split('_merged')[0]
    #                 lfcols = ['status_published_unix_UTC_Zurich', 'user_id', 'likes_change1y', 'likes_change1q', 'likes_change1m', 'likes_change1w', 'likes_attraction', 'likes_aversion', 'likes_acquisition', 'fgc_likes_change1y', 'fgc_likes_change1q', 'fgc_likes_change1m', 'fgc_likes_change1w', 'fgc_likes_attraction', 'fgc_likes_aversion', 'fgc_likes_acquisition', 'ugc_likes_change1y', 'ugc_likes_change1q', 'ugc_likes_change1m', 'ugc_likes_change1w', 'ugc_likes_attraction', 'ugc_likes_aversion', 'ugc_likes_acquisition', 'overall_likes_change1y', 'overall_likes_change1q', 'overall_likes_change1m', 'overall_likes_change1w', 'overall_fgc_likes_change1y', 'overall_fgc_likes_change1q', 'overall_fgc_likes_change1m', 'overall_fgc_likes_change1w', 'overall_ugc_likes_change1y', 'overall_ugc_likes_change1q', 'overall_ugc_likes_change1m', 'overall_ugc_likes_change1w', 'issuer_likes_change1y', 'issuer_likes_change1q', 'issuer_likes_change1m', 'issuer_likes_change1w', 'issuer_likes_attraction', 'issuer_likes_aversion', 'issuer_fgc_likes_change1y', 'issuer_fgc_likes_change1q', 'issuer_fgc_likes_change1m', 'issuer_fgc_likes_change1w', 'issuer_ugc_likes_change1y', 'issuer_ugc_likes_change1q', 'issuer_ugc_likes_change1m', 'issuer_ugc_likes_change1w']
    #                 lfdefs = {'status_published_unix_UTC_Zurich': str, 'user_id': str, 'likes_change1y': float, 'likes_change1q': float, 'likes_change1m': float, 'likes_change1w': float, 'likes_attraction': float, 'likes_aversion': float, 'likes_acquisition': float, 'fgc_likes_change1y': float, 'fgc_likes_change1q': float, 'fgc_likes_change1m': float, 'fgc_likes_change1w': float, 'fgc_likes_attraction': float, 'fgc_likes_aversion': float, 'fgc_likes_acquisition': float, 'ugc_likes_change1y': float, 'ugc_likes_change1q': float, 'ugc_likes_change1m': float, 'ugc_likes_change1w': float, 'ugc_likes_attraction': float, 'ugc_likes_aversion': float, 'ugc_likes_acquisition': float, 'overall_likes_change1y': float, 'overall_likes_change1q': float, 'overall_likes_change1m': float, 'overall_likes_change1w': float, 'overall_fgc_likes_change1y': float, 'overall_fgc_likes_change1q': float, 'overall_fgc_likes_change1m': float, 'overall_fgc_likes_change1w': float, 'overall_ugc_likes_change1y': float, 'overall_ugc_likes_change1q': float, 'overall_ugc_likes_change1m': float, 'overall_ugc_likes_change1w': float, 'issuer_likes_change1y': float, 'issuer_likes_change1q': float, 'issuer_likes_change1m': float, 'issuer_likes_change1w': float, 'issuer_likes_attraction': float, 'issuer_likes_aversion': float, 'issuer_fgc_likes_change1y': float, 'issuer_fgc_likes_change1q': float, 'issuer_fgc_likes_change1m': float, 'issuer_fgc_likes_change1w': float, 'issuer_ugc_likes_change1y': float, 'issuer_ugc_likes_change1q': float, 'issuer_ugc_likes_change1m': float, 'issuer_ugc_likes_change1w': float}
    #                 parse_dates = ['status_published_unix_UTC_Zurich']
    #                 df_likefeatures = pd.read_csv('./like_features/'+likefeaturesfilename+'_merged_likefeatures.csv', parse_dates = parse_dates, sep="\t",header='infer', usecols=lfcols,dtype=lfdefs)
    #                 df_likefeatures['index'] = pd.DatetimeIndex(df_likefeatures['status_published_unix_UTC_Zurich'])
    #                 df_likefeatures['index_int']= df_likefeatures['index'].astype(np.int64) // 10**9
    #                 df_likefeatures['index_string']=df_likefeatures['index_int'].astype(float).round(decimals=1).astype(str)
    #                 df_likefeatures['post_id_new']=df_likefeatures['index_string']+df_likefeatures['user_id']
    #                 
    #                 df_likefeatures.set_index(['post_id_new'], drop=True, inplace=True, append=False)
    #                 df_likefeatures.drop(['index','index_int','index_string','status_published_unix_UTC_Zurich','user_id'], inplace=True, axis=1)
    #                 
    #                 
    #                 df_final['new_id'] = df_final['status_published_unix_UTC_Zurich']+df_final['user_id']
    #                 
    #                 
    #                 df_final = df_final.join(df_likefeatures, on='new_id', how='left')
    #                 
    #                 
    #                 
    #           
    #                 
    #                 #left join on post_id
    #                 #respectively for now on 
    # 
    #                 finalfilename='./features_final/'+filename_out+'_features-*.csv'
    #                 new_final_columns = ["c_CON_Negativity_Adjusted","c_CON_LSM_OP","c_CON_LSM_OP_Netspeak","c_OP_Directedness_Adjusted",'c_OP_Causality_Adjusted','X_Neg_Direct','X_Neg_Causal','X_LSM_Direct','X_LSM_Causal','X_LSM_Netspeak_Direct','X_LSM_Netspeak_Causal','c_CON_Negativity_Squared','c_CON_LSM_OP_Squared','c_CON_LSM_OP_Netspeak_Squared','X_Neg2_Direct','X_Neg2_Causal','X_LSM2_Direct','X_LSM2_Causal','X_LSM_Netspeak2_Direct','X_LSM_Netspeak2_Causal','TOTAL_likes_adjusted','TOTAL_comments_adjusted','TOTAL_shares_adjusted','FGC_comments_adjusted','UGC_comments_adjusted','T_Duration_adjusted','Post_UGC_Affect_mean_adjusted','Post_UGC_negemo_mean_adjusted','Frequency_USER','T_Reaction_adjusted','posemo_before', 'negemo_before', 'posemo_3meanback', 'posemo_3meanforward', 'posemo_3meandiff', 'negemo_3meanback', 'negemo_3meanforward', 'negemo_3meandiff', 'posemo-negemo_3meandiff' , 'user_posemo_sum' , 'user_posemo_mean' , 'user_negemo_sum' , 'user_negemo_mean', "likes_change1y", "likes_change1q", "likes_change1m", "likes_change1w", "likes_attraction", "likes_aversion", "likes_acquisition", "fgc_likes_change1y", "fgc_likes_change1q", "fgc_likes_change1m", "fgc_likes_change1w", "fgc_likes_attraction", "fgc_likes_aversion", "fgc_likes_acquisition", "ugc_likes_change1y", "ugc_likes_change1q", "ugc_likes_change1m", "ugc_likes_change1w", "ugc_likes_attraction", "ugc_likes_aversion", "ugc_likes_acquisition", "overall_likes_change1y", "overall_likes_change1q", "overall_likes_change1m", "overall_likes_change1w", "overall_fgc_likes_change1y", "overall_fgc_likes_change1q", "overall_fgc_likes_change1m", "overall_fgc_likes_change1w", "overall_ugc_likes_change1y", "overall_ugc_likes_change1q", "overall_ugc_likes_change1m", "overall_ugc_likes_change1w", "issuer_likes_change1y", "issuer_likes_change1q", "issuer_likes_change1m", "issuer_likes_change1w", "issuer_likes_attraction", "issuer_likes_aversion", "issuer_fgc_likes_change1y", "issuer_fgc_likes_change1q", "issuer_fgc_likes_change1m", "issuer_fgc_likes_change1w", "issuer_ugc_likes_change1y", "issuer_ugc_likes_change1q", "issuer_ugc_likes_change1m", "issuer_ugc_likes_change1w"]
    #                 final_columns = imcolumnames[:-1]
    #                 final_columns.extend(new_final_columns)
    #                 df_final.drop_duplicates().to_csv(finalfilename, sep='\t',index=False,encoding='utf-8',columns=final_columns)
    #                 os.remove("./features_immediate/"+filename_out+"_features.csv")
    #                 del user_numbers
    #                 del df_final
            else:
                print("WARNING: no posts found in file %s" % (filename_out))
                os.remove("./features_immediate/"+filename_out+"_features.csv")
        return dropcnt
    except Exception as e:
        print("caught exception: %s at file: %s - DO THIS ONE AGAIN") % (e,filename_raw)
        return 0
                

###################################################################################################
####entry point which organizes multiprocessing
####################################################################################################
if __name__ == '__main__':
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname("__filename__")))         # Setzt Pfad gleich Ablagepfad der .py Datei
    files = glob.glob('./*_feed_*.csv')   
    totalfiles = len(files)
    if not os.path.exists("features_new"):
        os.makedirs("features_new")
    
    if not os.path.exists("features_immediate"):
        os.makedirs("features_immediate")
        
    if not os.path.exists("features_final"):
        os.makedirs("features_final")
 
    #get the list of files which already are processed in order to skip them afterwords  
    files_done = glob.glob('./features_final/*.csv')
    
    if os.name == 'nt':
        files_done = [filedone.split("\\")[1].split("_")[0] for filedone in files_done] #this windows
    else:
        files_done = [filedone.split("/")[2].split("_")[0] for filedone in files_done] #this linux
    
    
    #filecount=0
    #dropcnt=0
    #for filename_raw in files:
    files_formated = dict()
    if os.name == 'nt':
        for filename_raw in files:
            files_formated[filename_raw] = filename_raw.split("\\")[1].split("_")[0] #this windows
    else:
        for filename_raw in files:
            files_formated[filename_raw] = filename_raw.split("/")[1].split("_")[0] #this linux
    
    #check whether we have likeinfo for the liwc files
#     files_likes = glob.glob('./like_features/*.csv')
#     if os.name == 'nt':
#         files_likes = [filedone.split("\\")[1].split("_")[0] for filedone in files_likes] #this windows
#     else:
#         files_likes = [filedone.split("/")[2].split("_")[0] for filedone in files_likes] #this linux
#     
#     
    
    #kick out the ones already done
    #files_selected = [rawname for rawname,thefile in files_formated.iteritems() if ((thefile not in files_done) and (thefile in files_likes))]
    files_selected = [rawname for rawname,thefile in files_formated.iteritems() if ((thefile not in files_done))]
   
    
    #files_selected = [rawname for rawname,thefile in files_formated.iteritems() if thefile not in files_done]

    pool = Pool(4)
    dropcntsum = 0
    filecnt = 0
    filetotal = len(files_selected )
    for dropcnt in pool.imap_unordered(process_file, files_selected):
        filecnt += 1
        dropcntsum += dropcnt
        print("%d of %d files processed at time %s" % (filecnt,filetotal,str(datetime.now()))) 
    print("finished with all  at "+str(datetime.now()))
    print("overall number of dropped lines during process: ")
    print(dropcntsum)
    os.rmdir("./features_immediate")
    os.rmdir("./features_new")

