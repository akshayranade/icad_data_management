"""
This function returns central tendancey for a target variable as a group of input attributes
"""

#Import libraries
import pandas as pd
import csv
import string
import os
from pandas.api.types import is_numeric_dtype
import re


#This is a generic function to calculate the central tendancies - mean, median & mode for a target attribute w.r.t. input attribute(s) in a dataframe
def central_tendancies(df, inputAtt:list, targetAtt):
    
    df_columns = list(df)
    
    #Function to check whether both inputAtt list & targetAtt are a part of dataframe df
    def search_func(to_check, columns):
        not_present = []
        
        for i in to_check:
            if i not in columns:
                #If the column is not present it will get appended to the list
                not_present.append(i)
        return len(not_present)

    #Check that the targetAtt is numeric in nature. If not return None
    if (search_func(inputAtt, df_columns) == 0) & (targetAtt in df):
        
    #Using the search_func check if the give list of columns is a part of dataframe
        if is_numeric_dtype(df[targetAtt]):
            #Find mean and median first
            mean_median = df\
            .groupby(inputAtt)[targetAtt]\
            .agg({'mean', 'median'})\
            .rename(columns={'mean':f'mean_{targetAtt}','median':f'median_{targetAtt}'}) \
            .reset_index()
        
            #Find mode
            mode = df\
            .groupby(inputAtt)[targetAtt]\
            .apply(lambda x: x.mode().iloc[0])\
            .reset_index()\
            .rename(columns={targetAtt:f'mode_{targetAtt}'})
        
            #join the dataframes to get all central tendancies
            central_tend = mean_median.join(mode,\
                                            how='inner',\
                                            lsuffix='_left',\
                                            rsuffix='_right')

            
            #drop repeated columns
            central_tend = central_tend.drop(columns=[col + '_right' for col in inputAtt])
        
            #Define dict to rename columns 
            rename_dict = {}
            for col in inputAtt:
                rename_dict.update({col+'_left':col})
        
            #Rename columns with suffix '_left' with original names
            central_tend.rename(columns=rename_dict, inplace=True)
            
            #print(central_tend.head(5))
            
            return central_tend
        
        else:
            return None
    
    else:
        return None