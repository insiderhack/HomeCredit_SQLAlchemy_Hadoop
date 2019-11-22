import pandas as pd
import numpy as np
import psycopg2
import sqlalchemy as db
import os


#func for list file on my folder dataset/
def list_file():
    path = 'dataset/'
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.csv' in file:
                files.append(os.path.join(file))
    return files, path

#secondary func for read the dataset only
def list_df():
    path = 'dataset/'
    df_lst = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.csv' in file:
                df_lst.append(os.path.join(file).split(".")[0])
    return df_lst

#contructing dataset using pd.dataframe from dataset folder (its only read csv)
def construct_ds():
    print('Dataset Loaded:')
    print('')
    path = 'dataset/'
    mod = 'models.'
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.csv' in file:
                files.append(os.path.join(file))
    for f in files:
        print(mod+f.split(".")[0])
        exec("%s = pd.read_csv('%s')" % (f.split(".")[0],path+f),globals())

#limiting the dataset, oh is refer to dataset, yes is the limitter use number only and dont input yes > len(dataset)
def limit_ds(oh, yes):
    no = 1
    limit = oh.sample(n=yes, random_state=no, replace=False)
    return limit

#filtering the dataset where, what, who
def compare_ds(umm, duh, ah ):
    compar = umm[umm[duh].isin(ah[duh])]
    return compar

#merger to left function using format (df1, sufixes1, keyword on, df2, sufixes2)
def mergeleft_ds(dul, yum, ih, dal,yam):
    merlef = dul.merge(dal, on=(ih), how='left', suffixes=(yum, yam))
    return merlef