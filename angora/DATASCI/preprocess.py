##encoding=utf8

"""

import:
    from angora.DATASCI.preprocess import prep_standardize, knn_impute
"""

from __future__ import print_function
from sklearn import preprocessing
import numpy as np

def prep_standardize(train, test, enable_verbose = False):
    """pre-processing, standardize data by eliminating mean and variance
    """
    scaler = preprocessing.StandardScaler().fit(train) # calculate mean and variance
    train = scaler.transform(train) # standardize train
    test = scaler.transform(test) # standardize test
    if enable_verbose:
        print("mean = %s" % scaler.mean_)
        print("var = %s" % scaler.std_)
    return train, test

def knn_impute(train, k):
    """nearest neighbor data imputation algorithm
    [Args]
    ------
    train: data set with missing value, {array like, m x n, m samples, n features}
    
    k: use the first k nearest neighbors' mean to fill the missing-value cell
    
    [Returns]
    ---------
    train: with filled missing value
    
    """
    from .knn import knn_find
    for i in np.where(np.isnan(train).sum(axis=1)!=0)[0]: # for the row has NA value
        sample = train[i] # i = row index, sample = ith sample in train
        na_col_ind, usable_col_ind = (np.where(np.isnan(sample) )[0], # NA value column index
                                      np.where(~np.isnan(sample))[0]) # Non-NA value column index 
        usable_row_ind = np.where(np.isnan(train[:, usable_col_ind]).sum(axis=1)==0)[0] # Non-NA row index if in Non-NA value column index has no NA value
        
        sub_train = train[np.ix_(usable_row_ind, usable_col_ind)] # select sub data set
        scaler = preprocessing.StandardScaler().fit(sub_train) # find the mean and var to remove
        
        if k**2 > sub_train.shape[0]: # usually to ensure we have more than k non-va value 
            potential_k = sub_train.shape[0] # we have to find k**2 nearest neighboor
        else:
            potential_k = k ** 2
              
        _, indices = knn_find(scaler.transform(sub_train), # standardize
                              scaler.transform(sample[usable_col_ind][np.newaxis]), # standardize
                              potential_k)
 
        candidates = train[np.ix_(usable_row_ind[indices[0]], na_col_ind)].T
         
        for j, candidate in enumerate(candidates): # use the average of first k non-NA value 
            train[(i, na_col_ind[j])] = candidate[~np.isnan(candidate)][:k+1].mean()
    
    return train