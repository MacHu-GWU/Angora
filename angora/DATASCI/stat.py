##################################
#encoding=utf8                   #
#version =py27, py33             #
#author  =sanhe                  #
#date    =2014-10-29             #
#                                #
#    (\ (\                       #
#    ( -.-)o    I am a Rabbit!   #
#    o_(")(")                    #
#                                #
##################################

"""
Import:
    from HSH.Data.stat import find_outlier, clear_outlier_onetime, clear_outlier_literally
"""
from __future__ import print_function
import numpy as np

def find_outlier(np_array, outlier_criterion = 2):
    """return outliers in np_array
    [Args]
    ------
    np_array: 1d numpy.ndarray samples
    
    outlier_criterion: sample with n of standard deviation bias 
        from mean value will be considered as outlier
    
    [Returns]
    ---------
    outliers in np_array
    """
    if type(np_array) != np.ndarray:
        np_array = np.array(np_array)
    m, std = np_array.mean(), np_array.std()
    return np_array[ np.where( abs(np_array - m) > outlier_criterion * std )]

def clear_outlier_onetime(np_array, outlier_criterion = 2):
    """remove outliers by criterion then returns.
    [Args]
    ------
    np_array: 1d numpy.ndarray samples
    
    outlier_criterion: sample with n of standard deviation bias 
        from mean value will be considered as outlier
    
    [Returns]
    ---------
    np_array with all outliers deleted
    """
    if type(np_array) != np.ndarray:
        np_array = np.array(np_array)
    m, std = np_array.mean(), np_array.std()
    return np_array[ np.where( abs(np_array - m) <= outlier_criterion * std )]

def clear_outlier_literally(np_array, outlier_criterion = 2):
    """recurrsively remove outliers, until there's no outliers at all. Then return.
    [Args]
    ------
    np_array: 1d numpy.ndarray samples
    
    outlier_criterion: sample with n of standard deviation bias 
        from mean value will be considered as outlier
    
    [Returns]
    ---------
    np_array with all outliers deleted
    """
    if type(np_array) != np.ndarray:
        np_array = np.array(np_array)
    while 1:
        n_before = np_array.size
        np_array = clear_outlier_onetime(np_array, outlier_criterion)
        if n_before == np_array.size:
            return np_array
