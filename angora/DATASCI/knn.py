##encoding=utf8

"""

compatibility: compatible to python2 and python3

prerequisites: sklearn, numpy

import:
    from angora.DATASCI.knn import dist, knn_find, knn_classify, knn_impute
"""

from __future__ import print_function
from sklearn.neighbors import DistanceMetric
from sklearn.neighbors import NearestNeighbors
from sklearn.neighbors import KNeighborsClassifier
        
def dist(X, Y, distance_function = "euclidean"):
    """calculate X, Y distance matrix
    [Args]
    ------
    X : m samples
    Y : n samples
    distance_function : user_defined distance
    
    [Returns]
    ---------
    distance_matrix: n * m distance matrix
    
    
    we have those built-in function. Default = euclidean
    
    "euclidean"    EuclideanDistance    sqrt(sum((x - y)^2))
    "manhattan"    ManhattanDistance    sum(|x - y|)
    "chebyshev"    ChebyshevDistance    sum(max(|x - y|))
    "minkowski"    MinkowskiDistance    sum(|x - y|^p)^(1/p)
    "wminkowski"    WMinkowskiDistance    sum(w * |x - y|^p)^(1/p)
    "seuclidean"    SEuclideanDistance    sqrt(sum((x - y)^2 / V))
    "mahalanobis"    MahalanobisDistance    sqrt((x - y)' V^-1 (x - y))
    """
    distance_calculator = DistanceMetric.get_metric(distance_function)
    return distance_calculator.pairwise(X, Y)

def knn_find(train, test, k = 2):
    """find first K knn neighbors of test samples from train samples
    
    [Args]
    ----
    train: train data {array like, m x n, m samples, n features}
        list of sample, each sample are list of features.
        e.g. [[age = 18, weight = 120, height = 167],
              [age = 45, weight = 180, height = 173],
              ..., ]
        
    test: test data {array like, m x n, m samples, n features}
        data format is the same as train data
    
    k: number of neighbors
        how many neighbors you want to find
        
    [Returns]
    -------
    distances: list of distance of knn-neighbors from test data
        [[dist(test1, train_knn1), dist(test1, train_knn2), ...],
         [dist(test2, train_knn1), dist(test2, train_knn2), ...],
         ..., ]
    
    indices: list of indice of knn-neighbors from test data
        [[test1_train_knn1_index, test1_train_knn2_index, ...],
         [test2_train_knn1_index, test2_train_knn2_index, ...],
         ..., ]    
    """
    nbrs = NearestNeighbors(n_neighbors=k, algorithm="kd_tree").fit(train) # default = "kd_tree" algorithm
    return nbrs.kneighbors(test)

def knn_classify(train, train_label, test, k=1, standardize=True):
    """classify test using KNN (k=1) algorithm
    
    usually the KNN classifier works good if all the features of the train
    data are continual value
    
    [Args]
    ------
    train: train data (see knn_find), {array like, m x n, m samples, n features}
    
    train_label: train data's label, {array like, m x n, m samples, n features}
    
    test: test data
    
    k: knn classify value
    
    standardize: remove mean and variance or not
    
    [Returns]
    ---------
    test_label: test data's label
    
    [Notice]
    --------
        The sklearn.neighbors.KNeighborsClassifier doesn't do pre-processing
        this wrapper provide an option for that
    """
    from .preprocess import prep_standardize
    if standardize:
        train, test = prep_standardize(train, test) # eliminate mean and variance
    neigh = KNeighborsClassifier(n_neighbors=k) # knn classifier
    neigh.fit(train, train_label) # training
    return neigh.predict(test) # classifying