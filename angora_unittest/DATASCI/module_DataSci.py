##encoding=utf8

from __future__ import print_function
try: # 可以一次性全部倒入
    from angora.DATASCI import *
except: # 也可以单独导入
    from angora.DATASCI import knn
    from angora.DATASCI import linreg
    from angora.DATASCI import preprocess
    from angora.DATASCI import psmatcher
    from angora.DATASCI import stat
import pandas as pd, numpy as np
import time

class Knn_unittest():
    @staticmethod
    def dist():
        print("{:=^100}".format("dist unittest"))
        train, test = ([[-1, -1], [-2, -1], [-3, -2], [1, 1], [2, 1], [3, 2]], 
                       [[0,0], [1,1]])
        
#         train, test = ([[1], 
#                         [2],
#                         [3]],
#                        [[1]])
        
        distances = knn.dist(train, test)
        print("=== distance matrix ===\n%s\n" % distances)
            
    @staticmethod
    def knn_find():
        print("{:=^100}".format("knn_find unittest"))
        train, test = ([[-1, -1], [-2, -1], [-3, -2], [1, 1], [2, 1], [3, 2]], 
                       [[0,0], [1,1]])
        distances, indices = knn.knn_find(train, test, 3)
        print("=== distance matrix ===\n%s\n" % distances)
        print("=== indices matrix ===\n%s\n" % indices)
        
    @staticmethod
    def knn_classify():
        print("{:=^100}".format("knn_classify unittest"))
        train_data = pd.read_csv(r"dataset\copd_1000.txt", header=None)
        train, train_label = train_data.loc[:, [0,2,3,4,5,6,7]].values, train_data.loc[:,8].values
        test = [[81, 66, 8, 0.71, 0.67, 0.87, 1.61],
                [75, 53, 7, 0.95, 0.92, 0.98, 1.49]] ## there's no label
        test_label = knn.knn_classify(train, train_label, test, k=1)
        print(test_label)


class Linreg_unittest():
    @staticmethod
    def linreg_predict():
        print("{:=^100}".format("linreg_predict unittest"))
        x = np.array([1,2,3,4,5,6,7,8,9,10])
        y = [3.4, 5.4, 4.3, 2.1, 7.8, 9.2, 11.4, 14.5, 17.3, 19.3]
        print(linreg.linreg_predict(x, y, x))

    @staticmethod
    def linreg_coef():
        print("{:=^100}".format("linreg_coef unittest"))
        x = np.array([1,2,3,4,5,6,7,8,9,10])
        y = [3.4, 5.4, 4.3, 2.1, 7.8, 9.2, 11.4, 14.5, 17.3, 19.3]
        print(linreg.linreg_coef(x, y))
        
    @staticmethod
    def glance_2d():
        print("{:=^100}".format("glance_2d unittest"))
        x = np.array([1,2,3,4,5,6,7,8,9,10])
        y = [3.4, 5.4, 4.3, 2.1, 7.8, 9.2, 11.4, 14.5, 17.3, 19.3]
        linreg.glance_2d(x,y)
        
        
class Preprocess_unittest():
    @staticmethod
    def prep_standardize():
        print("{:=^100}".format("prep_standardize unittest"))
        train = np.random.randn(3, 3.)
        test = np.array([[1., 2., 3.],
                         [4., 5., 6.],
                         [7., 8., 9.]])
        std_train, std_test = preprocess.prep_standardize(train, test)
        print(std_train)
        print(std_test)
    
    @staticmethod
    def knn_impute():
        print("{:=^100}".format("knn_impute unittest"))
        train_data = pd.read_csv(r"dataset\copd_1000_missing.txt", header=None)
        
        ## === time complexity test
        st = time.clock()
        filled_data = preprocess.knn_impute(train_data.values, 5)
        print("elapse time = ", time.clock() - st)
        ## === compare original value and filled value ===
        filled_ind = np.where(np.isnan(train_data.values))
        
        origin = pd.read_csv(r"dataset\copd_1000.txt", header=None).values[filled_ind]
        predict = filled_data[filled_ind]
        print("{:=^80}\n".format("origin"), origin)   # the data we deleted
        print("{:=^80}\n".format("predict"), predict)  # the date we imputed
        print("{:=^80}\n".format("percentage difference"), abs(origin-predict)/predict) # percentage difference
        
        
class Psmatcher_unittest():
    @staticmethod
    def stratified_matching():
        print("{:=^100}".format("stratified_matching unittest"))
        data = pd.read_csv(r"dataset\re78.csv", index_col=0) # read all data
        control, treatment = (data[data["treat"] == 0], # split to control and treatment
                              data[data["treat"] == 1])
        
        psm_control, psm_treatment = (control.loc[:, "treat":"married"].values, # select the columns
                                      treatment.loc[:, "treat":"married"].values) # you want to use. number column only
                
        indices = psmatcher.stratified_matching(psm_control, psm_treatment, stratified_col = [[1],[3],[0,2,4],[5]])
        selected_control_index, selected_for_each_treatment = psmatcher.index_matching(indices, k = 1)
        
        for i, j in zip(psm_treatment, selected_for_each_treatment):
            print("===============")
            print(i, type(i))
            print(psm_control[j])
            
    @staticmethod
    def psm():
        print("{:=^100}".format("psm unittest"))    
        data = pd.read_csv(r"dataset\re78.csv", index_col=0) # read all data
        control, treatment = (data[data["treat"] == 0].values, # split to control and treatment
                              data[data["treat"] == 1].values)

        selected_control, selected_control_foreach = psmatcher.psm(control, treatment,
                                                                   use_col = [0, 1, 2, 3, 4, 5], # choose columns for matching
                                                                   stratified_col = [[1],[3],[0,2,4],[5]], k = 1) # define stratified order
        
        for tr, ct in zip(treatment, selected_control_foreach):
            print("==============")
            print(tr.tolist())
            print("------")
            print(ct.tolist())
    

class Stat_unitest():
    @staticmethod
    def outlier():
        data = np.array([3, 2.9, 3.1, 4.2, 2.7, 3.5, 6.8, 12.7])
        print(stat.clear_outlier_onetime(data) )
        print(stat.clear_outlier_literally(data) )
        print(stat.find_outlier(data) )
            
if __name__ == "__main__":
#     Knn_unittest.dist()
#     Knn_unittest.knn_find()
#     Knn_unittest.knn_classify()
#  
#     Linreg_unittest.linreg_predict()
#     Linreg_unittest.linreg_coef()
#     Linreg_unittest.glance_2d()
#    
#     Preprocess_unittest.prep_standardize()
#     Preprocess_unittest.knn_impute()
#    
#     Psmatcher_unittest.stratified_matching()
    Psmatcher_unittest.psm()
 
#     Stat_unitest.outlier()
    
    
    print("COMPLETE")