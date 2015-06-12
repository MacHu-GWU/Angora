##encoding=utf-8

"""
Copyright (c) 2015 by Sanhe Hu
------------------------------
    Author: Sanhe Hu
    Email: husanhe@gmail.com
    Lisence: LGPL
    
Keyword
-------
    MachineLearning
    
    
Compatibility
-------------
    Python2: Yes
    Python3: Yes
    
    
Prerequisites
-------------
    numpy, sklearn, scipy
    
Import Command
--------------
    from angora.DATASCI.experiment import traintest_seperator
"""

from __future__ import print_function
from sklearn import cross_validation
from scipy.stats import itemfreq
import numpy as np

class TrainTestSeperator():
    """This class provides some methods to split train/test data-set for cross validation
    in classification task.
    """
    def __init__(self):
        pass

    def exam_input(self, samples, labels):
        """example dimension, and returns the numpy.ndarray. Because we will use numpy index
        to access rows.
        [Args]
        ------
        samples:
            [sample1 = [feature11, feature12, ...],
             sample2 = [feature21, feature22, ...],
             ...,
             sampleN = [featureN1, featureN2, ...],]
        labels:
            [label1, label2, ..., labelN]
        """
        if len(samples) != len(labels):
            lines = ["samples and labels must have same size!",
                     "your samples size = %s, labels size = %s" % (len(samples), len(labels))]
            raise Exception("\n".join(lines))
        
        if not isinstance(samples, np.ndarray):
            samples = np.array(samples)
        if not isinstance(labels, np.ndarray):
            labels = np.array(labels)
            
        return samples, labels

    def KFold(self, samples, labels, n_folds=10):
        """
        将全部样本完全打乱顺序, 然后分成n_folds份, 每次取其中的1份作为test, n_folders-1份作为train。
        重复n_folders次。最后取识别率的平均值。
        """
        samples, labels = self.exam_input(samples, labels)
        shuffled_index = np.random.permutation(len(labels))
        
        for train_index, test_index in cross_validation.KFold(len(labels), n_folds=n_folds):
            train_index, test_index = shuffled_index[train_index], shuffled_index[test_index]
            yield samples[train_index], labels[train_index], samples[test_index], labels[test_index]
    
    def KFold_by_label(self, samples, labels, n_folds=10):
        """
        将全部样本按照类标分开, 然后对于每类的份执行Kfold, 最后将其合并。换言之假如samples的类标有
        1, 2, 3... 那么执行KFold_by_label之后能保证test中类标1, 类标2, ... 的数量都是全部类标1, 类
        标2, ... 的1/n_folds。这样能够避免出现在train中完全没有出现类标n, 而test中大量出现类标n的
        情况。
        注: 本算法使用生成器输出, 确保了不会出现消耗: 总数据 x n_folds 这么多内存的情况出现
        """
        samples, labels = self.exam_input(samples, labels)
        histgram = itemfreq(labels) # [[item1, freq1], [item2, freq2], ...]
        
        most_rare_label, most_rare_freq = None, len(labels)
        for item, freq in histgram:
            if freq <= most_rare_freq:
                most_rare_freq = freq
                most_rare_label = item
            
        if freq < n_folds:
            print("Warning! label %s only present %s time!" % (most_rare_label, most_rare_freq))
        
        generator_list = list()
        for label in histgram.T[0]: # 对于每个unique label, 取出各自的子集
            sub_index = np.where(labels == label)[0]
            sub_samples, sub_labels = samples[sub_index], labels[sub_index]
            # 对其子集使用KFold方法生成子train, train_label, test, test_label
            generator_list.append(self.KFold(sub_samples, sub_labels)) 
            
        # 从生成器解包, 重新组装成train, train_label, test, test_label
        # tp = (
        #     (label1_train, label1_train_label, label1_test, label1_test_label),
        #     (label2_train, label2_train_label, label2_test, label2_test_label),
        #     ...
        #     )
        for tp in zip(*generator_list): 
            train, train_label, test, test_label = list(), list(), list(), list()
            for sub_train, sub_train_label, sub_test, sub_test_label in tp:
                for i in sub_train:
                    train.append(i)
                for i in sub_train_label:
                    train_label.append(i)
                for i in sub_test:
                    test.append(i)
                for i in sub_test_label:
                    test_label.append(i)
            yield np.array(train), np.array(train_label), np.array(test), np.array(test_label)
        
traintest_seperator = TrainTestSeperator()

if __name__ == "__main__":
    import unittest
    ttsep = TrainTestSeperator()
    class everythingUnittest(unittest.TestCase):
        def test_exam_input(self):
            samples = [[1,2], [3,4]]
            labels = [0, 1]
            samples, labels = ttsep.exam_input(samples, labels)
            self.assertIsInstance(samples, np.ndarray)
            self.assertIsInstance(labels, np.ndarray)
            
            samples1 = [[1,2], [3,4]]
            labels1 = [0, 1, 2]
            self.assertRaises(Exception, ttsep.exam_input, samples1, labels1)
        
        def test_KFold(self):
            # === create test case data ===
            samples = np.array([[i, i, i] for i in range(1, 51)] + [[-i, -i, -i] for i in range(1, 51)])
            labels = np.array([1 for _ in range(50)] + [-1 for _ in range(50)])
             
            for train, train_label, test, test_label in ttsep.KFold(samples, labels, n_folds=10):
                for i, j in zip(test, test_label):
                    self.assertGreater(i[0] * j, 0) # 同正负, 说明分拆正确
        
        def test_KFold_by_label(self):
            # === create test case data ===
            samples = np.array([[i, i, i] for i in range(1, 81)] + [[-i, -i, -i] for i in range(1, 21)])
            labels = np.array([1 for _ in range(80)] + [-1 for _ in range(20)])
            for train, train_label, test, test_label in ttsep.KFold_by_label(samples, labels, n_folds=10):
                histgram = itemfreq(test_label)
                self.assertEqual(histgram[0][1], 2) # -1的频数是2
                self.assertEqual(histgram[1][1], 8) # 1的频数是8
            
    unittest.main()