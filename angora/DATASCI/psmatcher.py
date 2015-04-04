##encoding=UTF8

"""
============================================
Propensity score matching (PSM) 倾向评分匹配
============================================
PSM主要用于生物统计中。在生物统计中，很多实验都要设置
"对照组/控制组（control group）"和"实验组（treatment group）"。
在一个精心设计的实验中，对照组和实验组通常在除了实验所导致的结果
部分，都要保证其他情况尽量一致，这样实验结果才有说服力。例如我们
研究X药品对A疾病的治疗效果：

    如果我们选择100个病人，他们的身体状况都比较相似，给其中50个人
    吃药，另外50个人不吃药。如果过了一个月对照组中的病人有30%身体
    恶化，10%的状态不变，10%的稍许好转。而实验组中的病人有30%都有
    好转，那么我们大体上可以说明这个药是有效的。

    
而在实际的实验中我们通常做不到这一点。因为我们很难找到大量的状态
相似的实验对象。同样是X药品和A疾病的例子：

    我们在测试A药对B病的治疗效果，于是我们对1000名病病人做实验，
    其中50人吃了药，另外950人什么其他药也没吃。而在选择实验对象时，
    并没有对病人的其他身体情况做预先的调查。
    
    所以为了研究X药的有效性，我们必须从950个病人的对照组中选取
    合适的子集用来验证
    === 下面是sanhe自己的理解 ===
    选择子集的方式：
        方法1. 对于实验组中的每一个，分层或者不分层找寻k个knn近邻。
        然后最终我们得到k*50个样本集。
        所谓分层是：
            比如我们按年龄分层，那么我们在找k近邻时

参考文献
--------
    http://en.wikipedia.org/wiki/Propensity_score_matching
    R版本的实现，和测试数据
    http://cran.r-project.org/web/packages/MatchIt/MatchIt.pdf

=========================
PSM python implementation
=========================

Test Data Format
----------------
path = HSH\DataSci\demo_data\re78.csv
A data frame with 313 observations (185 treated, 429 control). There are 10 variables measured for
each individual. "treat" is the treatment assignment (1=treated, 0=control). "age" is age in years.
"educ" is education in number of years of schooling. "black" is an indicator for African-American
(1=African-American, 0=not). "hispan" is an indicator for being of Hispanic origin (1=Hispanic,
0=not). "married" is an indicator for married (1=married, 0=not married). "nodegree" is an indicator
for whether the individual has a high school degree (1=no degree, 0=degree). "re74" is income in
1974, in U.S. dollars. "re75" is income in 1975, in U.S. dollars. "re78" is income in 1978, in U.S.
dollars.

Import:
    from HSH.DataSci.psmatcher import psm, stratified_matching, index_matching
os:
    from HSH.DataSci import psmatcher
"""

from __future__ import print_function
from .knn import dist
from .preprocess import prep_standardize
from angora.DATA.dtype import OrderedSet
import pandas as pd, numpy as np

def stratified_matching(control, treatment, stratified_col = None):
    """Propensity score matching using stratification matching
    [Args]
    ------
    control: control group data, HAS TO BE ALL NUMBER NUMPY.NDARRAY
        control = [sample_1, sample_2, ..., sample_n] 
        sample = [feature1, feature2, ..., feature_k]
    
    treatment: treatment group data, HAS TO BE ALL NUMBER NUMPY.NDARRAY
        treatment = [sample_1, sample_2, ..., sample_n]
        sample = [feature1, feature2, ..., feature_k]
    
    stratified_col:
        e.g. stratified_col = [[3], [1], [0, 2], [4]], control has k = 5 columns
        so we first stratify column treatment[[3]],
        then stratify column treatment[[1]], then
        matching by Knn distance using treatment[[0,2]], then
        stratify column treatment[[4]]

    [Returns]
    ---------
    Matched control group samples
    """
    
    if len(treatment.shape) == 1: # 如果是一维行向量
        treatment = treatment[np.newaxis] # 转化成二维但是只有一行的行向量
    
    if stratified_col == None: # if stratified_col not defined, use normal knn matching mode
        stratified_col = list(range(control.shape[1]))
    
    if (control.dtype == np.float64) & (treatment.dtype == np.float64):
        std_control, std_treatment = prep_standardize(control, treatment) # pre-processing standardization
    else:
        std_control, std_treatment = prep_standardize(control.astype(np.float64), 
                                                      treatment.astype(np.float64))
    
    """
    [CN]
    对于每个treatment_sample，我们按照stratified列的顺序，计算对于被分层的列，control和treatment的距离。
    如果是单个列，我们直接计算点距离。如果是多个列，计算knn距离。
    
    然后我们可以得到一个 m x n 矩阵。m为stratified_col中的规则数，n为control group中的样本数
    
    最后我们按照根据距离，并结合分层的优先顺序，进行排序。这样就可以得到在分层标准下，对于每个
    treatment_sample得到其matching的control_samples的index列表
    """
    indices = list()
    for treatment_sample in std_treatment: # 对于每个 treatment_sample 样本
        strat_dist = np.zeros((len(stratified_col), std_control.shape[0] ) )
         
        for i in range(len(stratified_col)):
            strat_ind = stratified_col[i] # 取得被分层的列的列标
            strat_dist[i] = dist(std_control.T[strat_ind].T, # 计算距离
                                 treatment_sample[strat_ind][np.newaxis] ).T[0]
             
        ## 按照分层的顺序排序
        strat_dist_df = pd.DataFrame(strat_dist.T)
        indices.append(
                       list( # 把dataframe的index转化成列表
                            strat_dist_df.sort( # 按照先stratify的的距离升序排列
                                               columns = list( # range(startify的列数)
                                                              range(len(stratified_col)) 
                                                              ) 
                                               ).index 
                            )
                       )

    return indices
    
def index_matching(indices, k = 1):
    """[EN] an index filter to select k control sample for each treatment sample
    
    [Args]
    ------
    indices: the returns of func stratified_matching
    
    k: how many sample selected from control group for each treatment sample
    
    [Returns]
    ---------
    selected_control_index: See Example
    
    selected_for_each_treatment: See Example
    
    
    
    e.g. 3 tr wants to match 9 cr from a 12 cr dataset. k = 9/3 = 3
        stratified order:
            tr1 -> [ 4, 11,  6,  7, 10,  5,  9,  1,  2,  3, 12,  8] indice1
            tr2 -> [ 3, 10, 11,  1, 12,  7,  9,  2,  8,  6,  5,  4] indice2
            tr3 -> [12, 10,  1,  3,  6,  7,  5,  4, 11,  9,  2,  8] indice3
        
        When return, we also want to know who matches who. So we do the following algorithm
        
        selected_control = []
        for tr1, we select first 3, then selected_control = [4, 11, 6]
        for tr2, we select first 3, but 11 already been selected, then select next. so after this
            round selected_contorl = [4, 11, 6, 3, 10, 1]
        for tr3, we do the same things. selected_control = [4, 11, 6, 3, 10, 1, 12, 7, 5]
        
        finally we have:
            selected_control  = [4, 11, 6, 3, 10, 1, 12, 7, 5]
            selected_for_each_treatment = [[4, 11, 6],
                                           [3, 10, 1],
                                           [12, 7, 5]]
                                           
    [CN]对index进行处理，为每个treatment sample匹配k个control sample
    """
    
    selected_control_index = OrderedSet(list()) # initial selected control group sample indices

    for indice in indices: # for indice that tr1 -> [4, 11, 6, 7, ...]
        counter = 0
        for ind in indice:
            if ind not in selected_control_index: # if has not been selected
                selected_control_index.add(ind)
                counter += 1
                if counter == k: # if already selected k control sample, then stop
                    break
                
    selected_control_index = list(selected_control_index)
    selected_for_each_treatment = np.array([selected_control_index[i::k] for i in range(k)]).T
    
    return selected_control_index, selected_for_each_treatment

def psm(control, treatment, use_col = None, stratified_col = None, k = 1):
    """Select matched sample from control by naming:
    
    [Args]
    ------
    
    use_col = the columns' index you want to use in the matching
    
    stratified_col = the stratify order you defined
    
    k = how many sample selected from control group for each treatment sample 
    """
    if use_col: # 选择要用的
        used_control, used_treatment = control[:, use_col], treatment[:, use_col]

    indices = stratified_matching(used_control, 
                                  used_treatment, 
                                  stratified_col = stratified_col)
    
    selected_control_index, selected_for_each_treatment = index_matching(indices, 
                                                                         k = k)
    
    return control[selected_control_index], control[selected_for_each_treatment]

