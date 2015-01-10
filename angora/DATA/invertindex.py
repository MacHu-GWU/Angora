##encoding=utf8

"""
[EN]create a invert index dictionary from a regular index dictionary

[CN]将一个正向索引的字典转化成反向索引的字典
    正向索引字典的例子:
        物品1 : {索引1, 索引2, ...}
        物品2 : {索引1, 索引2, ...}
    
    反向索引字典的例子:
        索引1 : {物品1, 物品2, ...}
        索引2 : {物品1, 物品2, ...}
        
import:
    from angora.DATA.invertindex import invertindex
"""

from __future__ import print_function
from six import iteritems

def invertindex(pos_index):
    """
    [Args]
    ------
    pos_index: normal index dictionary
        key: value = item_id: set{[index1, index2, ..., ]}
        
    [Returns]
    ---------
    inv_index:
        key: value = index: set{[item_id1, item_id2, ...,]}
    """
    invert_index = dict()
    for item_id, indices in iteritems(pos_index):
        for index in indices:
            if index not in invert_index:
                invert_index[index] = set({item_id})
            else:
                invert_index[index].add(item_id)
    return invert_index

if __name__ == "__main__":
    def test_inv_index():
        print("{:=^40}".format("test_inv_index"))
        pos_index = {"let it go": {"mp3", "pop", "dance"},
                     "can you feel the love tonight": {"acc", "pop", "movie"},
                     "Just dance": {"pop", "dance", "club"}}
        print(invertindex(pos_index))
    
    test_inv_index()