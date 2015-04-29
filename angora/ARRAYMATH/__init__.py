##encoding=utf-8

"""
This package is desgiend for IronPython27 only. Because IronPython27 doens't support numpy yet.
"""

from .interpolate import LinearInterpolator, arange
try:
    from .outlier import find_outlier, clear_outlier_onetime, clear_outlier_literally
except:
    pass