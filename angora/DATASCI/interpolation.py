##encoding=utf8

from __future__ import print_function
from scipy.interpolate import interp1d

def interpolate(x, y, x_new, kind = "cubic"):
    if kind == "linear":
        f = interp1d(x, y)
    else:
        f = interp1d(x, y, kind = kind)
    return f(x_new)

def interpolate_datetime(x, y, x_new, kind = "cubic"):
    x_ts = [i.timestamp() for i in x]
    x_new_ts = [i.timestamp() for i in x_new]
    return interpolate(x_ts, y, x_new_ts, kind)
    
if __name__ == "__main__":
    import numpy as np, pandas as pd
    import matplotlib.pyplot as plt
    
    def interpolate_test():
        x = np.linspace(0, 10, 10)
        y = np.cos(-x**2/8.0)
        x_new = np.linspace(0, 10, 40)
        
        y_cubic = interpolate(x, y, x_new)
        y_linear = interpolate(x, y, x_new, kind="linear")
        
        plt.plot(x, y, "o", 
                 x_new, y_cubic,"g-", 
                 x_new, y_linear,"r--" )
        plt.show()
    
#     interpolate_test()
    
    def interpolate_datetime_test():
        x = pd.date_range("2014-01-01 00:00:00", "2014-01-02 01:00:00", freq = "2H")
        y = np.cos( np.linspace(0, 10, len(x) ) ** 2 / 8.0 )
        x_new = pd.date_range("2014-01-01 00:00:00", "2014-01-01 23:59:59", freq = "10min")
        y_cubic = interpolation_datetime(x, y, x_new)
        y_linear = interpolation_datetime(x, y, x_new, kind="linear")
        plt.plot(x, y, "o",
                 x_new, y_cubic, "g-",
                 x_new, y_linear, "r--")
        plt.show()
    
#     interpolate_datetime_test()