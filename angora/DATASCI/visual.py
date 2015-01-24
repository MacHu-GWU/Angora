##encoding=utf8

"""

import:
    from angora.DATASCI.visual plot_one_day, plot_one_week, plot_one_month, plot_one_quarter, plot_one_year
"""

import matplotlib.pyplot as plt
from matplotlib.dates import YearLocator, MonthLocator, WeekdayLocator, DayLocator
from matplotlib.dates import HourLocator, MinuteLocator
from matplotlib.dates import DateFormatter
from matplotlib.dates import MO, TU, WE, TH, FR, SA, SU

def plot_one_day(x, y, xlabel=None, ylabel=None, title=None):
    """时间跨度为一天
    major tick = every hours
    minor tick = every 15 minutes
    """
    plt.close("all")
    
    fig = plt.figure(figsize=(14, 9))
    ax = fig.add_subplot(111)
    ax.plot(x, y)
    
    hours = HourLocator(range(24))
    hoursFmt = DateFormatter("%H:%M")
    minutes = MinuteLocator([15, 30, 45])
    minutesFmt = DateFormatter("%M")
    
    ax.xaxis.set_major_locator(hours)
    ax.xaxis.set_major_formatter(hoursFmt)
    ax.xaxis.set_minor_locator(minutes)
    ax.xaxis.set_minor_formatter(minutesFmt)
    
    ax.autoscale_view()
    ax.grid()
    plt.setp( ax.xaxis.get_majorticklabels(), rotation=90 )
    plt.setp( ax.xaxis.get_minorticklabels(), rotation=90 )
    
    plt.ylim([min(y) - (max(y) - min(y) ) * 0.05, 
              max(y) + (max(y) - min(y) ) * 0.05])
    
    if xlabel:
        plt.xlabel(xlabel)
    else:
        plt.xlabel(str(x[0].date()))
    if ylabel:
        plt.ylabel(ylabel)
    else:
        plt.ylabel("value")
    if title:
        plt.title(title)
    else:
        pass
    return plt

def plot_one_week(x, y):
    """时间跨度为一周
    major tick = every days
    minor tick = every 3 hours
    """
    plt.close("all")
    
    fig = plt.figure(figsize=(14, 9))
    ax = fig.add_subplot(111)
    ax.plot(x, y)
    
    days = DayLocator(range(365))
    daysFmt = DateFormatter("%Y-%m-%d")
    hours = HourLocator([3, 6, 9, 12, 15, 18, 21])
    hoursFmt = DateFormatter("%H:%M")
    
    ax.xaxis.set_major_locator(days)
    ax.xaxis.set_major_formatter(daysFmt)
    ax.xaxis.set_minor_locator(hours)
    ax.xaxis.set_minor_formatter(hoursFmt)
    
    ax.autoscale_view()
    ax.grid()
    plt.setp( ax.xaxis.get_majorticklabels(), rotation=90 )
    plt.setp( ax.xaxis.get_minorticklabels(), rotation=90 )
    
    plt.ylim([min(y) - (max(y) - min(y) ) * 0.05, 
              max(y) + (max(y) - min(y) ) * 0.05])
    
    plt.xlabel(
               "%s to %s" % (
                             str(x[0]),
                             str(x[-1]),
                             ) 
               )
    
    return plt

def plot_one_month(x, y):
    """时间跨度为一月
    major tick = every days
    """
    plt.close("all")
    
    fig = plt.figure(figsize=(14, 9))
    ax = fig.add_subplot(111)
    ax.plot(x, y)
    

    days = DayLocator(range(365))
    daysFmt = DateFormatter("%Y-%m-%d")
    
    ax.xaxis.set_major_locator(days)
    ax.xaxis.set_major_formatter(daysFmt)
    
    ax.autoscale_view()
    ax.grid()
    plt.setp( ax.xaxis.get_majorticklabels(), rotation=90 )
    
    plt.ylim([min(y) - (max(y) - min(y) ) * 0.05, 
              max(y) + (max(y) - min(y) ) * 0.05])
    
    plt.xlabel(
               "%s to %s" % (
                             str(x[0].date()),
                             str(x[-1].date()),
                             ) 
               )
    
    return plt

def plot_one_quarter(x, y):
    """时间跨度为一年
    major tick = every months
    minor tick = every 15th day in a month
    """
    plt.close("all")
    
    fig = plt.figure(figsize=(14, 9))
    ax = fig.add_subplot(111)
    ax.plot(x, y)
    
    months = MonthLocator(range(12))
    monthsFmt = DateFormatter("%Y-%m")
    days = DayLocator([7, 14, 21, 28])
    daysFmt = DateFormatter("%dth")
    
    ax.xaxis.set_major_locator(months)
    ax.xaxis.set_major_formatter(monthsFmt)
    ax.xaxis.set_minor_locator(days)
    ax.xaxis.set_minor_formatter(daysFmt)
    
    ax.autoscale_view()
    ax.grid()
    plt.setp( ax.xaxis.get_majorticklabels(), rotation=90 )
    plt.setp( ax.xaxis.get_minorticklabels(), rotation=90 )
    
    plt.ylim([min(y) - (max(y) - min(y) ) * 0.05, 
              max(y) + (max(y) - min(y) ) * 0.05])
    
    plt.xlabel(
               "%s to %s" % (
                             str(x[0].date()),
                             str(x[-1].date()),
                             ) 
               )
    
    return plt

def plot_one_year(x, y):
    """时间跨度为一年
    major tick = every months
    minor tick = every 15th day in a month
    """
    plt.close("all")
    
    fig = plt.figure(figsize=(14, 9))
    ax = fig.add_subplot(111)
    ax.plot(x, y)
    
    months = MonthLocator(range(12))
    monthsFmt = DateFormatter("%Y-%m")
    days = DayLocator([15])
    daysFmt = DateFormatter("%m-%d")
    
    ax.xaxis.set_major_locator(months)
    ax.xaxis.set_major_formatter(monthsFmt)
    ax.xaxis.set_minor_locator(days)
    ax.xaxis.set_minor_formatter(daysFmt)
    
    ax.autoscale_view()
    ax.grid()
    plt.setp( ax.xaxis.get_majorticklabels(), rotation=90 )
    plt.setp( ax.xaxis.get_minorticklabels(), rotation=90 )
    
    plt.ylim([min(y) - (max(y) - min(y) ) * 0.05, 
              max(y) + (max(y) - min(y) ) * 0.05])
    
    plt.xlabel(
               "%s to %s" % (
                             str(x[0].date()),
                             str(x[-1].date()),
                             ) 
               )
    
    return plt

if __name__ == "__main__":
    import pandas as pd, numpy as np
    def unittest_plot_one_day():
        x = pd.date_range(start = "2014-01-01 00:00:00",
                          end = "2014-01-02 00:00:00",
                          freq = "15min")
        
        y = np.random.rand(len(x))
        plt = plot_one_day(x, y)
        plt.show()
        
#     unittest_plot_one_day()
    
    def unittest_plot_one_week():
        x = pd.date_range(start = "2014-01-01 00:00:00",
                          end = "2014-01-08 00:00:00",
                          freq = "1H")
        
        y = np.random.rand(len(x))
        plt = plot_one_week(x, y)
        plt.show()
        
#     unittest_plot_one_week()
    
    def unittest_plot_one_month():
        x = pd.date_range(start = "2014-01-01 00:00:00",
                          end = "2014-02-01 00:00:00",
                          freq = "1H")
        
        y = np.random.rand(len(x))
        plt = plot_one_month(x, y)
        plt.show()
        
#     unittest_plot_one_month()

    def unittest_plot_one_quarter():
        x = pd.date_range(start = "2014-01-01 00:00:00",
                          end = "2014-04-01 00:00:00",
                          freq = "D")
        
        y = np.random.rand(len(x))
        plt = plot_one_quarter(x, y)
        plt.show()
        
#     unittest_plot_one_quarter()
    
    def unittest_plot_one_year():
        x = pd.date_range(start = "2014-01-01 00:00:00",
                          end = "2015-01-01 00:00:00",
                          freq = "D")
        
        y = np.random.rand(len(x))
        plt = plot_one_year(x, y)
        plt.show()
        
#     unittest_plot_one_year()