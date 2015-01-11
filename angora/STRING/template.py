##encoding=utf8

from __future__ import print_function

def straightline(title, length = 100, linestyle = "="):
    text = "{:%s^%s}" % (linestyle, length)
    return text.format(title)

if __name__ == "__main__":
    print(straightline("yesman"))