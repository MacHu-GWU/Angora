#!/usr/bin/env python
# -*- coding: UTF8 -*-

"""
python setup.py build --plat-name=win32 bdist_wininst # 32bit version
python setup.py build --plat-name=win-amd64 bdist_wininst # 64bit version
"""

from distutils.core import setup

setup(name="Angora",
      version="1.0.0",
      description="A tool set for Data Scientist, DBA, WebDeveloper",
      author="Sanhe",
      author_email="husanhe@gmail.com",
      packages=[
                "angora", 
                "angora\BOT", 
                "angora\DATA", 
                "angora\DATASCI", 
                "angora\FIELDSEARCH", 
                "angora\GADGET", 
                "angora\GEO", 
                "angora\LIBRARIAN", 
                "angora\LINEARSPIDER", 
                "angora\PandasSQL",
                "angora\POSTGRES",
                "angora\SQLITE",
                "angora\STRING",
                ],
     )