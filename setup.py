#!/usr/bin/env python
# -*- coding: UTF8 -*-

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
                "angora\SQLITE",
                "angora\STRING",
                ],
     )