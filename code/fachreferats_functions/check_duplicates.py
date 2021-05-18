# -*- coding: utf-8 -*-
"""
Created on 2021

@author: jct
"""

import pandas as pd
from lxml import etree
import requests
import os
import time
import urllib.parse
import re
import numpy as np

def check_duplicate_with_isbn( df, 
    database = "opac-de-7",
    xpath = '//zs:numberOfRecords/text()',
    name_column_isbn = "ISBN",
    name_column_title = "Title",
    ):


    namespaces = {'zs':"http://www.loc.gov/zing/srw/"}


    df[name_column_isbn] = df[name_column_isbn].fillna(0).astype(np.int64).astype(str)


    for index, row in df.iterrows():
        if len(row[ name_column_isbn ]) > 9:
            api_url = "http://sru.k10plus.de/opac-de-7!rec=1?version=1.1&query=pica.isb=" + str(row[ name_column_isbn ]) + "&operation=searchRetrieve&maximumRecords=10&recordSchema=picaxml"


            tree = etree.parse(api_url).getroot()
            
            value_lt =  tree.xpath(xpath, namespaces = namespaces)
            print(row[ name_column_title ], row[ name_column_isbn ], value_lt[0])
            df.loc[index, "based_on_" + name_column_isbn + "_number_GUK"] = value_lt[0]
            df.loc[index, "based_on_" + name_column_isbn + "_in_GUK?"] = bool(int(value_lt[0]))
            df.loc[index, "based_on_" + name_column_isbn + "_url_GUK"] = "https://opac.sub.uni-goettingen.de/DB=1/SET=6/TTL=1/CMD?ACT=SRCHA&IKT=1016&SRT=YOP&TRM=isb+" + str(row[ name_column_isbn ]) + "&MATCFILTER=N&MATCSET=N&NOSCAN=N&ADI_BIB="
    return df





def check_duplicate_with_title( df, 
    database = "opac-de-7",
    xpath = '//zs:numberOfRecords/text()',
    name_column_title = "Title",
    ):

    namespaces = {'zs':"http://www.loc.gov/zing/srw/"}

    for index, row in df.iterrows():

        title = re.sub(r"[/\.]", r"", row[ name_column_title ], flags=re.M)

        print(title)
        
        api_url = "http://sru.k10plus.de/opac-de-7!rec=1?version=1.1&query=pica.tit=" + title + "&operation=searchRetrieve&maximumRecords=10&recordSchema=picaxml"

        try:
            tree = etree.parse(api_url).getroot()
        
            value_lt =  tree.xpath(xpath, namespaces = namespaces)
            print(value_lt)

            df.loc[index, "based_on_" + name_column_title + "_number_GUK"] = value_lt[0]
            df.loc[index, "based_on_" + name_column_title + "_in_GUK?"] = bool(int(value_lt[0]))

            df.loc[index, "based_on_" + name_column_title + "_in_GUK?"] = bool(int(value_lt[0]))

            df.loc[index, "based_on_" + name_column_title + "_url_GUK"] = "https://opac.sub.uni-goettingen.de/DB=1/SET=2/TTL=1/CMD?ACT=SRCHA&IKT=1016&SRT=YOP&TRM=tit " +  title + "&MATCFILTER=N&MATCSET=N&NOSCAN=N&ADI_BIB="

        except:
            df.loc[index, "based_on_" + name_column_title + "_error"] = 1

    return df



