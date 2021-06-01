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



def extract_fields_with_isbn( df, 
    database = "k10plus",
    xpaths = {
        'Titel' : '//pica:datafield[@tag="021A"]/pica:subfield[@code="a"]/text()',
        'Vorname_Autor' : '//pica:datafield[@tag="028A"]/pica:subfield[@code="D"]/text()',
        'Nachname_Autor' : '//pica:datafield[@tag="028A"]/pica:subfield[@code="A"]/text()',
        'Erscheinungsjahr' : '//pica:datafield[@tag="011@"]/pica:subfield[@code="a"]/text()',
        'nach_ISBN_Sprache_Text' : '//pica:datafield[@tag="010@"]/pica:subfield[@code="a"]/text()',
        'Verlag' : '//pica:datafield[@tag="033A"]/pica:subfield[@code="n"]/text()',
        'Erscheinungsort' : '//pica:datafield[@tag="033A"]/pica:subfield[@code="p"]/text()',
        'nach_ISBN_ILNs' : '//pica:datafield[@tag="001@"]/pica:subfield[@code="0"]/text()',

        },
    name_column = "ISBN",
    delete_ILN = True,
    ):


    namespaces = {'zs':"http://www.loc.gov/zing/srw/", 'pica':'info:srw/schema/5/picaXML-v1.0'}


    for index, row in df.iterrows():
        if len(str(row[ name_column ])) > 9:
            api_url = "http://sru.k10plus.de/" + database + "!rec=1?version=1.1&query=pica.isb=" + str(row[ name_column ]) + "&operation=searchRetrieve&maximumRecords=100&recordSchema=picaxml"
            #print(api_url)


            tree = etree.parse(api_url).getroot()
            
            for key, xpath in xpaths.items():

                if key == "Titel":
                    key = "nach_" + name_column + "_" + key

                value_lt =  tree.xpath(xpath, namespaces = namespaces)

                #print(key, value_lt)
                if len(value_lt) > 0:
                    if "ILN" in key:
                        #print(value_lt)
                        df.loc[index, key] = ";".join(value_lt)
                    else:
                        df.loc[index, key] = value_lt[0]
    
    if "nach_ISBN_ILNs" in df.columns.tolist():
        df["nach_" + name_column+ "_Bestand_K10"] = df["nach_ISBN_ILNs"].str.count("[,;\-]") + 1

    if delete_ILN == True:
        for column in df.columns.tolist():
            if "ILN" in column:
                df.drop(column, axis=1, inplace=True)

    return df


def extract_fields_with_title( df, 
    database = "k10plus",
    xpaths = {
        'nach_Titel_ILNs' : '//pica:datafield[@tag="001@"]/pica:subfield[@code="0"]/text()',
        },
    name_column = "Titel",
    delete_ILN = True,
    ):


    namespaces = {'zs':"http://www.loc.gov/zing/srw/", 'pica':'info:srw/schema/5/picaXML-v1.0'}


    for index, row in df.iterrows():
        if len(str(row[ name_column ])) > 9:
            api_url = "http://sru.k10plus.de/" + database + "!rec=1?version=1.1&query=pica.tit=" + str(row[ name_column ]) + "&operation=searchRetrieve&maximumRecords=100&recordSchema=picaxml"
            #print(api_url)


            tree = etree.parse(api_url).getroot()
            
            for key, xpath in xpaths.items():

                value_lt =  tree.xpath(xpath, namespaces = namespaces)

                if len(value_lt) > 0:
                    if "ILN" in key:
                        df.loc[index, key] = ";".join(value_lt)
                    else:
                        df.loc[index, key] = value_lt[0]
    
    if "nach_Titel_ILNs" in df.columns.tolist():
        df["nach_" + name_column+ "_Bestand_K10"] = df["nach_Titel_ILNs"].str.count("[,;\-]") + 1

    if delete_ILN == True:
        for column in df.columns.tolist():
            if "ILN" in column:
                df.drop(column, axis=1, inplace=True)

    return df


