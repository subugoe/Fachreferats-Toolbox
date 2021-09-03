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
from difflib import SequenceMatcher


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
    find_key = "isb"
    ):


    namespaces = {'zs':"http://www.loc.gov/zing/srw/", 'pica':'info:srw/schema/5/picaXML-v1.0'}


    for index, row in df.iterrows():
        if len(str(row[ name_column ])) > 6:
            api_url = "http://sru.k10plus.de/" + database + "!rec=1?version=1.1&query=pica." + find_key + "=" + str(row[ name_column ]) + "&operation=searchRetrieve&maximumRecords=100&recordSchema=picaxml"
            #print(api_url)

            try:
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
            except:
                print("error")
    
        try:
            df.loc[index, "Titel_und_nach_ISBN_Titel_Ähnlichkeit_Score"] = round(SequenceMatcher(None, df.loc[index, "Titel"], df.loc[index, "nach_ISBN_Titel"]).ratio(), 2)
        except:
            pass

        

    if "nach_" + name_column + "_ILNs" in df.columns.tolist():
        df["nach_" + name_column+ "_Bestand_K10"] = df["nach_" + name_column + "_ILNs"].str.count("[,;\-]") + 1

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
    verbose =False,
    ):


    namespaces = {'zs':"http://www.loc.gov/zing/srw/", 'pica':'info:srw/schema/5/picaXML-v1.0'}


    for index, row in df.iterrows():

        title = str(row[ name_column ])
        if verbose == True: print(title)
        if len(title) > 7:
            try:
                api_url = "http://sru.k10plus.de/" + database + "!rec=1?version=1.1&query=pica.tit=" + title  + "&operation=searchRetrieve&maximumRecords=100&recordSchema=picaxml"
                if verbose == True: print(api_url)


                tree = etree.parse(api_url).getroot()
                
                for key, xpath in xpaths.items():

                    value_lt =  tree.xpath(xpath, namespaces = namespaces)

                    if len(value_lt) > 0:
                        if "ILN" in key:
                            df.loc[index, key] = ";".join(value_lt)
                        else:
                            df.loc[index, key] = value_lt[0]
            except:
                print("error")
    
    if "nach_Titel_ILNs" in df.columns.tolist():
        df["nach_" + name_column+ "_Bestand_K10"] = df["nach_Titel_ILNs"].str.count("[,;\-]") + 1

    if delete_ILN == True:
        for column in df.columns.tolist():
            if "ILN" in column:
                df.drop(column, axis=1, inplace=True)

    return df



def extract_fields_with_title_author( df, 
    database = "k10plus",
    xpaths = {
        'nach_Titel_Autor_ILNs' : '//pica:datafield[@tag="001@"]/pica:subfield[@code="0"]/text()',
        },
    name_column_title = "Titel",
    name_column_author = "Autor",
    delete_ILN = True,
    verbose = False,

    ):


    namespaces = {'zs':"http://www.loc.gov/zing/srw/", 'pica':'info:srw/schema/5/picaXML-v1.0'}



    for index, row in df.iterrows():
        try:
            api_url = "http://sru.k10plus.de/" + database + "!rec=1?version=1.1&query=pica.tit=" + str(row[ name_column_title ]) + " and pica.per=" + str(row[ name_column_author ]) + "&operation=searchRetrieve&maximumRecords=100&recordSchema=picaxml"

            if verbose == True: print(api_url)


            tree = etree.parse(api_url).getroot()
            
            for key, xpath in xpaths.items():

                value_lt =  tree.xpath(xpath, namespaces = namespaces)

                if len(value_lt) > 0:
                    if "ILN" in key:
                        df.loc[index, key] = ";".join(value_lt)
                    else:
                        df.loc[index, key] = value_lt[0]
        except:
            print("error")
    
    if "nach_Titel_Autor_ILNs" in df.columns.tolist():
        df["nach_Titel_Autor_Bestand_K10"] = df["nach_Titel_Autor_ILNs"].str.count("[,;\-]") + 1

    if delete_ILN == True:
        for column in df.columns.tolist():
            if "ILN" in column:
                df.drop(column, axis=1, inplace=True)

    return df



