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
        'ppn' : '//pica:datafield[@tag="003@"]/pica:subfield[@code="0"]/text()',
        'Titel' : '//pica:datafield[@tag="021A"]/pica:subfield[@code="a"]/text()',
        'Vorname_Autor' : '//pica:datafield[@tag="028A"]/pica:subfield[@code="D"]/text()',
        'Nachname_Autor' : '//pica:datafield[@tag="028A"]/pica:subfield[@code="A"]/text()',
        'Erscheinungsjahr' : '//pica:datafield[@tag="011@"]/pica:subfield[@code="a"]/text()',
        'nach_ISBN_Sprache_Text' : '//pica:datafield[@tag="010@"]/pica:subfield[@code="a"]/text()',
        'Verlag' : '//pica:datafield[@tag="033A"]/pica:subfield[@code="n"]/text()',
        'Erscheinungsort' : '//pica:datafield[@tag="033A"]/pica:subfield[@code="p"]/text()',
        'nach_ISBN_ILNs' : '//pica:datafield[@tag="001@"]/pica:subfield[@code="0"]/text()',
        'medium' : '//pica:datafield[@tag="002@"]/pica:subfield[@code="0"]/text()',

        },
    name_column = "ISBN",
    delete_ILN = True,
    find_key = "isb",
    verbose = False
    ):


    namespaces = {'zs':"http://www.loc.gov/zing/srw/", 'pica':'info:srw/schema/5/picaXML-v1.0'}


    for index, row in df.iterrows():
        if len(str(row[ name_column ])) > 6:
            api_url = "http://sru.k10plus.de/" + database + "!rec=1?version=1.1&query=pica." + find_key + "=" + str(row[ name_column ]) + "&operation=searchRetrieve&maximumRecords=100&recordSchema=picaxml"
            if verbose == True: print(api_url)

            try:
                tree = etree.parse(api_url).getroot()
                
                for key, xpath in xpaths.items():

                    if key == "Titel":
                        key = "nach_" + name_column + "_" + key

                    value_lt =  tree.xpath(xpath, namespaces = namespaces)

                    #print(key, value_lt)
                    if len(value_lt) > 0:
                        if "ILN" in key or "ppn" in key or "medium" in key or "BK" in key or "DDC" in key:
                            #print(value_lt)
                            df.loc[index, key] = "|".join(value_lt)
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
        'nach_Titel_ppn' : '//pica:datafield[@tag="003@"]/pica:subfield[@code="0"]/text()',
        'nach_Titel_medium' : '//pica:datafield[@tag="002@"]/pica:subfield[@code="0"]/text()',
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
                api_url = 'http://sru.k10plus.de/' + database + '!rec=1?version=1.1&query=pica.tit="' + title  + '"&operation=searchRetrieve&maximumRecords=100&recordSchema=picaxml'
                if verbose == True: print(api_url)


                tree = etree.parse(api_url).getroot()
                
                for key, xpath in xpaths.items():

                    value_lt =  tree.xpath(xpath, namespaces = namespaces)

                    if len(value_lt) > 0:
                        df.loc[index, key] = "|".join(value_lt)
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
        'nach_Titel_Autor_ppn' : '//pica:datafield[@tag="003@"]/pica:subfield[@code="0"]/text()',
        'nach_Titel_Autor_medium' : '//pica:datafield[@tag="002@"]/pica:subfield[@code="0"]/text()',
        },
    name_column_title = "Titel",
    name_column_author = "Autor",
    delete_ILN = True,
    verbose = False,

    ):


    namespaces = {'zs':"http://www.loc.gov/zing/srw/", 'pica':'info:srw/schema/5/picaXML-v1.0'}



    for index, row in df.iterrows():
        try:
            api_url = 'http://sru.k10plus.de/' + database + '!rec=1?version=1.1&query=pica.tit="' + str(row[ name_column_title ]) + '" and pica.per="' + str(row[ name_column_author ]) + '"&operation=searchRetrieve&maximumRecords=100&recordSchema=picaxml'

            if verbose == True: print(api_url)


            tree = etree.parse(api_url).getroot()
            
            for key, xpath in xpaths.items():

                value_lt =  tree.xpath(xpath, namespaces = namespaces)

                if len(value_lt) > 0:
                    df.loc[index, key] = "|".join(value_lt)
        except:
            print("error")
    
    if "nach_Titel_Autor_ILNs" in df.columns.tolist():
        df["nach_Titel_Autor_Bestand_K10"] = df["nach_Titel_Autor_ILNs"].str.count("[,;\-]") + 1

    if delete_ILN == True:
        for column in df.columns.tolist():
            if "ILN" in column:
                df.drop(column, axis=1, inplace=True)

    return df

def extract_fields_with_title_author_language( df, 
    database = "k10plus",
    xpaths = {
        'nach_Titel_Autor_Sprache_ILNs' : '//pica:datafield[@tag="001@"]/pica:subfield[@code="0"]/text()',
        'nach_Titel_Autor_Sprache_ppn' : '//pica:datafield[@tag="003@"]/pica:subfield[@code="0"]/text()',
        'nach_Titel_Autor_Sprache_medium' : '//pica:datafield[@tag="002@"]/pica:subfield[@code="0"]/text()',
        },
    name_column_title = "Titel",
    name_column_author = "Autor",
    name_column_language = "Sprache",
    delete_ILN = True,
    verbose = False,

    ):


    namespaces = {'zs':"http://www.loc.gov/zing/srw/", 'pica':'info:srw/schema/5/picaXML-v1.0'}



    for index, row in df.iterrows():
        try:
            api_url = 'http://sru.k10plus.de/' + database + '!rec=1?version=1.1&query=pica.tit="' + str(row[ name_column_title ]) + '" and pica.per="' + str(row[ name_column_author ]) + '" and pica.spr="' + str(row[ name_column_language ]) + '"&operation=searchRetrieve&maximumRecords=100&recordSchema=picaxml'

            if verbose == True: print(api_url)


            tree = etree.parse(api_url).getroot()
            
            for key, xpath in xpaths.items():

                value_lt =  tree.xpath(xpath, namespaces = namespaces)

                if len(value_lt) > 0:
                    df.loc[index, key] = "|".join(value_lt)
        except:
            print("error")
    
    if "nach_Titel_Autor_Sprache_ILNs" in df.columns.tolist():
        df["nach_Titel_Autor_Sprache_Bestand_K10"] = df["nach_Titel_Autor_Sprache_ILNs"].str.count("[,;\-]") + 1

    if delete_ILN == True:
        for column in df.columns.tolist():
            if "ILN" in column:
                df.drop(column, axis=1, inplace=True)

    return df


def extract_fields_and_subfields_with_isbn( df, 
    database = "k10plus",
    main_xpath = '//pica:datafield[@tag="045Q"]',
    xpaths = [
        './pica:subfield[@code="a"]/text()',
        './pica:subfield[@code="j"]/text()',
    ],
    name_column = "ISBN",
    name_new_column = "BK_notation_klassenbenennung",
    prefix = "BK",
    find_key = "isb",
    verbose = False
    ):


    namespaces = {'zs':"http://www.loc.gov/zing/srw/", 'pica':'info:srw/schema/5/picaXML-v1.0'}


    for index, row in df.iterrows():
        if len(str(row[ name_column ])) > 6:
            api_url = "http://sru.k10plus.de/" + database + "!rec=1?version=1.1&query=pica." + find_key + "=" + str(row[ name_column ]) + "&operation=searchRetrieve&maximumRecords=100&recordSchema=picaxml"
            if verbose == True: print(api_url)

            tree = etree.parse(api_url).getroot()
            
            sub_tree_lt = tree.xpath(main_xpath, namespaces = namespaces)

            values = ""
            for sub_tree in sub_tree_lt:
                
                for xpath in xpaths:
                    values = values + " " + ", ".join(sub_tree.xpath(xpath, namespaces = namespaces)).strip()
                
                values = values + "|"

            print(values)
            values = "|".join([prefix + ":" + value.strip() for value in values.split("|")])

            if values == prefix + ":": values = np.NaN

            df.loc[index, name_new_column] = values


    return df


def get_isbn_from_ppn(df, ppn_column = "first_ppn"):

    for index, row in df.iterrows():
        
        api_url =   "http://unapi.k10plus.de/?id=gvk:ppn:"  + str(row[ppn_column]) +  "&format=isbd"

        r = requests.get(api_url)
        r.encoding = 'utf-8'
        text = r.text
        text = re.sub(r"(\x1a|\x1b)", r"\n", text)

        df.loc[index, "isbd"] = text

    df["isbd_short"] = df["isbd"].str.replace(r"\s+(.*?)Abstract:.+\s*", r"\1", flags=re.M | re.DOTALL)
    df["isbd_short"] = df["isbd_short"].str.replace(r"\n+", r"\n")
    df["isbd_short"] = df["isbd_short"].str.replace(r"\n\Z", r"")
    df["isbd_short"] = "\n" + df["isbd_short"]

    return df

