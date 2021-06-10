# -*- coding: utf-8 -*-
"""
Created on 2021

@author: jct
"""


"""

- SUB = HG + BB + BWald +  
    - Definition: FMAG, LS1, HG, Normales Magazin (keine Unterfeld in $f)


https://www.sub.uni-goettingen.de/nc/standorte-raumangebote/standorte-mit-oeffnungszeiten/bibliotheken-in-goettingen/sigelliste/
- Magazinexemplar ohne sst-Angabe: PPN 232624712

https://www.sub.uni-goettingen.de/standorte-raumangebote/standorte-mit-oeffnungszeiten/
BBF, BBK, BBM, BBN, BBW, BBWISO, 

https://intranet.sub.uni-goettingen.de/display/grpBEInfo/Sonderstandorte+ZB+und+HG
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
    xpath_1 = '//zs:numberOfRecords/text()',
    xpath_2 = '//pica:datafield[@tag="209A"]/pica:subfield[@code="f"]/text()',
    name_column_isbn = "ISBN",
    name_column_title = "Titel",
    verbose = False
    ):


    namespaces = {'zs':"http://www.loc.gov/zing/srw/", 'pica':'info:srw/schema/5/picaXML-v1.0'}


    #df[name_column_isbn] = df[name_column_isbn].fillna(0).astype(np.int64).astype(str)


    for index, row in df.iterrows():
        if len(row[ name_column_isbn ]) > 9:
            api_url = "http://sru.k10plus.de/" + database + "!rec=1?version=1.1&query=pica.isb=" + str(row[ name_column_isbn ]) + "&operation=searchRetrieve&maximumRecords=10&recordSchema=picaxml"

            if verbose == True: print(api_url)


            tree = etree.parse(api_url).getroot()

            value_lt =  tree.xpath(xpath_1, namespaces = namespaces)

            if verbose == True: print(row[ name_column_title ], row[ name_column_isbn ], value_lt[0])

            #df.loc[index, "nach_" + name_column_isbn + "_in_Bestand_Göttingen?"] = bool(int(value_lt[0]))
            df.loc[index, "nach_" + name_column_isbn + "_Bestand_Göttingen"] = value_lt[0]

            value_lt =  tree.xpath(xpath_2, namespaces = namespaces)

            value_lt = [value for value in value_lt if value in ["LS1", "FMAG", ""]]

            df.loc[index, "nach_" + name_column_isbn + "_Bestand_SUB"] = len(value_lt)

            df.loc[index, "nach_" + name_column_isbn + "_URL_GUK"] = 'https://opac.sub.uni-goettingen.de/DB=1/SET=6/TTL=1/CMD?ACT=SRCHA&IKT=1016&SRT=YOP&TRM=isb+' + str(row[ name_column_isbn ]) + '&MATCFILTER=N&MATCSET=N&NOSCAN=N&ADI_BIB='

    return df





def check_duplicate_with_title(df, 
    database = "opac-de-7",
    xpath_1 = '//zs:numberOfRecords/text()',
    xpath_2 = '//pica:datafield[@tag="209A"]/pica:subfield[@code="f"]/text()',
    name_column_title = "Titel",
    verbose = False,
    ):

    namespaces = {'zs':"http://www.loc.gov/zing/srw/", 'pica':'info:srw/schema/5/picaXML-v1.0'}

    for index, row in df.iterrows():

        title = re.sub(r"[/\.]", r"", row[ name_column_title ], flags=re.M)

        if verbose == True: print(title)

        api_url = 'http://sru.k10plus.de/'  + database + '!rec=1?version=1.1&query=pica.tit="' + title + '"&operation=searchRetrieve&maximumRecords=10&recordSchema=picaxml'
        try:
            tree = etree.parse(api_url).getroot()

            if verbose == True: print(api_url)

            value_lt =  tree.xpath(xpath_1, namespaces = namespaces)

            if verbose == True: print(value_lt)

            df.loc[index, "nach_" + name_column_title + "_Bestand_Göttingen"] = value_lt[0]

            value_lt =  tree.xpath(xpath_2, namespaces = namespaces)

            value_lt = [value for value in value_lt if value in ["LS1", "FMAG"]]

            df.loc[index, "nach_" + name_column_title + "_Bestand_SUB"] = len(value_lt)

            if verbose == True: print(value_lt)

            df.loc[index, "nach_" + name_column_title + "_URL_GUK"] = "https://opac.sub.uni-goettingen.de/DB=1/SET=2/TTL=1/CMD?ACT=SRCHA&IKT=1016&SRT=YOP&TRM=tit " +  title + "&MATCFILTER=N&MATCSET=N&NOSCAN=N&ADI_BIB="
        except:
            df.loc[index, "error"] = 1            



    return df



def check_duplicate_with_title_author( df, 
    database = "opac-de-7",
    xpath_1 = '//zs:numberOfRecords/text()',
    xpath_2 = '//pica:datafield[@tag="209A"]/pica:subfield[@code="f"]/text()',
    name_column_title = "Titel",
    name_column_author = "Nachname_Autor",
    verbose = False,

    ):

    namespaces = {'zs':"http://www.loc.gov/zing/srw/", 'pica':'info:srw/schema/5/picaXML-v1.0'}

    for index, row in df.iterrows():

        title = re.sub(r"[/\.]", r"", row[ name_column_title ], flags=re.M)
        author = re.sub(r"[/\.]", r"", row[ name_column_author ], flags=re.M)

        if verbose == True: print(title, author)
        
        api_url = 'http://sru.k10plus.de/'  + database + '!rec=1?version=1.1&query=pica.tit="' + title + '" and pica.per=' + author+ '&operation=searchRetrieve&maximumRecords=10&recordSchema=picaxml'

        if verbose == True: print(api_url)

        try:
            tree = etree.parse(api_url).getroot()
        
            value_lt =  tree.xpath(xpath_1, namespaces = namespaces)

            if verbose == True: print(value_lt)

            if len(value_lt) > 0:
                df.loc[index, "nach_" + name_column_title + "_" + name_column_author + "_Bestand_Göttingen"] = value_lt[0]

            value_lt =  tree.xpath(xpath_2, namespaces = namespaces)

            if verbose == True: print(value_lt)

            value_lt = [value for value in value_lt if value in ["LS1", "FMAG"]]

            df.loc[index, "nach_" + name_column_title + "_" + name_column_author + "_Bestand_SUB"] = len(value_lt)

            df.loc[index, "nach_" + name_column_title + "_" + name_column_author + "_URL_GUK"] = 'https://opac.sub.uni-goettingen.de/DB=1/SET=2/TTL=1/CMD?ACT=SRCHA&IKT=1016&SRT=YOP&TRM=TIT"' +  title + '" and PER ' + author+  '&MATCFILTER=N&MATCSET=N&NOSCAN=N&ADI_BIB='
        except:
            df.loc[index, "error"] = 1
    return df
