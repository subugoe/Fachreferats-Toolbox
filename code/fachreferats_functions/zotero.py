# -*- coding: utf-8 -*-
"""
Created on 2020.07.20

@author: jct
"""
import pandas as pd
import matplotlib.pyplot as plt
import pandas as pd
from collections import Counter
import os
import sys
import requests
sys.path.append(os.path.abspath("./Dropbox/MTB/Göttingen/research/"))


def download_zotero_bibliography_json(
        group = "113737",
        path = "https://api.zotero.org/groups/",
        query = "/items?format=json&limit=100&",
        item_type = "book", # bookSection,  book || bookSection
    ):

    complete_query = path + group + query + "itemType=" + item_type

    response = requests.get(complete_query)

    total_number = int(response.headers['Total-Results'])
    total_number

    i = 0

    while i  < total_number + 100:
        print(i)
        
        temporal_data = requests.get(complete_query + "&start="+str(i)).json()

        if i == 0:
            data = temporal_data

        else:

            data = data + temporal_data
        i += 100
    return data

def convert_zotero_json_to_zotero_csv(
    data,
    columns = ["itemType", "title", "bookTitle", "firstName", "lastName","publisher","place","date", "language","numPages","ISBN","url"]
    ):
    df = pd.DataFrame(columns = columns)

    i = 0
    for entry in data:
        for column in columns:
            if column in ["lastName", "firstName"]:
                try:
                    df.loc[i, column] = entry["data"]["creators"][0][column]
                except:
                    df.loc[i, column] = ""
            else:
                try:
                    df.loc[i, column] = entry["data"][column]
                except:
                    pass
        i += 1
    
    return df

