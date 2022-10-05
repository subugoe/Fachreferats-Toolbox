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
import json

def download_zotero_bibliography_json(
        group = "113737",
        path = "https://api.zotero.org/groups/",
        collections = "", # "collections/9DVPHXPP/"
        query = "/items?format=json&limit=100&",
        item_type = "book", # bookSection,  book || bookSection,
        verbose = False

    ):

    if collections == "":
        complete_query = path + group + "/" + query + "itemType=" + item_type

    else:
        complete_query = path + group + "/collections/" + collections + query + "itemType=" + item_type

    print(complete_query)
    response = requests.get(complete_query)

    total_number = int(response.headers['Total-Results'])
    total_number

    i = 0

    while i  < total_number + 100:
        if verbose == True: print(i)
        
        temporal_data = requests.get(complete_query + "&start="+str(i)).json()

        if i == 0:
            data = temporal_data

        else:

            data = data + temporal_data
        i += 100
    return data

def convert_zotero_json_to_zotero_csv(
    data,
    columns = ["key", "itemType", "title", "bookTitle", "firstName", "lastName","publisher","place","date", "language","numPages","ISBN","url"]
    ):
    df = pd.DataFrame(columns = columns, dtype=object)

    i = 0
    for entry in data:
        for column in columns:
            if column in ["lastName", "firstName"]:
                try:
                    df.loc[i, column] = entry["data"]["creators"][0][column]
                except:
                    df.loc[i, column] = ""
            elif column == "tags":
                if entry["data"][column] != []:

                    df.loc[i, column] = "|".join(list([tag["tag"]  for tag in entry["data"][column] ]))
                        

                else:
                    df.loc[i, column] = ""

            else:
                try:
                    df.loc[i, column] = entry["data"][column]
                except:
                    pass
        i += 1
    
    return df



def modify_zotero_bibliography_with_tags_from_column(df, key, group = "4583003", column_tag = "BK_notation_klassenbenennung", verbose = False):
    """
    modify_zotero_bibliography_with_tags_from_column(df, key = "HERE THE KEY FROM ZOTERO")
    """
    for index, row in df.iterrows():

        if verbose == True: print(index)

        if row[column_tag] == row[column_tag] and row[column_tag] != "":

            url = "https://api.zotero.org/groups/" + group + "/items/"  + row["key"] + "/?key="+ key + "&format=json"

            if verbose == True: print(url)


            response = requests.get(url)
            data = response.json()
            #print(data)
            for tag in row[column_tag].split("|"):
                if len(tag) > 3 and ":nan" not in tag:
                    data["data"]["tags"].append({'tag': tag, 'type': 1})

    
            #url = "https://api.zotero.org/groups/" + group + "/items/"  + row["key"] + "/?key="+ key + "&format=json"


            r = requests.put(url, data = json.dumps(data))
            if verbose == True:  print(r)

    return r


def modify_zotero_bibliography(df, key, group, tag = "SUB:bereits_als_Ebook", url_column = "nach_title_URL_GUK"):
    
    for index, row in df.iterrows():
        print(index)


        url = "https://api.zotero.org/groups/" + group + "/items/" + row["key"] + "/?key="+ key + "&format=json"


        response = requests.get(url)
        data = response.json()
        
        data["data"]["tags"].append({'tag': tag, 'type': 0})

        if url_column in df.columns.tolist():
            data["data"]["url"] = row[url_column]#.append({'tag': 'SUB:bereits_als_Ebook', 'type': 0})

        if "nach_title_URL_GUK" in df.columns.tolist():
            data["data"]["extra"] = data["data"]["extra"] + "| Göttingen_ppns: " +  str(row["nach_title_URL_GUK"])

        url = "https://api.zotero.org/groups/" + group + "/items/"  + row["key"] + "/?key=" + key


        r = requests.put(url, data = json.dumps(data))
        

    return r
