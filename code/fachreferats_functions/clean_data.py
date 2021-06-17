# -*- coding: utf-8 -*-
"""
Created on 2021

@author: jct
"""

import pandas as pd
import re



def identify_ISBN_column(df):
    if "ISBN" not in df.columns.tolist() and len([column for column in df.columns.tolist() if "isbn" in column.lower()]) > 1:
        print("Your table contains several columns for ISBN and none of them is just called 'ISBN'. Please, open the table in Excel and Calc and the column that you want to use for the checking 'ISBN', and read the table again with your Jupyter Notebook. Otherwise it won't work.")


def strip_columns(df):
    if " " in [column[0] for column in df.columns.tolist()] or " " in [column[-1] for column in df.columns.tolist()]:
        print("One column has whitespace before or after the name of the column. This white space is going to be deleted.")
        for column in df.columns.tolist():
            if column[0] == " " or column[-1] == " ":
                print("Deleting white space in '" + column + "'")
                df.rename(columns = {column: column.strip()}, inplace = True)
    return df

def try_to_find_missing_columns(df, mandatory_columns):
    for mantadory_column_name, mantadory_column_alternatives  in mandatory_columns.items():
        if mantadory_column_name not in df.columns.tolist():
            print("The column ", mantadory_column_name, " is mandatory but missing in the table. We are going to try to find another one in the table that contains this information, but perhaps this creates errors!")
            for mantadory_column_alternative in mantadory_column_alternatives:
                if mantadory_column_alternative in df.columns.tolist():
                    print("Replacing ", mantadory_column_alternative, " with ", mantadory_column_name)
                    df.rename(columns = {mantadory_column_alternative: mantadory_column_name}, inplace = True)
    return df


def convert_ISBN_to_str(df):
    if df["ISBN"].dtype == float:
        print("Changing ISBN type from float to str")
        df["ISBN"] = df["ISBN"].fillna(0).astype('Int64').astype(str)
        
    if df["ISBN"].dtype == int or df["ISBN"].dtype == "int64":
        print("Changing ISBN type from int to str")
        df["ISBN"] = df["ISBN"].astype(str)
    return df

def clean_ISBN(df):
    df["ISBN"].fillna("", inplace = True)
    df["ISBN"] = df["ISBN"].str.replace(r"\D", "", regex=True)
    return df

def clean_data(
    df,
    mandatory_columns =
        {
            "Titel" : ["titel", "title"],
            "Vorname_Autor" : ["vorname_autor", "vorname", "Vorname"],
            "Nachname_Autor": ["nachname_autor", "author", ],
            "Erscheinungsjahr" : ["erscheinungsjahr", "jahr"],
            "ISBN" : ["isbn", "isbn-10", "ISBN-10"]
        },
    ):

    identify_ISBN_column(df)

    df = strip_columns(df)

    df = try_to_find_missing_columns(df, mandatory_columns)

    df = convert_ISBN_to_str(df)

    df = clean_ISBN(df)
    
    return df