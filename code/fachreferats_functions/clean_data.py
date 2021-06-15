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


# Check for columns with isbn

# if floating, than books_donated["ISBN"] = books_donated["ISBN"].fillna(0).astype('Int64').astype(str)

# strip columns 

