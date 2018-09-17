import nhl_functions as nhl
import pandas as pd
import sqlite3

daterange = pd.date_range('2017-10-12','2017-10-16')
for dt in daterange:
    nhl.scrape_date_to_db(dt.strftime("%Y-%m-%d"),dt.strftime("%Y-%m-%d"))
    
path = 'C:/Users/geoff/Documents/GitHub/Py/nhl'
conn = sqlite3.connect(path + '/nhl.db')
