import nhl_functions as nhl
import pandas as pd
import sqlite3


daterange = pd.date_range('2017-02-06','2018-10-12')
for dt in daterange:
    nhl.scrape_date_to_db(dt.strftime("%Y-%m-%d"),dt.strftime("%Y-%m-%d"))

#model = nhl.train_model('2018-10-01')


#1. Finish pulling all of the data
#2. Train a model well on all of the training data
#3. Write a query to extract this years data and calculate a team's stats going into the game like in the NFL package
#4. Predict the score using the model