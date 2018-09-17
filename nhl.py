import nhl_functions as nhl
import pandas as pd

daterange = pd.date_range('2016-02-20','2016-04-10')
for dt in daterange:
    nhl.scrape_date_to_db(dt.strftime("%Y-%m-%d"),dt.strftime("%Y-%m-%d"))
    

#conn = sqlite3.connect('C:/Users/geoff/Documents/GitHub/Py/nhl.db')
#out = pd.read_sql_query("select * from player_game_raw_stats",conn)
#conn.commit()
#conn.close()

# Should add a metric around what % of actions a player is on for is their action vs. linemate

#Ideas

# Do stats by shift, by minute, by game
# Do that players stats relative to the team's /shift and /minute stats when they aren't on the ice
# Need to figure out time in defensive zone, time in offensive zone
# Something to do with the x/y...avg position?
# Think through players playing with the same people - measure of line consistency? Or total # of ppl played with in a game? or /shift or /minute?

# Need a table that breaks down by player by game the total # of shifts and total minutes played - use the shift data csv
# For a player's actions, do ratio of his actions to total actions when he is on the ice

