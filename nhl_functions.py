import pandas as pd
import sqlite3
import hockey_scraper as hs
import psutil as ps

path = 'C:/Users/geoff/Documents/GitHub/nhl'

def scrape_date_to_db(dt1,dt2):
    hs.scrape_date_range(dt1,dt2,True,docs_dir=path)
    try:
        df_pbp = pd.read_csv(path+'/nhl_pbp'+dt1+'--'+dt2+'.csv')
        df_shft = pd.read_csv(path+'/nhl_shifts'+dt1+'--'+dt2+'.csv')
        
        conn = sqlite3.connect(path + '/nhl.db')
        c = conn.cursor()
        c.execute("DROP TABLE IF EXISTS raw_pbp")
        conn.commit()
        c.execute("DROP TABLE IF EXISTS raw_shift")
        conn.commit()
        
        
        df_pbp.to_sql('raw_pbp',con=conn,if_exists='replace')
        df_shft.to_sql('raw_shift',con=conn,if_exists='replace')
    
        conn.commit()
        conn.close()
        
        print("DB")
        player_game()
        print("Player Game")
        player_game_shifts()
        print("Player Shifts")
        player_game_events()
        print("Game Events")
        player_game_events_off()
        print("Game Events Off")
        player_game_actions()    
        print("Player Actions")
        player_game_raw_stats()
        print("Raw Stats")
        print("Successfully added games between " + dt1 + " and " + dt2)
    except FileNotFoundError:
        print("No Games between " + dt1 + " and " + dt2)
    
def player_game():
    conn = sqlite3.connect(path + '/nhl.db')
    c = conn.cursor()
    
    sql = '''
    select distinct
        Date as date,
        game_id as game_id,
        team as team,
        player as player,
        player_id as player_id
    from
        raw_shift
    '''
    
    out = pd.read_sql_query(sql,conn)
    
    out.to_sql('player_game',con=conn,if_exists='append')
    
    c.execute("DROP TABLE IF EXISTS raw_player_game")
    conn.commit()
    out.to_sql('raw_player_game',con=conn)
    
    conn.commit()
    conn.close()
    
    #return out

def player_game_shifts():
    conn = sqlite3.connect(path + '/nhl.db')
    
    sql = '''
    select
        Date as date,
        game_id as game_id,
        team as team,
        player as player,
        player_id as player_id,
        count(*) as shifts,
        sum(duration) as sec_played
    from
        raw_shift
    group by
        Date,
        game_id,
        team,
        player,
        player_id
    '''
    out = pd.read_sql_query(sql,conn)
    
    out.to_sql('player_game_shifts',con=conn,if_exists='append')
    
    conn.commit()
    conn.close()
    
    #return out

def player_game_events():
    conn = sqlite3.connect(path + '/nhl.db')
    
    sql = '''
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "BLOCKED_SHOT" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'BLOCK' and
        a.team <> b.ev_team
    group by
        a.date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "BLOCKED_SHOT"   
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "SHOT_BLOCKED" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'BLOCK' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "SHOT_BLOCKED" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "FAC_WON" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'FAC' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "FAC_WON" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "FAC_LOSS" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'FAC' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "FAC_LOSS" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "GIVEAWAY" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'GIVE' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GIVEAWAY" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "GIVEAWAY_AG" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'GIVE' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GIVEAWAY_AG" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "GOAL_SCORE" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'GOAL' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GOAL_SCORE" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "GOAL_AGAINST" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'GOAL' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GOAL_AGAINST" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "HIT_GIVE" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'HIT' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "HIT_GIVE" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "HIT_TAKE" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'HIT' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "HIT_TAKE" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "MISS_SHOT" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'MISS' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "MISS_SHOT" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "MISS_SHOT_AG" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'MISS' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "MISS_SHOT_AG"
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "PENL_TAKE" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'PENL' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "PENL_TAKE"
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "PENL_DRAW" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'PENL' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "PENL_DRAW"
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "SHOT_TAKE" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'SHOT' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "SHOT_TAKE"
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "SHOT_AG" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'SHOT' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "SHOT_AG"
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "TAKE_AWAY" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'TAKE' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "TAKE_AWAY"
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "TAKE_AWAY_AG" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id = homePlayer1_id or
        a.player_id = homePlayer2_id or
        a.player_id = homePlayer3_id or
        a.player_id = homePlayer4_id or
        a.player_id = homePlayer5_id or
        a.player_id = homePlayer6_id or
        a.player_id = awayPlayer1_id or
        a.player_id = awayPlayer2_id or
        a.player_id = awayPlayer3_id or
        a.player_id = awayPlayer4_id or
        a.player_id = awayPlayer5_id or
        a.player_id = awayPlayer6_id 
        ) and
        b.event = 'TAKE' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "TAKE_AWAY_AG"
    '''
        
    out = pd.read_sql_query(sql,conn).groupby(['game_id','player','player_id','event']).sum().reset_index()
    
    out.to_sql('player_game_events',con=conn,if_exists='append')
      
    conn.commit()
    conn.close()
    
    #return out

def player_game_events_off():
    conn = sqlite3.connect(path + '/nhl.db')
    
    sql = '''
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "BLOCKED_SHOT" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'BLOCK' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "BLOCKED_SHOT"   
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "SHOT_BLOCKED" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'BLOCK' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "SHOT_BLOCKED" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "FAC_WON" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'FAC' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "FAC_WON" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "FAC_LOSS" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'FAC' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "FAC_LOSS" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "GIVEAWAY" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'GIVE' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GIVEAWAY" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "GIVEAWAY_AG" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'GIVE' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GIVEAWAY_AG" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "GOAL_SCORE" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'GOAL' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GOAL_SCORE" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "GOAL_AGAINST" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'GOAL' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GOAL_AGAINST" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "HIT_GIVE" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'HIT' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "HIT_GIVE" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "HIT_TAKE" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'HIT' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "HIT_TAKE" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "MISS_SHOT" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'MISS' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "MISS_SHOT" 
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "MISS_SHOT_AG" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'MISS' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "MISS_SHOT_AG"
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "PENL_TAKE" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'PENL' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "PENL_TAKE"
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "PENL_DRAW" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'PENL' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "PENL_DRAW"
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "SHOT_TAKE" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'SHOT' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "SHOT_TAKE"
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "SHOT_AG" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'SHOT' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "SHOT_AG"
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "TAKE_AWAY" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'TAKE' and
        a.team = b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "TAKE_AWAY"
    UNION
    select
        a.Date as date,
        a.game_id as game_id,
        a.team as team,
        a.player as player,
        a.player_id as player_id,
        "TAKE_AWAY_AG" as event,
        count(*) as cnt
    from
        player_game a
    inner join
        raw_pbp b
        on a.game_id = b.game_id and
        (
        a.player_id <> homePlayer1_id and
        a.player_id <> homePlayer2_id and
        a.player_id <> homePlayer3_id and
        a.player_id <> homePlayer4_id and
        a.player_id <> homePlayer5_id and
        a.player_id <> homePlayer6_id and
        a.player_id <> awayPlayer1_id and
        a.player_id <> awayPlayer2_id and
        a.player_id <> awayPlayer3_id and
        a.player_id <> awayPlayer4_id and
        a.player_id <> awayPlayer5_id and
        a.player_id <> awayPlayer6_id 
        ) and
        b.event = 'TAKE' and
        a.team <> b.ev_team
    group by
        a.Date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "TAKE_AWAY_AG"
    '''
        
    out = pd.read_sql_query(sql,conn)
    
    out.to_sql('player_game_events_off',con=conn,if_exists='append')
      
    conn.commit()
    conn.close()
    
    #return out

def player_game_actions():
    conn = sqlite3.connect(path + '/nhl.db')
    
    sql = '''
    select 
        date as date,
        game_id as game_id,
        case when away_team = ev_team then home_team else away_team end as team,
        p1_name as player,
        p1_ID as player_id,
        "BLOCKED_SHOT" as event,
        count(*) as cnt
    from 
        raw_pbp
    where
        event = 'BLOCK'
    group by
        date,
        game_id,
        case when away_team = ev_team then home_team else away_team end,
        p1_name,
        p1_ID,
        "BLOCKED_SHOT"
    ''' 
        
    out = pd.read_sql_query(sql,conn)
    
    sql = '''
    select 
        date as date,
        game_id as game_id,
        ev_team as team,
        p2_name as player,
        p2_ID as player_id,
        "SHOT_BLOCKED" as event,
        count(*) as cnt
    from 
        raw_pbp
    where
        event = 'BLOCK'
    group by
        date,
        game_id,
        ev_team,
        p2_name,
        p2_ID,
        "SHOT_BLOCKED"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
        date as date,
        game_id as game_id,
        ev_team as team,
        p1_name as player,
        p1_ID as player_id,
        "FAC_WON" as event,
        count(*) as cnt
    from 
        raw_pbp
    where
        event = 'FAC'
    group by
        date,
        game_id,
        ev_team,
        p1_name,
        p1_ID,
        "FAC_WON"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
        date as date,
        game_id as game_id,
        case when away_team = ev_team then home_team else away_team end as team,
        p2_name as player,
        p2_ID as player_id,
        "FAC_LOSS" as event,
        count(*) as cnt
    from 
        raw_pbp
    where
        event = 'FAC'
    group by
        date,
        game_id,
        case when away_team = ev_team then home_team else away_team end,
        p2_name,
        p2_ID,
        "FAC_LOSS"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))

    sql = '''
    select 
        date as date,
        game_id as game_id,
        ev_team as team,
        p1_name as player,
        p1_ID as player_id,
        "GIVEAWAY" as event,
        count(*) as cnt
    from 
        raw_pbp
    where
        event = 'GIVE'
    group by
        date,
        game_id,
        ev_team,
        p1_name,
        p1_ID,
        "GIVEAWAY"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
        date as date,
        game_id as game_id,
        ev_team as team,
        p1_name as player,
        p1_ID as player_id,
        "GOAL_SCORE" as event,
        count(*) as cnt
    from 
        raw_pbp
    where
        event = 'GOAL'
    group by
        date,
        game_id,
        ev_team,
        p1_name,
        p1_ID,
        "GOAL_SCORE"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))
  
    sql = '''
    select 
        date as date,
        game_id as game_id,
        ev_team as team,
        p2_name as player,
        p2_ID as player_id,
        "GOAL_ASSIST1" as event,
        count(*) as cnt
    from 
        raw_pbp
    where
        event = 'GOAL' and
        p2_name is not null
    group by
        date,
        game_id,
        ev_team,
        p2_name,
        p2_ID,
        "GOAL_ASSIST1"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))

    sql = '''
    select 
        date as date,
        game_id as game_id,
        ev_team as team,
        p3_name as player,
        p3_ID as player_id,
        "GOAL_ASSIST2" as event,
        count(*) as cnt
    from 
        raw_pbp
    where
        event = 'GOAL' and
        p3_name is not null
    group by
        date,
        game_id,
        ev_team,
        p3_name,
        p3_ID,
        "GOAL_ASSIST2"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
        date as date,
        game_id as game_id,
        ev_team as team,
        p1_name as player,
        p1_ID as player_id,
        "HIT_GIVE" as event,
        count(*) as cnt
    from 
        raw_pbp
    where
        event = 'HIT' 
    group by
        date,
        game_id,
        ev_team,
        p1_name,
        p1_ID,
        "HIT_GIVE"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
        date as date,
        game_id as game_id,
        case when away_team = ev_team then home_team else away_team end as team,
        p2_name as player,
        p2_ID as player_id,
        "HIT_TAKE" as event,
        count(*) as cnt
    from 
        raw_pbp
    where
        event = 'HIT'
    group by
        date,
        game_id,
        case when away_team = ev_team then home_team else away_team end,
        p2_name,
        p2_ID,
        "HIT_TAKE"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
        date as date,
        game_id as game_id,
        ev_team as team,
        p1_name as player,
        p1_ID as player_id,
        "MISS_SHOT" as event,
        count(*) as cnt
    from 
        raw_pbp
    where
        event = 'MISS' 
    group by
        date,
        game_id,
        ev_team,
        p1_name,
        p1_ID,
        "MISS_SHOT"
    ''' 
    out = out.append(pd.read_sql_query(sql,conn))    
    
    sql = '''
    select 
        date as date,
        game_id as game_id,
        ev_team as team,
        p1_name as player,
        p1_ID as player_id,
        "PENL_TAKE" as event,
        count(*) as cnt
    from 
        raw_pbp
    where
        event = 'PENL' 
    group by
        date,
        game_id,
        ev_team,
        p1_name,
        p1_ID,
        "PENL_TAKE"
    ''' 
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
        date as date,
        game_id as game_id,
        case when away_team = ev_team then home_team else away_team end as team,
        p2_name as player,
        p2_ID as player_id,
        "PENL_DRAW" as event,
        count(*) as cnt
    from 
        raw_pbp
    where
        event = 'PENL' and
        p2_name is not null
    group by
        date,
        game_id,
        case when away_team = ev_team then home_team else away_team end,
        p2_name,
        p2_ID,
        "PENL_DRAW"
    ''' 
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
        date as date,
        game_id as game_id,
        ev_team as team,
        p1_name as player,
        p1_ID as player_id,
        "SHOT_TAKE" as event,
        count(*) as cnt
    from 
        raw_pbp
    where
        event = 'SHOT' 
    group by
        date,
        game_id,
        ev_team,
        p1_name,
        p1_ID,
        "SHOT_TAKE"
    ''' 
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
        date as date,
        game_id as game_id,
        ev_team as team,
        p1_name as player,
        p1_ID as player_id,
        "TAKE_AWAY" as event,
        count(*) as cnt
    from 
        raw_pbp
    where
        event = 'TAKE' 
    group by
        date,
        game_id,
        ev_team,
        p1_name,
        p1_ID,
        "TAKE_AWAY"
    ''' 
    out = out.append(pd.read_sql_query(sql,conn))
    
    out.to_sql('player_game_actions',con=conn,if_exists='append')

    conn.commit()
    conn.close()
    
    #return out

def player_game_raw_stats():
    conn = sqlite3.connect(path + '/nhl.db')
    
    sql = '''
    select
        a.date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        case when b.event = 'BLOCKED_SHOT' then b.cnt else 0 end as blocked_shot,
        case when b.event = 'FAC_LOSS' then b.cnt else 0 end as fac_loss,
        case when b.event = 'FAC_WON' then b.cnt else 0 end as fac_won,
        case when b.event = 'GIVEAWAY' then b.cnt else 0 end as giveaway,
        case when b.event = 'GOAL_ASSIST1' then b.cnt else 0 end as goal_assist1,
        case when b.event = 'GOAL_ASSIST2' then b.cnt else 0 end as goal_assist2,
        case when b.event = 'GOAL_SCORE' then b.cnt else 0 end as goal_score,
        case when b.event = 'HIT_GIVE' then b.cnt else 0 end as hit_give,
        case when b.event = 'HIT_TAKE' then b.cnt else 0 end as hit_take,
        case when b.event = 'MISS_SHOT' then b.cnt else 0 end as miss_shot,
        case when b.event = 'PENL_DRAW' then b.cnt else 0 end as penl_draw,
        case when b.event = 'PENL_TAKE' then b.cnt else 0 end as penl_take,
        case when b.event = 'SHOT_BLOCKED' then b.cnt else 0 end as shot_blocked,
        case when b.event = 'SHOT_TAKE' then b.cnt else 0 end as shot_take,
        case when b.event = 'TAKE_AWAY' then b.cnt else 0 end as take_away
    from
        raw_player_game a
    inner join
        player_game_actions b
        on a.game_id = b.game_id and
        a.player_id = b.player_id
    '''
    
    out1 = pd.read_sql_query(sql,conn).groupby(['game_id','team','player','player_id'],sort=False).max().reset_index()
    
    sql = '''
    select
        a.date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,        
        case when c.event = 'BLOCKED_SHOT' then c.cnt else 0 end as blocked_shot_on,
        case when c.event = 'FAC_LOSS' then c.cnt else 0 end as fac_loss_on,
        case when c.event = 'FAC_WON' then c.cnt else 0 end as fac_won_on,
        case when c.event = 'GIVEAWAY' then c.cnt else 0 end as giveaway_on,
        case when c.event = 'GIVEAWAY_AG' then c.cnt else 0 end as giveaway_ag_on,
        case when c.event = 'GOAL_AGAINST' then c.cnt else 0 end as goal_against_on,
        case when c.event = 'GOAL_SCORE' then c.cnt else 0 end as goal_score_on,
        case when c.event = 'HIT_GIVE' then c.cnt else 0 end as hit_give_on,
        case when c.event = 'HIT_TAKE' then c.cnt else 0 end as hit_take_on,
        case when c.event = 'MISS_SHOT' then c.cnt else 0 end as miss_shot_on,
        case when c.event = 'MISS_SHOT_AG' then c.cnt else 0 end as miss_shot_ag_on,
        case when c.event = 'PENL_DRAW' then c.cnt else 0 end as penl_draw_on,
        case when c.event = 'PENL_TAKE' then c.cnt else 0 end as penl_take_on,
        case when c.event = 'SHOT_AG' then c.cnt else 0 end as shot_ag_on,
        case when c.event = 'SHOT_BLOCKED' then c.cnt else 0 end as shot_blocked_on,
        case when c.event = 'SHOT_TAKE' then c.cnt else 0 end as shot_take_on,
        case when c.event = 'TAKE_AWAY' then c.cnt else 0 end as take_away_on,
        case when c.event = 'TAKE_AWAY_AG' then c.cnt else 0 end as take_away_ag_on
        
    from
        raw_player_game a
    inner join
        player_game_events c
        on a.game_id = c.game_id and
        a.player_id = c.player_id
    '''
    out2 = pd.read_sql_query(sql,conn).groupby(['game_id','team','player','player_id'],sort=False).max().reset_index()
    
    sql = '''
    select
        a.date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        
        case when d.event = 'BLOCKED_SHOT' then d.cnt else 0 end as blocked_shot_off,
        case when d.event = 'FAC_LOSS' then d.cnt else 0 end as fac_loss_off,
        case when d.event = 'FAC_WON' then d.cnt else 0 end as fac_won_off,
        case when d.event = 'GIVEAWAY' then d.cnt else 0 end as giveaway_off,
        case when d.event = 'GIVEAWAY_AG' then d.cnt else 0 end as giveaway_ag_off,
        case when d.event = 'GOAL_AGAINST' then d.cnt else 0 end as goal_against_off,
        case when d.event = 'GOAL_SCORE' then d.cnt else 0 end as goal_score_off,
        case when d.event = 'HIT_GIVE' then d.cnt else 0 end as hit_give_off,
        case when d.event = 'HIT_TAKE' then d.cnt else 0 end as hit_take_off,
        case when d.event = 'MISS_SHOT' then d.cnt else 0 end as miss_shot_off,
        case when d.event = 'MISS_SHOT_AG' then d.cnt else 0 end as miss_shot_ag_off,
        case when d.event = 'PENL_DRAW' then d.cnt else 0 end as penl_draw_off,
        case when d.event = 'PENL_TAKE' then d.cnt else 0 end as penl_take_off,
        case when d.event = 'SHOT_AG' then d.cnt else 0 end as shot_ag_off,
        case when d.event = 'SHOT_BLOCKED' then d.cnt else 0 end as shot_blocked_off,
        case when d.event = 'SHOT_TAKE' then d.cnt else 0 end as shot_take_off,
        case when d.event = 'TAKE_AWAY' then d.cnt else 0 end as take_away_off,
        case when d.event = 'TAKE_AWAY_AG' then d.cnt else 0 end as take_away_ag_off
        
    from
        raw_player_game a
    inner join
        player_game_events_off d
        on a.game_id = d.game_id and
        a.player_id = d.player_id
    '''
    
    out3 = pd.read_sql_query(sql,conn).groupby(['game_id','team','player','player_id'],sort=False).max().reset_index()
    
    sql = '''
    select
        a.date,
        a.game_id,
        a.team,
        a.player,
        a.player_id,        
        e.shifts,
        e.sec_played,
        (3600 - e.sec_played) as sec_off        
    from
        raw_player_game a
    inner join
        player_game_shifts e
        on a.game_id = e.game_id and
        a.player_id = e.player_id
    '''
    out4 = pd.read_sql_query(sql,conn).groupby(['date','game_id','team','player','player_id'],sort=False).max().reset_index()
    
    out = out4.merge(out1,how='left',left_on=['date','game_id','team','player','player_id'],right_on=['date','game_id','team','player','player_id'])
    out = out.merge(out2,how='left',left_on=['date','game_id','team','player','player_id'],right_on=['date','game_id','team','player','player_id'])
    out = out.merge(out3,how='left',left_on=['date','game_id','team','player','player_id'],right_on=['date','game_id','team','player','player_id'])
    
    out.to_sql('player_game_raw_stats',con=conn,if_exists='append')

    conn.commit()
    conn.close()
    
    #return out
    
def train_model(dt):
    
        
    path = 'C:/Users/geoff/Documents/GitHub/nhl'
    conn = sqlite3.connect(path + '/nhl.db')

    sql = '''
    select distinct * from player_game_raw_stats where shifts >= 3 and sec_played <= 2000 and date < %s
    '''%(dt)
    
    df = pd.read_sql_query(sql,conn)
    df2 = df.iloc[:,9:].div(df['sec_played'],axis=0)
    df3 = df.iloc[:,1:6]
    df4 = df3.merge(df2,how='inner',left_index=True,right_index=True)
    df5 = df4.groupby(['date','game_id','team'],sort=False).mean().reset_index().drop(columns=['player_id'])
    #date = df5['date']
    #gid = df5['game_id']
    #team = df5['team']
    #df5 = df5.drop(['game_id'],axis=1)
    #df6 = df5.groupby(['team']).transform(lambda x: x.expanding().mean().shift()).add_prefix('exp_')
    #df6['date'] = date
    #df6['game_id'] = gid
    #df6['team'] = team
    
    df7 = df5.merge(df5,how='inner',left_on=['date','game_id'],right_on=['date','game_id'])
    df8 = df7[~(df7['team_x'] == df7['team_y'])]
    df9 = df8.drop_duplicates(subset=['date','game_id'])
    
    
    sql = '''
    select * from player_game_raw_stats
    '''
    
    d = pd.read_sql_query(sql,conn)
    d2 = d.groupby(['date','game_id'],sort=False).sum().reset_index()
    d3 = d2[['date','game_id','goal_score']]
    
    df10 = d3.merge(df9,how='inner',left_on=['date','game_id'],right_on=['date','game_id']).drop(columns=['date','game_id','team_x','team_y'])    
    
    import numpy as np
    import matplotlib.pyplot as plt
    
    from sklearn import ensemble
    from sklearn import datasets
    from sklearn.utils import shuffle
    from sklearn.metrics import mean_squared_error
    from sklearn.model_selection import train_test_split
    
    df10 = df10.drop(['goal_assist1_x','goal_assist1_y','goal_assist2_x','goal_assist2_y','goal_score_x','goal_score_y','goal_score_on_x','goal_score_on_y','goal_against_on_x','goal_against_on_y','goal_against_off_x','goal_against_off_y','goal_score_off_x','goal_score_off_y'],axis=1)
    df10 = df10.fillna(0)
    train, test = train_test_split(df10, test_size=0.2)
    x_train = train.iloc[:,1:]
    y_train = train.iloc[:,0]
    x_test = test.iloc[:,1:]
    y_test = test.iloc[:,0]
    
    params = {'n_estimators': 500, 'max_depth': 4, 'min_samples_split': 2,
              'learning_rate': 0.01, 'loss': 'ls'}
    clf = ensemble.GradientBoostingRegressor(**params)
    
    clf.fit(x_train, y_train)
    mse = mean_squared_error(y_test, clf.predict(x_test))
    print("MSE: %.4f" % mse)
    
    # compute test set deviance
    test_score = np.zeros((params['n_estimators'],), dtype=np.float64)
    
    for i, y_pred in enumerate(clf.staged_predict(x_test)):
        test_score[i] = clf.loss_(y_test, y_pred)
    
    y_pred=clf.predict(x_test)
    
    z = y_pred - np.mean(y_pred)
    
    out = np.column_stack((y_test,y_pred,z))
    out = out[out[:,2].argsort()]
    
    return clf
    
    
    
#    from sklearn.preprocessing import StandardScaler
## Separating out the features
#x = df6.iloc[:, 1:].values
## Separating out the target
#y = df6.loc[:,['team']].values
#
#x = StandardScaler().fit_transform(x)
#
#from sklearn.decomposition import PCA
#pca = PCA(n_components=2)
#principalComponents = pca.fit_transform(x)
#principalDf = pd.DataFrame(data = principalComponents
#             , columns = ['pc1', 'pc2'])
#
#finalDf = pd.concat([principalDf, df6[['team']]], axis = 1)
#
#ax = principalDf.plot(x='pc1',y='pc2',style=['o'])
#for i, txt in enumerate(df6.team):
#    ax.annotate(txt, (principalDf.pc1.iat[i],principalDf.pc2.iat[i]))