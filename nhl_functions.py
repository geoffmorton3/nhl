import pandas as pd
import sqlite3
import hockey_scraper as hs

path = 'C:/Users/geoff/Documents/GitHub/Py/nhl'

def scrape_date_to_db(dt1,dt2):
    hs.scrape_date_range(dt1,dt2,True,docs_dir=path)
    try:
        df_pbp = pd.read_csv(path+'/nhl_pbp'+dt1+'--'+dt2+'.csv')
        df_shft = pd.read_csv(path+'/nhl_shifts'+dt1+'--'+dt2+'.csv')
        
        conn = sqlite3.connect('C:/Users/geoff/Documents/GitHub/Py/nhl.db')
        
        df_pbp.to_sql('raw_pbp',con=conn,if_exists='replace')
        df_shft.to_sql('raw_shift',con=conn,if_exists='replace')
    
        conn.commit()
        conn.close()
        
        player_game()
        player_game_shifts()
        player_game_events()
        player_game_events_off()
        player_game_actions()    
        player_game_raw_stats()
        print("Successfully added games between " + dt1 + " and " + dt2)
    except FileNotFoundError:
        print("No Games between " + dt1 + " and " + dt2)
    
def player_game():
    conn = sqlite3.connect('C:/Users/geoff/Documents/GitHub/Py/nhl.db')
    
    sql = '''
    select distinct
        game_id as game_id,
        team as team,
        player as player,
        player_id as player_id
    from
        raw_shift
    '''
    
    out = pd.read_sql_query(sql,conn)
    
    out.to_sql('player_game',con=conn,if_exists='append')
    
    conn.commit()
    conn.close()
    
    #return out

def player_game_shifts():
    conn = sqlite3.connect('C:/Users/geoff/Documents/GitHub/Py/nhl.db')
    
    sql = '''
    select
        game_id as game_id,
        team as team,
        player as player,
        player_id as player_id,
        count(*) as shifts,
        sum(duration) as sec_played
    from
        raw_shift
    group by
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
    conn = sqlite3.connect('C:/Users/geoff/Documents/GitHub/Py/nhl.db')
    
    sql = '''
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "BLOCKED_SHOT"   
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "SHOT_BLOCKED" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "FAC_WON" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "FAC_LOSS" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GIVEAWAY" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GIVEAWAY_AG" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GOAL_SCORE" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GOAL_AGAINST" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "HIT_GIVE" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "HIT_TAKE" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "MISS_SHOT" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "MISS_SHOT_AG"
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "PENL_TAKE"
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "PENL_DRAW"
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "SHOT_TAKE"
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "SHOT_AG"
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "TAKE_AWAY"
    UNION
    select
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
    conn = sqlite3.connect('C:/Users/geoff/Documents/GitHub/Py/nhl.db')
    
    sql = '''
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "BLOCKED_SHOT"   
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "SHOT_BLOCKED" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "FAC_WON" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "FAC_LOSS" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GIVEAWAY" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GIVEAWAY_AG" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GOAL_SCORE" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "GOAL_AGAINST" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "HIT_GIVE" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "HIT_TAKE" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "MISS_SHOT" 
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "MISS_SHOT_AG"
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "PENL_TAKE"
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "PENL_DRAW"
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "SHOT_TAKE"
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "SHOT_AG"
    UNION
    select
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
        a.game_id,
        a.team,
        a.player,
        a.player_id,
        "TAKE_AWAY"
    UNION
    select
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
    conn = sqlite3.connect('C:/Users/geoff/Documents/GitHub/Py/nhl.db')
    
    sql = '''
    select 
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
        game_id,
        case when away_team = ev_team then home_team else away_team end,
        p1_name,
        p1_ID,
        "BLOCKED_SHOT"
    ''' 
        
    out = pd.read_sql_query(sql,conn)
    
    sql = '''
    select 
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
        game_id,
        ev_team,
        p2_name,
        p2_ID,
        "SHOT_BLOCKED"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
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
        game_id,
        ev_team,
        p1_name,
        p1_ID,
        "FAC_WON"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
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
        game_id,
        case when away_team = ev_team then home_team else away_team end,
        p2_name,
        p2_ID,
        "FAC_LOSS"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))

    sql = '''
    select 
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
        game_id,
        ev_team,
        p1_name,
        p1_ID,
        "GIVEAWAY"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
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
        game_id,
        ev_team,
        p1_name,
        p1_ID,
        "GOAL_SCORE"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))
  
    sql = '''
    select 
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
        game_id,
        ev_team,
        p2_name,
        p2_ID,
        "GOAL_ASSIST1"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))

    sql = '''
    select 
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
        game_id,
        ev_team,
        p3_name,
        p3_ID,
        "GOAL_ASSIST2"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
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
        game_id,
        ev_team,
        p1_name,
        p1_ID,
        "HIT_GIVE"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
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
        game_id,
        case when away_team = ev_team then home_team else away_team end,
        p2_name,
        p2_ID,
        "HIT_TAKE"
    ''' 
        
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
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
        game_id,
        ev_team,
        p1_name,
        p1_ID,
        "MISS_SHOT"
    ''' 
    out = out.append(pd.read_sql_query(sql,conn))    
    
    sql = '''
    select 
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
        game_id,
        ev_team,
        p1_name,
        p1_ID,
        "PENL_TAKE"
    ''' 
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
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
        game_id,
        case when away_team = ev_team then home_team else away_team end,
        p2_name,
        p2_ID,
        "PENL_DRAW"
    ''' 
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
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
        game_id,
        ev_team,
        p1_name,
        p1_ID,
        "SHOT_TAKE"
    ''' 
    out = out.append(pd.read_sql_query(sql,conn))
    
    sql = '''
    select 
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
    conn = sqlite3.connect('C:/Users/geoff/Documents/GitHub/Py/nhl.db')
    
    sql = '''
    select
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
        case when b.event = 'TAKE_AWAY' then b.cnt else 0 end as take_away,
        
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
        case when c.event = 'TAKE_AWAY_AG' then c.cnt else 0 end as take_away_ag_on,
        
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
        case when d.event = 'TAKE_AWAY_AG' then d.cnt else 0 end as take_away_ag_off,
        
        e.shifts,
        e.sec_played,
        (3600 - e.sec_played) as sec_off
        
        
    from
        player_game a
    inner join
        player_game_actions b
        on a.game_id = b.game_id and
        a.player_id = b.player_id
    inner join
        player_game_events c
        on a.game_id = c.game_id and
        a.player_id = c.player_id
    inner join
        player_game_events_off d
        on a.game_id = d.game_id and
        a.player_id = d.player_id
    inner join
        player_game_shifts e
        on a.game_id = e.game_id and
        a.player_id = e.player_id
    '''
    
    out = pd.read_sql_query(sql,conn).groupby(['game_id','team','player','player_id'],sort=False).max().reset_index()
    
    out.to_sql('player_game_raw_stats',con=conn,if_exists='append')

    conn.commit()
    conn.close()
    
    #return out