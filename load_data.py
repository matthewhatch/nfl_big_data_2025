import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, select, ForeignKey, Float
CONNECTION = 'postgresql://big_data:big_data_bowl@localhost:5432/nfl'

def load_games():
    # Load the data
    games = pd.read_csv('data/games.csv')
    # load data into postgres table
    # if games table does not exits, create it
    engine = create_engine(CONNECTION)
    create_games_table(engine)
    games.to_sql('games', con=engine, if_exists='replace', index=False)
    print_all_games(engine)
    # all games loaded, print them

    return games

def load_players():
    # Load the data
    players = pd.read_csv('data/players.csv')
    engine = create_engine(CONNECTION)
    players.to_sql('players', con=engine, if_exists='replace', index=False)
    create_players_table(engine)
    print_all_players(engine)
    # return players

def load_player_play():
    pp = pd.read_csv('data/player_play.csv')
    print(pp.columns)

def load_plays():
    plays = pd.read_csv('data/plays.csv')
    engine = create_engine(CONNECTION)
    plays.to_sql('plays', con=engine, if_exists='replace', index=False)
    create_plays_table(engine)
    print_all_plays(engine)

def create_plays_table(engine):
    metadata = MetaData()

    plays_table = Table('plays', metadata,
                        Column('playId', Integer, primary_key=True),
                        Column('gameId', Integer, ForeignKey('games.id')),
                        Column('playDescription', String),
                        Column('quarter', Integer),
                        Column('down', Integer),
                        Column('yardToGo', Integer),
                        Column('defensiveTeam', String),
                        Column('yardlineSide', String),
                        Column('yardlineNumber', Integer),
                        Column('gameClock', String),
                        Column('preSnapHomeScore', Integer),
                        Column('preSnapVisitorScore', Integer),
                        Column('playNullifiedByPenalty', String),
                        Column('absoluteYardlineNumber', Integer),
                        Column('preSnapHomeTeamWinProbability', Float),
                        Column('preSnapVisitorTeamWinProbability', Float),
                        Column('expectedPoints', Float),
                        Column('offenseFormation', String),
                        Column('receiverAlignment', String),
                        Column('playClockAtSnap', String),
                        Column('passResult', String),
                        Column('passLength', Integer),
                        Column('targetX', Integer),
                        Column('targetY', Integer),
                        Column('playAction', String),
                        Column('dropbackType', String),
                        Column('dropbackDistance', Float),
                        Column('passLocationType', String),
                        Column('timeToThrow', Float),
                        Column('timeInTackleBox', Float),
                        Column('timeToSack', Float),
                        Column('passTippedAtLine', String),
                        Column('unblockedPressure', String),
                        Column('qbSpike', String),
                        Column('qbKneel', String),
                        Column('qbSneak', String),
                        Column('rushLocationType', String),
                        Column('penaltyYards', Float),
                        Column('prePenaltyYardsGained', Float),
                        Column('yardsGained', Float),
                        Column('homeTeamWinProbabilityAdded', Float),
                        Column('visitorTeamWinProbilityAdded', Float),
                        Column('expectedPointsAdded', Float),
                        Column('isDropback', String),
                        Column('pff_runConceptPrimary', String),
                        Column('pff_runConceptSecondary', String),
                        Column('pff_runPassOption', String),
                        Column('pff_passCoverage', String),
                        Column('pff_manZone', String)                        
                        )
    metadata.create_all(engine)
    
    
def create_games_table(engine):
    metadata = MetaData()
    
    games_table = Table('games', metadata,
                        Column('id', Integer, primary_key=True),
                        Column('season', String),
                        Column('week', String),
                        Column('gameDate', String),
                        Column('gameTimeEastern', String),
                        Column('homeTeamAbbr', String),
                        Column('vistorTeamAbbr', String),
                        Column('homeFinalScore', Integer),
                        Column('weatvisitorFinalScoreher', String)
                        )
    metadata.create_all(engine)
    

def create_players_table(engine):
    metadata = MetaData()

    players_table = Table('players', metadata,
                        Column('nflId', Integer, primary_key=True),
                        Column('height', String),
                        Column('weight', String),
                        Column('birthDate', String),
                        Column('collegeName', String),
                        Column('position', String),
                        Column('displayName', String)
                        )
    metadata.create_all(engine)

def print_all_games(engine):
    metadata = MetaData()
    games_table = Table('games', metadata, autoload_with=engine)
    
    with engine.connect() as connection:
        query = select(games_table)
        result = connection.execute(query)
        
        for row in result:
            print(row)

def print_all_players(engine):
    metadata = MetaData()
    players_table = Table('players', metadata, autoload_with=engine)
    
    with engine.connect() as connection:
        query = select(players_table)
        result = connection.execute(query)
        
        for row in result:
            print(row)

def print_all_plays(engine):
    metadata = MetaData()
    plays_table = Table('plays', metadata, autoload_with=engine)
    
    with engine.connect() as connection:
        query = select(plays_table)
        result = connection.execute(query)
        
        for row in result:
            print(row)
    
