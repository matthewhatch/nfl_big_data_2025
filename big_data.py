import argparse
import pandas as pd
from load_data import load_games, load_players, load_plays, load_player_play

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('action', type=str, choices=['load'])
    parser.add_argument('object', type=str, choices=['games','players', 'plays', 'pp'])

    args = parser.parse_args()
    if args.action == 'load':
        if args.object == 'games':
            load_games()
        if args.object == 'players':
            load_players()
        if args.object == 'plays':
            load_plays()
        if args.object == 'pp':
            load_player_play()
