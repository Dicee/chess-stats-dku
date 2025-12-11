# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs
games_prepared = dataiku.Dataset("games_prepared")
games_prepared_df = games_prepared.get_dataframe()

def expand_to_prefix_moves(df, max_prefixes):
    df['game_moves_list'] = df['game_moves'].str.split()

    def get_prefixes(moves):
        prefixes = []
        current_prefix = []
        for move in moves:
            current_prefix.append(move)
            prefixes.append(' '.join(current_prefix))
            if len(prefixes) == max_prefixes:
                break
        return prefixes

    df['game_moves_prefix'] = df['game_moves_list'].apply(get_prefixes)
    df_expanded = df.explode('game_moves_prefix')
    df_expanded = df_expanded.drop(columns=['game_moves_list'])
    
    # will be used to take the longest common prefix, which means it is the most precise characterization of the opening
    df_expanded['game_moves_prefix_length'] = df_expanded['game_moves_prefix'].str.len()

    return df_expanded

# account for an opening length of at most 15 moves from white and 15 from black
expanded_games = expand_to_prefix_moves(games_prepared_df.copy(), 30)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
games_exploded_by_moves_prefix = dataiku.Dataset("games_exploded_by_moves_prefix")
games_exploded_by_moves_prefix.write_with_schema(expanded_games)
