# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu

# Read recipe inputs
pgn_files = dataiku.Folder("b2MJgQKd")
pgn_files_info = pgn_files.get_info()


# Compute recipe outputs
# TODO: Write here your actual code that computes the outputs
# NB: DSS supports several kinds of APIs for reading and writing data. Please see doc.

new_games_df = ... # Compute a Pandas dataframe to write into new_games


# Write recipe outputs
new_games = dataiku.Dataset("new_games")
new_games.write_with_schema(new_games_df)
