# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
# -*- coding: utf-8 -*-
import dataiku
import pandas as pd, numpy as np
from dataiku import pandasutils as pdu
from pathlib import Path
import dataiku
import io
import argparse
import datetime as dt
import zoneinfo
import csv
import re
import sys
import os
import json
import requests
import logging

logger = logging.getLogger("recipe_logger")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
project = dataiku.Project()
variables = project.get_variables()['standard']

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
managed_folder = dataiku.Folder("b2MJgQKd")

def cached_pgn_exists(path):
    try:
        details = managed_folder.get_path_details(path)
        return details['exists']
    except Exception:
        return False

def download_locally_with_same_path(path):
    logger.info(f"Downloading {path} from cache")
    with managed_folder.get_download_stream(Path('/') / path) as stream:
        with open(path, "wb") as f:
            f.write(stream.read())

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
GAME_SYNTHESIS_HEADER = ["link", "date", "color", "elo", "time_control", "variant", "termination", "result", "opening_family", "eco", "moves"]
EXCLUDED_VARIANTS = ["Atomic", "Horde", "Crazyhouse", "Chess960"]

def get_month_range(start_date, end_date):
    """
    Generates a sequence of (year, month) tuples between two dates.
    """
    current_date = start_date
    while current_date <= end_date:
        yield (current_date.year, current_date.month)
        # Move to the next month
        if current_date.month == 12:
            current_date = current_date.replace(year=current_date.year + 1, month=1)
        else:
            current_date = current_date.replace(month=current_date.month + 1)


def fetch_chess_com_games(username, start_date, end_date, platform_base_dir):
    """
    Fetches games for a chess.com user and saves them to PGN files.
    """
    print(f"\nProcessing chess.com games for user: {username}")

    user_games_dir = platform_base_dir / username
    user_games_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"  - Created user directory: {user_games_dir}")

    months = list(get_month_range(start_date, end_date))

    for year, month in months:
        pgn_path = user_games_dir / f"{username}-{year}-{month:02d}.pgn"

        if cached_pgn_exists(pgn_path):
            download_locally_with_same_path(pgn_path)
            continue

        logger.info(f"{pgn_path} is missing from cache, fetching it...")
        url = f"https://api.chess.com/pub/player/{username}/games/{year}/{month:02d}"
        try:
            response = requests.get(url, headers={"User-Agent": "chess-stats-retriever/1.0"})
            response.raise_for_status()  # Raise an exception for bad status codes

            with open(pgn_path, "w") as f:
                f.write(response.text)

        except requests.exceptions.RequestException as e:
            logger.info(f"Warning: Could not retrieve games for {year}-{month:02d}. Error: {e}", file=sys.stderr)


def fetch_lichess_games(username, start_date, end_date, platform_base_dir):
    """
    Fetches games for a lichess.org user and saves them to a PGN file.
    """
    logger.info(f"\nProcessing lichess.org games for user: {username}")

    user_games_dir = platform_base_dir / username
    user_games_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"  - Created user directory: {user_games_dir}")

    months = list(get_month_range(start_date.date(), end_date.date()))

    for year, month in months:
        pgn_path = user_games_dir / f"{username}-{year}-{month:02d}.pgn"

        if cached_pgn_exists(pgn_path):
            download_locally_with_same_path(pgn_path)
            continue

        logger.info(f"{pgn_path} is missing from cache, fetching it...")

        # Lichess API uses timestamps in milliseconds
        start_of_month = dt.datetime(year, month, 1)

        if month == 12:
            end_of_month = dt.datetime(year + 1, 1, 1) - dt.timedelta(milliseconds=1)
        else:
            end_of_month = dt.datetime(year, month + 1, 1) - dt.timedelta(milliseconds=1)

        since = int(start_of_month.timestamp() * 1000)
        until = int(end_of_month.timestamp() * 1000)

        url = f"https://lichess.org/api/games/user/{username}"
        params = {
            "since": since,
            "until": until,
            "pgnInJson": "false",
            "clocks": "true",
            "evals": "true",
        }

        try:
            with requests.get(url, params=params, stream=True, headers={"User-Agent": "chess-stats-retriever/1.0"}) as response:
                response.raise_for_status()

                with open(pgn_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)

        except requests.exceptions.RequestException as e:
            logger.info(f"Warning: Could not retrieve games for {year}-{month:02d}. Error: {e}", file=sys.stderr)


def validate_date(date_string):
    """
    Validates and parses date strings in YYYY-MM format.
    Returns a datetime.date object.
    """
    if not date_string:
        return None

    try:
        return dt.datetime.strptime(date_string, '%Y-%m').date()
    except ValueError:
        pass

    raise argparse.ArgumentTypeError(
        f"Invalid date format: {date_string}. Expected YYYY-MM format."
    )

def parse_chess_com_games(username, output_file, chess_com_base_dir):
    """
    Parses chess.com games and appends them to a TSV file.
    """
    logger.info(f"\nParsing chess.com games for user: {username}")
    user_games_dir = chess_com_base_dir / username

    pgn_files = os.listdir(user_games_dir)

    file_exists = output_file.exists() and output_file.stat().st_size > 0
    rows = 0

    with open(output_file, "a", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        if not file_exists:
            writer.writerow(GAME_SYNTHESIS_HEADER)

        for pgn_file in pgn_files:
            pgn_file = user_games_dir / pgn_file

            with open(pgn_file, "r") as pgn:
                content = pgn.read()

            games = json.loads(content)["games"]

            for json_game in games:
                url = json_game["url"]
                file_name_parts = pgn_file.stem.split('-')
                date = f"{file_name_parts[1]}-{file_name_parts[2]}"

                if not "pgn" in json_game:
                    continue # can happen for example for bughouse games

                game = parse_pgn(username, json_game["pgn"])

                if game is None or game["variant"] in EXCLUDED_VARIANTS:
                    continue

                rows += 1
                writer.writerow([
                    url,
                    date,
                    game["color"],
                    game["elo"],
                    game["time_control"],
                    game["variant"],
                    game["termination"],
                    game["result"],
                    game["opening_family"],
                    game["eco"],
                    game["moves"],
                ])
    logger.info(f"  - Wrote {rows} rows for chess.com games to {output_file}")

def parse_lichess_games(username, output_file, lichess_base_dir):
    """
    Parses lichess games and appends them to a TSV file.
    """
    logger.info(f"\nParsing lichess.org games for user: {username}")
    user_games_dir = lichess_base_dir / username

    pgn_files = os.listdir(user_games_dir)

    file_exists = output_file.exists() and output_file.stat().st_size > 0
    rows = 0

    with open(output_file, "a", newline="") as f:
        writer = csv.writer(f, delimiter="\t")
        if not file_exists:
            writer.writerow(GAME_SYNTHESIS_HEADER)

        for pgn_file in pgn_files:
            pgn_file = user_games_dir / pgn_file
            with open(pgn_file, "r") as pgn:
                content = pgn.read()

            games = re.split(r'\n\n(?=\[Event)', content)

            for game_raw in games:
                if not game_raw:
                    continue

                file_name_parts = pgn_file.stem.split('-')
                date = f"{file_name_parts[1]}-{file_name_parts[2]}"
                game = parse_pgn(username, game_raw)

                if game is None or game["variant"] in EXCLUDED_VARIANTS or game["opponent"] == "lichess AI":
                    continue

                rows += 1
                writer.writerow([
                    game["site"],
                    date,
                    game["color"],
                    game["elo"],
                    game["time_control"],
                    game["variant"],
                    game["termination"],
                    game["result"],
                    game["opening_family"],
                    game["eco"],
                    game["moves"],
                ])
    logger.info(f"  - Wrote {rows} rows for lichess games to {output_file}")

def parse_pgn(username, pgn_raw):
    lines = pgn_raw.strip().split("\n")
    metadata = {}
    moves_line = ""

    for line in lines:
        if line.startswith("["):
            match = re.match(r'\[(\w+)\s+"([^"]+)"\]', line)
            if match:
                key, value = match.groups()
                metadata[key] = value
        elif line.startswith("1."):
            moves_line = line

    white_player = metadata.get("White", "")
    black_player = metadata.get("Black", "")

    color = "White" if white_player.lower() == username.lower() else "Black"
    opponent = metadata.get("Black") if color == "White" else  metadata.get("White")
    elo = metadata.get(f"{color}Elo")
    variant = metadata.get("Variant", "")
    eco = metadata.get("ECO", "")

    if len(moves_line) == 0:
        return None # can happen if the game was immediately abandoned

    return {
        "site": metadata.get("Site", ""),
        "color": color,
        "elo": elo,
        "opponent": opponent,
        "time_control": classify_time_control(metadata.get("TimeControl", "")),
        "variant": variant,
        "termination": metadata.get("Termination", ""),
        "result": metadata.get("Result", ""),
        "variant": metadata.get("Variant", ""),
        "opening_family": classify_opening_family(eco),
        "eco": eco,
        "moves": moves_line,
    }

# reference: https://www.saremba.de/chessgml/standards/pgn/pgn-complete.htm#c9.6
def classify_time_control(time_control):
    if time_control == "-":
        return "N/A"

    if time_control.find('/') >= 0:
        average_game_time = int(time_control.split('/')[1])
    else:
        # Seems like an average amateur game lasts about 30 moves: https://chess-teacher.com/the-average-number-of-moves/.
        # We will therefore take into account 30 increments (technically should 29 but ok, this is just an approximation).
        parts = time_control.split('+')
        increment = 0 if len(parts) == 1 else int(parts[1])
        average_game_time = int(parts[0]) + 30 * increment

    if average_game_time <= 120:
        return "Bullet"
    elif average_game_time <= 300:
        return "Blitz"
    elif average_game_time <= 1800:
        return "Rapid"
    else:
        return "Daily"

# reference: https://www.365chess.com/eco.php
def classify_opening_family(eco):
    if eco == "?":
        return None # can happen in variant games

    volume = eco[0]
    id = int(eco[1:])
    if volume == 'A':
        if id == 0:
            return "Polish opening"
        elif id == 1:
            return "Nimzovich-Larsen attack"
        elif id <= 3:
            return "Bird's opening"
        elif id <= 9:
            return "Reti opening"
        elif id <= 39:
            return "English opening"
        elif id <= 41:
            return "Queen's pawn"
        elif id == 42:
            return "Modern defence, Averbakh system"
        elif id <= 44:
            return "Old Benoni defence"
        elif id <= 46:
            return "Queen's pawn game"
        elif id == 47:
            return "Queen's Indian defence"
        elif id <= 49:
            return "King's Indian, East Indian defence"
        elif id == 50:
            return "Queen's pawn game"
        elif id <= 52:
            return "Budapest defence"
        elif id <= 55:
            return "Old Indian defence"
        elif id == 56:
            return "Benoni defence"
        elif id <= 59:
            return "Benko gambit"
        elif id <= 79:
            return "Benoni defence"
        elif id <= 99:
            return "Dutch"
    elif volume == 'B':
        if id == 0:
            return "King's pawn opening"
        elif id == 1:
            return "Scandinavian (centre counter) defence"
        elif id <= 5:
            return "Alekhine's defence"
        elif id == 6:
            return "Robatsch (modern) defence"
        elif id <= 9:
            return "Pirc defence"
        elif id <= 19:
            return "Caro-Kann defence"
        elif id <= 99:
            return "Sicilian defence"
    elif volume == 'C':
        if id <= 19:
            return "French defence"
        elif id == 20:
            return "King's pawn game"
        elif id <= 22:
            return "Center game"
        elif id <= 24:
            return "Bishop's opening"
        elif id <= 29:
            return "Vienna game"
        elif id <= 39:
            return "King's gambit"
        elif id == 40:
            return "King's knight opening"
        elif id == 41:
            return "Philidor's defence"
        elif id <= 43:
            return "Petrov's defence"
        elif id == 44:
            return "King's pawn game"
        elif id == 45:
            return "Scotch game"
        elif id <= 46:
            return "Three knights game"
        elif id <= 49:
            return "Four knights game, Scotch variation"
        elif id == 50:
            return "Italian game"
        elif id <= 52:
            return "Evan's gambit"
        elif id <= 54:
            return "Giuoco piano"
        elif id <= 59:
            return "Two knights defence"
        elif id <= 99:
            return "Ruy Lopez (Spanish opening)"
    elif volume == 'D':
        if id == 0:
            return "Queen's pawn game"
        elif id == 1:
            return "Richter-Veresov attack"
        elif id == 2:
            return "Queen's pawn game"
        elif id == 3:
            return "Torre attack (Tartakower variation)"
        elif id <= 5:
            return "Queen's pawn game"
        elif id == 6:
            return "Queen's gambit"
        elif id <= 9:
            return "Queen's gambit declined, Chigorin defence"
        elif id <= 15:
            return "Queen's gambit declined, Slav defence"
        elif id == 16:
            return "Queen's gambit declined Slav accepted, Alapin variation"
        elif id <= 19:
            return "Queen's gambit declined Slav, Czech defence"
        elif id <= 29:
            return "Queen's gambit accepted"
        elif id <= 42:
            return "Queen's gambit declined"
        elif id <= 49:
            return "Queen's gambit declined, semi-Slav"
        elif id <= 69:
            return "Queen's gambit declined, 4. Bg5"
        elif id <= 79:
            return "Neo-Gruenfeld defence"
        elif id <= 99:
            return "Gruenfeld defence"
    elif volume == 'E':
        if id == 0:
            return "Queen's pawn game"
        elif id <= 9:
            return "Catalan, closed"
        elif id == 10:
            return "Queen's pawn game"
        elif id == 11:
            return "Bogo-Indian defence"
        elif id <= 19:
            return "Queen's indian defence"
        elif id <= 59:
            return "Nimzo-Indian defence"
        elif id <= 99:
            return "King's Indian defence"

    raise Error(f"Unrecognized ECO code: {eco}")

def cli(args):
    parser = argparse.ArgumentParser(
        description="Collects and parses chess games from Lichess and Chess.com"
    )

    parser.add_argument(
        "--chess-com-username",
        type=str,
        help="Chess.com username to collect games from.",
    )
    parser.add_argument(
        "--lichess-username",
        type=str,
        help="Lichess username to collect games from.",
    )
    parser.add_argument(
        "--start-date",
        "-s",
        type=validate_date,
        help="Start date for game collection (YYYY-MM).",
        required=True,
    )
    parser.add_argument(
        "--end-date",
        "-e",
        type=validate_date,
        help="End date for game collection (YYYY-MM). Defaults to current day if not specified.",
    )

    args = parser.parse_args(args)

    if not args.chess_com_username and not args.lichess_username:
        parser.error("At least one of --chess-com-username or --lichess-username must be provided.")

    end_date = args.end_date if args.end_date else dt.date.today()

    if args.start_date and args.start_date > end_date:
        parser.error(f"Start date ({args.start_date}) cannot be after end date ({end_date}).")

    # Create base directories
    pgn_dir = Path("pgn-cache")
    chess_com_base_dir = pgn_dir / "chess.com"
    lichess_base_dir = pgn_dir / "lichess.org"

    print("Creating base directories...")
    chess_com_base_dir.mkdir(parents=True, exist_ok=True)
    lichess_base_dir.mkdir(parents=True, exist_ok=True)
    print(f"  - Created: {chess_com_base_dir}")
    print(f"  - Created: {lichess_base_dir}")

    output_dir = Path("game-synthesis") / normalized_username(args.chess_com_username, args.lichess_username)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / "games.tsv"

    if output_file.exists():
        os.remove(output_file)

    if args.chess_com_username:
        fetch_chess_com_games(args.chess_com_username, args.start_date, end_date, chess_com_base_dir)
        parse_chess_com_games(args.chess_com_username, output_file, chess_com_base_dir)

    if args.lichess_username:
        start_datetime = dt.datetime.combine(args.start_date, dt.time.min)
        end_datetime = dt.datetime.combine(end_date, dt.time.max)
        fetch_lichess_games(args.lichess_username, start_datetime, end_datetime, lichess_base_dir)
        parse_lichess_games(args.lichess_username, output_file, lichess_base_dir)

    return output_file

# if both usernames are provided, we return the chess.com one even if the Lichess username might be different
def normalized_username(chess_com_username, lichess_username):
    if chess_com_username is None:
        return lichess_username
    return chess_com_username

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
def upload_directory_contents(local_dir, remote_folder, remote_base_path=""):
    """
    Recursively uploads a local directory to a Dataiku Managed Folder.

    :param local_dir: Local path to the directory.
    :param remote_folder: dataiku.Folder object.
    :param remote_base_path: Optional prefix for the remote path.
    """
    local_path = Path(local_dir)
    if not local_path.is_dir():
        logger.info(f"Warning: {local_dir} is not a directory. Skipping directory upload.")
        return

    logger.info(f"Starting recursive upload from {local_dir}...")
    for root, dirs, files in os.walk(local_path):
        for file in files:
            file_path = Path(root) / file
            rel_path = file_path.relative_to(local_path)
            # Convert to string and ensure forward slashes for remote path (managed folders use /)
            remote_path = (Path(remote_base_path) / rel_path).as_posix()

            logger.info(f"Uploading {file} -> {remote_path}")
            with open(file_path, "rb") as f:
                remote_folder.upload_stream(remote_path, f)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
now = dt.datetime.now(tz=zoneinfo.ZoneInfo("UTC"))

month = now.month - 1 if now.month > 1 else 12
year = now.year if month != 12 else now.year - 1
previous_month = dt.datetime(year, month, 1)

endDate = previous_month.strftime('%Y-%m')

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
args = ['-s', variables['startDate'], '-e', endDate]

chess_com_username = variables.get('chessComUsername', '');
lichess_username = variables.get('lichessUsername', '');

if len(chess_com_username) > 0:
    args.extend(['--chess-com-username', chess_com_username])

if len(lichess_username) > 0:
    args.extend(['--lichess-username', lichess_username]

games_file = cli(args)
upload_directory_contents(Path("pgn-cache"), managed_folder, "pgn-cache")

logger.info(f"Uploading games file {games_file} to managed folder...", "pgn-cache")
with open(games_file, 'rb') as f:
    managed_folder.upload_stream(games_file, f)

# -------------------------------------------------------------------------------- NOTEBOOK-CELL: CODE
games_df = pd.read_csv(games_file, sep='\t')

# Dataset new_games renamed to game by david.courtinot@dataiku.com on 2025-12-19 19:01:47
# Dataset game renamed to games by david.courtinot@dataiku.com on 2025-12-20 16:52:44
DiciDicee_games = dataiku.Dataset("games")
DiciDicee_games.write_with_schema(games_df)