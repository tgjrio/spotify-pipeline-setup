import logging
import google.auth
import os

from pandas_gbq import to_gbq
from dotenv import load_dotenv

from core.biqquery_manager import BigQueryManager
from core.dataform_manager import DataformManager
from core.spotify_data_manager import SpotifyDataManager
from core.spotify_data_fetcher import SpotifyDataFetcher

# Configuration Variables
project_id = ""
location = ""

# Big Query Default Settings
staging_dataset_id = 'staging'
table_names = ['albums', 'features', 'tracks', 'artist']

# Dataform Default Settings
repository_id = "spotify-data-pull"
workspace_name = "spotify_upload"

# Artist data to search and fetch
artist_list = []

load_dotenv()  # take environment variables from .env.
client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    # Initialize instances
    logging.info("Initializing instances")
    spotify_manager = SpotifyDataManager(client_id, client_secret)  # Provide your client_id and client_secret
    spotify_fetcher = SpotifyDataFetcher(spotify_manager.get_token())
    dataform_manager = DataformManager()

    # Spotify Data Extraction and Processing
    logging.info("Extracting and processing Spotify albums data")
    album_ids = spotify_manager.get_album_ids(artist_list)
    albums_data = spotify_manager.get_album_data(album_ids)
    albums_data = spotify_manager.albums_df(albums_data)
    albums_data = albums_data.drop_duplicates(subset=["track_id"])
    albums_data = spotify_fetcher.cleanup(albums_data)

    logging.info("Now fetching track ids")
    track_ids = albums_data['track_id'].to_list()
    logging.info("Extracting and processing features data")
    features_data = spotify_fetcher.features_df(track_ids)
    logging.info("Extracting and processing tracks data")
    track_data = spotify_fetcher.tracks_df(track_ids)
    logging.info("Now fetching artist ids")
    artist_ids = albums_data['artist_id'].unique()
    logging.info("Now fetching artist data")
    artist_data = spotify_fetcher.artists_df(artist_ids)

    logging.info("Changing genre data type for artist df")
    artist_data['artist_genres'] = artist_data['artist_genres'].astype(str)

    logging.info("Changing artist data type for albums df")
    albums_data['artists'] = albums_data['artists'].astype(str)

    logging.info("Creating list of dataframes to load into BQ tables")
    dfs = [albums_data, features_data, track_data, artist_data]
    logging.info("--------------------------------------------------")

    credentials, project = google.auth.default()
    for df, table_name in zip(dfs, table_names):
        try:
            logging.info(f"Inserting data into staging table {table_name}")
            table_id = f'{project_id}.{staging_dataset_id}.{table_name}' 
            to_gbq(
                dataframe=df, 
                destination_table=table_id, 
                project_id=project_id, 
                if_exists='replace', 
                credentials=credentials
            )
        except Exception as e:
            logging.error(f'Error with table: {table_name}')
            logging.error(f'Error message: {str(e)}')
            logging.error(f'If error is invalid data type or weirdness with structure, just run the script again')

    logging.info(f"Now starting invocation for Dataform workspace: {workspace_name}")
    compilation_result_name=dataform_manager.create_compilation_result(project_id, location, repository_id)
    dataform_manager.create_workflow_invocation(project_id, location, repository_id, compilation_result_name)
    logging.info(f"Invocation process is complete.  Check execution logs in your dataform UI to see if it's done processing.")  

    logging.info("Script finished")

if __name__ == "__main__":
    main()