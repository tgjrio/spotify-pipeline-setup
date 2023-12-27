import logging
import google.auth
import os
import schemas as sch

from dotenv import load_dotenv
from pandas_gbq import to_gbq
from core.biqquery_manager import BigQueryManager
from core.dataform_manager import DataformManager
from core.spotify_data_manager import SpotifyDataManager
from core.spotify_data_fetcher import SpotifyDataFetcher

# Configuration Variables
project_id = ""
location = ""
user_name = ""
user_email = ""

# Big Query Default Settings
staging_dataset_id = 'staging'
production_dataset_id = "production"
table_names = ['albums', 'features', 'tracks', 'artist']
table_schemas = [sch.schema_albums, sch.schema_features, sch.schema_tracks, sch.schema_artists]

# Dataform Default Settings
repository_id = "spotify-data-pull"
workspace_name = "spotify_upload"

# Artist data to search and fetch
artist_list = ["Drake", "Taylor Swift"]

# Path for Dataform logic
dataform_files_path = "dataform_logic"

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
    bigquery__manager = BigQueryManager()
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

    logging.info("Creating list of dataframes to load into BQ tables")
    dfs = [albums_data, features_data, track_data, artist_data]

    logging.info("Creating staging and production datasets in Big Query")
    # CREATE DATASETS STAGING & PRODUCTION
    bigquery__manager.create_bigquery_dataset(project_id, staging_dataset_id, description="")
    bigquery__manager.create_bigquery_dataset(project_id, production_dataset_id, description="")

    logging.info("Creating tables and schemas in staging and production datasets")
    # CREATE TABLES IN STAGING DATASET
    for table, schema in zip(table_names, table_schemas):
        bigquery__manager.create_bigquery_table(project_id, table, staging_dataset_id, schema)

    # CREATE TABLES IN PRODUCTION DATASET
    for table, schema in zip(table_names, table_schemas):
        bigquery__manager.create_bigquery_table(project_id, table, production_dataset_id, schema)

    logging.info(f"Starting Dataform Setup for repository named {repository_id} and workspace named {workspace_name}")
    dataform_manager.create_dataform_repository(project_id, location, repository_id)
    dataform_manager.create_dataform_workspace(project_id, location, repository_id, workspace_name)
    
    logging.info(f"Workspace is setup. Now adding files to workspace: {workspace_name} and installing NPM packages.  Then we'll push and commit to main branch.")
    dataform_manager.insert_logic_to_workspace(project_id, location, repository_id, workspace_name, dataform_files_path)
    dataform_manager.install_npm_packages_in_workspace(project_id, location, repository_id, workspace_name)
    dataform_manager.commit_changes_to_workspace(project_id, location, repository_id, workspace_name, user_name, user_email)
    dataform_manager.push_git_commits_to_default_branch(project_id, location, repository_id, workspace_name)

    credentials, project = google.auth.default()
    for df, table_name in zip(dfs, table_names):
        try:
            logging.info(f"Inserting data into staging table {table_name}")
            table_id = f'{project_id}.{staging_dataset_id}.{table_name}' 
            to_gbq(
                dataframe=df, 
                destination_table=table_id, 
                project_id=project, 
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