import operators as op
import gcp_operators as gcp
import logging
import google.auth
import schemas as sch

from pandas_gbq import to_gbq

# CHANGE THESE VARIABLE TO WHAT YOUR ENVIRONMENT IS
project_id = "" # CHANGE DEFAULTDATABASE IN DATAFORM.JSON FILE TO THIS VALUE AS WELL
location = ""
user_name = ""
user_email = ""

# USED FOR DATAFORM REPOSITORY AND WORSKSPACE SETUP. MORE THAN WELCOME TO CHANGE THE NAME OF THESE
repository_id = "spotify-data-pull"
workspace_name = "spotify_upload"

# SELECT ARTIST DATA YOU WANT FOR INITIAL LOAD - MUST BE MORE THAN ONE AND RECOMMEND NOT DOING MORE THAN 5
arist_list = ["Drake", "Taylor Swift"]

# NAME OF THE DATASETS THAT WILL BE CREATED IN BIGQUERY
staging_dataset_id= 'staging'
production_dataset_id = "production"

# TABLES THAT WILL BE CREATED IN BIGQUERY AND YOUR DATA WILL BE LOADED INTO WITH THEIR SCHEMAS
table_names = ['albums', 'features', 'tracks', 'artist']
table_schemas = [sch.schema_albums, sch.schema_features, sch.schema_tracks, sch.schema_artists]

# PATH OF DATAFORM LOGIC THAT WILL CREATING REPORTING TABLES AND UPSERT NEW RECORDS FOR EACH INCREMENTAL LOAD
dataform_files_path = "dataform_logic"

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    logging.info("Script started")

    token = op.get_token()

    album_ids = op.get_album_ids(arist_list, token)

    logging.info("Extracting data for all albums using album ids")
    albums_data_extract = op.get_album_data(album_ids, token)

    logging.info("Cleaning up data")
    albums_data = op.albums_df(albums_data_extract)
    albums_data = albums_data.drop_duplicates(subset=["track_id"])
    albums_data = op.cleanup(albums_data)

    logging.info("Getting track IDs")
    track_ids = albums_data['track_id']
    track_ids = track_ids.to_list()

    logging.info("Fetching features data using track ids")
    features_data = op.features_df(track_ids, token)

    logging.info("Fetching track data using track ids")
    track_data = op.tracks_df(track_ids, token)

    logging.info("Fetching artist data")
    artist_ids = albums_data['artist_id'].unique()
    artist_data = op.artists_df(artist_ids, token)
    
    logging.info("Changing genre data type for artist df")
    artist_data['artist_genres'] = artist_data['artist_genres'].astype(str)

    logging.info("Changing artist data type for albums df")
    albums_data['artists'] = albums_data['artists'].astype(str)

    logging.info("Creating list of dataframes to load into BQ tables")
    dfs = [albums_data, features_data, track_data, artist_data]
    
    # CREATE DATASETS STAGING & PRODUCTION
    gcp.create_bigquery_dataset(project_id, staging_dataset_id, description="")
    gcp.create_bigquery_dataset(project_id, production_dataset_id, description="")

    # CREATE TABLES IN STAGING DATASET
    for table, schema in zip(table_names, table_schemas):
        gcp.create_bigquery_table(project_id, table, staging_dataset_id, schema)

    # CREATE TABLES IN PRODUCTION DATASET
    for table, schema in zip(table_names, table_schemas):
        gcp.create_bigquery_table(project_id, table, production_dataset_id, schema)
    
    logging.info(f"Starting Dataform Setup for repository named {repository_id} and workspace named {workspace_name}")
    gcp.create_dataform_repository(project_id, location, repository_id)
    gcp.create_dataform_workspace(project_id, location, repository_id, workspace_name)
    
    logging.info(f"Workspace is setup. Now adding files to workspace: {workspace_name}")
    gcp.insert_logic_to_workspace(project_id, location, repository_id, workspace_name, dataform_files_path)
    gcp.install_npm_packages_in_workspace(project_id, location, repository_id, workspace_name)
    gcp.commit_changes_to_workspace(project_id, location, repository_id, workspace_name, user_name, user_email)
    gcp.push_git_commits_to_default_branch(project_id, location, repository_id, workspace_name)

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

    logging.info(f"Now starting invocation for {workspace_name}")
    compilation_result_name=gcp.create_compilation_result(project_id, location, repository_id)
    gcp.create_workflow_invocation(project_id, location, repository_id, compilation_result_name)
    logging.info(f"Invocation process is complete.  Check execution logs in your dataform UI to see if it's done processing.")  

    logging.info("Script finished")

if __name__ == "__main__":
    main()



