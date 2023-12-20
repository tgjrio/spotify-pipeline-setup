
"""
This file contains Python functions for working with Spotify API data. The functions are designed to handle various operations, such as retrieving access tokens, fetching album and track data in bulk, cleaning up data frames, and obtaining track features.

Please ensure you have the necessary dependencies installed, such as the `requests` library for making HTTP requests, the `base64` library for encoding client credentials, the `json` library for parsing JSON responses, and the `pandas` library for data manipulation.

Make sure to import the required libraries and define any necessary helper functions before using the functions provided in this file.

For error handling, the functions include checks for valid response codes and data types. If an error occurs, appropriate exceptions will be raised with informative error messages.

Note: Some functions rely on other functions in this file, so it's important to include all the necessary functions when copying and using them.
"""

from requests import post, get
import base64
import json
import pandas as pd
import logging
from dotenv import load_dotenv
import os
# import time

load_dotenv()  # take environment variables from .env.

client_id = os.getenv("client_id")
client_secret = os.getenv("client_secret")





def get_token():

    """
    Retrieves an access token from the Spotify API using client credentials OAuth flow.
    - Combines client_id and client_secret, encodes in base64 for authentication.
    - Makes a POST request to Spotify's token API with appropriate headers.
    - Validates response and parses the JSON content to extract the access token.
    - Returns the access token for authenticated API requests.
    Requires client_id and client_secret to be available in the scope.
    """

    # Combine the client ID and client secret into a single string, separated by a colon
    auth_string = client_id + ":" + client_secret

    # Encode the auth_string to bytes using utf-8 encoding
    auth_bytes = auth_string.encode("utf-8")

    # Base64 encode the bytes and then decode it back into a string
    auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

    # Define the URL for the token API
    url = "https://accounts.spotify.com/api/token"

    # Define the headers for the API request, including the base64-encoded auth string
    headers = {
        "Authorization": "Basic " + auth_base64,
        "Content-Type": "application/x-www-form-urlencoded",
    }

    # Define the data to be sent in the API request
    data = {"grant_type": "client_credentials"}

    # Make a POST request to the token API with the defined headers and data
    result = post(url, headers=headers, data=data)

    # Check if the response status code is 200 (HTTP OK)
    if result.status_code != 200:
        raise ValueError(
            f"Invalid response code: {result.status_code} -- Reason: {result.reason}"
        )

    # Load the content of the response into a JSON object
    json_result = json.loads(result.content)

    # Extract the access token from the JSON object
    token = json_result["access_token"]

    # Return the access token
    return token


def get_auth_header(token):

    """
    Generates an Authorization header dictionary for API requests.
    - Verifies the provided token is not None or empty.
    - Returns a dictionary with the Authorization header, formatted as 'Bearer <token>'.
    Raises ValueError if the token is None or empty.
    """

    # Check if token is not None or empty
    if not token:
        raise ValueError("Token is None or empty")

    # Return a dictionary containing the Authorization header
    # The value of the Authorization header is "Bearer " followed by the token
    return {"Authorization": "Bearer " + token}


def get_album_ids(artist_names, token):

    """
    Retrieves album IDs for a list of artists from the Spotify API.
    - Iterates over each artist name, fetching albums using the Spotify search API.
    - Uses pagination to handle large number of albums, fetching up to 1000 albums per artist.
    - Constructs and sends GET requests with proper query parameters and headers.
    - Extracts album IDs from the API response and accumulates them in a list.
    - Returns a list of album IDs.
    - Logs information and handles potential errors in the request process.
    Note: The function internally calls get_token() and get_auth_header() for fresh token and headers.
    """

    # Define the base URL for the Spotify search API
    url = "https://api.spotify.com/v1/search?"

    # Get a new access token
    token = get_token()

    # Get the Authorization header using the new token
    headers = get_auth_header(token)

    # Initialize an empty list to store the album IDs
    ids = []

    # Iterate over each artist name in the provided list
    for artist in artist_names:
        logging.info(f"Fetching album IDs for artist: {artist}")
        # Define the offset for pagination (starting at the first page)
        offset = 0

        # Define the total number of albums to fetch
        total = 1000

        # Continue fetching albums while the offset is less than the total
        while offset < total:
            # Define the query parameters for the API request
            # The query is searching for albums by the current artist, limited to 50 results per page, in the US market
            query = f"q={artist}&type=album&limit=50&market=US&offset={offset}"

            # Combine the base URL and the query parameters
            query_url = url + query

            # Make a GET request to the search API with the defined URL and headers
            result = get(query_url, headers=headers)

            # Check if the response status code is 200 (HTTP OK)
            if result.status_code != 200:
                print(
                    f"Request failed with status code {result.status_code}, Reason: {result.reason}"
                )
                break

            # Load the content of the response into a JSON object
            content = json.loads(result.content)["albums"]

            # Check if the 'items' key is in the content
            if "items" in content:
                # Extract the list of album items from the JSON object
                items = content["items"]

                # Extend the list of album IDs with the IDs from the current page of results
                ids.extend([item["id"] for item in items])

            else:
                # If the 'items' key is not in the content, print an error message
                print(f"No 'albums' key in response content for artist '{artist}'")

            # Increase the offset by 50 for the next iteration
            offset += 50

    # Return the list of album IDs
    return ids


def get_album_data(album_ids, token):

    """
    Retrieves data for a list of album IDs from the Spotify API.
    - Splits the album IDs into chunks (20 IDs per chunk) to manage API request limits.
    - For each chunk, constructs and sends a GET request to the albums API endpoint.
    - Validates response and parses the JSON content to extract album data.
    - Accumulates and returns all album data in a list.
    Raises ValueError for invalid response codes.
    Note: Uses the internal 'chunks' function for splitting the album IDs list.
    """

    # This internal function will generate chunks from the list
    def chunks(lst, n):
        # Loop over the list in increments of n (chunk size)
        for i in range(0, len(lst), n):
            # Yield a chunk of the list
            yield lst[i : i + n]

    # Get the Authorization header using the provided token
    headers = get_auth_header(token)

    # Initialize an empty list to store the album data
    all_content = []

    # Now iterate over each chunk of album IDs
    for chunk in chunks(album_ids, 20):
        # Join the IDs in the chunk with commas
        chunk_ids = ",".join(chunk)

        # Define the URL for the API request, including the chunk of IDs and the market
        url = f"https://api.spotify.com/v1/albums?ids={chunk_ids}&market=us"

        # Make a GET request to the albums API with the defined URL and headers
        result = get(url, headers=headers)

        # Check if the response status code is 200 (HTTP OK)
        if result.status_code != 200:
            raise ValueError(
                f"Invalid response code: {result.status_code}, Reason: {result.reason}"
            )

        # Load the content of the response into a JSON object
        content = json.loads(result.content)

        # Append the album data to the list of all content
        all_content.append(content)

        

    # Return the list of all album data
    return all_content


def albums_df(data):
    # Check if 'data' is a list
    if not isinstance(data, list):
        raise TypeError("data must be a list")

    track_objects = []

    for data_item in data:  # iterate over list of dictionaries
        # Check if 'data_item' is a dictionary
        if not isinstance(data_item, dict):
            raise TypeError("data_item must be a dictionary")

        for album in data_item.get(
            "albums", []
        ):  # access 'albums' list in each dictionary
            # Check if 'album' is a dictionary
            if not isinstance(album, dict):
                raise TypeError("album must be a dictionary")

            album_info = {
                "album_type": album.get("album_type", ""),
                "total_tracks": album.get("total_tracks", 0),
                "album_name": album.get("name", ""),
                "release_date": album.get("release_date", ""),
                "label": album.get("label", ""),
                "album_popularity": album.get("popularity", 0),
                "album_id": album.get("id", ""),
            }

            for artist in album.get("artists", []):
                # Check if 'artist' is a dictionary
                if not isinstance(artist, dict):
                    raise TypeError("artist must be a dictionary")

                artist_info = {"artist_id": artist.get("id", "")}

                for item in album.get("tracks", {}).get("items", []):
                    track_info = {}
                    track_info["track_name"] = item.get("name", "")
                    track_info["track_id"] = item.get("id", "")
                    track_info["track_number"] = item.get("track_number", 0)
                    track_info["duration_ms"] = item.get("duration_ms", 0)

                    # Adding artist(s) information
                    artist_names = [
                        artist["name"] for artist in item.get("artists", [])
                    ]
                    track_info["artists"] = artist_names

                    track_info.update(album_info)
                    track_info.update(artist_info)
                    track_objects.append(track_info)

    # Try to convert the list of track objects to a DataFrame
    try:
        df = pd.DataFrame(track_objects)
    except Exception as e:
        raise ValueError("Could not convert track_objects to DataFrame: " + str(e))

    return df


def features_df(track_ids, token):
    # Check if 'track_ids' is a list
    if not isinstance(track_ids, list):
        raise TypeError("track_ids must be a list")

    # This internal function will generate chunks from the list
    def chunks(lst, n):
        # Loop over the list in increments of n (chunk size)
        for i in range(0, len(lst), n):
            # Yield a chunk of the list
            yield lst[i : i + n]

    # Get the Authorization header using the provided token
    headers = get_auth_header(token)

    # Initialize an empty list to store the audio features
    all_content = []

    # Now iterate over each chunk of track IDs
    for chunk in chunks(track_ids, 100):
        # Join the IDs in the chunk with commas
        chunk_ids = ",".join(chunk)

        # Define the URL for the API request, including the chunk of IDs
        url = f"https://api.spotify.com/v1/audio-features?ids={chunk_ids}"

        # Make a GET request to the audio features API with the defined URL and headers
        result = get(url, headers=headers)

        # Check if the response status code is 200 (HTTP OK)
        if result.status_code != 200:
            raise ValueError(
                f"Invalid response code: {result.status_code}, Reason: {result.reason}"
            )

        # Load the content of the response into a JSON object
        content = json.loads(result.content)

        # Extract the audio features from the content
        audio_features = content.get("audio_features", [])

        # Filter out any None values from the audio features list
        audio_features = [feature for feature in audio_features if feature is not None]

        # Extend the list of audio features with the filtered audio features from the current chunk
        all_content.extend(audio_features)

        

    # Convert the list of audio features to a DataFrame
    df = pd.DataFrame(all_content)

    return df


def tracks_df(track_ids, token):
    # This function will generate chunks from the list
    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    headers = get_auth_header(token)
    data = []

    # Now iterate over each chunk
    for chunk in chunks(track_ids, 50):
        chunk_ids = ",".join(chunk)  # Join the IDs with commas
        url = f"https://api.spotify.com/v1/tracks?ids={chunk_ids}"
        result = get(url, headers=headers)

        if result.status_code != 200:
            raise ValueError(
                f"Invalid response code: {result.status_code}, Reason: {result.reason}"
            )

        content = json.loads(result.content)["tracks"]

        for track_content in content:
            features = {}
            # Ensure each track_content is a dictionary
            if isinstance(track_content, dict):
                features["id"] = track_content.get("id", "")
                features["track_popularity"] = track_content.get("popularity", 0)
                features["explicit"] = track_content.get("explicit", False)

                data.append(features)                    

    return pd.DataFrame(data)


def artists_df(track_ids, token):
    # This function will generate chunks from the list
    def chunks(lst, n):
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    headers = get_auth_header(token)
    data = []

    # Now iterate over each chunk
    for chunk in chunks(track_ids, 25):
        chunk_ids = ",".join(chunk)  # Join the IDs with commas
        url = f"https://api.spotify.com/v1/artists?ids={chunk_ids}"
        result = get(url, headers=headers)

        if result.status_code != 200:
            raise ValueError(
                f"Invalid response code: {result.status_code}, Reason: {result.reason}"
            )

        content = json.loads(result.content)

        for artist_content in content["artists"]:
            if artist_content is None:  # Check if artist_content is None
                continue  # Skip this iteration

            artist_data = {}
            artist_data["id"] = artist_content.get("id", "")
            artist_data["name"] = artist_content.get("name", "")
            artist_data["artist_popularity"] = artist_content.get("popularity", 0)
            artist_data["artist_genres"] = artist_content.get("genres", [])

            if artist_content.get("followers"):
                artist_data["followers"] = artist_content["followers"].get("total", 0)
            else:
                artist_data["followers"] = 0

            data.append(artist_data)
      

    df = pd.DataFrame(data)

    # Split 'artist_genres' into separate columns
    df_genres = df["artist_genres"].apply(pd.Series)

    # Rename the new columns
    df_genres = df_genres.rename(columns=lambda x: "genre_" + str(x))

    # Join the new columns with the original DataFrame
    df = pd.concat([df, df_genres], axis=1)

    return df


def cleanup(df):
    df["artist_id"] = (
        df["artist_id"].astype(str).str.replace("(", "").str.replace(")", "")
    )
    df["artist_id"] = df["artist_id"].str.replace("'", "")

    # Split 'artist' into separate columns
    df_artist = df["artists"].apply(pd.Series)

    # Rename the new columns
    df_artist = df_artist.rename(columns=lambda x: "artist_" + str(x))

    # Join the new columns with the original DataFrame
    df = pd.concat([df[:], df_artist[:]], axis=1)

    # Convert milliseconds to seconds
    df["duration_sec"] = df["duration_ms"] / 1000

    # Changing the casing for track_name & album_name
    df["track_name"] = df["track_name"].str.title()
    df["album_name"] = df["album_name"].str.title()

    # Changing data type for release_DateData
    df["release_date"] = pd.to_datetime(
        df["release_date"], format="mixed", errors="coerce"
    )

    return df