"""
SpotifyDataFetcher Class
------------------------
Purpose:
    To fetch and process detailed track, artist, and audio feature data from Spotify, tailored for analytical purposes.
Functionality:
    - Retrieves audio features and track details based on track IDs.
    - Provides methods for cleaning and transforming the data into a more analytical-friendly format.
    - Manages efficient data fetching through batch processing and pagination.
Use Cases:
    This class is particularly useful in music data analysis, such as studying music trends, audio feature analysis, or artist popularity studies. 
    It can be integrated into music analytics platforms, academic research on music, or applications that leverage music data for insights and recommendations.
"""

import requests
import json
import pandas as pd
import os 

from requests import post, get

class SpotifyDataFetcher:
    """
    SpotifyDataFetcher class to retrieve and process track, artist, and audio feature data from Spotify.
    """

    def __init__(self, token):
        self.token = token

    def get_auth_header(self):
        """
        Generates an Authorization header dictionary for API requests.
        """
        if not self.token:
            raise ValueError("Token is None or empty")
        return {"Authorization": "Bearer " + self.token}

    def chunks(self, lst, n):
        """
        Internal function to generate chunks from a list.
        """
        for i in range(0, len(lst), n):
            yield lst[i : i + n]

    def features_df(self, track_ids):
        """
        Retrieves audio features for a list of track IDs from the Spotify API.
        """
        headers = self.get_auth_header()

        # Check if 'track_ids' is a list
        if not isinstance(track_ids, list):
            raise TypeError("track_ids must be a list")
        
        all_content = []

            # Now iterate over each chunk of track IDs
        for chunk in self.chunks(track_ids, 100):
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

    def tracks_df(self, track_ids):
        """
        Retrieves track data for a list of track IDs from the Spotify API.
        """
        headers = self.get_auth_header()
        data = []

        # Now iterate over each chunk
        for chunk in self.chunks(track_ids, 50):
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

    def artists_df(self, track_ids):
        """
        Retrieves artist data for a list of track IDs from the Spotify API.
        """
        headers = self.get_auth_header()
        data = []

        # Now iterate over each chunk
        for chunk in self.chunks(track_ids, 25):
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

    def cleanup(self, df):
        """
        Cleans up and reformats the data in the provided DataFrame.
        """
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
            df["release_date"],
            format="mixed",
            errors="coerce"
        )

        return df