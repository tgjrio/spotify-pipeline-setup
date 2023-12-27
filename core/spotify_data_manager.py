"""
SpotifyDataManager Class
-------------------------
Purpose:
    To offer a streamlined and efficient way to interact with the Spotify Web API for accessing extensive music-related data.
Functionality:
    - Retrieves access tokens using client credentials for authenticated API requests.
    - Gathers data about artists, albums, and tracks from Spotify.
    - Manages pagination and data accumulation from Spotify’s API responses.
Use Cases:
    Ideal for building music recommendation systems, conducting music industry analysis, or creating applications that require detailed insights into Spotify's music catalog. 
    It can be used in data science projects, marketing analysis, or by music enthusiasts for creating personalized music experiences.
"""

import base64
import json
import requests
import pandas as pd
import logging

from requests import post, get

class SpotifyDataManager:
    """
    SpotifyDataManager class to interact with the Spotify API.
    Manages token retrieval, album ID fetching, and data processing for Spotify data.
    """

    def __init__(self, client_id, client_secret):
        self.client_id = client_id
        self.client_secret = client_secret

    def get_token(self):
        """
        Retrieves an access token from the Spotify API.
        """
        auth_string = self.client_id + ":" + self.client_secret
        auth_bytes = auth_string.encode("utf-8")
        auth_base64 = str(base64.b64encode(auth_bytes), "utf-8")

        url = "https://accounts.spotify.com/api/token"
        headers = {"Authorization": "Basic " + auth_base64, "Content-Type": "application/x-www-form-urlencoded"}
        data = {"grant_type": "client_credentials"}

        result = requests.post(url, headers=headers, data=data)
        if result.status_code != 200:
            raise ValueError(f"Invalid response code: {result.status_code} -- Reason: {result.reason}")

        json_result = json.loads(result.content)
        return json_result["access_token"]

    def get_auth_header(self, token):
        """
        Generates an Authorization header dictionary for API requests.
        """
        if not token:
            raise ValueError("Token is None or empty")
        return {"Authorization": "Bearer " + token}
    
    def get_album_ids(self, artist_names):
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
        url = "https://api.spotify.com/v1/search?"
        token = self.get_token()
        headers = self.get_auth_header(token)
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

    def get_album_data(self, album_ids):
        """
        Retrieves data for a list of album IDs from the Spotify API.
        - Splits the album IDs into chunks (20 IDs per chunk) to manage API request limits.
        - For each chunk, constructs and sends a GET request to the albums API endpoint.
        - Validates response and parses the JSON content to extract album data.
        - Accumulates and returns all album data in a list.
        Raises ValueError for invalid response codes.
        Note: Uses the internal 'chunks' function for splitting the album IDs list.
        """
        def chunks(lst, n):
            for i in range(0, len(lst), n):
                yield lst[i : i + n]

        token = self.get_token()
        headers = self.get_auth_header(token)
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
        
        return all_content

    def albums_df(self, data):
        """
        Converts album data into a pandas DataFrame.
        """
        # # Check if 'data' is a list
        # if not isinstance(data, list):
        #     raise TypeError("data must be a list")

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