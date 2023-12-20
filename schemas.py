from google.cloud import bigquery

# https://developer.spotify.com/documentation/web-api/reference/get-an-album

schema_albums = [
    bigquery.SchemaField(
        name="track_name",
        field_type="STRING",
        description="Name of the track"
    ),
    bigquery.SchemaField(
        name="track_id",
        field_type="STRING",
        description="The Spotify ID for the track."
    ),
    bigquery.SchemaField(
        name="track_number",
        field_type="INTEGER",
        description="The number of the track. If an album has several discs, the track number is the number on the specified disc."
    ),
    bigquery.SchemaField(
        name="duration_ms",
        field_type="INTEGER",
        description="The track length in milliseconds."
    ),
    bigquery.SchemaField(
        name="album_type",
        field_type="STRING",
        description="The type of the album. Allowed values: album, single, compilation"
    ),
    bigquery.SchemaField(
        name="artists",
        field_type="STRING",
        description="The artists who performed the track."
    ),
    bigquery.SchemaField(
        name="total_tracks",
        field_type="INTEGER",
        description="The number of tracks in the album."
    ),
    bigquery.SchemaField(
        name="album_name",
        field_type="STRING",
        description="The name of the album. In case of an album takedown, the value may be an empty string."
    ),
    bigquery.SchemaField(
        name="release_date",
        field_type="TIMESTAMP",
        description="The date the album was first released."
    ),
    bigquery.SchemaField(
        name="label",
        field_type="STRING",
        description="The label associated with the album."
    ),
    bigquery.SchemaField(
        name="album_popularity",
        field_type="INTEGER",
        description="The popularity of the album. The value will be between 0 and 100, with 100 being the most popular."
    ),
    bigquery.SchemaField(
        name="album_id",
        field_type="STRING",
        description="The Spotify ID for the album."
    ),
    bigquery.SchemaField(
        name="artist_id",
        field_type="STRING",
        description="The Spotify ID for the artist."
    ),
    bigquery.SchemaField(
        name="artist_0",
        field_type="STRING",
        description="Main Artist"
    ),
    bigquery.SchemaField(
        name="artist_1",
        field_type="STRING",
        description="Featuring Artist"
    ),
    bigquery.SchemaField(
        name="artist_2",
        field_type="STRING",
        description="Featuring Artist"
    ),
    bigquery.SchemaField(
        name="artist_3",
        field_type="STRING",
        description="Featuring Artist"
    ),
    bigquery.SchemaField(
        name="artist_4",
        field_type="STRING",
        description="Featuring Artist"
    ),
    bigquery.SchemaField(
        name="artist_5",
        field_type="STRING",
        description="Featuring Artist"
    ),
    bigquery.SchemaField(
        name="artist_6",
        field_type="STRING",
        description="Featuring Artist"
    ),
    bigquery.SchemaField(
        name="artist_7",
        field_type="STRING",
        description="Featuring Artist"
    ),
    bigquery.SchemaField(
        name="artist_8",
        field_type="STRING",
        description="Featuring Artist"
    ),
    bigquery.SchemaField(
        name="artist_9",
        field_type="STRING",
        description="Featuring Artist"
    ),
    bigquery.SchemaField(
        name="artist_10",
        field_type="STRING",
        description="Featuring Artist"
    ),
    bigquery.SchemaField(
        name="artist_11",
        field_type="STRING",
        description="Featuring Artist"
    ),
    bigquery.SchemaField(
        name="duration_sec",
        field_type="FLOAT",
        description="Track length in seconds"
    ),
]

# https://developer.spotify.com/documentation/web-api/reference/get-several-audio-features

schema_features = [
    bigquery.SchemaField(
        name="danceability",
        field_type="FLOAT",
        description="Danceability describes how suitable a track is for dancing based on a combination of musical elements including tempo, rhythm stability, beat strength, and overall regularity. A value of 0.0 is least danceable and 1.0 is most danceable."
    ),
    bigquery.SchemaField(
        name="energy",
        field_type="FLOAT",
        description="Energy is a measure from 0.0 to 1.0 and represents a perceptual measure of intensity and activity. Typically, energetic tracks feel fast, loud, and noisy. For example, death metal has high energy, while a Bach prelude scores low on the scale. Perceptual features contributing to this attribute include dynamic range, perceived loudness, timbre, onset rate, and general entropy."
    ),
    bigquery.SchemaField(
        name="key",
        field_type="INTEGER",
        description="The key the track is in. Integers map to pitches using standard Pitch Class notation. E.g. 0 = C, 1 = C♯/D♭, 2 = D, and so on. If no key was detected, the value is -1."
    ),
    bigquery.SchemaField(
        name="loudness",
        field_type="FLOAT",
        description="The overall loudness of a track in decibels (dB). Loudness values are averaged across the entire track and are useful for comparing relative loudness of tracks. Loudness is the quality of a sound that is the primary psychological correlate of physical strength (amplitude). Values typically range between -60 and 0 db."
    ),
    bigquery.SchemaField(
        name="mode",
        field_type="INTEGER",
        description="Mode indicates the modality (major or minor) of a track, the type of scale from which its melodic content is derived. Major is represented by 1 and minor is 0."
    ),
    bigquery.SchemaField(
        name="speechiness",
        field_type="FLOAT",
        description="Speechiness detects the presence of spoken words in a track. The more exclusively speech-like the recording (e.g. talk show, audio book, poetry), the closer to 1.0 the attribute value. Values above 0.66 describe tracks that are probably made entirely of spoken words. Values between 0.33 and 0.66 describe tracks that may contain both music and speech, either in sections or layered, including such cases as rap music. Values below 0.33 most likely represent music and other non-speech-like tracks."
    ),
    bigquery.SchemaField(
        name="acousticness",
        field_type="FLOAT",
        description="A confidence measure from 0.0 to 1.0 of whether the track is acoustic. 1.0 represents high confidence the track is acoustic."
    ),
    bigquery.SchemaField(
        name="instrumentalness",
        field_type="FLOAT",
        description="Predicts whether a track contains no vocals. Ooh and aah sounds are treated as instrumental in this context. Rap or spoken word tracks are clearly vocal. The closer the instrumentalness value is to 1.0, the greater likelihood the track contains no vocal content. Values above 0.5 are intended to represent instrumental tracks, but confidence is higher as the value approaches 1.0."
    ),
    bigquery.SchemaField(
        name="liveness",
        field_type="FLOAT",
        description="Detects the presence of an audience in the recording. Higher liveness values represent an increased probability that the track was performed live. A value above 0.8 provides strong likelihood that the track is live."
    ),
    bigquery.SchemaField(
        name="valence",
        field_type="FLOAT",
        description="A measure from 0.0 to 1.0 describing the musical positiveness conveyed by a track. Tracks with high valence sound more positive (e.g. happy, cheerful, euphoric), while tracks with low valence sound more negative (e.g. sad, depressed, angry)."
    ),
    bigquery.SchemaField(
        name="tempo",
        field_type="FLOAT",
        description="The overall estimated tempo of a track in beats per minute (BPM). In musical terminology, tempo is the speed or pace of a given piece and derives directly from the average beat duration."
    ),
    bigquery.SchemaField(
        name="type",
        field_type="STRING",
        description="The object type."
    ),
    bigquery.SchemaField(
        name="id",
        field_type="STRING",
        description="The Spotify ID for the track."
    ),
    bigquery.SchemaField(
        name="uri",
        field_type="STRING",
        description="The Spotify URI for the track."
    ),
    bigquery.SchemaField(
        name="track_href",
        field_type="STRING",
        description="A link to the Web API endpoint providing full details of the track."
    ),
    bigquery.SchemaField(
        name="analysis_url",
        field_type="STRING",
        description="A URL to access the full audio analysis of this track. An access token is required to access this data."
    ),
    bigquery.SchemaField(
        name="duration_ms",
        field_type="INTEGER",
        description="The duration of the track in milliseconds."
    ),
    bigquery.SchemaField(
        name="time_signature",
        field_type="INTEGER",
        description="An estimated time signature. The time signature (meter) is a notational convention to specify how many beats are in each bar (or measure). The time signature ranges from 3 to 7 indicating time signatures of 3/4, to 7/4."
    ),
]

# https://developer.spotify.com/documentation/web-api/reference/get-track

schema_tracks = [
    bigquery.SchemaField(
        name="id",
        field_type="STRING",
        description="The Spotify ID for the track."
    ),
    bigquery.SchemaField(
        name="track_popularity",
        field_type="INTEGER",
        description="The popularity of a track is a value between 0 and 100, with 100 being the most popular. The popularity is calculated by algorithm and is based, in the most part, on the total number of plays the track has had and how recent those plays are.  Duplicate tracks (e.g. the same track from a single and an album) are rated independently. Artist and album popularity is derived mathematically from track popularity."
    ),
    bigquery.SchemaField(
        name="explicit",
        field_type="BOOLEAN",
        description="Whether or not the track has explicit lyrics ( true = yes it does; false = no it does not OR unknown)."
    ),
]

# https://developer.spotify.com/documentation/web-api/reference/get-an-artist

schema_artists = [
    bigquery.SchemaField(
        name="id",
        field_type="STRING",
        description="The Spotify ID of the artist."
    ),
    bigquery.SchemaField(
        name="name",
        field_type="STRING",
        description="Name of the Artist"
    ),
    bigquery.SchemaField(
        name="artist_popularity",
        field_type="INTEGER",
        description="The popularity of the artist. The value will be between 0 and 100, with 100 being the most popular. The artist's popularity is calculated from the popularity of all the artist's tracks."
    ),
    bigquery.SchemaField(
        name="artist_genres",
        field_type="STRING",
        description="A list of the genres the artist is associated with. If not yet classified, the array is empty."
    ),
    bigquery.SchemaField(
        name="followers",
        field_type="INTEGER",
        description="The total number of followers."
    ),
    bigquery.SchemaField(
        name="genre_0",
        field_type="STRING",
        description="Main Genre"
    ),
    bigquery.SchemaField(
        name="genre_1",
        field_type="STRING",
        description="Sub Genre"
    ),
    bigquery.SchemaField(
        name="genre_2",
        field_type="STRING",
        description="Sub Genre"
    ),
    bigquery.SchemaField(
        name="genre_3",
        field_type="STRING",
        description="Sub Genre"
    ),
    bigquery.SchemaField(
        name="genre_4",
        field_type="STRING",
        description="Sub Genre"
    ),
    bigquery.SchemaField(
        name="genre_5",
        field_type="STRING",
        description="Sub Genre"
    ),
    bigquery.SchemaField(
        name="genre_6",
        field_type="STRING",
        description="Sub Genre"
    ),
]