import dlt

@dlt.table
def dimuser_stg():
    df = spark.readStream.table("spotify.silver.dimuser")
    return df