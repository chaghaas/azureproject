from pyspark import pipelines as dp

@dp.table()
def factstream_stg():
    df = spark.readStream.table("spotify.silver.factstream")
    return df

dp.create_streaming_table("factstream")

dp.create_auto_cdc_flow(
  target = "factstream",
  source = "factstream_stg",
  keys = ["stream_id"],
  sequence_by = "stream_timestamp",
  track_history_except_column_list = None,
  stored_as_scd_type = "1",
  name = None,
  once = False
)