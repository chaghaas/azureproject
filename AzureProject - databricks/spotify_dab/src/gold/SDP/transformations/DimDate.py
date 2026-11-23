from pyspark import pipelines as dp

@dp.table()
def dimdate_stg():
    df = spark.readStream.table("spotify.silver.dimdate")
    return df

dp.create_streaming_table("dimdate")

dp.create_auto_cdc_flow(
  target = "dimdate",
  source = "dimdate_stg",
  keys = ["date_key"],
  sequence_by = "date",
  track_history_except_column_list = None,
  stored_as_scd_type = "2",
  name = None,
  once = False
)