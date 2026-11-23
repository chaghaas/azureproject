from pyspark import pipelines as dp

expectations = {
    "rule_1" : "user_id is NOT NUll"
}

@dp.table()
@dp.expect_all_or_drop(expectations)
def dimuser_stg():
    df = spark.readStream.table("spotify.silver.dimuser")
    return df

dp.create_streaming_table(
    name = "dimuser",
    expect_all_or_drop=expectations
    )

dp.create_auto_cdc_flow(
  target = "dimuser",
  source = "dimuser_stg",
  keys = ["user_id"],
  sequence_by = "updated_at",
  track_history_except_column_list = None,
  stored_as_scd_type = "2",
  name = None,
  once = False
)