import dlt

expects = {
    "rule1":"user_id IS NOT NULL"
}

@dlt.table()
@dlt.expect_all_or_drop(expects)
def dimuser_stage():
    df=spark.readStream.table("spotify.silver.dimuser")
    return df


dlt.create_streaming_table(name="dimuser", expect_all_or_drop=expects)

dlt.create_auto_cdc_flow(
    target="dimuser",
    source="dimuser_stage",
    keys=['user_id'],
    sequence_by="updated_at",
    stored_as_scd_type= 2 ,
    track_history_except_column_list= None,
    name = None,
    once = False
)