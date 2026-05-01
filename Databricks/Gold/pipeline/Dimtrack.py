import dlt

@dlt.table()
def track_stage():
    df=spark.readStream.table("spotify.silver.dimtrack")
    return df


dlt.create_streaming_table("track")

dlt.create_auto_cdc_flow(
    target="track",
    source="track_stage",
    keys=['track_id'],
    sequence_by="updated_at",
    stored_as_scd_type= 2 ,
    track_history_except_column_list= None,
    name = None,
    once = False
)