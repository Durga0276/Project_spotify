import dlt

@dlt.table()
def art_stage():
    df=spark.readStream.table("spotify.silver.dimart")
    return df


dlt.create_streaming_table("art")

dlt.create_auto_cdc_flow(
    target="art",
    source="art_stage",
    keys=['artist_id'],
    sequence_by="updated_at",
    stored_as_scd_type= 2 ,
    track_history_except_column_list= None,
    name = None,
    once = False
)