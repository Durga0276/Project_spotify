import dlt

@dlt.table()
def date_stage():
    df=spark.readStream.table("spotify.silver.dimdate")
    return df


dlt.create_streaming_table("date")

dlt.create_auto_cdc_flow(
    target="date",
    source="date_stage",
    keys=['date_key'],
    sequence_by="date",
    stored_as_scd_type= 2 ,
    track_history_except_column_list= None,
    name = None,
    once = False
)