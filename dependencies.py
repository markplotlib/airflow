
# tasks fan out
Begin_execution >> Stage_events
Begin_execution >> Stage_songs

# tasks funnel in
Load_songplays_fact_table << Stage_events
Load_songplays_fact_table << Stage_songs

# fan out: 1 to 4
Load_songplays_fact_table >> Load_song_dim_table
Load_songplays_fact_table >> Load_user_dim_table
Load_songplays_fact_table >> Load_artist_dim_table
Load_songplays_fact_table >> Load_time_dim_table

# funnel in: 4 to 1
Run_data_quality_checks << Load_song_dim_table
Run_data_quality_checks << Load_user_dim_table
Run_data_quality_checks << Load_artist_dim_table
Run_data_quality_checks << Load_time_dim_table

Run_data_quality_checks >> Stop_execution
