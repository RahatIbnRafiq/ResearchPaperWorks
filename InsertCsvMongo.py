import database_codes as db_codes
import constants

# adding vine labeled data

db_codes.insert_csv_to_database("vine", "labeled_media_sessions", constants.LABELED_DATA_PATH_VINE+"vine_meta_data.csv")


# adding instagram labeled data

file_list = ["sessions_0plus_to_10_metadata.csv", "sessions_10plus_to_40_metadata.csv", "sessions_40plus_metadata.csv"]
for file in file_list:
    db_codes.insert_csv_to_database("instagram", "labeled_media_sessions", constants.LABELED_DATA_PATH_INSTAGRAM+file)
