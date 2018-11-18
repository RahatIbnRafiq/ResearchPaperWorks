from pymongo import MongoClient
import constants
import csv

def insertCsvFileMongo(filename):
    connection = MongoClient()
    db = connection["vine"]
    collection = db["labeled_media_session"]
    with open(constants.LABELED_DATA_PATH_VINE + filename, 'r') as csvfile:
        csvreader = csv.DictReader(csvfile)
        for row in csvreader:
            collection.insert_one(row)
            break



insertCsvFileMongo("vine_meta_data.csv")