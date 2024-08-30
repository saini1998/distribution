### Write your solution in this file and submit it for review ###

import os
from pathlib import Path
from part_1.tools.ingestion import Ingestion
import logging
import pandas as pd
import calendar
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

# Define database credentials
account = "" 
database = ""
user = ""
password = ""
schema = ""

# Define database variables
db_table_name = "FLIGHTS"

# Define file variables
raw_data_file_path = os.path.join(Path(__file__).parent.resolve(), "../data/raw_flight_data.csv")
upload_data_file_path = os.path.join(Path(__file__).parent.resolve(), "../data/stg_flight_data.csv")


# ADD YOUR CODE HERE


# Ingest data
injestor = Ingestion(account, database, user, password, schema)
injestor.ingest(db_table_name, upload_data_file_path)