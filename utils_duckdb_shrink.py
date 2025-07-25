#! /usr/bin/env python3
import os
import shutil
import tempfile

import config
import utils
from database_duckdb import DatabaseDuckdb

db_duckdb = DatabaseDuckdb()

utils.print_log('Delete no longer needed BAG tables')
db_duckdb.delete_no_longer_needed_bag_tables()

dbdir = os.path.dirname(config.file_db_duckdb)
(tmpfile, tmp_file_name) = tempfile.mkstemp(dir=dbdir,suffix='.duckdb')
os.close(tmpfile)

# Copy the database, this will shrink it.
utils.print_log("Creating a copy of the BAG database to shrink it")
db_duckdb.enable_progress_bar()
db_duckdb.copy_database(tmp_file_name)
db_duckdb.close()

utils.print_log("Replacing original database with the shrunk copy")
shutil.move(tmp_file_name, config.file_db_duckdb)

utils.print_log('Ready')
