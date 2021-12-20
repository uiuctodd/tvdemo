#########-#########-#########-#########-#########-#########-#########-#########
# Library of datamart ETL functions
#########-#########-#########-#########-#########-#########-#########-#########-

import os
import logging

# sqlalchemy would be the tool for doing loads from Pandas
# But I've decided to do direct file loads instead.
#from sqlalchemy import create_engine
#import MySQLdb
import mysql.connector

import tvdemo.filesystem as FS
from tvdemo.config import get_config

cached_db = None # cache the DB handle in global scope

#########-#########-#########-#########-#########-#########-#########-#########-
# create a mysql data connection
# I should really cache this in global scope
#########-#########-#########-#########-#########-#########-#########-#########-
def get_dbh():
  global cached_db
  config = get_config('database')

  if cached_db is not None:
    return cached_db        # cached!

  logging.info('connecting to mysql')
  pw_var = config['pw_env_var_name']
  pw = os.getenv(pw_var)
  cached_db  = mysql.connector.connect( 
                        user=config['user'], 
                        password=pw,
                        host=config['host'],
                        database=config['db'],
                        port = int(config['port'])
                       )
  return cached_db

#########-#########-#########-#########-#########-#########-#########-#########-
# execute a statement with no result returned
#########-#########-#########-#########-#########-#########-#########-#########-
def do_command(q, param=None):
  cnx = get_dbh()
  cursor = cnx.cursor()
  try:
    cursor.execute(q, param)
    cnx.commit()
  except Exception as e:
    cursor.close()
    raise e
  cursor.close
  return True

#########-#########-#########-#########-#########-#########-#########-#########-
# run query, return first record
#########-#########-#########-#########-#########-#########-#########-#########-
def fetch_rec(q, param=None):
  result = fetch_set(q, param)
  try:
    return result[0]
  except:
    return None

#########-#########-#########-#########-#########-#########-#########-#########-
# return the set of records
#########-#########-#########-#########-#########-#########-#########-#########-
def fetch_set(q, param=None):
  cnx = get_dbh()
  cursor = cnx.cursor()
  try:
    cursor.execute(q, param )
    result = cursor.fetchall()
  except:
    cursor.close
    raise
  cursor.close
  return result

#########-#########-#########-#########-#########-#########-#########-#########-
# SQL to create a temporary table to load shows
#########-#########-#########-#########-#########-#########-#########-#########-
def create_show_load_table_q():
  q = '''create temporary table load_shows (
           `id` mediumint(8) unsigned NOT NULL,   
           `name` varchar(128) NOT NULL,
           `show_type` varchar(32) NOT NULL,
           `language` varchar(32) NOT NULL,
           `runtime` decimal(5,1) DEFAULT NULL,
           `premiered` date DEFAULT NULL,
           `ended` date DEFAULT NULL,
           `genres` varchar(128) NOT NULL,
           `src_url` varchar(255) NOT NULL,
          ) ENGINE=MEMORY CHARSET=utf8 '''
  return q
  
def drop_show_load_table_q():
  return 'DROP TEMPORARY TABLE IF EXISTS load_shows';

def load_shows_q(date):
  path = FS.show_data_file_path(date);

#########-#########-#########-#########-#########-#########-#########-#########-
# main program

q = 'select 1'
result = fetch_rec(q)
print(result);




