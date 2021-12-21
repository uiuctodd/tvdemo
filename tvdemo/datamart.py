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
# SQL to create/drop a temporary table to load shows
# load data and merge
#########-#########-#########-#########-#########-#########-#########-#########-
def create_show_load_table_q():
  q = '''create temporary table load_shows (
           `id` mediumint(8) unsigned NOT NULL,   
           `name` varchar(128) NOT NULL,
           `show_type_str` varchar(32) NOT NULL,
           `language` varchar(32) NOT NULL,
           `runtime` decimal(5,1) DEFAULT NULL,
           `premiered` date DEFAULT NULL,
           `ended` date DEFAULT NULL,
           `genres` varchar(128) NOT NULL,
           `src_url` varchar(255) NOT NULL,
           `show_type` int(11)
          ) ENGINE=MEMORY CHARSET=utf8 '''
  return q
  
def drop_show_load_table_q():
  return 'DROP TEMPORARY TABLE IF EXISTS load_shows';

def load_shows_q(date):
  path = FS.show_data_filepath(date);
  q = 'LOAD DATA LOCAL INFILE "' + path + '"' + \
      ''' into table load_shows CHARACTER SET utf8 
          FIELDS TERMINATED BY ',' ENCLOSED BY '"' IGNORE 1 LINES'''
  return q;

def extract_showtypes_q():
  q = '''insert ignore into show_types (code) 
         select show_type_str from load_shows'''

def merge_show_data_q():
  q = '''INSERT IGNORE INTO shows 
         SELECT l.id, l.name, t.id, l.language, l.runtime, l.premiered, 
                l.ended, l.genres, l.src_url
         FROM load_shows l LEFT JOIN show_types t 
              ON l.show_type_str = t.code'''
  return q

#########-#########-#########-#########-#########-#########-#########-#########-
# Load show data for one day
# There is no need to transactionalize this. If it fails early, the temp table
# will be dropped on the next attept, and unwritten records re-inserted
# if two processes attempt this at once, the second one will drop the 
# table out under the first. The first fails. Looser wins race-to-lock
#########-#########-#########-#########-#########-#########-#########-#########-
def load_show_table(date):
  do_command(drop_show_load_table_q())
  do_command(create_show_load_table_q())
  do_command(load_shows_q(date))
  do_command(merge_show_data_q())
  do_command(drop_show_load_table_q())
  return

#########-#########-#########-#########-#########-#########-#########-#########-
# SQL to create/drop a temporary table to load shows
# load data and merge
#########-#########-#########-#########-#########-#########-#########-#########-
def create_ep_load_table_q():
  q = '''create temporary table load_eps (
         `id` mediumint(8) unsigned NOT NULL,
         `airdate` date NOT NULL,
         `airstamp` datetime NOT NULL,
         `airtime` time DEFAULT NULL,
         `name` varchar(128) NOT NULL,
         `ep_num` decimal(6,1) DEFAULT NULL,
         `runtime` decimal(5,1) DEFAULT NULL,
         `season` tinyint(3) unsigned NOT NULL,
         `ep_type` varchar(32) NOT NULL,
         `show_id` mediumint(8) unsigned NOT NULL,
         `src_url` varchar(255) NOT NULL    
        ) ENGINE=MEMORY CHARSET=utf8 '''
  return q

def drop_ep_load_table_q():
  return 'DROP TEMPORARY TABLE IF EXISTS load_eps';

def load_ep_q(date):
  path = FS.episode_data_filepath(date);
  q = 'LOAD DATA LOCAL INFILE "' + path + '"' + \
      ''' into table load_eps CHARACTER SET utf8 
          FIELDS TERMINATED BY ',' ENCLOSED BY '"' IGNORE 1 LINES'''
  return q

def merge_ep_data_q():
  q = '''INSERT IGNORE INTO episodes 
         SELECT * from load_eps''' 
  return q

#########-#########-#########-#########-#########-#########-#########-#########-
# Load epsisode data for one day
# There is no need to transactionalize this. If it fails early, the temp table
# will be dropped on the next attept, and unwritten records re-inserted
# if two processes attempt this at once, the second one will drop the 
# table out under the first. The first fails. Looser wins race-to-lock
#########-#########-#########-#########-#########-#########-#########-#########-
def load_episode_table(date):
  do_command(drop_ep_load_table_q())
  do_command(create_ep_load_table_q())
  do_command(load_ep_q(date))
  do_command(merge_ep_data_q())
  do_command(drop_ep_load_table_q())
  return

  
#########-#########-#########-#########-#########-#########-#########-#########-
# main program
d='2021-12-20'
load_episode_table(d)

