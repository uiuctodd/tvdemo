#########-#########-#########-#########-#########-#########-#########-#########
# Library of many utilities to parse and operate schedule information
#########-#########-#########-#########-#########-#########-#########-#########-

import logging
import datetime
import pandas as pd
import pytz 
import random
import urllib

from tvdemo.config import get_config

#########-#########-#########-#########-#########-#########-#########-#########
# raw data of the entire schedudle, loaded into pandas
#########-#########-#########-#########-#########-#########-#########-#########
def fetch_data_from_url():
  config = get_config()
  url = config['schedule_url']
  logging.info('hitting schedule service at ' + url)
  f = urllib.request.urlopen(url)
  schedule = pd.read_json(f.read())    
  return schedule

#########-#########-#########-#########-#########-#########-#########-#########
# raw data of schedule, for one day,loaded into pandas
# pass in a dataset of all days, or don't if you want to fetch it
#########-#########-#########-#########-#########-#########-#########-#########
def get_data_by_date(date, ds=None):
  if (ds is None):  
    ds = fetch_data_from_url()  # get the list of all days
  one_day = ds.loc[ds['airdate'] == date]
  return one_day

#########-#########-#########-#########-#########-#########-#########-#########
# just here for debugging -- gimme count of scheduled items broken by day
#########-#########-#########-#########-#########-#########-#########-#########
def inspect_days(ds):
  daily = ds.groupby("airdate")
  print(daily.size())

#########-#########-#########-#########-#########-#########-#########-#########
# build a dictionary to map "episode" to "show" for data reduction
# pass this the pandas schedule, and maybe filter by date first
#########-#########-#########-#########-#########-#########-#########-#########
def build_show_dictionary(ds, copy=False):

  # consider making a copy if you want the original unharmed
  if copy:
    ep = ds.copy(deep=True)
  else:
    ep = ds

  # make a new column of the show_id, extracted from the _embedded column
  show_id = ep.apply(lambda row: row['_embedded']['show']['id'], axis=1)

  # and another for language
  lang = ep.apply(lambda row: row['_embedded']['show']['language'], axis=1)

  # append the new columns
  ep['show'] = show_id
  ep['lang'] = lang

  # slice off just the episode ID and new cols
  ep_dict = ep.loc[:, ['show', 'lang']]
  return ep_dict 

#########-#########-#########-#########-#########-#########-#########-#########
# given the "episode" info dataset from the schedule service, normalize it
# that is, return a dataset of episodes and a dataset of shows
#########-#########-#########-#########-#########-#########-#########-#########
def normalize_show_data(ds, copy=False):

  # consider making a copy if you want the original unharmed
  if copy:
    ep = ds.copy(deep=True)
  else:
    ep = ds

  # pull out the show info, packed inside each episode
  show_recs = ep['_embedded'].apply(lambda rec: rec['show'])

  # To normalize episodes....
  # make a new column of the show id, extracted from the record
  # and append this to the end of the schedule dataset. Discard original
  show_id = show_recs.apply(lambda row: row['id'])
  ep['show'] = show_id
  ep.drop(['_embedded'], axis=1, inplace=True) 

  # preserve the source url, which is deeply packed in json
  src = ep.apply(lambda row: row['_links']['self']['href'], axis=1)
  ep['src_url'] = src

  # dropping some tables we don't need for analytics
  drop_cols = ['rating', 'summary', '_links', 'image', 'url']
  ep.drop(drop_cols, axis=1, inplace=True)

  # re-index by id (we've been by record number all this time)
  ep.drop_duplicates(subset=['id'], inplace=True)
  ep.set_index('id', inplace=True)

  # build the show data frame  This is a mess. Pandas keeps complaining. 
  # Need to extract all the data and build a new set
  shows = pd.DataFrame({'id': show_id.values })

  # extract the records into columns
  field_list = ['name', 'type', 'language', 'runtime', 
                'premiered', 'ended', 'genres']
  for f in field_list:
    col = show_recs.apply(lambda row: row[f])
    shows[f] = col.values

  # the service url for the show requires special unpacking
  src = show_recs.apply(lambda row: row['_links']['self']['href'])
  shows['src_url'] = src.values

  # de-dupe this table, which has a row for each original episode, and index
  shows.drop_duplicates(subset=['id'], inplace=True)
  shows.set_index('id', inplace=True)
  
  return(ep, shows)

#########-#########-#########-#########-#########-#########-#########-#########
# for one date, build a dictionary mapping ep to show
# if passed a dataset, use the entire thing and assume it's what's wanted
# otherwise, hit the service and filter it to the requested data
#########-#########-#########-#########-#########-#########-#########-#########
def build_dict_for_day(date, ds=None):
  if (ds is None):
    ds = get_data_by_date(date)
  return build_show_dictionary(ds)

#########-#########-#########-#########-#########-#########-#########-#########
# make a filename for the dictionary of episodes to shows
#########-#########-#########-#########-#########-#########-#########-#########
def show_dict_filepath(date):
  config = get_config()
  file_base = config['storage_root']
  stem = '/show_episode_dict/show_dict_'
  return file_base + stem + date

#########-#########-#########-#########-#########-#########-#########-#########
# make a filename for the show data load file
#########-#########-#########-#########-#########-#########-#########-#########
def show_data_filepath(date):
  config = get_config()
  file_base = config['storage_root']
  stem = '/show_data/show_data_'
  return file_base + stem + date

#########-#########-#########-#########-#########-#########-#########-#########
# make a filename for the episode data load file
#########-#########-#########-#########-#########-#########-#########-#########
def episode_data_filepath(date):
  config = get_config()
  file_base = config['storage_root']
  stem = '/episode_data/ep_data_'
  return file_base + stem + date

#########-#########-#########-#########-#########-#########-#########-#########
# Generate a show-episode dictionary for a day and write to disk
#########-#########-#########-#########-#########-#########-#########-#########
def write_show_dict(date, ds=None):
  dd = build_dict_for_day(date, ds)
  path = show_dict_filepath(date)
  logging.info('writing show-episode dictionary at: ' + path)
  dd.to_csv(path)

#########-#########-#########-#########-#########-#########-#########-#########
# Main program
date = '2021-12-20'
ds = get_data_by_date(date)
(ep,shows) = normalize_show_data(ds)

path = show_data_filepath(date)
logging.info('writing show data at: ' + path)
shows.to_csv(path)

path = episode_data_filepath(date)
ep.to_csv(path)

