#########-#########-#########-#########-#########-#########-#########-#########
# Library of many utilities to parse and operate schedule information
#########-#########-#########-#########-#########-#########-#########-#########-

import logging
import pandas as pd
import urllib

import tvdemo.filesystem as FS
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
# build a dictionary to map "episode" to "show" for map-reduce joins
# pass this the pandas schedule as input, and maybe filter by date first
#########-#########-#########-#########-#########-#########-#########-#########
def build_show_lookup(ds, do_copy=False):

  # consider making a copy if you want the original unharmed
  if do_copy:
    ep = ds.copy(deep=True)
  else:
    ep = ds

  # episodes come in indexed by a synthetic record number.
  # we can start by re-indexing to episode_id
  ep.drop_duplicates(subset=['id'], inplace=True)
  ep.set_index('id', inplace=True)

  # make a new column of the show_id, extracted from the _embedded column
  show_id = ep.apply(lambda row: row['_embedded']['show']['id'], axis=1)

  # and another for language
  lang = ep.apply(lambda row: row['_embedded']['show']['language'], axis=1)

  # append the new columns
  ep['show'] = show_id
  ep['lang'] = lang

  # slice off just the episode ID and new cols
  show_lookup = ep.loc[:, ['show', 'lang']]
  return show_lookup 

#########-#########-#########-#########-#########-#########-#########-#########
# given the "episode" info dataset from the schedule service, normalize it
# that is, return a dataset of episodes and a dataset of shows
#########-#########-#########-#########-#########-#########-#########-#########
def normalize_show_data(ds, do_copy=False):

  # consider making a copy if you want the original unharmed
  if do_copy:
    eps = ds.copy(deep=True)
  else:
    eps = ds

  # episodes come in indexed by a synthetic record number.
  # we can start by re-indexing to episode_id
  eps.drop_duplicates(subset=['id'], inplace=True)
  eps.set_index('id', inplace=True)

  # pull out the show info, packed inside each episode
  show_recs = eps['_embedded'].apply(lambda rec: rec['show'])

  # Normalize out the show data:
  # make a new column of the show id, extracted from the record
  # and append this to the end of the schedule dataset. Discard original
  show_id = show_recs.apply(lambda row: row['id'])
  eps['show'] = show_id
  eps.drop(['_embedded'], axis=1, inplace=True) 

  # preserve the source url, which is deeply packed in json
  src = eps.apply(lambda row: row['_links']['self']['href'], axis=1)
  eps['src_url'] = src

  # dropping some tables we don't need for analytics
  drop_cols = ['rating', 'summary', '_links', 'image', 'url']
  eps.drop(drop_cols, axis=1, inplace=True)

  # build the show data frame  This is a mess. Pandas keeps complaining. 
  # Need to extract all the data and build a new set with "values" method
  shows = pd.DataFrame({'id': show_id.values })

  # extract the records into columns
  field_list = ['name', 'type', 'language', 'runtime', 
                'premiered', 'ended']
  for f in field_list:
    col = show_recs.apply(lambda row: row[f])
    shows[f] = col.values

  # For reasons of downstream ease, I am packing genres into a string
  genres = show_recs.apply( lambda row: '_'.join(row['genres']) )
  shows['genres'] = genres.values

  # the service url for the show requires special unpacking
  src = show_recs.apply(lambda row: row['_links']['self']['href'])
  shows['src_url'] = src.values

  # de-dupe table, which has row for each pre-normalized episode, and reindex
  shows.drop_duplicates(subset=['id'], inplace=True)
  shows.set_index('id', inplace=True)
  
  return(eps, shows)

#########-#########-#########-#########-#########-#########-#########-#########
# for one date, build a dictionary mapping ep to show
# if passed a dataset, use the entire thing and assume it's what's wanted
# otherwise, hit the service and filter it to the requested data
#########-#########-#########-#########-#########-#########-#########-#########
def build_lookup_for_day(date, schedule=None, do_copy=True):
  if (schedule is None):
    schedule = get_data_by_date(date)
  return build_show_lookup(schedule, do_copy)

#########-#########-#########-#########-#########-#########-#########-#########
# for one date, build the episode and show data tables
# if passed a dataset, use the entire thing and assume it's what's wanted
# otherwise, hit the service and filter it to the requested data
#########-#########-#########-#########-#########-#########-#########-#########
def build_eps_shows_for_day(date, schedule=None, do_copy=True):
  if (schedule is None):
    schedule = get_data_by_date(date)
  (eps,shows) = normalize_show_data(schedule, do_copy)
  return (eps,shows)

#########-#########-#########-#########-#########-#########-#########-#########
# Generate a show-episode dictionary for a day and write to disk
# You can pass this the daily schedule, or have it generate one
#########-#########-#########-#########-#########-#########-#########-#########
def write_show_lookup(date, schedule=None):
  ds = build_lookup_for_day(date, schedule)
  path = FS.show_lookup_filepath(date)
  logging.info('writing show-episode lookup at: ' + path)
  ds.to_csv(path)

#########-#########-#########-#########-#########-#########-#########-#########
# Generate a show-episode dictionary for a day and write to disk
# Pass the schedule for the day, or have it generate one
#########-#########-#########-#########-#########-#########-#########-#########
def write_ep_show_data(date, schedule=None):
  (eps,shows) = build_eps_shows_for_day(date, schedule)

  path = FS.episode_data_filepath(date)
  logging.info('writing episode data at: ' + path)
  eps.to_csv(path)

  path = FS.show_data_filepath(date)
  logging.info('writing show data at: ' + path)
  shows.to_csv(path)

#########-#########-#########-#########-#########-#########-#########-#########
# Just a wrapper method to do all daily chores around schedule service
#########-#########-#########-#########-#########-#########-#########-#########
def create_daily_files(date):
  schedule = get_data_by_date(date)
  write_show_lookup(date, schedule)
  write_ep_show_data(date, schedule)
  return True

#########-#########-#########-#########-#########-#########-#########-#########
# A non-prod debug method to pull files for the entire schedule
#########-#########-#########-#########-#########-#########-#########-#########
def create_files_all_days():
  schedule = fetch_data_from_url()
  write_show_lookup('ALL_DAYS', schedule)
  write_ep_show_data('ALL_DAYS', schedule)
  return True

#########-#########-#########-#########-#########-#########-#########-#########
# Main program (nothing here)

