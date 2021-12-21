#########-#########-#########-#########-#########-#########-#########-#########
# Consolidated information on where to find files
# should work whether on local filesystem or S3 (set root in config)
# EXCEPT: check and build of file storage tree only works locally
#         (unset these options in the app.ini file)
#########-#########-#########-#########-#########-#########-#########-#########-
import logging
import os
from tvdemo.config import get_config

stems = { 'sl' : '/show_lookup/',
          'sd' : '/show_data/',
          'ed' : '/episode_data/',
          'rpe': '/play_event_files/',
          'pd' : '/play_data/',
          'up' : '/user_profile/',
          'us' : '/usershow/',
        }

#########-#########-#########-#########-#########-#########-#########-#########
# utility function to make the directory
#########-#########-#########-#########-#########-#########-#########-#########
def dirname(ftype):
  if ftype not in stems:
    raise KeyError('A request was made for unkown file type: ' + ftype)

  config = get_config()
  file_base = config['storage_root']
  return file_base + stems[ftype]

#########-#########-#########-#########-#########-#########-#########-#########
# make a filename for the dictionary of episodes to shows
#########-#########-#########-#########-#########-#########-#########-#########
def show_lookup_filepath(date):
  d = dirname('sl')
  return d + 'show_lookup_' + date

#########-#########-#########-#########-#########-#########-#########-#########
# make a filename for the show data files
#########-#########-#########-#########-#########-#########-#########-#########
def show_data_filepath(date):
  d = dirname('sd')
  return d + 'show_data_' + date

#########-#########-#########-#########-#########-#########-#########-#########
# make a filename for the episode data files
#########-#########-#########-#########-#########-#########-#########-#########
def episode_data_filepath(date):
  d = dirname('ed')
  return d + 'ep_data_' + date

#########-#########-#########-#########-#########-#########-#########-#########
# make a filename for the raw play event files
#########-#########-#########-#########-#########-#########-#########-#########
def play_event_filepath(date, hour):
  d = dirname('rpe')
  return d + '_'.join(['play_events', date, hour])

#########-#########-#########-#########-#########-#########-#########-#########
# make a filename for the summary play data
#########-#########-#########-#########-#########-#########-#########-#########
def play_data_filepath(date, hour):
  d = dirname('pd')
  return d + '_'.join(['play_data', date, hour])

#########-#########-#########-#########-#########-#########-#########-#########
# make a filename for intermidiate user-show counts
#########-#########-#########-#########-#########-#########-#########-#########
def usershow_filepath(date, hour):
  d = dirname('us')
  return d + '_'.join(['usershow', date, hour])

#########-#########-#########-#########-#########-#########-#########-#########
# check that a directory exists, consider making it.
#########-#########-#########-#########-#########-#########-#########-#########
def check_dir_exists(path, do_fix=False):
  if not os.path.isdir(path):
    logging.warning('Missing directory at: ' + path)

    if do_fix:
      try:
        os.mkdir(path)
        logging.info('successfully created directory: ' + path)
      except Exception as E:
          logging.error('unable to create dir at: ' + path)
          raise e

#########-#########-#########-#########-#########-#########-#########-#########
# check that all the needed subdirs exist
#########-#########-#########-#########-#########-#########-#########-#########
def check_filesystem(do_fix=False):
  config = get_config()
  file_base = config['storage_root']
  check_dir_exists(file_base, do_fix)
    
  for ftype in stems:
    path = dirname(ftype)
    check_dir_exists(path, do_fix)

#########-#########-#########-#########-#########-#########-#########-#########
# RUN ON IMPORT
config = get_config()
do_file_check = config['check_filesystem']
do_fix = config['build_filesystem']
if do_file_check:
  check_filesystem(do_fix)

