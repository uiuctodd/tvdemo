#########-#########-#########-#########-#########-#########-#########-#########
# configuration handler
# find the config file, read it in, cache it
#########-#########-#########-#########-#########-#########-#########-#########

import os            #need to read env vars
import logging
import configparser  

p = {}                  #cache conf in global scope so we only read once

default_filename = '~/tvdemo/conf/tvdemo.ini'  #look here if no env var set
config_filename = os.getenv('TVDEMO_CONFIG_FILE', default_filename)

# this in itself really should be in the config file...
logging.basicConfig(level=logging.INFO)

#########-#########-#########-#########-#########-#########-#########-#########
# import this function. This will throw a keyerror if not a known section
# with no section set, assume we want "settings"
#########-#########-#########-#########-#########-#########-#########-#########
def get_config(section='settings'):
  if section is 'ALL':
    return p
  return p[section]

#########-#########-#########-#########-#########-#########-#########-#########
# Read the config file, cache results
#########-#########-#########-#########-#########-#########-#########-#########
def read_mainfile():
  global p
  p = configparser.ConfigParser()
  logging.info("Looking for config file at "  + config_filename)
  p.read(config_filename)

#########-#########-#########-#########-#########-#########-#########-#########-
# On import, process config and stash for later
#########-#########-#########-#########-#########-#########-#########-#########-
read_mainfile()
