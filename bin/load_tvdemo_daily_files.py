#! /usr/bin/python3

import sys
from tvdemo.datamart import load_show_table, load_episode_table

try:
   date = sys.argv[1]
except IndexError:
  sys.exit('Argument is either date YYYY-MM-DD or the word ALL')

if ( date.count('-') != 2 and date != 'ALL'):
  sys.exit('Argument is either date YYYY-MM-DD or the word ALL')

load_show_table(date)
load_episode_table(date)

