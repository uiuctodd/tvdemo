#! /usr/bin/python3

import sys
from tvdemo.schedules import create_daily_files, create_files_all_days

try:
   date = sys.argv[1]
except IndexError:
  sys.exit('Argument is either date YYYY-MM-DD or the word ALL')

if (date == 'ALL'):
   create_files_all_days()

else:
  if ( date.count('-') != 2 ):
    sys.exit('Argument is either date YYYY-MM-DD or the word ALL')
  create_daily_files(date)

