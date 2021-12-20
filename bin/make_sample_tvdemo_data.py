#! /usr/bin/python3
#########-#########-#########-#########-#########-#########-#########-#########
# Adapted from assignment document-- create a file of sample events
# methods changed to support sensible date:
#    - user bound to one country
#    - some users have multiple views in same day (limited pool)
#    - episodes all broadcast in the appropriate day

import sys
import pytz
import random
import pandas

import tvdemo.filesystem as FS 

default_num_recs = 100

#########-#########-#########-#########-#########-#########-#########-#########
# create a pair of arrays containint user_id and countries
#########-#########-#########-#########-#########-#########-#########-#########
def make_userpool(size):
  users = {}
  countries = []

  loc = list(pytz.country_names.keys()) # list of countries

  while len(countries) < size:
    id = random.randint(1000000, 10000000)
    if id not in users:
      users[id] = True
      c = random.choice(loc)
      countries.append(c)

  return (list(users.keys()), countries)

#########-#########-#########-#########-#########-#########-#########-#########
# main program

try:
   date = sys.argv[1]
   hour = sys.argv[2]
except IndexError:
    sys.exit('program takes three arguments: YYYY-MM-DD HH rec_count')

try:
  num_recs = int(sys.argv[3])
except IndexError:
  num_recs = default_num_recs

# In order to isolate users to countries and simulate users with 
# multiple views, we will make our user_ids deterministic (non-random)
# and create an arry containing their country.
# the userpool will users half as long as data the record set. 
# Therefore, many users will watch 1 show, but some will watch 2 or three.
user_pool_size = int(num_recs/2)
(userpool, usercountry) = make_userpool(user_pool_size)

# read in the schedule for this date
path = FS.episode_data_filepath(date)
episodes = pandas.read_csv(path)['id'].values.tolist()

# build dataset proper
data = []
for i in range(num_recs):
  u_idx = random.randint(1, user_pool_size) -1
  e = random.choice(episodes)
  vh = round(abs(random.normalvariate(3, 1)), 2) #view hours
  record = [ e, date, usercountry[u_idx], vh, userpool[u_idx] ]
  data.append(record)

# I'm only using pandas here because it does csv to s3 well
cols = ['episode_id', 'view_date', 'country', 'view_hours', 'viewer_id']
df = pandas.DataFrame(data, columns=cols)
path = FS.play_event_filepath(date, hour)
print(path)
df.to_csv(path, index=False)

