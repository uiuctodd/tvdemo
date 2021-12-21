#! /usr/bin/python3
#########-#########-#########-#########-#########-#########-#########-#########
# A pipeline to sumarise many (100M/day) play events by user activity
# (However, we will cheat and do just one hour)

import sys
from pyspark.sql import SparkSession 
import tvdemo.filesystem as FS 
from tvdemo.sparkpipes import cluster as get_sc

appname='tvdemo_view_pipe'  # pass this to the spark cluster

# Mapper for main pipleline
# emit date-user-show, 1-time-country
# in theory country is a key... however it is one/per user
# so I'm throwing it as a value
def emitter(r):
  show = showmap.get(r[0])
  return ( (r[1], r[4], show), (1, float(r[3]), r[2] ) )

# Mapper to unpack obtuse structure at end. Emits normal columns
def unpacker(r):
  return (r[0][0], r[0][1], r[0][2], r[1][0], r[1][1])

#########-#########-#########-#########-#########-#########-#########-#########
# main program
try:
   date = sys.argv[1]
   hour = sys.argv[2]
except IndexError:
    sys.exit('program takes two arguments: YYYY-MM-DD HH rec_count')

sc = get_sc(appname)
spark = SparkSession(sc)

# the dataframe method is nice for reading our csv files
infile = FS.play_event_filepath(date, hour) # the main data file
df = spark.read.option("header",True).csv(infile)

# read in the show mapper
mapfile = FS.show_lookup_filepath(date)     # converts eps to shows
map_rdd = spark.read.option("header",True).csv(mapfile).rdd
showmap = map_rdd.map( lambda r: (r[0],r[1]) ).collectAsMap()

# I'm not using spark sqk agg function here as they can have bad
# consequences to memory (pull in all values for a key)
# so I'll select push the df into standard map-reduce
keyvals = df.rdd.map(emitter)

feed = keyvals.aggregateByKey( 
         (0,0),            # initial value, count 0, sum 0
         lambda agg, step: ( agg[0] + step[0], agg[1] + step[1], step[2] ),   # combine recs
         lambda agg, part: ( agg[0] + part[0], agg[1] + part[1], step[2] ),   # combine tasks
         )

# For easy handling, I'm unpacking it back to a flat format
# There will be a really large number of records. This might crash
schema = ['date', 'user', 'show', 'views', 'view_time', 'country']
feed_df = feed.map(unpacker).toDF(schema)

outfile = FS.usershow_filepath(date, hour)
feed_df.write.csv(outfile, header="true", mode='overwrite')

