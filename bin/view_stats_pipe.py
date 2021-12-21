#! /usr/bin/python3
#########-#########-#########-#########-#########-#########-#########-#########
# A pipeline to sumarise many (100M/day) play events into a simple feed
# (However, we will cheat and do just one hour)

import sys
from pyspark.sql import SparkSession 
import tvdemo.filesystem as FS 
from tvdemo.sparkpipes import cluster as get_sc

appname='tvdemo_view_pipe'  # pass this to the spark cluster

# Mapper for main pipleline
# emit date-episode-country, 1-playtime
def emitter(r):
  return ( (r[1], r[0], r[2]), (1,float(r[3])))

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

# the dataframe method is nice for reading our csv files
sc = get_sc(appname)
spark = SparkSession(sc)
infile = FS.play_event_filepath(date, hour)
df = spark.read.option("header",True).csv(infile)

# I'm not using spark sqk agg function here as they can have bad
# consequences to memory (pull in all values for a key)
# so I'll select push the df into standard map-reduce
keyvals = df.rdd.map(emitter)

feed = keyvals.aggregateByKey( 
         (0,0),            # initial value, count 0, sum 0
         lambda agg, step: ( agg[0] + step[0], agg[1] + step[1] ),   # combine recs
         lambda agg, part: ( agg[0] + part[0], agg[1] + part[1] ),   # combine tasks
         )

# the final number of rows will be very small
# For easy handling, I'm unpacking it back to a flat format
schema = ['date', 'episode', 'country', 'views', 'view_time']
feed_df = feed.map(unpacker).toDF(schema)

outfile = FS.play_data_filepath(date, hour)
feed_df.coalesce(1).write.csv(outfile, header="true", mode='overwrite')

