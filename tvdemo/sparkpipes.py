#########-#########-#########-#########-#########-#########-#########-#########
# just a place to put tools common to all pipeles
#########-#########-#########-#########-#########-#########-#########-#########
from pyspark import SparkContext, SparkConf

#########-#########-#########-#########-#########-#########-#########-#########
# setup a cluster.
# this needs to be driven from the config handler
#########-#########-#########-#########-#########-#########-#########-#########
def cluster(appname='tvdemo_pipe'):
   cluster='local' # make config driven
    
   conf = SparkConf().setAppName(appname).setMaster(cluster)
   sc = SparkContext(conf=conf)
   return sc


