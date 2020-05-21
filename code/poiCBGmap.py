'''
This script merges the pre-available mapping between Safegraph place_id and census block group
And uses that to give a census block group to a new dataframe

Author: Zichang Ye
Date: 2020/05/10

Example Command Line:
python3 poiCBGmap.py --abs_path /Volumes/MyDataDrive/Data/safegraph/weekly_data/v1/main-file --week_ls '2020-04-19' '2020-04-26' --map_path /Volumes/MyDataDrive/Data/safegraph/data

'''
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import *
import json
import time
import argparse

def set_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--abs_path", help="The absolute path of which the data is stored")
    parser.add_argument(
        "--map_path", \
            help="The path where the poi_cbg mapping is located")
    parser.add_argument(
        "--week_ls",'--list', nargs='+', \
            help="The list of weeks of data you are working on")
    args = parser.parse_args()
    return args


def settings():
    '''This function sets the setting of the spark session'''
    spark = SparkSession.builder.appName('SafegraphDataQuery').getOrCreate()
    return spark

if __name__ == "__main__":
    spark = settings()

    ### 0. setting the paths of the data ###
    args = set_arguments()
    abs_path = args.abs_path
    week_ls = args.week_ls
    map_path = args.map_path
    print("Working on {}".format(abs_path))
    print("Working on data in weeks of {}".format(week_ls))
    print("Mapping file is in {}".format(map_path))
    # time_ls = ['2020-04-12', '2020-04-19','2020-04-26']

    # it works!

    # format the address
    for week in week_ls:
        path = abs_path + '/' + "visitor_home_cbgs_{}_v0517.csv".format(week)
        ### 1. load the data ###
        load_start = time.time()
        print("Reading the data...")
        df = spark.read.format('csv').option('escape',"\"").load(path)
        load_end = time.time()
        print("It takes {} seconds to read the data.".format((load_end - load_start)))
        
        # rename the visitor cbgs columns
        df = df.withColumnRenamed("_c0","safegraph_place_id") \
            .withColumnRenamed("_c1","visitor_home_cbg") \
                .withColumnRenamed("_c2","num_visits")

        # create a view for the SQL queries
        df.createOrReplaceTempView('df')

        ### 2. load the poi data ### 
        poi_cbg = spark.read.format('csv')\
            .option('header', True)\
                .load(map_path + '/poi_sgpids_cbgs_map.csv')
        poi_cbg.createOrReplaceTempView('poi_cbg')
        
        ### 3. joining ### 
        # sort it to accelerate the joining
        join_start = time.time()
        poi_cbg = poi_cbg.sort('safegraph_place_id')
        df = df.sort('safegraph_place_id')
        df = df.join(poi_cbg, "safegraph_place_id")
        df.show(5)
        join_end = time.time()
        print("The joining takes {} seconds, results showing above"\
            .format(join_end - join_start))

        ### 4. save the exploded file locally ###
        write_start = time.time()
        df.write.csv(abs_path+'/visitor_home_cbgs_joined_places_{}_v0517.csv'.format(week), header=True)
        write_end = time.time()
        print("It takes {} seconds to write the data to your working directory. Congrats!"\
            .format(write_end - write_start))
