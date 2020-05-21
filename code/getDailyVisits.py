
'''
This script uses Spark to explode the json-formatted columns in Safegraph weekly dataset.

Author: Zichang Ye
Date: 2020/05/10

Example Command Line:
python3 getDailyVisits.py --abs_path /Volumes/MyDataDrive/Data/safegraph/weekly_data/v1/main-file --week_ls '2020-04-05'
'''
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.ml.recommendation import ALS
from pyspark.mllib.evaluation import RankingMetrics
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import monotonically_increasing_id, col, expr
import pyspark.sql.functions as F
from pyspark.sql.functions import udf, explode
from functools import reduce
from pyspark.sql import DataFrame
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
from pyspark.ml.evaluation import Evaluator
import argparse
import numpy as np
from itertools import product
import time
from pyspark.sql.types import *
import json

def set_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument("--abs_path", help="The absolute path of which the data is stored")
    parser.add_argument("--week_ls",'--list', nargs='+', \
        help="The list of weeks of data you are working on")
    args = parser.parse_args()
    return args

def settings():
    '''This function sets the setting of the spark session'''
    spark = SparkSession.builder.appName('SafegraphDataQuery').getOrCreate()
    return spark
def parser(element):
    return json.loads(element)
def parser_maptype(element):
    return json.loads(element, MapType(StringType(), IntegerType()))
jsonudf = udf(parser, MapType(StringType(), IntegerType()))

convert_array_to_dict_udf = udf(lambda arr: {idx: x for idx, x in enumerate(json.loads(arr))}, MapType(StringType(), IntegerType()))

def readSpecificColumns(df, col_lists, conditions, col_names):
    '''return the df with selected columns
    Inputs
    1. spark: the spark session
    2. df: the corresponding Safegraph Dataframe, a spark object
    3. col_lists: the columns that needs to select, e.g. "_c0"
    4. conditions: the filtering conditions 
    5. col_names: a list of strings that you want to rename the columns
    '''
    df_selected = df.select(col_lists).filter(conditions)
    df_selected = df_selected.toDF(*col_names)

    return df_selected
    
# unwrap the json


def explode_json_column_with_labels(df_parsed, column_to_explode, key_col="key", value_col="value"):
    '''This function takes in the parsed dataframe'''
    '''returned a dataframe with the columns with json format exploded'''
    df_exploded = df_parsed.select("safegraph_place_id",\
        explode(column_to_explode)).selectExpr("safegraph_place_id", \
        "key as {0}".format(key_col), "value as {0}".format(value_col))
    return(df_exploded)

def explode_safegraph_json_column(df, column_to_explode, key_col="key", value_col="value"):
    '''This function takes in a parsed dataframe, returns a df with the columns in array format exploded'''
    # create a temporary df
    df_parsed = df.withColumn("parsed_"+column_to_explode, \
        jsonudf(column_to_explode))
    # explode it
    df_exploded = explode_json_column_with_labels(df_parsed, \
        "parsed_"+column_to_explode,\
            key_col=key_col, value_col=value_col)
    return(df_exploded)

def explode_safegraph_array_colum(df, column_to_explode, key_col="index", value_col="value"):
    '''This function takes in a parsed dataframe, returns a df with the columns in array format exploded'''
    
    # create a temporary df with columns needed
    df_prepped = df.select("safegraph_place_id", column_to_explode).\
        withColumn(column_to_explode+"_dict", convert_array_to_dict_udf(column_to_explode))
    # explode it
    df_exploded = explode_json_column_with_labels(df_prepped, \
        column_to_explode=column_to_explode+"_dict", \
            key_col=key_col, value_col=value_col)
    return(df_exploded)

if __name__ == "__main__":
    spark = settings()

    ### 0. setting the paths of the data ###
    args = set_arguments() 
    abs_path = args.abs_path
    time_ls = args.week_ls

    # time_ls = ['2020-04-12', '2020-04-19','2020-04-26']

    # testing
    # format the address
    for week in time_ls:
        path = abs_path + "/{}-weekly-patterns.csv.gz".format(week)
    ### 1. load the data ### 
        load_start = time.time()
        print("Reading the data...")
        df = spark.read.format('csv').option('escape',"\"").load(path)
        load_end = time.time()
        print("It takes {} seconds to read the data.".format(load_end - load_start))

        # create a view for the SQL queries
        df.createOrReplaceTempView('df')

        # read the visitor_home_cbg in specified places
        df_cbg = readSpecificColumns(df = df, col_lists = ["_c0", "_c14"],\
            conditions = "_c4 = 'NY'", col_names = ['safegraph_place_id','visitor_home_cbgs'])

        # explode the json 
        explode_start = time.time()
        print("Exploding the data...")       
        df_visitor_home_cbgs = explode_safegraph_json_column(df_cbg, \
            column_to_explode="visitor_home_cbgs", \
            key_col="census_block_group", value_col="num_visits")
        explode_end = time.time()
        print("It takes {} seconds to explode the data.".format(explode_end - explode_start))          
        df_visitor_home_cbgs.createOrReplaceTempView('df_visitor_home_cbgs')
        print(df_visitor_home_cbgs.show(5))

        # write to the absolute path
        df_visitor_home_cbgs.write.csv(abs_path+'/visitor_home_cbgs_{}_v0517.csv'.format(week))
