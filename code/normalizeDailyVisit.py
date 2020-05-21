'''This file work on flattened visitors_home_cbgs dafaframe,
 and normalize the visits with normalization file.
 In this phase, they are the home-summary-file series.
 
 Author: Zichang Ye
 Date: 2020/05/10

 Example Command Line
 python3 normalizeDailyVisit.py --abs_path /Users/yezichang/Desktop/NYU/COVID-19/data/weekly_data/place_and_visitor_cbgs_v4 
 --week_ls '2020-04-12' '2020-04-19' '2020-04-26' 
 --norm_path /Users/yezichang/Desktop/NYU/COVID-19/data/reference:
 
 '''

import time
import argparse
import pandas as pd
import glob 

def set_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--abs_path", help="The absolute path of which the data is stored")
    parser.add_argument(
        "--norm_path", \
            help="The path where the normalization files mapping is located")
    parser.add_argument(
        "--week_ls",'--list', nargs='+', \
            help="The list of weeks of data you are working on")
    args = parser.parse_args()
    return args


if __name__ == "__main__":

    ### 0. setting the paths of the data ###
    args = set_arguments()
    abs_path = args.abs_path
    week_ls = args.week_ls
    norm_path = args.norm_path
    print("Working on {}".format(abs_path))
    print("Working on data in weeks of {}".format(week_ls))
    print("Mapping file is in {}".format(norm_path))

    # format the address
    dfs = []
    for week in week_ls:
        path = abs_path + "/visitor_home_cbgs_joined_places_{}_v0517.csv".format(week)
        norm_path_week = norm_path + '/{}-home-panel-summary.csv'.format(week)
        print(norm_path_week)
        ### 1. load the data ###
        load_start = time.time()
        print("Reading the data...")
        all_files = glob.glob(path + "/*.csv")
        li = []
        for filename in all_files:
            df = pd.read_csv(filename, index_col=None, header=0)
            li.append(df)
        df = pd.concat(li, axis=0, ignore_index=True)
        load_end = time.time()

        print("It takes {} seconds to read the data.".format((load_end - load_start)))
        
        ### 2. load the normalization data ### 
        norm_df = pd.read_csv(norm_path_week, header = 0)

        ### 3. Preprocess the data ### 

        # change the CBGs code to string
        df['census_block_group'] = df['census_block_group'].apply(int).apply(str)
        df['visitor_home_cbg'] = df['visitor_home_cbg'].apply(int).apply(str)

        # drop the traces of concatenation
        df = df.drop(df[df['num_visits'] == 'num_visits'].index)

        # get the county code
        df['visitor_county_code'] = \
            df['visitor_home_cbg'].\
                apply(lambda x: str(x)[0:5])

        df['place_county_code'] = \
            df['census_block_group'].\
                apply(lambda x: str(x)[0:5])

        print(df.head(5))
        ### 4. join with the normalization stats, and normalize ### 
        
        # filter out New York CBG
        norm_df_ny = norm_df

        # prepare the columns
        normalization_ny = norm_df_ny[['census_block_group',\
            'number_devices_residing']]
        normalization_ny['census_block_group'] = \
            normalization_ny['census_block_group'].apply(int).apply(str)
    

        # join with the dataframe
        ### 2020-05-16 Update: try to join by the visitor_home_cbg, because number_devices_residing  ### 
        df = pd.merge(df, normalization_ny,\
            left_on = 'visitor_home_cbg', 
            right_on = "census_block_group",
            how = 'left')
    
        # normalize
        df['num_visits'] = df['num_visits'].apply(int)
        df['num_visitors_nmlz'] = df['num_visits']/df['number_devices_residing']

        # keep relevant columns for NYU project
        to_keep = ['place_county_code','visitor_county_code', \
            'num_visits','num_visitors_nmlz']
        df = df[to_keep]

        # add a column that specify the week
        df['date'] = pd.Series([str(week)]*df.shape[0])
        
        ### 5. aggregate the visits for each placecbg - visitorcbg pair ### 
        df_agg = df.groupby(['place_county_code', \
            'visitor_county_code','date']).agg({'num_visits':'sum','num_visitors_nmlz':'sum'})
        
        df_agg = df_agg.reset_index()[['date','place_county_code', \
            'visitor_county_code','num_visits','num_visitors_nmlz']]
    
        print(df_agg.head(5))
        # append to a list
        dfs.append(df_agg)
        ### 6. write to working directory ###
    
    total_df = pd.concat(dfs)    
    ### 2020-05-16 Update v2: try to join by the visitor_home_cbg, because number_devices_residing  ### 
    total_df.to_csv(abs_path + \
    '/{}-TO-{}-visitors_home_county_normalized_v2.csv'.format(week_ls[0],week_ls[-1]))



        





        

        