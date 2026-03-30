#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May  8 19:04:22 2020

@author: federicafilippini
"""

import sys
import os
import pandas as pd



def build_input_file (original_data):
    
    input_files = []    
    if original_data:
        input_files.append(('data','data.csv'))
    else:
        results_directory = 'build/data/from_loading_data'
        for content in os.listdir(results_directory):
            combined_path = os.path.join(results_directory, content)
            if os.path.isdir(combined_path):
                for file in os.listdir(combined_path):
                    if os.path.isdir(file):
                        continue
                    if not file.startswith('Select'):
                        continue
                    input_files.append((content,file))
    
    return input_files



def count_jobs_per_time (original_data):
    
    if original_data:
        jobs_name = 'Jobs'
        folder = 'build/'
    else:
        jobs_name = 'UniqueJobsID'
        folder = 'build/data/from_loading_data/'
    
    input_files = build_input_file(original_data)
    
    for elem in input_files:
        print(elem[0])
        
        input_file = folder + elem[0] + '/' + elem[1]
        data = pd.read_csv(input_file)
        
        one_hour  = 3600
        one_day   = 24 * one_hour
        one_week  = 7 * one_day
        one_month = 4 * one_week
        
        # group data by 'Jobs'
        group_by_jobs = data.groupby([jobs_name])
        average_times = pd.DataFrame(group_by_jobs.mean()['ExecutionTime'])
        
        in_day = average_times[average_times.ExecutionTime <= one_day]
        over_day = average_times[average_times.ExecutionTime > one_day]
        print('\t# jobs with time < one day: ', in_day.count()[0])
        
        in_week = over_day[over_day.ExecutionTime <= one_week]
        over_week = over_day[over_day.ExecutionTime > one_week]
        print('\t# jobs with time between one day and one week: ', 
              in_week.count()[0])
        
        in_month = over_week[over_week.ExecutionTime <= one_month]
        over_month = over_week[over_week.ExecutionTime > one_month]
        print('\t# jobs with time between one week and one month: ',
              in_month.count()[0])
        
        print('\t# jobs with time > one month: ', over_month.count()[0])



if __name__ == "__main__":
    if len(sys.argv) > 1:
        original_data = int(sys.argv[1])
    else:
        original_data = False
    count_jobs_per_time(original_data)