#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 19 10:14:02 2021

@author: federicafilippini
"""


import os
import sys
import ast
import configparser as cp
import argparse
import logging
import pandas as pd


def createFolder (directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ("Error: Creating directory. " +  directory)    


def read_costs_files (directory, all_distributions, all_methods):
    
    for file in os.listdir(directory):
        combined_path = os.path.join(directory, file)
        if os.path.isdir(combined_path):
            continue
        if not file.startswith("total_costs_averaged"):
            continue
        
        # read file
        costs = pd.read_csv(combined_path)
        
        # loop over existing distributions
        for distribution in all_distributions:
            costs_d = costs[costs["Distribution"] == distribution]
            new_costs_d = pd.DataFrame()
            is_first = True
            
            # loop over existing methods
            for method in all_methods:
                costs_m = costs_d[costs_d["Method"] == method].copy()
                costs_m.rename(columns = {"TardiCost": "TardiCost_"+method,
                                          "EnergyCost": "EnergyCost_"+method,
                                          "TotalCost": "TotalCost_"+method}, 
                               inplace = True)
                costs_m.drop("Method", axis=1, inplace=True)
                if is_first:
                    new_costs_d = costs_m.copy()
                    is_first = False
                else:
                    merge_cols = ["Nodes", "Jobs", "Lambda", "Distribution", 
                                  "Seed"]
                    new_costs_d = pd.merge(new_costs_d, costs_m, 
                                           on = merge_cols)
            
            # save new dataframe on file
            filename = "TC_" + distribution + ".csv"
            path = os.path.join(directory, "aggregated_results/")
            createFolder(path)
            new_costs_d.to_csv(path + filename, index=False)


def read_sim_times (directory, all_distributions, all_methods):
    
    for file in os.listdir(directory):
        combined_path = os.path.join(directory, file)
        if os.path.isdir(combined_path):
            continue
        if not file.startswith("sim_times_averaged"):
            continue
        
        # read file
        times = pd.read_csv(combined_path)
        
        # loop over existing distributions
        for distribution in all_distributions:
            times_d = times[times["Distribution"] == distribution]
            new_times_d = pd.DataFrame()
            is_first = True
            
            # loop over existing methods
            for method in all_methods:
                times_m = times_d[times_d["Method"] == method].copy()
                times_m.rename(columns = {"sim_time": "sim_time_"+method}, 
                               inplace = True)
                times_m.drop("Method", axis=1, inplace=True)
                if is_first:
                    new_times_d = times_m.copy()
                    is_first = False
                else:
                    merge_cols = ["Nodes", "Jobs", "Lambda", "Distribution", 
                                  "Seed"]
                    new_times_d = pd.merge(new_times_d, times_m, 
                                           on = merge_cols)
            
            # save new dataframe on file
            filename = "SimTimes_" + distribution + ".csv"
            path = os.path.join(directory, "aggregated_results/")
            createFolder(path)
            new_times_d.to_csv(path + filename, index=False)


def read_times (directory, all_distributions, all_methods):
    
    for file in os.listdir(directory):
        combined_path = os.path.join(directory, file)
        if os.path.isdir(combined_path):
            continue
        if not file.startswith("elapsed_time_averaged"):
            continue
        
        # read file
        times = pd.read_csv(combined_path)
        
        # loop over existing distributions
        for distribution in all_distributions:
            times_d = times[times["Distribution"] == distribution]
            new_times_d = pd.DataFrame()
            is_first = True
            
            # loop over existing methods
            for method in all_methods:
                times_m = times_d[times_d["Method"] == method].copy()
                times_m.rename(columns = {"AvgTime": "AvgTime_"+method,
                                  "AvgTimePerIter": "AvgTimePerIter_"+method}, 
                               inplace = True)
                times_m.drop("Method", axis=1, inplace=True)
                if is_first:
                    new_times_d = times_m.copy()
                    is_first = False
                else:
                    merge_cols = ["Nodes", "Jobs", "Lambda", "Distribution", 
                                  "Seed"]
                    new_times_d = pd.merge(new_times_d, times_m, 
                                           on = merge_cols)
            
            # save new dataframe on file
            filename = "Time_" + distribution + ".csv"
            path = os.path.join(directory, "aggregated_results/")
            createFolder(path)
            new_times_d.to_csv(path + filename, index=False)


def compute_averages (directory):
    
    path = os.path.join(directory, "aggregated_results/")
    for file in os.listdir(path):
        combined_path = os.path.join(path, file)
        if os.path.isdir(combined_path):
            continue
        if "averaged" in file:
            continue
        
        # get distribution
        tokens = file.split("_")
        distribution = tokens[-1].split(".")[0]
        
        # read file
        data = pd.read_csv(combined_path)
        average_data = pd.DataFrame(columns = data.columns)
        
        # compute averages
        for nodes in data.groupby(["Nodes"]):
            average = pd.DataFrame(nodes[1].mean()).transpose()
            average_data = pd.concat([average_data, average])
        
        # restore distribution data
        average_data["Distribution"] = distribution
        
        # drop seed column
        average_data.drop("Seed", axis=1, inplace=True)
        
        # adjust data types
        average_data = average_data.astype({"Nodes": int, "Jobs": int,
                                            "Lambda": int})
        
        # save new dataframe on file
        filename = tokens[0] + "_averaged_" + distribution + ".csv"
        new_path = os.path.join(path, filename)
        average_data.to_csv(new_path, index=False)


def main ():
    
    parser = argparse.ArgumentParser(description="Compute and plot averages of random methods")

    parser.add_argument("results_directory", help="The directory containing results files")
    parser.add_argument("-c", "--config", help="Configuration file", 
                        default="config.ini")
    parser.add_argument('-d', "--debug", help="Enable debug messages", 
                        default=False, action="store_true")
    
    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    if not os.path.exists(args.results_directory):
        logging.error("%s does not exist", args.results_directory)
        sys.exit(1)

    ##########################################################################
        
    ## configuration parameters
    config = cp.ConfigParser()
    config.read(args.config)

    # distributions
    all_distributions = ast.literal_eval(config["RandomParameters"]["distributions"])
    
    # methods
    all_methods = ast.literal_eval(config["Methods"]["methods"])
    
    ##########################################################################
    
    logging.debug("reading costs files...")
    read_costs_files(args.results_directory, all_distributions, all_methods)

    logging.debug("reading simulation times...")    
    read_sim_times(args.results_directory, all_distributions, all_methods)

    logging.debug("reading times...")    
    read_times(args.results_directory, all_distributions, all_methods)
    
    compute_averages(args.results_directory)
    


if __name__ == "__main__":
    main()
