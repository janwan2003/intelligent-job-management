#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Mar 19 17:05:25 2021

@author: federicafilippini
"""


import sys
import os
import ast
import configparser as cp
import argparse
import logging
import collections
from datetime import datetime


def createFolder (directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ("Error: Creating directory. " +  directory)


def recursivedict():
    return collections.defaultdict(recursivedict)


def main ():

    parser = argparse.ArgumentParser(description="Compute costs of all methods")

    parser.add_argument("logs_directory", 
                        help="The directory containing the logs folders")
    parser.add_argument('-c', "--config", help="Configuration file", 
                        default="config.ini")
    parser.add_argument('-d', "--debug", help="Enable debug messages", 
                        default=False, action="store_true")
    parser.add_argument('-o', "--output", 
                        help="String to be added to the name of results files", 
                        default="")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, 
                            format='%(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, 
                            format='%(levelname)s: %(message)s')

    if not os.path.exists(args.logs_directory):
        logging.error("%s does not exist", args.results_directory)
        sys.exit(1)

    ##########################################################################

    ## configuration parameters
    config = cp.ConfigParser()
    config.read(args.config)

    # methods
    existing_heuristics = ast.literal_eval(config['Methods']['existing_heuristics'])
    existing_random = ast.literal_eval(config['Methods']['existing_random'])

    ##########################################################################

    all_times = recursivedict()

    found_methods = set()

    for log_dir in os.listdir(args.logs_directory):
        combined_path = os.path.join(args.logs_directory, log_dir)
        if os.path.isdir(combined_path):
            tokens = log_dir.split("_")
            method = tokens[0]
            
            if method not in existing_heuristics and \
                method not in existing_random:
                continue
            
            nodes = tokens[2]
            jobs = tokens[3]
            seed = tokens[4]
            distribution = tokens[5]
            lambdaa = tokens[6]
            #delta = tokens[7]
            
            if method in existing_random:
                cppseed = tokens[8]
            else:
                cppseed = seed
            
            logging.debug("Examining %s", str(log_dir))
            
            file = os.path.join(combined_path, "CppTests_execution_output")
            if os.path.exists(file):
                file_reader = open(file, "r")
                
                experiment = (method, nodes, jobs, lambdaa, #delta, 
                              distribution, seed)
                found_methods.add(method)
                
                n_iterations = 0
                
                for row in file_reader:
                    if row.startswith("start time"):
                        start_time = datetime.strptime(row[11:-1], 
                                                       "%Y-%m-%d %H:%M:%S.%f")
                    elif row.startswith("finish time"):
                        end_time = datetime.strptime(row[12:-1], 
                                                     "%Y-%m-%d %H:%M:%S.%f")
                    elif "ITER" in row:
                        n_iterations = row.split(" ")[2]
                
                # compute and add elapsed time
                elapsed_time = (end_time - start_time).total_seconds()
                time_per_iter = elapsed_time / (int(n_iterations) - 1)
                all_times[experiment][cppseed] = (elapsed_time, time_per_iter)
    
    logging.debug("Found methods %s", str(found_methods))

    if not found_methods:
        logging.error("No cpp result found")
        sys.exit(1)

    if (args.logs_directory).split("/")[-1] == "output":
        basedir = "/".join((args.logs_directory).split("/")[:-1])
    else:
        basedir = args.logs_directory
    results_folder = os.path.join(basedir, "csv")
    createFolder(results_folder)
    
    times_file_name = "elapsed_time" + args.output + ".csv"
    times_file = open(os.path.join(results_folder,times_file_name), "w")
    times_file.write("Method,Nodes,Jobs,Lambda,Distribution,")
    times_file.write("Seed,CppSeed,Time,TimePerIter\n")

    times_file_name_avg = "elapsed_time_averaged" + args.output + ".csv"
    times_file_avg = open(os.path.join(results_folder,times_file_name_avg), "w")
    times_file_avg.write("Method,Nodes,Jobs,Lambda,Distribution,")
    times_file_avg.write("Seed,AvgTime,AvgTimePerIter\n")
    
    for experiment in sorted(all_times):
        logging.debug("Examining %s", str(experiment))
        times = all_times[experiment]
        times_file.write(",".join(experiment))
        times_file_avg.write(",".join(experiment))

        average1 = 0.0
        average2 = 0.0
        ncppseed = 0

        for cppseed in times:
            if ncppseed == 0:
                times_file.write("," + str(cppseed) + "," + \
                                 str(times[cppseed][0])+ "," + \
                                 str(times[cppseed][1]) + "\n")
            else:
                times_file.write(",,,,,," + str(cppseed) + "," + \
                                 str(times[cppseed][0])+ "," + \
                                 str(times[cppseed][1]) + "\n")
            average1 += times[cppseed][0]
            average2 += times[cppseed][1]
            ncppseed += 1

        average1 /= ncppseed
        average2 /= ncppseed
        times_file_avg.write("," + str(average1) + "," + str(average2) + "\n")

    times_file.close()
    times_file_avg.close()


if __name__ == "__main__":
    main()
