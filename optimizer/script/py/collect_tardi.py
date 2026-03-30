#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jul  9 12:07:39 2020

@author: federicafilippini
"""

import sys
import os
import ast
import configparser as cp
import argparse
import csv
import logging
import collections


def createFolder (directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ("Error: Creating directory. " +  directory)


def recursivedict():
    return collections.defaultdict(recursivedict)


def main ():

    parser = argparse.ArgumentParser(description="Compute tardiness of all methods")

    parser.add_argument("results_directory", 
                        help="The directory containing the results folders")
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

    if not os.path.exists(args.results_directory):
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

    all_tardi = recursivedict()

    found_methods = set()

    for content in os.listdir(args.results_directory):
        combined_path = os.path.join(args.results_directory, content)
        if os.path.isdir(combined_path):
            if not content.startswith("call_1-"):
                continue
            tokens = content.split("-")
            nodes = tokens[1]
            jobs = tokens[2]
            seed = tokens[3]
            distribution = tokens[4]
            lambdaa = tokens[5]
            for res_dir in os.listdir(combined_path):
                res_path = os.path.join(combined_path, res_dir)
                if not os.path.isdir(res_path):
                    continue
                if not res_dir.startswith("result"):
                    continue
                #tokens = res_dir.split("_")
                #delta = tokens[1]

                logging.debug("Examining %s", str(res_dir))
                
                for file in os.listdir(res_path):
                    if os.path.isdir(file):
                        continue
                
                    if not file.startswith("all_results"):
                        continue
                    cost_data = csv.reader(open(os.path.join(res_path, 
                                                             file), "r"))
                    for row in cost_data:
                        # method and cpp_seed for random methods
                        method = row[0]
                        if method not in existing_heuristics and \
                            method not in existing_random:
                            continue
                        if method in existing_random:
                            cppseed = row[1]
                        else:
                            cppseed = seed
                        logging.debug("Examining %s", str(method+"-"+cppseed))
                        total_tardi = float(row[2])
                        experiment = (method, nodes, jobs, lambdaa, #delta, 
                                      distribution, seed)
                        found_methods.add(method)
                        all_tardi[experiment][cppseed] = total_tardi
    
    logging.debug("Found methods %s", str(found_methods))

    if not found_methods:
        logging.error("No cpp result found")
        sys.exit(1)

    if (args.results_directory).split("/")[-1] == "results":
        basedir = "/".join((args.results_directory).split("/")[:-1])
    else:
        basedir = args.results_directory
    results_folder = os.path.join(basedir, "csv")
    createFolder(results_folder)

    results_file_name = "total_tardi" + args.output + ".csv"
    results_file = open(os.path.join(results_folder,results_file_name), "w")
    results_file.write("Method,Nodes,Jobs,Lambda,Distribution,")
    results_file.write("Seed,CppSeed,TotalTardi\n")

    averages_file_name = "total_tardi_averaged" + args.output + ".csv"
    averages_file = open(os.path.join(results_folder,averages_file_name), "w")
    averages_file.write("Method,Nodes,Jobs,Lambda,Distribution,")
    averages_file.write("Seed,TotalTardi\n")

    for experiment in sorted(all_tardi):
        logging.debug("Examining %s", str(experiment))

        results = all_tardi[experiment]

        results_file.write(",".join(experiment))
        averages_file.write(",".join(experiment))

        average = 0.0
        ncppseed = 0

        for cppseed in results:
            if ncppseed == 0:
                results_file.write("," + str(cppseed) +\
                                   "," + str(results[cppseed]) + "\n")
            else:
                results_file.write(",,,,,," + str(cppseed) +\
                                   "," + str(results[cppseed]) + "\n")
            average += results[cppseed]
            ncppseed += 1

        average /= ncppseed
        averages_file.write("," + str(average) + "\n")

    results_file.close()
    averages_file.close()


if __name__ == "__main__":
    main()
