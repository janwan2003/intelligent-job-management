#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Feb  9 12:12:33 2021

@author: federicafilippini
"""


import sys
import os
import ast
import configparser as cp
import argparse

def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ("Error: Creating directory. " +  directory)


def createInput(basedir, inputs, delta, method, existing_random, n_cpp_seed, 
                myseed, from_cpp_seed, n_random_iter, verbose, current_time, 
                LList_file, simulation, scheduling_interval):
    #directory = basedir + "/call_" + inputs.replace(" ", "-") + "/results_" + \
    #            str(delta)
    directory = basedir + "/call_" + inputs.replace(" ", "-")
    createFolder(directory)
    createFolder(directory + "/results")
    
    if method in existing_random:
        ncppseed = n_cpp_seed                    
        for j in range(ncppseed):
            cppseed = myseed + 10*(j+from_cpp_seed+2)
            benchmark_name = method + "_" + inputs.replace(" ", "_") + "_" +\
                             str(delta) + "_" + str(cppseed)
            Linputs = "method=" + method +\
                      " folder=" + directory +\
                      " seed=" + str(cppseed) +\
                      " verbose=" + str(verbose) +\
                      " current_time=" + str(current_time) +\
                      " simulation=" + simulation
            if simulation == "true" or simulation == "True":
                Linputs += " scheduling_interval=" + str(scheduling_interval)
            Linputs += " benchmark_name=" + benchmark_name
            LList_file.write(Linputs+"\n")
    else:
        benchmark_name = method + "_" + inputs.replace(" ", "_") + "_" +\
                         str(delta)
        Linputs = "method=" + method + \
                  " folder=" + directory +\
                  " verbose=" + str(verbose) +\
                  " current_time=" + str(current_time) +\
                  " simulation=" + simulation
        if simulation == "true" or simulation == "True":
            Linputs += " scheduling_interval=" + str(scheduling_interval)
        Linputs += " benchmark_name=" + benchmark_name
        LList_file.write(Linputs+"\n")


def custom_distribution (distribution):
    return (distribution != "poisson" and distribution != "exponential")


def main ():

    parser = argparse.ArgumentParser(description="Generate input list")

    parser.add_argument('-c', "--config", help="Configuration file", 
                        default="config.ini")
    parser.add_argument('-o', "--output",
                        help="String to be added to names of the list files",
                        default="")

    args = parser.parse_args()

    ##########################################################################

    # absolute path of current script
    current_script = os.path.abspath(sys.argv[0])
    basedir = current_script.split("/")[:-3]
    basedir = "/".join(basedir) + "/build"

    ##########################################################################

    ## configuration parameters
    config = cp.ConfigParser()
    config.read(args.config)

    # number of nodes
    nodes_min         = int(config["Nodes"]["nodes_min"])
    nodes_max         = int(config["Nodes"]["nodes_max"])
    nodes_step        = int(config["Nodes"]["nodes_step"])

    # number of instances
    instances       = int(config["Instances"]["instances"])
    from_seed       = int(config["Instances"]["from_seed"])

    # parameters for randomization
    distributions   = ast.literal_eval(config["RandomParameters"]["distributions"])
    lambdaas        = ast.literal_eval(config["RandomParameters"]["lambdaas"])
    n_random_iter   = int(config["RandomParameters"]["n_random_iter"])
    n_cpp_seed      = int(config["RandomParameters"]["n_cpp_seed"])
    from_cpp_seed   = int(config["RandomParameters"]["from_cpp_seed"])

    # scenario for data generation
    scenario        = config["Scenario"]["scenario"]

    # methods
    methods         = ast.literal_eval(config["Methods"]["methods"])
    existing_heur   = ast.literal_eval(config["Methods"]["existing_heuristics"])
    existing_random = ast.literal_eval(config["Methods"]["existing_random"])

    # jobs
    nInitialJ       = int(config["Jobs"]["nInitialJ"])
    job_times_nodes = float(config["Jobs"]["job_times_nodes"])

    # current time
    current_time    = float(config["Time"]["current_time"])

    # other parameters
    delta           = float(config["OtherParams"]["delta"])
    verbose         = int(config["OtherParams"]["verbose"])
    simulation      = config["OtherParams"]["simulation"]
    if simulation == "true" or simulation == "True":
        sched_interval = config["OtherParams"]["scheduling_interval"]
        if sched_interval != "inf" and sched_interval != "INF":
            sched_interval = float(sched_interval)
    else:
        sched_interval = None

    ##########################################################################

    ## list generation
    folder = basedir + "/data/lists/"
    createFolder(folder)
    List_file = open(folder + "list_of_data" + args.output + ".txt", "w")
    LList_file = open(folder + "list_of_inputs" + args.output + ".txt", "w")
    nodes = nodes_min

    basedir = basedir + "/calls"
    
    while nodes <= nodes_max:
        print(nodes)
    
        Jobs = int(job_times_nodes * nodes)

        for distribution in distributions:
            for i in range(instances):
                myseed = from_seed + 1000 * (i**2)
                inputs = str(nodes) + " " + str(Jobs) + " " + str(myseed)
                if not custom_distribution(distribution):
                    for lambdaa in lambdaas:
                        inputs2 = inputs + " " + str(distribution) + " " + \
                                  str(scenario) + " " + simulation + " " + \
                                  str(current_time) + " " + str(lambdaa)
                        List_file.write(inputs2 +"\n")
                else:
                    inputs2 = inputs + " " + str(distribution) + " " + \
                              str(scenario) + " " + simulation + " " + \
                              str(current_time)
                    List_file.write(inputs2 +"\n")
                    
                inputs = str(nInitialJ) + " " + inputs

                for method in methods:
                    inputs2 = inputs + " " + str(distribution)
                    if not custom_distribution(distribution):
                        for lambdaa in lambdaas:
                            inputs3 = inputs2 + " " + str(lambdaa)
                            createInput(basedir, inputs3, delta, method, 
                                        existing_random, n_cpp_seed, 
                                        myseed, from_cpp_seed, n_random_iter, 
                                        verbose, current_time, LList_file,
                                        simulation, sched_interval)
                    else:
                        inputs3 = inputs2 + " 000"
                        createInput(basedir, inputs3, delta, method, 
                                    existing_random, n_cpp_seed, 
                                    myseed, from_cpp_seed, n_random_iter, 
                                    verbose, current_time, LList_file,
                                    simulation, sched_interval)

        nodes = nodes + nodes_step

    List_file.close()
    LList_file.close()
    

if __name__ == "__main__":
    main()
