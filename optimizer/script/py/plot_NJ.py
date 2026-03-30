#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Aug  4 12:23:21 2020

@author: federicafilippini
"""


import os
import sys
import argparse
import ast
import configparser as cp
import logging
import pandas as pd
import matplotlib.pyplot as plt


def createFolder(directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ("Error: Creating directory. " +  directory)


def plot_nodes_and_jobs (all_data, all_methods, nN, title = "", path = ""):
    
    # folder for figures
    if path != "":
        fig_folder1 = path + "/nodes/n" + str(nN)
        fig_folder2 = path + "/jobs/n" + str(nN)
        createFolder(fig_folder1)
        createFolder(fig_folder2)
    else:
        fig_folder1 = ""
        fig_folder2 = ""
    
    # define colormap
    cmap = plt.get_cmap("Set1")
    
    figN = plt.figure()
    axN = figN.gca()
    figJ = plt.figure()
    axJ = figJ.gca()
    
    for method in all_data:
        nNJ = pd.DataFrame(columns = ["sim_time", "n_nodes", "n_jobs", 
                                      "n_running_jobs", "nN"])
        data = all_data[method]
        colorVal = cmap.colors[all_methods[method]]
        for iterate in data.groupby(["n_iterate"]):
            temp = pd.DataFrame()
            temp["sim_time"] = [iterate[1]["sim_time"].iloc[0] / 3600]
            temp["n_nodes"] = [len(iterate[1]["node_ID"].dropna().unique())]
            temp["n_jobs"] = [len(iterate[1])]
            temp["n_running_jobs"] = [iterate[1]["node_ID"].count()]
            temp["nN"] = [nN]
            nNJ = nNJ.append(temp, ignore_index=True)
        nNJ.plot(x = "sim_time", y = "n_nodes", linestyle = "solid", 
                 ax = axN, label = ("nodes - " + method), color = colorVal)
        nNJ.plot(x = "sim_time", y = "n_jobs", linestyle = "solid", 
                 ax = axJ, label = ("jobs - " + method), color = colorVal)
        nNJ.plot(x = "sim_time", y = "n_running_jobs", linestyle = "dotted", 
                 ax = axJ, label = ("running jobs - " + method), 
                 color = colorVal)
    nNJ.plot(x = "sim_time", y = "nN", linestyle = "solid", ax = axN, 
             label = "available nodes", color = 'k')
    
    handles,labels = axN.get_legend_handles_labels()
    axN.legend(handles, labels, loc='center left', bbox_to_anchor=(1, 0.5))
    axN.set_xlabel("Simulation time [h]")
    axN.set_ylabel("Number of nodes")
    axN.set_title(title)
    
    handles,labels = axJ.get_legend_handles_labels()
    axJ.legend(handles, labels, loc='center left', bbox_to_anchor=(1, 0.5))
    axJ.set_xlabel("Simulation time [h]")
    axJ.set_ylabel("Number of nodes")
    axJ.set_title(title)
    
    if path != "":
        figN.savefig(fig_folder1 + "/" + title + ".pdf", format='pdf', 
                     dpi=1000, bbox_inches = "tight")
        plt.close(figN)
        figJ.savefig(fig_folder2 + "/" + title + ".pdf", format='pdf', 
                     dpi=1000, bbox_inches = "tight")
        plt.close(figJ)
    else:
        plt.show()


def main():
    
    parser = argparse.ArgumentParser(description="Compute averages of random methods")

    parser.add_argument("results_directory", help="The directory containing the results to be processed")
    parser.add_argument("size", help="The number of available nodes")
    parser.add_argument("-c", "--config", help="Configuration file", 
                        default="config.ini")
    parser.add_argument('-o', "--output", 
                        help="String to be added to the name of the results files", 
                        default="")
    parser.add_argument('-p', "--plot", 
                        help="Path to the directory where plots should be saved", 
                        default="")
    
    args = parser.parse_args()

    if not os.path.exists(args.results_directory):
        logging.error("%s does not exist", args.results_directory)
        sys.exit(1)

    ##########################################################################
    
    # all methods
    config = cp.ConfigParser()
    config.read(args.config)
    all_methods_list = ast.literal_eval(config["Methods"]["methods"])
    all_methods = {}
    for m in range(len(all_methods_list)):
        all_methods[all_methods_list[m]] = m
    
    # read data
    all_data = {}
    for file in os.listdir(args.results_directory):
        if file.startswith("all"):
            continue
        tokens = file.split("_")
        method = tokens[0]
        if method in all_methods and method not in all_data:
            logging.debug("Examining %s -- %s", str(method))        
            path = os.path.join(args.results_directory, file)
            all_data[method] = pd.read_csv(path)
    
    # plot number of used nodes, number of jobs in queue
    plot_nodes_and_jobs(all_data, all_methods, int(args.size), 
                        args.output, args.plot)


if __name__ == "__main__":
    main()
