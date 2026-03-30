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
import matplotlib.pyplot as plt


def createFolder (directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ("Error: Creating directory. " +  directory)


def plot_time (times, all_methods, colname, distribution, plot_path):
    
    current_figure = plt.figure()
    ax = current_figure.gca()
    for method in all_methods:
        times.plot(x = "Nodes", y = colname + "_" + method, 
                   linestyle = "solid", ax = ax, label = method, 
                   fontsize = 14, linewidth = 3, logy = True)
    handles,labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels, loc='center left', 
              bbox_to_anchor=(1, 0.5), fontsize = 14)
    plt.xlabel("Number of nodes", fontsize = 14)
    plt.ylabel("Average time [s]", fontsize = 14)
    title = colname + " - " + distribution
    plt.title(title, fontsize = 14)
    if plot_path == "":
        plt.show()
    else:
        title = title.replace(" ", "_")
        title = title.replace("-_","")
        fig_path = plot_path + "/figures"
        createFolder(fig_path)
        current_figure.savefig(fig_path + "/" + title + ".pdf", 
                               format='pdf', dpi=1000, 
                               bbox_inches = "tight")
        plt.close(current_figure)


def plot_times (directory, distribution, all_methods, plot_path):

    file = os.path.join(directory, "Time_averaged_"+distribution+".csv")
    if os.path.exists(file):
        
        times = pd.read_csv(file)
        
        plot_time(times, all_methods, "AvgTime", distribution, plot_path)
        plot_time(times, all_methods, "AvgTimePerIter", distribution, 
                  plot_path)


def plot_sim_times (directory, distribution, all_methods, plot_path):

    file = os.path.join(directory, "SimTimes_averaged_"+distribution+".csv")
    if os.path.exists(file):
        
        times = pd.read_csv(file)
        
        current_figure = plt.figure()
        ax = current_figure.gca()
        for method in all_methods:
            times.plot(x = "Nodes", y = "sim_time_" + method, 
                       linestyle = "solid", ax = ax, label = method, 
                       fontsize = 14, linewidth = 3, logy = True)
        handles,labels = ax.get_legend_handles_labels()
        ax.legend(handles, labels, loc='center left', 
                  bbox_to_anchor=(1, 0.5), fontsize = 14)
        plt.xlabel("Number of nodes", fontsize = 14)
        plt.ylabel("Simulation Time [h]", fontsize = 14)
        title = "Simulation Time - " + distribution
        plt.title(title, fontsize = 14)
        if plot_path == "":
            plt.show()
        else:
            title = title.replace(" ", "_")
            title = title.replace("-_","")
            fig_path = plot_path + "/figures"
            createFolder(fig_path)
            current_figure.savefig(fig_path + "/" + title + ".pdf", 
                                   format='pdf', dpi=1000, 
                                   bbox_inches = "tight")
            plt.close(current_figure)
        

def plot_cost (costs, all_methods, colname, distribution, plot_path):
    
    current_figure = plt.figure()
    ax = current_figure.gca()
    for method in all_methods:
        costs.plot(x = "Nodes", y = colname + "_" + method, 
                   linestyle = "solid", ax = ax, label = method, 
                   fontsize = 14, linewidth = 3, logy = True)
    handles,labels = ax.get_legend_handles_labels()
    ax.legend(handles, labels, loc='center left', 
              bbox_to_anchor=(1, 0.5), fontsize = 14)
    plt.xlabel("Number of nodes", fontsize = 14)
    plt.ylabel("Average cost [€]", fontsize = 14)
    title = colname + " - " + distribution
    plt.title(title, fontsize = 14)
    if plot_path == "":
        plt.show()
    else:
        title = title.replace(" ", "_")
        title = title.replace("-_","")
        fig_path = plot_path + "/figures"
        createFolder(fig_path)
        current_figure.savefig(fig_path + "/" + title + ".pdf", 
                               format='pdf', dpi=1000, 
                               bbox_inches = "tight")
        plt.close(current_figure)


def plot_costs (directory, distribution, all_methods, plot_path):
    
    file = os.path.join(directory, "TC_averaged_" + distribution + ".csv")
    if os.path.exists(file):
        
        costs = pd.read_csv(file)
        
        plot_cost(costs, all_methods, "TardiCost", distribution, plot_path)
        plot_cost(costs, all_methods, "EnergyCost", distribution, plot_path)
        plot_cost(costs, all_methods, "TotalCost", distribution, plot_path)


def main ():
    
    parser = argparse.ArgumentParser(description="Compute and plot averages of random methods")

    parser.add_argument("results_directory", help="The directory containing results files")
    parser.add_argument("-c", "--config", help="Configuration file", 
                        default="config.ini")
    parser.add_argument('-d', "--debug", help="Enable debug messages", 
                        default=False, action="store_true")
    parser.add_argument('-p', "--plot", 
                        help="Path to the directory where plots should be saved", 
                        default="")
    
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
    
    for distribution in all_distributions:
        plot_costs(args.results_directory, distribution, all_methods, 
                   args.plot)
        plot_sim_times(args.results_directory, distribution, all_methods, 
                   args.plot)
        plot_times(args.results_directory, distribution, all_methods, 
                   args.plot)


if __name__ == "__main__":
    main()