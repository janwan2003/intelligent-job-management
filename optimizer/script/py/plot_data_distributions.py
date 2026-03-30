#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Sep 18 14:40:02 2020

@author: federicafilippini
"""

import os
import sys
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.colors as colors
import matplotlib.cm as cmx
from matplotlib.font_manager import FontProperties
#
from generate_data import generate_df_times
from generate_data import generate_custom_submissions
from generate_data import generate_submissions


def createFolder (directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ("Error: Creating directory. " +  directory)


def plot_times_by_nGPU (df_times, title = ""):
    # group times by job
    times_by_job = df_times.groupby(["Jobs"])
    n_jobs = len(times_by_job)
    # define colormap
    jet = plt.get_cmap("nipy_spectral")
    cNorm = colors.Normalize(vmin=0, vmax=n_jobs)
    scalarMap = cmx.ScalarMappable(norm=cNorm, cmap=jet)
    # define ax (to be used as hold on)
    fig1 = plt.figure(1)
    ax1 = fig1.gca()
    idx = 0
    for job in times_by_job:
        jID = job[0]
        first_uid = job[1]["UniqueJobsID"].iloc[0]
        jDF = job[1][job[1]["UniqueJobsID"] == first_uid].copy()
        jDF["ExecutionTime"] = jDF["ExecutionTime"] / 3600
        jDF = jDF[jDF["max_nGPUs"] == jDF["max_nGPUs"].max()]
        colorVal = scalarMap.to_rgba(idx)
        jDF.plot(kind = "line", x = "nGPUs", y = "ExecutionTime", 
                 color = colorVal, label = jID, ax = ax1, title = title)
        idx += 1
    fontP = FontProperties()
    handles1,labels1 = ax1.get_legend_handles_labels()
    ax1.legend(handles1, labels1, loc="center left", ncol = 2, prop=fontP, 
                bbox_to_anchor=(1, 0.5))
    plt.show()


def plot_submissions (SubmissionTimes, title = ""):
    ST = pd.DataFrame.from_dict(SubmissionTimes.items())
    ST.columns = ["Jobs", "SubmissionTime"]
    ax = plt.gca()
    ST.plot(kind = "scatter", x = "Jobs", y = "SubmissionTime", ax = ax,
            title = title)
    plt.tick_params(
        axis='x',          # changes apply to the x-axis
        which='both',      # both major and minor ticks are affected
        bottom=False,      # ticks along the bottom edge are off
        top=False,         # ticks along the top edge are off
        labelbottom=False) # labels along the bottom edge are off
    plt.show()


def plot_arrivals (arrivals, title = ""):
    plt.plot(range(len(arrivals)), arrivals, 'o-')
    plt.xlabel("Iteration")
    plt.ylabel("Inter-arrival time")
    plt.title(title)
    plt.show()


def plot_submissions_in_range (arrivals, H = 3600, title = ""):
    elapsed_time = 0.0
    submitted_jobs = [0]
    total_jobs = [0]
    idx = 0
    for time in arrivals:
        if elapsed_time > H:
            elapsed_time = 0
            submitted_jobs.append(0)
            total_jobs.append(total_jobs[-1])
            idx += 1
        elapsed_time += time
        submitted_jobs[idx] += 1
        total_jobs[idx] += 1
    # plot number of submitted jobs in each interval
    intervals = range(1,len(submitted_jobs)+1)
    fig1 = plt.figure(1)
    ax1 = fig1.gca()
    ax1.plot(intervals, submitted_jobs, 'o-')
    plt.xlabel("Time [h]")
    plt.ylabel("New submitted jobs")
    plt.title(title)
    # plot total number of submitted jobs in each interval
    fig2 = plt.figure(2)
    ax2 = fig2.gca()
    ax2.plot(intervals, total_jobs, 'o-')
    plt.xlabel("Time [h]")
    plt.ylabel("Total number of submitted jobs")
    plt.title(title)
    plt.show()


def cfr_submissions (arrivals_dict, H = 3600, title = ""):
    # all distributions
    all_distributions = list(arrivals_dict.keys())
    idxs = list(range(len(all_distributions)))
    all_distributions = dict(zip(all_distributions, idxs))
    # define colormap
    cmap = plt.get_cmap("Set1")
    # plot
    fig1 = plt.figure(1)
    ax1 = fig1.gca()
    out_idx = 0
    for distribution in arrivals_dict:
        arrivals = arrivals_dict[distribution]
        elapsed_time = 0.0
        submitted_jobs = [0]
        total_jobs = [0]
        idx = 0
        for time in arrivals:
            if elapsed_time > H:
                elapsed_time = 0
                submitted_jobs.append(0)
                total_jobs.append(total_jobs[-1])
                idx += 1
            elapsed_time += time
            submitted_jobs[idx] += 1
            total_jobs[idx] += 1
        # plot number of submitted jobs in each interval
        intervals = range(1,len(submitted_jobs)+1)
        #colorVal = cmap.colors[out_idx]
        colorVal = cmap.colors[all_distributions[distribution]]
        ax1.plot(intervals, submitted_jobs, 'o-', color = colorVal, 
                  label = distribution)
        out_idx += 1
    handles1,labels1 = ax1.get_legend_handles_labels()
    ax1.legend(handles1, labels1, loc="upper right", fontsize=16)
    plt.xlabel("Time [h]", fontsize=16)
    plt.ylabel("Number of submitted jobs", fontsize=16)
    plt.title(title)
    plt.show()


def plot_data (nN, nJ, myseed, possible_distributions, possible_lambdas, 
               scenario):
    
    # set seed to control random number generation and job selection
    np.random.seed(myseed)
    
    # load data and costs
    if scenario == "datacenter":
        # ["Application","Images","Epochs","Batchsize","Jobs","GpuType",
        #  "GpuNumber","ExecutionTime","min","max"]
        data_filename = "data_2d10d_filtered.csv"
        # ["GPUtype","nGPUs","PowerConsumption[W]","EnergyCost[€/kWh]",
        #  "PUE","cost"]
        cost_filename = "GPU-cost_server.csv"
    elif scenario == "regular":
        # ["Application","Images","Epochs","Batchsize","Jobs","GpuType",
        #  "GpuNumber","ExecutionTime","min","max"]
        data_filename = "data_2d10d_filtered.csv"
        # ["GPUtype","nGPUs","cost"]
        cost_filename = "GPU-cost.csv"
    else:
        # ["Jobs","Application","Epochs","Batchsize","GpuType","GpuNumber",
        #  "TimePerEpoch","ExecutionTime"]
        data_filename = "data_server.csv"
        # ["GPUtype","nGPUs","PowerConsumption[W]","EnergyCost[€/kWh]",
        #  "PUE","cost"]
        cost_filename = "GPU-cost_server_4-2.csv"
    data_file = "./build/data/" + data_filename
    df_data = pd.read_csv(data_file)
    costs_file = "./build/data/" + cost_filename
    df_costs = pd.read_csv(costs_file)

    # generate list of selected jobs
    all_jobs = df_data["Jobs"].unique()
    selected_jobs = []
    for idx in range(1,nJ+2):
        # generate an unique id for the new job
        unique_id = "JJ" + str(idx)
        # randomy pick a job from the list of all available jobs
        random_idx = np.random.randint(0, len(all_jobs))
        # add the corresponding job to the list (unique_id, job_id)
        selected_jobs.append((unique_id, all_jobs[random_idx]))
          
    # generate expected execution times of the selected jobs with all the
    # available setups
    #
    # df_times = ["Jobs", "BatchSize", "VMtype", "GPUtype", "nGPUs", 
    #             "max_nGPUs", "ExecutionTime"]
    #
    # df_times_dict = {}
    # gpu_types = ["M60", "K80", "GTX 1080Ti", "Quadro P600"]
    # for idx in range(len(gpu_types)):
    #     gputype = gpu_types[idx]
    #     negl_types = [*gpu_types[:idx], *gpu_types[idx+1:]]
    #     df_times_dict[gputype] = generate_df_times(df_data, df_costs, 
    #                                                 selected_jobs, 
    #                                                 negl_types)
    #     plot_times_by_nGPU(df_times_dict[gputype], title=gputype)
    df_times = generate_df_times(df_data, selected_jobs, 
                                 neglected_types = ["Quadro P600", 
                                                    "GTX 1080Ti"])
    
    # generate submission times of all selected jobs
    SubmissionTimes_dict = {}
    arrivals_dict = {}
    for distr in possible_distributions["custom"]:
        (SubmissionTimes_dict[distr], 
        arrivals_dict[distr]) = generate_custom_submissions(df_times, 
                                                            len(selected_jobs), 
                                                            nN, distr)
    for distr in possible_distributions["standard"]:
        for lambdaa in possible_lambdas:
            distr1 = distr + "_" + str(lambdaa)
            (SubmissionTimes_dict[distr1],
            arrivals_dict[distr1]) = generate_submissions(selected_jobs, 
                                                         lambdaa, nN, nJ,
                                                         distr)
    cfr_submissions(arrivals_dict, title=("nN=" + str(nN)))
    

if __name__ == "__main__":
    nN           = int(sys.argv[1])
    nJ           = int(sys.argv[2])
    myseed       = int(sys.argv[3])
    scenario     = sys.argv[4]
    
    possible_lambdas = [75000]
    possible_distributions = {"standard": ["exponential"],
                              "custom": ["low", "high", "mixed"]}
    possible_scenarios = ["regular", "datacenter", "ANDREAS"]
    
    if (nN == 0 or nJ == 0 or scenario not in possible_scenarios):
        print("Sorry!! the inputs are not valuable. Data generation failed.")
        sys.exit(1)
    else:
        plot_data(nN, nJ, myseed, possible_distributions, possible_lambdas, 
                  scenario)
