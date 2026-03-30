#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 8 10:28:23 2020

@author: federicafilippini
"""

# data:
#
# {"jobs":              {ID:    {"SubmissionTime":    "%a %d %b %Y, %H.%M.%S",
#                                "Deadline":          "%a %d %b %Y, %H.%M.%S",
#                                "Priority":          int,
#                                "Epochs":            int,
#                                "ProfilingData":     {string: {int: float, [s]
#                                                               int: float, [s]
#                                                               ...},
#                                                      string: {int: float, [s]
#                                                               int: float, [s]
#                                                               ...},
#                                                      ...}
#                                },
#                        ...},
#  "nodes":             {ID:     {"GPUtype":           string,
#                                 "free_nGPUs":        int,
#                                 "total_nGPUs":       int,
#                                 "cost":              float [$/h]
#                                },
#                        ...},
#  "GPUcosts":          {string: {int:                 float [$/h],
#                                 int:                 float [$/h],
#                                 ...},
#                        string: {int:                 float [$/h],
#                                 int:                 float [$/h],
#                                 ...},
#                        ...},
#  "currentScheduling": {ID:     {"nGPUs": int, 
#                                 "GPUtype": string,
#                                 "node": string
#                                 },
#                        ...},
#  "currentTime":       "%a %d %b %Y, %H.%M.%S",
#  "method":            string,
#  "verbose":           int,
#  "seed":              int
# }
# Example: see post_request.py
#
#
# result:
#
# {"estimated_cost": float, 
#  "estimated_rescheduling_time":   "%a %d %b %Y, %H.%M.%S", 
#  "jobs":                          {ID:  {"expected_tardiness": float, [h] 
#                                          "nGPUs": int, 
#                                          "node": string
#                                         },
#                                    ...}
# }
# Example: see post_request.py


from flask import Flask, request, jsonify
import pandas as pd
import os
from datetime import datetime
from waitress import serve
from math import inf, isnan


def createFolder (directory):
    try:
        if not os.path.exists(directory):
            os.makedirs(directory)
    except OSError:
        print ('Error: Creating directory. ' +  directory)


def str_to_time (time):
    t1 = datetime.strptime(time, "%a %d %b %Y, %H.%M.%S")
    return (t1.timestamp())


def time_to_str (time_str):
    t1 = datetime.fromtimestamp(time_str)
    t2 = datetime.strftime(t1, "%a %d %b %Y, %H.%M.%S")
    return (t2)


def is_zero (s):
    zero = False
    if s == "0.0" or s == "0" or s == "0.":
        zero = True
    return (zero)


def get_tw (priority):
    tw = -1
    if priority == 1:
        tw = 0.00025371
    elif priority == 2:
        tw = 0.00030128
    elif priority == 3:
        tw = 0.00034885
    elif priority == 4:
        tw = 0.00039642
    elif priority == 5:
        tw = 0.00044399
    return (tw)


def get_method (data):
    if "method" in data.keys():
        temp = data["method"].split("_")
        method = temp[0]
        simulation = (len(temp) > 1)
    else:
        method = "RG"
        simulation = False
    return (method, simulation)


def get_nodes_data (data, is_FPM):
    tNodes_full = pd.DataFrame.from_dict(data["nodes"]).transpose()
    selected_columns = ["GPUtype", "free_nGPUs", "cost"]
    tNodes = tNodes_full[selected_columns].copy()
    tNodes.rename(columns = {"free_nGPUs": "nGPUs"}, inplace = True)
    return (tNodes)


def get_GPUcosts (data):
    # initialize container
    GPUcosts = pd.DataFrame(columns = ["GPUtype", "nGPUs", "cost"])
    key_error = 0
    #
    #         GPUtype   GPUtype   ...
    # nGPUs    time      time
    # nGPUs    ...       ...
    # ...
    temp_costs = pd.DataFrame.from_dict(data["GPUcosts"])
    GPUtype_list = []
    nGPUs_list = []
    costs_list = []
    for GPUtype in temp_costs:
        for g in temp_costs.index:
            cost = temp_costs[GPUtype][g]
            if not isnan(cost):
                GPUtype_list.append(GPUtype)
                nGPUs_list.append(int(g))
                costs_list.append(float(cost))
    temp_S = pd.DataFrame()
    temp_S["GPUtype"] = GPUtype_list
    temp_S["nGPUs"] = nGPUs_list
    temp_S["cost"] = costs_list
    #
    if not GPUtype_list or not nGPUs_list or not costs_list:
        key_error = 80
    else:
        GPUcosts = pd.concat([GPUcosts, temp_S], ignore_index=True)
    #
    return (GPUcosts, key_error)


def get_jobs_data (data, tNodes):
    # initialize containers
    Lof_selectjobs = pd.DataFrame(columns = ["ID", "SubmissionTime", 
                                             "Deadline", "TardinessWeight", 
                                             "MinExecTime", "MaxExecTime"])
    SelectJobs_times = pd.DataFrame(columns = ["ID", "GPUtype", "nGPUs", 
                                               "ExecutionTime"])
    key_error = 0
    #
    jobs = data["jobs"]
    for job_id in jobs:
        # get information about current job
        temp_L = pd.DataFrame()
        temp_L["ID"] = [job_id]
        temp_L["SubmissionTime"] = [str_to_time(jobs[job_id]["SubmissionTime"])]
        temp_L["Deadline"] = [str_to_time(jobs[job_id]["Deadline"])]
        tw = get_tw(int(jobs[job_id]["Priority"]))
        temp_L["TardinessWeight"] = [tw]

        # exit if tardiness weight is not an admissible value
        if tw == -1:
            key_error = 40
            break

        # set initial value for min and max execution time
        temp_L["MinExecTime"] = [0.0]
        temp_L["MaxExecTime"] = [0.0]

        # temp_GT:
        #
        #         GPUtype   GPUtype   ...
        # nGPUs    time      time
        # nGPUs    ...       ...
        # ...
        temp_GT = pd.DataFrame.from_dict(jobs[job_id]["ProfilingData"])
        
        # get number of epochs
        nEpochs = int(jobs[job_id]["Epochs"])
        
        # get execution times data
        GPUtype_list = []
        nGPUs_list = []
        times_list = []
        for GPUtype in temp_GT:
            if GPUtype in tNodes["GPUtype"].values:
                for j in temp_GT.index:
                    if not isnan(temp_GT[GPUtype][j]):
                        GPUtype_list.append(GPUtype)
                        nGPUs_list.append(int(j))
                        # get time and multiply by the number of epochs
                        time = float(temp_GT[GPUtype][j]) * nEpochs
                        times_list.append(time)
        temp_S = pd.DataFrame()
        temp_S["ID"] = [job_id] * len(GPUtype_list)
        temp_S["GPUtype"] = GPUtype_list
        temp_S["nGPUs"] = nGPUs_list
        temp_S["ExecutionTime"] = times_list

        if not GPUtype_list or not nGPUs_list or not times_list:
            key_error = 50
            break
        
        temp_L["MinExecTime"] = [temp_S["ExecutionTime"].min()]
        temp_L["MaxExecTime"] = [temp_S["ExecutionTime"].max()]
        
        Lof_selectjobs = pd.concat(
            [Lof_selectjobs, temp_L], 
            ignore_index=True
        )
        SelectJobs_times = pd.concat(
            [SelectJobs_times, temp_S], 
            ignore_index=True
        )
    #
    return (Lof_selectjobs, SelectJobs_times, key_error)


def get_current_solution (data):
    # get data
    currentScheduling = data["currentScheduling"]
    jobs = data["jobs"]
    GPUcosts = data["GPUcosts"]
    #
    JobSch = pd.DataFrame(columns = ["n_iterate", "sim_time", 
                                     "UniqueJobsID", "SubmissionTime",
                                     "Deadline", "TardinessWeight",
                                     "MinExecTime", "MaxExecTime",
                                     "SelectedTime", "ExecutionTime",
                                     "CompletionPercent", "StartTime",
                                     "FinishTime", "node_ID", 
                                     "GPUtype", "n_assigned_GPUs",
                                     "Tardiness", "GPUcost", 
                                     "TardinessCost", "TotalCost"])
    for job_id in currentScheduling:
        # check if the job is currently submitted and get its data
        if job_id in jobs.keys():
            job_data = jobs[job_id]
            # get information about current job
            temp_L = pd.DataFrame()
            temp_L["n_iterate"] = [""]
            temp_L["sim_time"] = [""]
            temp_L["UniqueJobsID"] = [job_id]
            temp_L["SubmissionTime"] = [str_to_time(job_data["SubmissionTime"])]
            temp_L["Deadline"] = [str_to_time(job_data["Deadline"])]
            temp_L["TardinessWeight"] = [get_tw(int(job_data["Priority"]))]
            temp_L["MinExecTime"] = [""]
            temp_L["MaxExecTime"] = [""]
            temp_L["ExecutionTime"] = [""]
            temp_L["CompletionPercent"] = [""]
            temp_L["StartTime"] = [""]
            temp_L["FinishTime"] = [""]
            nodeID = currentScheduling[job_id]["node"]
            if nodeID != "nan":
                GPUtype = currentScheduling[job_id]["GPUtype"]
                nGPUs = int(currentScheduling[job_id]["nGPUs"])
                sel_time = float(job_data["ProfilingData"][GPUtype][str(nGPUs)])
                sel_time *= int(job_data["Epochs"])
                GPUcost = (sel_time * GPUcosts[GPUtype][str(nGPUs)]) / 3600
                temp_L["node_ID"] = [nodeID]
                temp_L["GPUtype"] = [GPUtype]
                temp_L["n_assigned_GPUs"] = [nGPUs]
                temp_L["SelectedTime"] = [sel_time]
                temp_L["GPUcost"] = [GPUcost]
            else:
                temp_L["node_ID"] = [""]
                temp_L["GPUtype"] = [""]
                temp_L["n_assigned_GPUs"] = [0]
                temp_L["SelectedTime"] = [""]
                temp_L["GPUcost"] = [""]
            temp_L["Tardiness"] = [""]
            temp_L["TardinessCost"] = [""]
            temp_L["TotalCost"] = [""]
            #
            JobSch = pd.concat([JobSch, temp_L], ignore_index=True)
    #
    return (JobSch)


def remove_running_jobs (Lof_Selectjobs, SelectJobs_times, tNodes, JobSch):
    # loop over currently running jobs
    for job in JobSch["UniqueJobsID"]:
        # drop jobs from list of submitted jobs
        toDrop = Lof_Selectjobs[Lof_Selectjobs["ID"] == job].index
        Lof_Selectjobs.drop(toDrop, inplace = True)
        # drop jobs from list of execution times
        toDrop = SelectJobs_times[SelectJobs_times["ID"] == job].index
        SelectJobs_times.drop(toDrop, inplace = True)


def update_nGPUs (Lof_Selectjobs, tNodes, JobSch):
    # loop over currently running jobs
    for idx in JobSch.index:
        row = JobSch.loc[idx]
        jobID = row["UniqueJobsID"]
        nodeID = row["node_ID"]
        nGPUs = row["n_assigned_GPUs"]
        # if the job should be reoptimized, release the relative resources
        if jobID in Lof_Selectjobs["ID"].values:
            tNodes.loc[nodeID, "nGPUs"] += nGPUs


def print_data (current_dir, Lof_Selectjobs, SelectJobs_times, tNodes, GPUcosts):
    Lof_Selectjobs_file = current_dir + "/Lof_Selectjobs.csv"
    Lof_Selectjobs.to_csv(Lof_Selectjobs_file, index=False)
    #
    SelectJobs_times_file = current_dir + "/SelectJobs_times.csv"
    SelectJobs_times.to_csv(SelectJobs_times_file, index=False)
    #
    tNodes_file = current_dir + "/tNodes.csv"
    tNodes.to_csv(tNodes_file, index_label="ID", index=True)
    #
    GPUcosts_file = current_dir + "/GPU-costs.csv"
    GPUcosts.to_csv(GPUcosts_file, index=False)


def collect_results (result_folder, method, random_methods, seed, simulation,
                     currentTime):
    # open result file
    schedule_file = result_folder + "/" + method + "_schedule"
    if method in random_methods:
        schedule_file += "_" + str(seed)
    schedule_file += ".csv"
    res_df_full = pd.read_csv(schedule_file)
    result = {"jobs": {}, "estimated_rescheduling_time": 0.0,
              "estimated_cost": 0.0}
    expected_cost = 0.0
    expected_fft = inf
    if simulation:
      n_iter = 1
    else:
      n_iter = 0
    result_df = res_df_full.loc[res_df_full["n_iterate"] == n_iter]
    for idx in result_df.index:
        # get job ID and information about its configuration
        jID = str(result_df.iloc[idx]["UniqueJobsID"])
        nodeID = str(result_df.iloc[idx]["node_ID"])
        nGPUs = int(result_df.iloc[idx]["n_assigned_GPUs"])
        # get expected tardiness (in hours)
        expected_tardiness = float(result_df.iloc[idx]["Tardiness"])
        expected_tardiness /= 3600
        # if job was in execution, write job information in result,
        # update total expected cost and expected first-finish time
        if not pd.isna(result_df.iloc[idx]["node_ID"]):
            result["jobs"][jID] = {"node": nodeID, "nGPUs": nGPUs, 
                                   "expected_tardiness": expected_tardiness}
            expected_cost += float(result_df.iloc[idx]["TotalCost"])
            selected_time = float(result_df.iloc[idx]["SelectedTime"])
            expected_fft = min(expected_fft, currentTime + selected_time)
    # write expected cost and first-finish time in result
    if expected_fft < inf:
        result["estimated_rescheduling_time"] = time_to_str(expected_fft)
    else:
        result["estimated_rescheduling_time"] = "inf"
    result["estimated_cost"] = expected_cost
    #
    return (result)


def restore_running_jobs (result, JobSch, current_time):
    # loop over previously running jobs and add to result the corresponding
    # characteristics
    expected_fft = result["estimated_rescheduling_time"]
    if expected_fft != "inf":
        expected_fft = str_to_time(expected_fft)
    else:
        expected_fft = inf
    expected_cost = 0.0
    for job in JobSch["UniqueJobsID"]:
        job_row = JobSch[JobSch["UniqueJobsID"] == job]
        nodeID = job_row["node_ID"].iloc[0]
        nGPUs = int(job_row["n_assigned_GPUs"])
        sel_time = float(job_row["SelectedTime"])
        tardi = max(current_time + sel_time - float(job_row["Deadline"]), 0)
        tardiCost = tardi * float(job_row["TardinessWeight"])
        tardi /= 3600
        result["jobs"][job] = {"node": nodeID, "nGPUs": nGPUs, 
                               "expected_tardiness": tardi}
        expected_fft = min(expected_fft, current_time + sel_time)
        expected_cost += (float(job_row["GPUcost"]) + tardiCost)
    #
    result["estimated_rescheduling_time"] = time_to_str(expected_fft)
    #
    result["estimated_cost"] += expected_cost



# app initialization
app = Flask(__name__)

# online path
exe_path = "/optimizer/v5"

# path of current script
current_script = os.path.abspath(os.path.dirname(__file__))
basedir = "/".join(current_script.split("/")[:-2])

# exit codes
NOT_FOUND = 404
POST_SUCCESS = 201

# execute
@app.route(exe_path, methods=['POST'])
def execute():
    # get all data
    data = request.get_json()
    
    # check existence of mandatory fields:
    KEY_ERROR = 0
    if "jobs" not in data.keys():
        KEY_ERROR = 10
    elif "nodes" not in data.keys():
        KEY_ERROR = 20
    elif "GPUcosts" not in data.keys():
        KEY_ERROR = 70
    elif "currentTime" not in data.keys():
        KEY_ERROR = 30

    # list of available first-principle and random methods
    random_methods = ["RG", "LS", "PR"]
    first_principle_methods = ["FIFO", "EDF", "PS"]
    
    # if all mandatory fields have been found
    if KEY_ERROR == 0:
        
        # get method name (if provided)
        method, simulation = get_method(data)
        
        # retrieve nodes data
        tNodes = get_nodes_data(data, (method in first_principle_methods))

        # retrieve GPU costs data:
        GPUcosts, KEY_ERROR = get_GPUcosts(data)
        
        # retrieve jobs and execution times data 
        if KEY_ERROR == 0:
            Lof_Selectjobs, SelectJobs_times, KEY_ERROR = get_jobs_data(data, 
                                                                        tNodes)

        # if no error occurred while loading data
        if KEY_ERROR == 0:
            # load current solution (if provided)
            if "currentScheduling" in data.keys():
                JobSch = get_current_solution(data)
            else:
                JobSch = pd.DataFrame()

            # if the current solution was provided...
            if len(JobSch.index) > 0:
                # adapt the list of submitted jobs and the list of available 
                # nodes for first principle methods
                if method in first_principle_methods:
                    remove_running_jobs(Lof_Selectjobs, SelectJobs_times, 
                                        tNodes, JobSch)
                # adapt the number of available GPUs to allow relocations for
                # all the other methods
                else:
                    update_nGPUs(Lof_Selectjobs, tNodes, JobSch)

            # get current time
            currentTime = str_to_time(data["currentTime"])
            
            # get verbosity level (if provided)
            if "verbose" in data.keys():
                verbose = data["verbose"]
            else:
                verbose = 0

            # get seed (if provided)
            if "seed" in data.keys():
                seed = data["seed"]
            else:
                seed = 4010
                
            # current directory for data and results
            current_dir = basedir + "/build/calls/call_" + str(currentTime)
            print("\nWriting results in ", current_dir)
            createFolder(current_dir)
            
            # print data to files
            print_data(current_dir, Lof_Selectjobs, SelectJobs_times, tNodes, 
                       GPUcosts)

            # create result folder
            result_folder = current_dir + "/results"
            createFolder(result_folder)

            # if the current scheduling was provided, write it to a file
            if len(JobSch.index) > 0:
                print("\nReading previous results from ", 
                      result_folder + "/old/previous_schedule.csv")
                createFolder(result_folder + "/old")
                JobSch.to_csv(result_folder + "/old/previous_schedule.csv",
                              index=False)

            # file for logs
            logs_folder = basedir + "/build/logs"
            createFolder(logs_folder)
            log_file = logs_folder + "/log_" + str(currentTime) + ".log"
            print("Logging on ", log_file)
            
            # run optimizer
            ARGS = "method=" + method + " folder=" + current_dir + \
                   " current_time=" + str(currentTime) + \
                   " verbose=" + str(verbose) + \
                   " simulation=" + str(simulation)
            if method in random_methods:
                ARGS = ARGS + " seed=" + str(seed)
            command = basedir + "/build/main " + ARGS + " > " + log_file + " 2>&1"
            exit_code = os.system(command)
            
            # if optimizer exits successfully, collect result
            if exit_code == 0:
                result = collect_results(result_folder, method, random_methods,
                                         seed, simulation, currentTime)

                # if the previous solution was provided, adapt the result with
                # the previously running jobs for first principle methods
                if len(JobSch.index) > 0 and method in first_principle_methods:
                    restore_running_jobs(result, JobSch, currentTime)
                
                # define output        
                output = (result, POST_SUCCESS)
            
            # if optimizer fails, return original data and error code
            else:
                KEY_ERROR = 60
    
    # if any mandatory field was missing, return original data and error code
    if KEY_ERROR > 0:
        output = (data, NOT_FOUND + KEY_ERROR)
           
    return jsonify(output[0]), output[1]


if __name__ == "__main__":
    # app.run(debug=True, port=8080)
    serve(app, host="0.0.0.0", port=8080)
