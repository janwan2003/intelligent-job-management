#!/usr/bin/env python3

import sys
import plotly.figure_factory as ff
import plotly as py
import pandas as pd
from datetime import datetime
import matplotlib

def gantt_chart (filename, initial_time):
    
    snapshot = pd.read_csv(filename)
    snapshot["Resource"] = snapshot["node_ID"] + "_" + snapshot["GPUtype"]
    
    info = []
    for it_group in snapshot.groupby("n_iterate"):
        for idx in it_group[1].index:
            row = it_group[1].loc[idx]
            if not pd.isna(row["Resource"]):
                jobID = row["UniqueJobsID"]
                start_time = initial_time
                if it_group[0] > 1:
                    start_time += float(snapshot[snapshot["n_iterate"]==it_group[0]-1].iloc[0]["sim_time"])
                start_time = datetime.fromtimestamp(start_time)
                if row["CompletionPercent"] == 100:
                    finish_time = float(row["FinishTime"])
                else:
                    finish_time = float(row["sim_time"])
                finish_time = datetime.fromtimestamp(initial_time + finish_time)
                config = str(row["Resource"])
                line = [jobID, start_time, finish_time, config]
                info.append(line)

    df = pd.DataFrame(info, columns=["Task", "Start", "Finish", "Resource"])

    existing_configurations = snapshot["Resource"].dropna().sort_values().unique()
    existing_color_names = list(matplotlib.colors.cnames.keys())

    rgb_colors = {}
    idx = 32
    for config in existing_configurations:
        color_name = existing_color_names[idx]
        rgb_colors[config] = matplotlib.colors.to_rgb(color_name)
        idx += 1

    fig = ff.create_gantt(df, colors=rgb_colors, index_col="Resource", 
                          bar_width=0.3, show_colorbar=True, group_tasks=True, 
                          showgrid_x=True, showgrid_y=True)
    fig.layout.xaxis["tickfont"] = {"size": 18}
    fig.layout.yaxis["tickfont"] = {"size": 18}
    fig.layout.legend["font"] = {"size": 18}

    title = filename.split("/")[-1].split(".")[0]
    py.offline.plot(fig, filename = title + ".html")


if __name__ == "__main__":
    if sys.argv[1] == "-h" or sys.argv[1] == "--help":
        print("Required parameters are:")
        print("\tfilename:      name of the schedule file")
        print("\tinitial_time:  initial time (in seconds) of the simulation")
    else:
        filename = sys.argv[1]
        if len(sys.argv) == 3:
            initial_time = float(sys.argv[2])
        else:
            initial_time = 0.0
        gantt_chart(filename, initial_time)
