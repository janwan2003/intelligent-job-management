import sys
import os
import ast
import configparser as cp
import argparse
import csv
import logging

import collections

import pandas as pd


def recursivedict():
    return collections.defaultdict(recursivedict)


def main ():

    parser = argparse.ArgumentParser(description="Compute averages of random methods")

    parser.add_argument("logs_directory", help="The directory containing the folders with the logs to be processed")
    parser.add_argument('-c', "--config", help="Configuration file", 
                        default="config.ini")
    parser.add_argument('-d', "--debug", help="Enable debug messages", 
                        default=False, action="store_true")
    parser.add_argument('-o', "--output", 
                        help="String to be added to the name of the results files", 
                        default="")

    args = parser.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG, format='%(levelname)s: %(message)s')
    else:
        logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    if not os.path.exists(args.logs_directory):
        logging.error("%s does not exist", args.logs_directory)
        sys.exit(1)

    ##########################################################################

    ## configuration parameters
    config = cp.ConfigParser()
    config.read(args.config)

    # methods
    existing_greedy = ast.literal_eval(config['Methods']['existing_greedy'])
    existing_random = ast.literal_eval(config['Methods']['existing_random'])

    ##########################################################################

    gains = recursivedict()
    average_gains = recursivedict()

    found_methods = set()

    for content in os.listdir(args.logs_directory):
        combined_path = os.path.join(args.logs_directory, content)
        if os.path.isdir(combined_path):
            tokens = content.split("-")
            method = tokens[0]
            if method not in existing_random:
                continue
            number_initial_jobs = tokens[1]
            if number_initial_jobs != "1":
                continue
            nodes = tokens[2]
            jobs = tokens[3]
            lambdaa = tokens[4]
            mu = tokens[5]
            seed = tokens[6]
            delta = tokens[7]
            cppseed = tokens[8]
            nrandomiter = tokens[9]

            logging.debug("Examining %s", str(method + "-" + cppseed))

            for file in os.listdir(combined_path):
                if os.path.isdir(file):
                    continue
                if not file.startswith("CppTests_execution"):
                    continue

                log_data = csv.reader(open(os.path.join(combined_path, file),"r"))
                
                experiment = (method, nodes, jobs, lambdaa, mu, seed)
                full_exp = (method, number_initial_jobs, nodes, jobs, lambdaa, 
                            mu, seed, delta, cppseed, nrandomiter)
                found_methods.add(method)

                row = next(log_data)
                while row[0] != "iter":
                    row = next(log_data)

                count = 0
                average_gain = 0.0
                for row in log_data:
                    if len(row) == 1:
                        break
                    n_iter = int(row[0])
                    baseline = float(row[1])
                    update = float(row[3])
                    gain = (baseline - update) / baseline
                    gains[full_exp][n_iter] = gain
                    average_gain += gain
                    count += 1

                average_gain /= count
                average_gains[experiment][cppseed] = average_gain
    
    logging.debug("Found methods %s", str(found_methods))

    if not found_methods:
        logging.error("No log found")
        sys.exit(1)

    results_file_name = "total_gain" + args.output + ".csv"
    results_file = open(results_file_name, "w")
    results_file.write("Method, Nodes, Jobs, Lambda, Mu, Seed, CppSeed, Gain")
    results_file.write("\n")

    averages_file_name = "total_gain_averaged" + args.output + ".csv"
    averages_file = open(averages_file_name, "w")
    averages_file.write("Method, Nodes, Jobs, Lambda, Mu, Seed, AverageGain")
    averages_file.write("\n")

    for experiment in sorted(average_gains):
        logging.debug("Examining %s", str(experiment))

        results = average_gains[experiment]

        results_file.write(",".join(experiment))
        averages_file.write(",".join(experiment))

        average = 0.0
        ncppseed = 0

        for cppseed in results:
            if ncppseed == 0:
                results_file.write("," + str(cppseed) +\
                                  ", {:.2f}%".format(results[cppseed] * 100) +\
                                  "\n")
            else:
                results_file.write(",,,,,," + str(cppseed) +\
                                  ", {:.2f}%".format(results[cppseed] * 100) +\
                                  "\n")
            average += results[cppseed]
            ncppseed += 1

        average /= ncppseed
        averages_file.write(", {:.2f}%".format(average * 100) + "\n")

    results_file.close()
    averages_file.close()

    if args.debug:
        full_results_file_name = "gain.csv"

        for full_exp in sorted(gains):
            folder = args.logs_directory + "/" + "-".join(full_exp) + "/"
            full_results_file = open(folder+full_results_file_name, "w")
            full_results_file.write("iter, Gain\n")
            ggg = gains[full_exp]
            for n_iter in sorted(ggg):
                full_results_file.write(str(n_iter) +\
                                        ", {:.2f}%".format(ggg[n_iter]*100) +\
                                        "\n")
            full_results_file.close()


if __name__ == "__main__":
    main()
