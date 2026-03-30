#include <iostream>
#include <cstring>

#include "simulator.hpp"
#include "GetPot.hpp"


void print_help (void);
bool is_registered (const std::string& method);
bool is_random (const std::string& method);
bool is_FPM (const std::string& method);


int main (int argc, char *argv[])
{
  int exit_code = 0;

  // initialze Getpot parser
  GetPot GP(argc, argv);
    
  if (GP.search(1,"-h"))
    print_help();
  else
  {
    std::string method = GP("method", "");
    std::string directory = GP("folder", "");
    unsigned cpp_seed = GP("seed", 4010);
    unsigned verbose = GP("verbose", 0);
    double current_time = GP("current_time", 0.0);
    std::string simulation = GP("simulation", "False");
    double scheduling_interval = GP("scheduling_interval", INF);

    if (method == "" || directory == "")
    {
      std::cerr << "\nERROR: missing or corrupted parameters" << std::endl;
      print_help();
      exit_code = 1;
    }
    else if (! is_registered(method))
    {
      std::cerr << "ERROR: method " << method << " is not registered"
                << std::endl;
      print_help();
      exit_code = 2;
    }
    else
    {
      // name of files containing data
      std::string jobs_list_filename = "Lof_Selectjobs.csv";
      std::string times_filename = "SelectJobs_times.csv";
      std::string nodes_filename = "tNodes.csv";
      std::string costs_filename = "GPU-costs.csv";

      if (simulation == "True" || simulation == "true")
      {
        // initialize simulator
        Simulator S(directory, jobs_list_filename, times_filename, 
                    nodes_filename, costs_filename);

        if (S.initialized())
        {
          // determine solution
          costs_struct all_costs = S.algorithm(method, current_time, verbose, 
                                               cpp_seed, scheduling_interval);

          // print total tardiness and total simulation cost
          std::string global_result_file = "all_results.csv";
          std::ofstream ofs(directory + "/results/" + global_result_file, 
                            std::ios::app);
          ofs << method << "," << cpp_seed << "," << all_costs.total_tardi 
              << "," << all_costs.total_tardiCost << "," 
              << all_costs.total_nodeCost << "," << all_costs.total_GPUcost 
              << "," << all_costs.total_energyCost << "," 
              << all_costs.total_cost << std::endl;
        }
        else
          exit_code = 3;
      }
      else
      {
        // initialize system
        System sys(directory, jobs_list_filename, times_filename, 
                      nodes_filename, costs_filename, verbose);

        if (! sys.isEmpty())
        {
          // initialize optimizer
          Optimizer OPT(sys);

          // determine solution
          Solution sol = OPT.algorithm(method,current_time,verbose,cpp_seed);

          // perform analysis
          std::string old_solution_file = "previous_schedule.csv";
          Analyzer analyzer(directory, sys, verbose);
          analyzer.perform_analysis (old_solution_file, sol, is_FPM(method));

          // print resulting schedule
          std::string result_filename = method + "_schedule";
          if (is_random(method))
            result_filename += ("_" + std::to_string(cpp_seed));
          result_filename += ".csv";
          sol.print(directory + "/results/" + result_filename);
        }
        else
          exit_code = 3;
      }  
    }
  }

  return exit_code;
}


void print_help (void)
{
  std::cout << "\nusage: ./main [arguments]\n";
  std::cout << "\narguments must be given in format name=value\n";
  std::cout << "\nrequired arguments (for all methods):\n";
  std::cout << "\tmethod:               name of the method you want to use\n";
  std::cout << "\tfolder:               complete path of data folder\n";
  std::cout << "\tcurrent_time:         current time\n";
  std::cout << "\nadditional arguments (for random methods):\n";
  std::cout << "\tseed:                 seed for randomization\n";
  std::cout << "\titer:                 number of random iterations\n";
  std::cout << "\noptional arguments:\n";
  std::cout << "\tverbose:              to increase the verbosity level\n";
  std::cout << "\tsimulation:           true to perform a full simulation\n";
  std::cout << "\tscheduling_interval:  scheduling interval for the simulation"
            << std::endl;

  std::cout << "\nlist of registered methods:\n";
  std::cout << "FIFO:             First In First Out\n";
  std::cout << "EDF:              Earliest Deadline First\n";
  std::cout << "PS:               Priority Scheduling\n";
  std::cout << "G:                Greedy\n";
  std::cout << "RG:               Random Greedy (G + RG)\n";
  std::cout << "LS:               Local Search (G + RG + LS)\n";
  std::cout << "PR:               Path Relinking (G + RG + PR)\n"
            << std::endl;
}

bool is_registered (const std::string& method)
{
  return (method == "FIFO" || method == "EDF" || method == "PS" || 
          method == "G" || method == "RG" || method == "LS" || method == "PR");
}

bool is_random (const std::string& method)
{
  return (method == "RG" || method == "LS" || method == "PR");
}

bool is_FPM (const std::string& method)
{
  return (method == "FIFO" || method == "EDF" || method == "PS");
}
