#include "optimizer.hpp"

Optimizer::Optimizer (const System& s):
  system(s)
{}

void
Optimizer::perform_scheduling (Solution& best_solution, unsigned seed) const
{
  if (verbose > 0)
  {
    std::string base = prepare_logging(level);
    std::cout << base << "--- optimizer - perform scheduling." << std::endl;
  }

  if (method == "G" || method == "RG" || method == "LS")
  {
    // define set of elite solutions
    Elite_solutions<std::less<double>> ES;

    // greedy
    Greedy<std::less<double>> G(system, proxy_function_min, level+1);
    G.perform_scheduling(current_time, ES, 1, verbose);

    if (method == "RG" || method == "LS")
    {
      // randomized greedy
      Random_greedy<std::less<double>> RG(system, proxy_function_min, 
                                          level+1, seed);
      RG.perform_scheduling(current_time, ES, 10, verbose);

      if (method == "LS")
      {
        // local search
        Local_search LS(system, proxy_function_min);
        LS.local_search(ES, 1, verbose);
      }
    }

    // define best solution
    best_solution = ES.get_solutions().cbegin()->second;
  }
  else if (method == "PR")
  {
    // define set of elite solutions
    Elite_solutions<std::greater<double>> ES;

    // greedy
    Greedy<std::greater<double>> G(system, proxy_function_max, level+1);
    G.perform_scheduling(current_time, ES, 1, verbose);

    // randomized greedy
    Random_greedy<std::greater<double>> RG(system, proxy_function_max,
                                           level+1, seed);
    RG.perform_scheduling(current_time, ES, 10, verbose);
/*
    // path relinking
    Path_relinking PR(proxy_function_max);
    PR.path_relinking(K_best);
*/
    // define best solution
    best_solution = ES.get_solutions().cbegin()->second;
  }
  else if (method == "FIFO")
  {
    // define set of elite solutions
    Elite_solutions<std::less<double>> ES;

    // FIFO
    FIFO<std::less<double>> F(system, proxy_function_min, level+1);
    F.perform_scheduling(current_time, ES, 1, verbose);

    // define best solution
    best_solution = ES.get_solutions().cbegin()->second;
  }
  else if (method == "EDF")
  {
    // define set of elite solutions
    Elite_solutions<std::less<double>> ES;

    // EDF
    EDF<std::less<double>> E(system, proxy_function_min, level+1);
    E.perform_scheduling(current_time, ES, 1, verbose);

    // define best solution
    best_solution = ES.get_solutions().cbegin()->second;
  }
  else if (method == "PS")
  {
    // define set of elite solutions
    Elite_solutions<std::less<double>> ES;

    // Priority
    Priority<std::less<double>> P(system, proxy_function_min, level+1);
    P.perform_scheduling(current_time, ES, 1, verbose);

    // define best solution
    best_solution = ES.get_solutions().cbegin()->second;
  }
  else
  {
    std::cerr << "ERROR: method " << method <<  " is not registered" 
              << std::endl;
  }
}

void
Optimizer::init (const System& s)
{
  system = s;
}

Solution
Optimizer::algorithm (const std::string& m, double ct, unsigned v, unsigned s)
{
  #ifdef SMALL_SYSTEM
    // change verbosity level
    verbose = v;
  #endif

  // set method
  method = m;

  // set current time
  current_time = ct;

  // compute best solution
  Solution best_solution;
  perform_scheduling(best_solution, s);

  return best_solution;
}

double
proxy_function_min (Solution& sol, const gpu_catalogue_ptr& costs, 
                    unsigned verbose, unsigned level)
{
  std::string base = "";
  if (verbose > 0)
  {
    base = prepare_logging(level);
    std::cout << base << "--- proxy function (min)." << std::endl;
  }

  double GPUcost = 0.;
  double tardiCost = 0.;
  double worstTardiCost = 0.;

  double current_time = sol.get_current_time();
  double elapsed_time = sol.get_first_finish_time();
  double sim_time = current_time + elapsed_time;
  
  // get last schedule
  job_schedule_t& new_schedule = sol.get_schedule();

  // loop over last schedule
  for (job_schedule_t::iterator it = new_schedule.begin();
       it != new_schedule.end(); ++it)
  {
    const Job& j = it->first;
    Schedule& sch = it->second;

    if (verbose > 1)
      std::cout << base << "\tanalyzing job... " << j.get_ID() << std::endl;

    // if schedule is not empty, compute cost of GPU and tardiness cost
    if (! sch.isEmpty())
    {
      // set execution time
      sch.set_executionTime(elapsed_time);

      // get number of used GPUs on the current node
      const std::string& node_ID = sch.get_nodeID();
      const std::string& node_GPUtype = sch.get_GPUtype();
      unsigned g = sol.get_usedGPUs(node_GPUtype, node_ID);

      // compute cost of GPUs
      sch.compute_GPUcost(g, costs->get_cost(node_GPUtype, g));
      GPUcost += sch.get_GPUcost();

      // compute tardiness
      double tardiness = std::max(sim_time - j.get_deadline(), 0.);
      sch.set_tardiness(tardiness);
      sch.compute_tardinessCost(j.get_tardinessWeight());
      tardiCost += sch.get_tardinessCost();

      if (verbose > 1)
      {
        std::cout << base << "\t\texecution_time = " << elapsed_time
                  << "; node = " << node_ID << "; GPUtype = "
                  << node_GPUtype << "; used_GPUs = " << g
                  << "; GPUcost = " << sch.get_GPUcost()
                  << "; tardiness = " << tardiness
                  << "; t_cost = " << sch.get_tardinessCost() << std::endl;
      }
    }
    else 
    {
      double wCT = std::max(current_time + j.get_maxExecTime() - 
                            j.get_deadline(), 0.0);
      double worstCaseTardinessCost = 100 * wCT * j.get_tardinessWeight();
      worstTardiCost += worstCaseTardinessCost;

      if (verbose > 1)
        std::cout << base << "\t\tworstCaseTardiness = " << wCT
                  << "; wCT_cost = " << worstCaseTardinessCost << std::endl;
    }
  }

  // compute nodes cost
  double nodeCost = sol.compute_nodeCost(elapsed_time);

  if (verbose > 0)
    std::cout << base << "\tGPUcost: " << GPUcost << "; nodeCost: "
              << nodeCost << "; tardiCost: " << tardiCost 
              << "; worstCaseTardiness: " << worstTardiCost << std::endl;

  return (GPUcost + nodeCost + tardiCost + worstTardiCost);
}

double
proxy_function_max (Solution& sol, const gpu_catalogue_ptr& costs, 
                    unsigned verbose, unsigned level)
{
  std::string base = "";
  if (verbose > 0)
  {
    base = prepare_logging(level);
    std::cout << base << "--- proxy function (max)." << std::endl;
  }

  double GPUcost = 0.;
  double tardiCost = 0.;
  double time_gain = 0.;

  double current_time = sol.get_current_time();
  double elapsed_time = sol.get_first_finish_time();
  
  // get last schedule
  job_schedule_t& new_schedule = sol.get_schedule();

  // loop over last schedule
  for (job_schedule_t::iterator it = new_schedule.begin();
       it != new_schedule.end(); ++it)
  {
    const Job& j = it->first;
    Schedule& sch = it->second;

    if (verbose > 1)
      std::cout << base << "\tanalyzing job... " << j.get_ID() << std::endl;

    // if schedule is not empty, compute cost of GPU and tardiness cost
    if (! sch.isEmpty())
    {
      // set as execution time the total required time
      double finish_time = sch.get_selectedTime();
      sch.set_executionTime(finish_time);

      // get number of used GPUs on the current node
      const std::string& node_ID = sch.get_nodeID();
      const std::string& node_GPUtype = sch.get_GPUtype();
      unsigned g = sol.get_usedGPUs(node_GPUtype, node_ID);

      // compute cost of GPUs
      sch.compute_GPUcost(g, costs->get_cost(node_GPUtype, g));
      GPUcost += time_gain / sch.get_GPUcost();

      // compute tardiness
      double tardiness = std::max(current_time+finish_time-j.get_deadline(), 
                                  0.);
      sch.set_tardiness(tardiness);
      sch.compute_tardinessCost(j.get_tardinessWeight());
      tardiCost += sch.get_tardinessCost();

      // update time gain
      time_gain += j.get_maxExecTime() / (sch.get_GPUcost() + sch.get_tardinessCost());

      if (verbose > 1)
      {
        std::cout << base << "\t\texecution_time = " << elapsed_time
                  << "; node_ID = " << node_ID << "; GPUtype = "
                  << node_GPUtype << "; used_GPUs = " << g
                  << "; time_gain = " << time_gain 
                  << "; GPUcost = " << sch.get_GPUcost()
                  << "; tardiness = " << tardiness
                  << "; t_cost = " << sch.get_tardinessCost() << std::endl;
      }
    }
  }

  if (verbose > 0)
    std::cout << base << "\ttime_gain: " << time_gain << "; GPUcost: " 
              << GPUcost << "; tardiCost: " << tardiCost << std::endl;

  return time_gain;
}
