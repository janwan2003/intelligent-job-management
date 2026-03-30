#include "simulator.hpp"

Simulator::Simulator (const std::string& dir, const std::string& file_jobs, 
                      const std::string& file_times, 
                      const std::string& file_nodes,
                      const std::string& file_costs)
{
  #ifdef SMALL_SYSTEM
    verbose = 2;
  #endif

  directory = dir + "/";
  if (verbose > 0)
    std::cout << "--- loading jobs list." << std::endl;
  load_jobs_list(jobs, directory + file_jobs);
  jobs.sort(compare_submissionTime);
  if (verbose > 0)
    std::cout << "--- loading time table." << std::endl;
  ttime = load_time_table(directory + file_times);
  if (verbose > 0)
    std::cout << "--- loading nodes list." << std::endl;
  nodes.init(directory + file_nodes);
  if (verbose > 0)
    std::cout << "--- loading GPU costs." << std::endl;
  GPU_costs = std::make_shared<GPU_catalogue>(directory + file_costs);
}

double
Simulator::submit_jobs (double elapsed_time, std::list<Job>& submitted_jobs)
{
  std::string base = "";
  if (verbose > 0)
  {
    base = prepare_logging(level);
    std::cout << base << "--- job submission." << std::endl;
  }

  std::list<Job>::iterator it = jobs.begin();
  double check_t = current_time + elapsed_time;

  if (it != jobs.end())
  {
    check_t = std::min(check_t, jobs.cbegin()->get_submissionTime());

    std::list<std::list<Job>::iterator> to_erase;

    // loop over jobs
    while (it != jobs.end() && it->get_submissionTime() <= check_t + TOL)
    {
      if (verbose > 1)
      {
        double t = it->get_submissionTime();
        std::cout << base << "\tt = " << t << " <? " << check_t;
      }

      // submit job
      submitted_jobs.push_back(*it);
      to_erase.push_back(it);
      if (verbose > 1)
        std::cout << base << " ---> SUBMITTED" << std::endl;

      ++it;
    }

    // erase from jobs the list of already submitted jobs
    for (std::list<Job>::iterator it : to_erase)
      jobs.erase(it);
  }

  return check_t;
}

jn_pairs_t
Simulator::remove_ended_jobs (std::list<Job>& submitted_jobs) const
{
  std::string base = "";
  if (verbose > 0)
  {
    base = prepare_logging(level);
    std::cout << base << "--- remove ended jobs." << std::endl;
  }

  const job_schedule_t& last_schedule = old_solution.get_schedule();
  jn_pairs_t ended_jobs;

  job_schedule_t::const_iterator cit;
  for (cit = last_schedule.cbegin(); cit != last_schedule.cend(); ++cit)
  {
    const Job& j = cit->first;
    const Schedule& sch = cit->second;

    if (sch.get_completionPercent() >= (100-TOL))
    {
      std::list<Job>::iterator it;
      bool found = false;
      for (it=submitted_jobs.begin(); it!=submitted_jobs.end() && !found; ++it)
      {
        if (*it == j)
        {
          found = true;
          submitted_jobs.erase(it);

          const std::string& GPUtype = sch.get_GPUtype();
          const std::string& nodeID = sch.get_nodeID();
          unsigned g = sch.get_nGPUs();
          ended_jobs.push_back({j, Node(nodeID, GPUtype, g)});
        }
      }
    }
  }

  return ended_jobs;
}

void
Simulator::update_execution_times (double elapsed_time)
{
  std::string base = "";
  if (verbose > 0)
  {
    base = prepare_logging(level);
    std::cout << base << "--- update execution times." << std::endl;
  }

  const job_schedule_t& last_schedule = old_solution.get_schedule();

  job_schedule_t::const_iterator schit;
  for (schit = last_schedule.cbegin(); schit != last_schedule.cend(); ++schit)
  { 
    const Job& j = schit->first;
    const Schedule& sch = schit->second;
    double cp = sch.get_completionPercent();
    double cp_step = sch.get_cP_step();
    if (! sch.isEmpty() && cp < (100-TOL))
    {
      setup_time_t& tjvg = ttime->at(j.get_ID());
      setup_time_t::iterator it;
      for (it = tjvg.begin(); it != tjvg.end(); ++it)
      {
        double t = it->second * (100 - cp_step) / 100;
        it->second = (t > 0) ? t : INF;
      }
    }
  }
}

void
Simulator::update_minMax_exec_time (Job& j) const
{
  double min_exec_time = INF;
  double max_exec_time = 0.;
  const setup_time_t& tjvg = ttime->at(j.get_ID());
  setup_time_t::const_iterator cit;
  for (cit = tjvg.cbegin(); cit != tjvg.cend(); ++cit)
  {
    min_exec_time = std::min(min_exec_time, cit->second);
    max_exec_time = std::max(max_exec_time, cit->second);
  }
  j.set_minExecTime(min_exec_time);
  j.set_maxExecTime(max_exec_time);
}

void
Simulator::preprocessing (std::list<Job>& submitted_jobs) const
{
  std::string base = "";
  if (verbose > 0)
  {
    base = prepare_logging();
    std::cout << base << "--- preprocessing step." << std::endl;
  }

  // update minimum execution time, maximum execution time and pressure of 
  // all submitted jobs
  std::string queue = "";
  for (Job& j : submitted_jobs)
  {
    queue += (j.get_ID() + "; ");
    update_minMax_exec_time(j);
    j.update_pressure(current_time);
  }

  if (verbose > 1)
    std::cout << base << "\tlist of submitted jobs: " << queue << std::endl;
}

std::list<Job>
Simulator::get_waiting_jobs (const std::list<Job>& submitted_jobs) const
{
  std::list<Job> waiting_jobs;
  
  // get old schedule
  const job_schedule_t& old_schedule = old_solution.get_schedule();
  
  // loop over submitted jobs and check if they are currently running
  for (const Job& j : submitted_jobs)
  {
    job_schedule_t::const_iterator it = old_schedule.find(j);
    
    // if not, insert them in the list
    if (it == old_schedule.cend() || (it->second).isEmpty())
      waiting_jobs.push_back(j);
  }

  return waiting_jobs;
}

void
Simulator::add_previously_running_jobs (Solution& current_solution, 
                                    const std::list<Job>& submitted_jobs) const
{
  // get current and old schedule
  job_schedule_t& current_schedule = current_solution.get_schedule();
  const job_schedule_t& old_schedule = old_solution.get_schedule();
  
  // loop over submitted jobs and check if they were previously running
  unsigned n_previously_running = 0;
  for (const Job& j : submitted_jobs)
  {
    job_schedule_t::const_iterator it = old_schedule.find(j);
    
    // if so, insert them in the current schedule
    if (it != old_schedule.cend() && ! (it->second).isEmpty())
    {
      Schedule new_schedule = it->second;

      // find new execution time
      const std::string& GPUtype = new_schedule.get_GPUtype();
      unsigned g = new_schedule.get_nGPUs();
      const setup_time_t& tjvg = ttime->at(j.get_ID());
      setup_time_t::const_iterator sit = tjvg.find({GPUtype, g});
      double new_time = INF;
      if (sit != tjvg.cend())
        new_time = sit->second;

      // set updated execution time
      new_schedule.change_configuration(new_time, g);

      current_schedule.insert({j, new_schedule});
      n_previously_running++;
    }
  }

  // update first_finish_time according to the previously running jobs
  current_solution.compute_first_finish_time();
}

bool
Simulator::update_scheduled_jobs (unsigned iter, double elapsed_time,
                                  Solution& current_solution)
{
  std::string base = "";
  if (verbose > 0)
  {
    base = prepare_logging(level);
    std::cout << base << "--- update scheduled jobs." << std::endl;
  }

  bool all_completed = true;
  double GPUcost = 0.;
  double tardi = 0.;
  double tardiCost = 0.;

  // get last schedule
  job_schedule_t& last_schedule = current_solution.get_schedule();

  // compute simulation time
  double sim_time = current_time;

  // loop over last schedule
  for (job_schedule_t::iterator it = last_schedule.begin();
       it != last_schedule.end(); ++it)
  {
    const Job& j = it->first;
    Schedule& sch = it->second;
    
    if (verbose > 1)
      std::cout << base << "\tanalyzing job... " << j.get_ID() << std::endl;

    // set iteration number and simulation time
    sch.set_iter(iter);
    sch.set_simTime(sim_time);
    
    // if schedule is not empty, compute execution time, completion percent
    // and cost of GPUs
    double cp = 0.0;
    if (! sch.isEmpty())
    {
      sch.set_executionTime(elapsed_time);
      cp = elapsed_time * 100 / sch.get_selectedTime();
      sch.set_cP_step(cp);

      // get number of used GPUs on the current node
      const std::string& node_ID = sch.get_nodeID();
      const std::string& node_GPUtype = sch.get_GPUtype();
      unsigned g = current_solution.get_usedGPUs(node_GPUtype, node_ID);

      // compute cost of GPUs
      sch.compute_GPUcost(g, GPU_costs->get_cost(node_GPUtype,g));
      GPUcost += sch.get_GPUcost();

      if (verbose > 1)
        std::cout << base << "\t\texecution_time = " << elapsed_time
                  << "; node = " << node_ID << "; GPUtype = "
                  << node_GPUtype << "; used_GPUs = " << g
                  << "; GPUcost = " << sch.get_GPUcost()
                  << "; partial cp = " << std::setprecision(10) << cp 
                  << std::setprecision(6) << std::endl;
    }

    // in all iterations after the first one, update start time and completion
    // percent of jobs that have already been partially executed
    if (iter > 1)
    {
      const job_schedule_t& previous_schedule = old_solution.get_schedule();
      job_schedule_t::const_iterator p_sch = previous_schedule.find(j);
      if (p_sch != previous_schedule.cend())
      {
        sch.set_startTime((p_sch->second).get_startTime());
        double prev_cp = (p_sch->second).get_completionPercent();
        cp = prev_cp + cp * (100 - prev_cp) / 100;

        if (verbose > 1)
          std::cout << base << "\t\tpreviously running --> previous cp = "
                    << std::setprecision(10) << prev_cp << "; cp = " << cp
                    << std::setprecision(6) << std::endl;
      }
      else
        sch.set_startTime(current_time - elapsed_time);
    }
    else
      sch.set_startTime(current_time - elapsed_time);

    // set completion percent
    sch.set_completionPercent(cp);

    // set finish time, tardiness and tardiness cost of jobs that are 
    // completed
    if (cp >= (100-TOL))
    {
      sch.set_finishTime(sim_time);
      double tardiness = std::max(sim_time - j.get_deadline(), 0.);
      sch.set_tardiness(tardiness);
      sch.compute_tardinessCost(j.get_tardinessWeight());
      tardi += sch.get_tardiness();
      tardiCost += sch.get_tardinessCost();

      if (verbose > 1)
        std::cout << base << "\t\ttardiness = " << tardiness
                  << "; t_cost = " << sch.get_tardinessCost() << std::endl;
    }
    else
    {
      sch.set_tardiness(0.0);
      all_completed = false;
    }
  }

  // compute nodes cost
  double nodeCost = current_solution.compute_nodeCost(elapsed_time);

  if (verbose > 0)
    std::cout << base << "\tGPUcost: " << GPUcost << "; nodeCost: "
              << nodeCost << "; tardiCost: " << tardiCost 
              << "; tardiness: " << tardi << std::endl;

  // update total costs
  total_costs.total_tardi += tardi;
  total_costs.total_tardiCost += tardiCost;
  total_costs.total_nodeCost += nodeCost;
  total_costs.total_GPUcost += GPUcost;
  total_costs.total_energyCost += (nodeCost + GPUcost);
  total_costs.total_cost += (tardiCost + nodeCost + GPUcost);

  return all_completed;
}

bool
Simulator::initialized (void) const
{
  return ! (jobs.empty() || nodes.isEmpty() || ! ttime || ! GPU_costs);
}

costs_struct
Simulator::algorithm (const std::string& method, double ct, unsigned v,
                      unsigned seed, double sched_int)
{
  #ifdef SMALL_SYSTEM
    // change verbosity level
    verbose = v;
  #endif

  // set scheduling interval
  scheduling_interval = sched_int;

  // intialization
  unsigned iter = 0;
  double first_finish_time = INF;
  double elapsed_time = 0.;
  bool all_completed = false;
  bool stop = false;
  std::list<Job> submitted_jobs;
  current_time = ct;
  Solution current_solution;

  while (!stop)
  {
    #ifdef SMALL_SYSTEM
      std::cout << "\n######################## ITER " << iter 
                << " ########################\n" << std::endl;
    #endif

    // update elapsed time and current time if necessary
    if (iter > 0)
      elapsed_time = std::min(scheduling_interval, first_finish_time);
    
    // add new jobs to the queue of submitted jobs and update current time
    // and elapsed time accordingly
    unsigned old_J = submitted_jobs.size();
    double last_scheduling_time = current_time;
    current_time = submit_jobs(elapsed_time, submitted_jobs);
    elapsed_time = current_time - last_scheduling_time;

    if (verbose > 1)
      std::cout << "last_scheduling_time = " << last_scheduling_time
                << "; first_finish_time = " << first_finish_time
                << "; current_time = " << current_time
                << "; elapsed_time = " << elapsed_time << std::endl;

    // for all iterations but the first one...
    if (iter > 0)
    {
      // ...update information about scheduled jobs in the old solution
      all_completed = update_scheduled_jobs(iter, elapsed_time, 
                                            current_solution);
      // update old solution
      std::swap(current_solution, old_solution);

      // if there is a gap between the completion of the last job and the
      // submission of the new one, force the submission of a new job
      if (all_completed && (submitted_jobs.size() == old_J))
      {
        current_time = submit_jobs(INF, submitted_jobs);
        elapsed_time = current_time - last_scheduling_time;
        if (verbose > 1)
          std::cout << "\tnew elapsed_time = " << elapsed_time 
                    << "; new current_time = " << current_time << std::endl;
      }
    }

    // check stopping cryterion
    stop = (submitted_jobs.size() == old_J) && all_completed;

    if (! stop)
    {
      // create new system
      System system;

      // check if the required method is one of first-principle methods
      bool is_FPM = (method == "FIFO" || method == "EDF" || method == "PS");

      // for all iterations but the first one...
      if (iter > 0)
      {
        // ...remove ended jobs from the queue
        jn_pairs_t ended_jobs = remove_ended_jobs(submitted_jobs);
        // ...update execution time of jobs that have been partially executed
        update_execution_times(elapsed_time);
        // ...perform preprocessing of submitted jobs
        preprocessing(submitted_jobs);

        // for first-principle methods...
        if (is_FPM)
        {
          // ...create list of not-yet-started jobs
          std::list<Job> not_started_jobs = get_waiting_jobs(submitted_jobs);

          // ...get set and number of available nodes
          Nodes_map available_nodes(old_solution.get_nodes());
          available_nodes.free_resources(ended_jobs, verbose);
          
          // ...initialize system with waiting jobs and available nodes
          system.init(not_started_jobs, available_nodes, ttime, GPU_costs);
        }
      }

      // initialize system for the other methods and/or first iteration
      if (system.isEmpty())
        system.init(submitted_jobs, nodes, ttime, GPU_costs);

      // initialize optimizer
      Optimizer OPT(system);

      // determine new solution
      current_solution = OPT.algorithm(method, current_time, verbose, seed);

      // for first-principle methods, add to the schedule the previously
      // running jobs
      if (is_FPM)
      {
        if (iter > 0)
          add_previously_running_jobs(current_solution, submitted_jobs);
      }
      else
      {
        // for all the other methods, perform analysis to reduce migrations
        Analyzer analyzer(system, verbose);
        analyzer.perform_analysis (old_solution, current_solution);
      }

      first_finish_time = current_solution.get_first_finish_time();
      ++iter;
    }

    // in the case of small systems, print resulting schedule
    #ifdef SMALL_SYSTEM
      std::string filename = method + "_schedule";
      if (method == "RG" || method == "LS" || method == "PR")
        filename += ("_" + std::to_string(seed));
      filename += ".csv";
      if (iter == 1)
      {
        std::ofstream ofs(directory + "results/" + filename);
        Schedule::print_names(ofs);
      }
      print_schedule(filename, true);
    #endif
  }

  return total_costs;
}

void
Simulator::print_schedule (const std::string& filename, bool append) const
{
  std::string complete_filename = directory + "results/" + filename;
  old_solution.print(complete_filename, append);
}
