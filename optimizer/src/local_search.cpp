#include "local_search.hpp"

Local_search::Local_search (const System& s, const obj_function_t& pf): 
  system(s), proxy_function(pf)
{}

setup_time_t::const_iterator
Local_search::select_setup (const Job& j, const std::string& GPUtype,
                            unsigned g) const
{
  const setup_time_t& tjvg = system.get_ttime()->at(j.get_ID());
  return tjvg.find({GPUtype, g});
}

void
Local_search::get_sorted_jobs (const Solution& BS, 
                               sorted_jobs_t& tardi_jobs,
                               sorted_jobs_t& expensive_jobs,
                               node_jobs_t& nodes_map) const
{
  using temp_map_t = std::multimap<double,job_schedule_t::const_iterator,
                                   std::greater<double>>;
  temp_map_t tardi_map;
  temp_map_t cost_map;

  const job_schedule_t& schedule = BS.get_schedule();
  for (job_schedule_t::const_iterator cit = schedule.cbegin();
       cit != schedule.cend(); ++cit)
  {
    if (! (cit->second).isEmpty())
    {
      double tardi = (cit->second).get_tardiness();
      double vm_cost = (cit->second).get_GPUcost();
      if (tardi > 0.0)
        tardi_map.insert({tardi,cit});
      else
        cost_map.insert({vm_cost,cit});
      nodes_map[(cit->second).get_nodeID()].push_back(cit);
    }
  }

  unsigned n = (k1 < tardi_map.size()) ? k1 : tardi_map.size();
  temp_map_t::const_iterator cit = tardi_map.cbegin();
  for (unsigned j = 0; j < n; ++j)
  {
    tardi_jobs.push_back(cit->second);
    ++cit;
  }

  n = (k1 < cost_map.size()) ? k1 : cost_map.size();
  cit = cost_map.cbegin();
  for (unsigned j = 0; j < n; ++j)
  {
    expensive_jobs.push_back(cit->second);
    ++cit;
  }
}

Local_search::best_change_t
Local_search::first_neighborhood (const sorted_jobs_t& jobs1,
                                  const sorted_jobs_t& jobs2, 
                                  const Solution& original_solution,
                                  double total_cost)
{
  std::string base = "";
  if (verbose > 1)
  {
    base = prepare_logging(level);
    std::cout << base << "\t--- first neighborhood." << std::endl;
  }

  // proposed modification
  double worst_val = comparator(INF,-INF) ? -INF : INF;
  best_change_t best_change({worst_val, job_schedule_t()});
  bool changed = false;

  // loop over jobs in order of tardiness
  for(sorted_jobs_t::const_iterator cit1 = jobs1.cbegin(); 
      cit1 != jobs1.cend(); ++cit1)
  {
    const Job& j1 = (*cit1)->first;
    const Schedule& sch1 = (*cit1)->second;

    // loop over jobs in order of vm cost
    for(sorted_jobs_t::const_iterator cit2 = jobs2.cbegin();
        cit2 != jobs2.cend(); ++cit2)
    {
      const Job& j2 = (*cit2)->first;
      const Schedule& sch2 = (*cit2)->second;

      // jobs are different by construction; if they are running on different
      // nodes, swap configurations
      bool different_node = (sch1.get_nodeID() != sch2.get_nodeID());
      bool different_GPUtype = (sch1.get_GPUtype() != sch2.get_GPUtype());
      bool different_nGPUs = (sch1.get_nGPUs() != sch2.get_nGPUs());
      
      if (different_node && (different_GPUtype || different_nGPUs))
      {
        changed = (changed || swap_running_jobs(j1, j2, sch1, sch2, 
                                                original_solution, 
                                                best_change));
      }
    }
  }

  if (changed && verbose > 3)
  {
    std::cout << base << "\t\t--> best choice of first neigh:\n";
    Schedule::print_names(std::cout);
    for (job_schedule_t::const_iterator fit = (best_change.second).cbegin();
         fit != (best_change.second).cend(); ++fit)
    {
      if (! (fit->second).isEmpty())
        (fit->second).print(fit->first, std::cout);
    }
  }

  return best_change;
}

Local_search::best_change_t
Local_search::second_neighborhood (const sorted_jobs_t& jobs1,
                                   const Solution& original_solution,
                                   double total_cost)
{
  if (verbose > 1)
  {
    std::string base = prepare_logging(level);
    std::cout << base << "\t--- second neighborhood." << std::endl;
  }

  std::string internal_base = prepare_logging(level+1);

  // get original schedule
  const job_schedule_t& original_schedule = original_solution.get_schedule();

  // proposed modification
  double worst_val = comparator(INF,-INF) ? -INF : INF;
  best_change_t best_change({worst_val, job_schedule_t()});
  bool changed = false;

  // loop over jobs in order of tardiness
  for(sorted_jobs_t::const_iterator cit1 = jobs1.cbegin(); 
      cit1 != jobs1.cend(); ++cit1)
  {
    const Job& j1 = (*cit1)->first;
    const Schedule& sch1 = (*cit1)->second;
    const std::string& n1_ID = sch1.get_nodeID();

    if (verbose > 3)
      std::cout << internal_base << "\texamining job... " << j1.get_ID() 
                << ": (node_ID: " << n1_ID << ")" << std::endl;

    // loop over jobs in reverse order of pressure
    const std::list<Job>& submitted_jobs = system.get_submittedJobs();
    for(std::list<Job>::const_reverse_iterator cit2 = submitted_jobs.rbegin();
       cit2 != submitted_jobs.rend(); ++cit2)
    {
      const Job& j2 = *cit2;
      
      // if the two jobs are different and job j2 is running on a different
      // node...
      if (j1 != j2)
      {
        job_schedule_t::const_iterator sch2_it = original_schedule.find(j2);
        if(sch2_it != original_schedule.cend() && !(sch2_it->second).isEmpty())
        {
          const Schedule& sch2 = sch2_it->second;
          const std::string& n2_ID = sch2.get_nodeID();

          if (verbose > 3)
            std::cout << internal_base << "\t\texamining job... " <<j2.get_ID() 
                      << ": (node_ID: " << n2_ID << ")" << std::endl;

          // ...swap configurations
          if (n1_ID != n2_ID)
          {
            level+=3;
            changed = (changed || swap_running_jobs(j1, j2, sch1, sch2, 
                                                    original_solution,
                                                    best_change));
            level-=3;
          }
        }
      }
    }
  }

  if (changed && verbose > 3)
  {
    std::cout << internal_base << "\t\t\t--> best choice of second neigh:\n";
    Schedule::print_names(std::cout);
    for (job_schedule_t::const_iterator fit = (best_change.second).cbegin();
         fit != (best_change.second).cend(); ++fit)
    {
      if (! (fit->second).isEmpty())
        (fit->second).print(fit->first, std::cout);
    }
  }

  return best_change;
}

std::pair<double,job_schedule_t>
Local_search::third_neighborhood (const Solution& original_solution,
                                  double total_cost)
{
  std::string base = "";
  if (verbose > 1)
  {
    base = prepare_logging(level);
    std::cout << base << "\t--- third neighborhood." << std::endl;
  }

  // get original schedule
  const job_schedule_t& original_schedule = original_solution.get_schedule();

  // proposed modification
  double worst_val = comparator(INF,-INF) ? -INF : INF;
  best_change_t best_change({worst_val, job_schedule_t()});
  bool changed = false;

  // get list of submitted jobs
  const std::list<Job>& submitted_jobs = system.get_submittedJobs();

  // loop over jobs in order of pressure
  for(std::list<Job>::const_iterator cit1 = submitted_jobs.cbegin();
      cit1 != submitted_jobs.cend(); ++cit1)
  {
    const Job& j1 = *cit1;
    job_schedule_t::const_iterator sch1_it = original_schedule.find(j1);
    
    // if job j1 has been postponed
    if(sch1_it != original_schedule.cend() && (sch1_it->second).isEmpty())
    {
      // loop over jobs in reverse order of pressure
      for(std::list<Job>::const_reverse_iterator cit2 = submitted_jobs.rbegin();
          cit2 != submitted_jobs.rend(); ++cit2)
      {
        const Job& j2 = *cit2;
        
        // if the two jobs are different and job j2 is running...
        if (j1 != j2 && j1.get_pressure() >= j2.get_pressure())
        {
          job_schedule_t::const_iterator sch2_it = original_schedule.find(j2);
          if(sch2_it != original_schedule.cend() && !(sch2_it->second).isEmpty())
          {
            const Schedule& sch2 = sch2_it->second;
            const std::string& n2_ID = sch2.get_nodeID();
            const std::string& n2_GPUtype = sch2.get_GPUtype();
            unsigned g2 = sch2.get_nGPUs();

            // ...swap the state of the two jobs
            setup_time_t::const_iterator new_it1 = select_setup(j1, n2_GPUtype,
                                                                g2);

            double new_time = new_it1->second;
            
            job_schedule_t modified_schedule({{j1,Schedule(n2_ID, n2_GPUtype, 
                                                           new_time, g2)},
                                              {j2,Schedule()}});

            // create new solution with the modified schedule
            Solution new_solution(original_solution);
            job_schedule_t& new_schedule = new_solution.get_schedule();
            for (job_schedule_t::iterator change_it=modified_schedule.begin(); 
                 change_it != modified_schedule.end(); ++change_it)
            {
              const Job& change_j = change_it->first;
              job_schedule_t::iterator original_it = new_schedule.find(change_j);
              
              if (original_it != new_schedule.end())
                std::swap(original_it->second, change_it->second);
            }

            // evaluate new cost
            unsigned v = (verbose > 0) ? 0 : verbose;
            double new_total_cost = proxy_function(new_solution, 
                                                   system.get_GPUcosts(),
                                                   v, level+2);

            // save modification if it gives a better solution
            if (comparator(new_total_cost, total_cost))
            {
              changed = true;
              best_change = {new_total_cost, new_schedule};
            }
          }
        }
      }
    }
  }

  if (changed && verbose > 3)
  {
    std::cout << base << "\t\t--> best choice of third neigh:\n";
    Schedule::print_names(std::cout);
    for (job_schedule_t::const_iterator fit = (best_change.second).cbegin();
         fit != (best_change.second).cend(); ++fit)
    {
      if (! (fit->second).isEmpty())
        (fit->second).print(fit->first, std::cout);
    }
  }

  return best_change;
}

bool
Local_search::swap_running_jobs (const Job& j1, const Job& j2, 
                                 const Schedule& sch1, const Schedule& sch2,
                                 const Solution& original_solution,
                                 best_change_t& best_change)
{
  bool swapped = false;

  // swap the schedules of the two jobs
  const std::string& n1 = sch1.get_nodeID();
  const std::string& n2 = sch2.get_nodeID();
  const std::string& GPUtype1 = sch1.get_GPUtype();
  const std::string& GPUtype2 = sch2.get_GPUtype();

  setup_time_t::const_iterator new_it1 = select_setup(j1, GPUtype2,
                                                      sch2.get_nGPUs());
  setup_time_t::const_iterator new_it2 = select_setup(j2, GPUtype1,
                                                      sch1.get_nGPUs());

  double new_time1 = new_it1->second;
  unsigned new_g1 = (new_it1->first).second;
  double new_time2 = new_it2->second;
  unsigned new_g2 = (new_it2->first).second;
  job_schedule_t modified_schedule({{j1,Schedule(n2, GPUtype2, new_time1, 
                                                 new_g1)},
                                    {j2,Schedule(n1, GPUtype1, new_time2, 
                                                 new_g2)}});

  Solution new_solution(original_solution);
  job_schedule_t& new_schedule = new_solution.get_schedule();

  // create full schedule with the modified jobs
  for (job_schedule_t::iterator change_it = modified_schedule.begin(); 
       change_it != modified_schedule.end(); ++change_it)
  {
    const Job& change_j = change_it->first;
    job_schedule_t::iterator original_it = new_schedule.find(change_j);
    if (original_it != new_schedule.end())
      std::swap(original_it->second, change_it->second);
  }

  double new_total_cost = proxy_function(new_solution, system.get_GPUcosts(), 
                                         verbose, level+3);

  // save modification if it gives a better solution
  if (comparator(new_total_cost, best_change.first))
  {
    swapped = true;
    best_change = {new_total_cost, new_schedule};
  }

  return swapped;
}

template<typename COMP>
void
Local_search::restore_map_size (std::map<double, Solution, COMP>& K_best, 
                                unsigned K)
{
  while (K_best.size() > K)
  {
    typename std::map<double, Solution, COMP>::iterator it = K_best.end();
    K_best.erase(--it);
  }
}

template <typename COMP>
void
Local_search::local_search_step (std::map<double, Solution, COMP>& K_best)
{
  std::string base = "";
  if (verbose > 0)
  {
    base = prepare_logging(level);
    std::cout << base << "--- local search step." << std::endl;
    base = prepare_logging(level+1);
  }

  // copy the map of elite solutions
  typename Elite_solutions<COMP>::K_best_t K_best_copy(K_best);

  // create new map of elite solutions
  typename Elite_solutions<COMP>::K_best_t new_K_best;
  new_K_best.insert(*K_best_copy.cbegin());

  // iterate over elite solutions
  for (typename Elite_solutions<COMP>::iterator it = K_best_copy.begin(); 
       it != K_best_copy.end(); ++it)
  {
    // get original solution and relative total cost
    double total_cost = it->first;
    const Solution& original_BS = it->second;

    // initialize best modification and index of best neighborhood
    double worst_val = comparator(INF,-INF) ? -INF : INF;
    best_change_t best_change({worst_val, job_schedule_t()});
    unsigned best_neigh_idx = 0;

    // list of jobs with highest tardiness
    sorted_jobs_t tardi_jobs;
    // list of most expensive jobs
    sorted_jobs_t expensive_jobs;
    // for each node, stores the list of jobs deployed on that node
    node_jobs_t jobs_per_node;
    //
    get_sorted_jobs(original_BS, tardi_jobs, expensive_jobs, jobs_per_node);

    // third neighborhood:    executed job (low pressure) VS
    //                        postponed job (high pressure)
    best_change_t best_change_third_neigh;
    best_change_third_neigh = third_neighborhood(original_BS,total_cost);
    if (verbose > 1)
      std::cout << base << "\tthird neighborhood proposes " 
                << best_change_third_neigh.first << std::endl;
    if (comparator(best_change_third_neigh.first, best_change.first))
    {
      std::swap(best_change_third_neigh, best_change);
      best_neigh_idx = 3;
      if (verbose > 1)
        std::cout << base << "\t\t---> CHANGED" << std::endl;
    }

    if (tardi_jobs.size() > 0)
    {     
      // if the proposed solution involves more than one node...
      if (jobs_per_node.size() >= 2)
      {
        // first neighborhood:    jobs executed on different nodes, 
        //                        high tardiness VS high vm cost
        best_change_t best_change_first_neigh;
        best_change_first_neigh = first_neighborhood(tardi_jobs,expensive_jobs,
                                                     original_BS,total_cost);
        if (verbose > 1)
          std::cout << base << "\tfirst neighborhood proposes " 
                    << best_change_first_neigh.first << std::endl;
        if (comparator(best_change_first_neigh.first, best_change.first))
        {
          std::swap(best_change_first_neigh, best_change);
          best_neigh_idx = 1;
          if (verbose > 1)
            std::cout << base << "\t\t---> CHANGED" << std::endl;
        }

        // second neighborhood:   jobs executed on different nodes, 
        //                        high tardiness VS low pressure
        best_change_t best_change_second_neigh;
        best_change_second_neigh = second_neighborhood(tardi_jobs,original_BS,
                                                       total_cost);
        if (verbose > 1)
          std::cout << base << "\tsecond neighborhood proposes " 
                    << best_change_second_neigh.first << std::endl;
        if (comparator(best_change_second_neigh.first, best_change.first))
        {
          std::swap(best_change_second_neigh, best_change);
          best_neigh_idx = 2;
          if (verbose > 1)
            std::cout << base << "\t\t---> CHANGED" << std::endl;
        }
      }
    }

    // if there exists a best solution
    if (best_neigh_idx > 0)
    {
      const job_schedule_t& best_schedule = best_change.second;

      unsigned new_verbosity_level = (verbose > 0) ? 0 : verbose;
      Solution new_BS(best_schedule, original_BS.get_nodes(),
                      original_BS.get_current_time());
      double best_cost = proxy_function(new_BS, system.get_GPUcosts(), 
                                        new_verbosity_level, level+1);
      
      new_K_best.insert({best_cost, new_BS});

      if (verbose > 0)
        std::cout << base << "best neighborhood: " << best_neigh_idx
                  << std::endl;
    }
  }

  // check that size of new_K_best does not exceed the size of K_best
  if (new_K_best.size() > 1 && new_K_best.size() > K_best.size())
    restore_map_size(new_K_best, K_best.size());

  // swap new and old set of best solutions
  std::swap(K_best, new_K_best);
}

template<typename COMP>
void
Local_search::local_search (Elite_solutions<COMP>& ES, unsigned max_ls_iter,
                            unsigned v)
{
  // change verobsity level
  verbose = v;

  // initialize comparator
  comparator = ES.get_comparator();

  // perform local search
  bool go_ahead = true;
  for (unsigned ls_iter = 0; ls_iter < max_ls_iter && go_ahead; ++ls_iter)
  {
    local_search_step(ES.get_solutions());
    go_ahead = (ES.get_n_solutions() > 1);
  }
}


/*
*   specializations of template functions
*/
template
void
Local_search::local_search (Elite_solutions<std::less<double>>&, unsigned,
                            unsigned);
/**/
template
void
Local_search::local_search (Elite_solutions<std::greater<double>>&, 
                            unsigned, unsigned);
/**/
template
void
Local_search::local_search_step (std::map<double, Solution, 
                                          std::less<double>>&);
/**/
template
void
Local_search::local_search_step (std::map<double, Solution, 
                                          std::greater<double>>&);
/**/
template
void
Local_search::restore_map_size (std::map<double, Solution, std::less<double>>&, 
                                unsigned);
/**/
template
void
Local_search::restore_map_size (std::map<double, Solution, std::greater<double>>&, 
                                unsigned);
