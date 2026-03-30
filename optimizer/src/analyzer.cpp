#include "analyzer.hpp"

Analyzer::Analyzer (const std::string& d, const System& s, unsigned v):
  directory(d + "/results/old"), system(s), verbose(v)
{}

Analyzer::Analyzer (const System& s, unsigned v): 
  system(s), verbose(v)
{}

bool 
Analyzer::adjust (job_schedule_t& adjusted_jobsch, const Schedule& new_sch, 
                  const Job& new_j, const std::string& old_GPUtype,
                  const std::string& old_nodeID, bool single)
{
  std::string base = "";
  if (verbose > 0)
    base = prepare_logging(level);

  bool still_running = false;

  // initialize adjusted schedule
  Schedule adjusted_sch(new_sch);

  // try to assign the job to the old node
  std::string ID = system.assign_to_node(old_GPUtype, new_sch.get_nGPUs(), 
                                         single, old_nodeID);
  if (! ID.empty())
  {
    adjusted_sch.change_configuration(old_nodeID, old_GPUtype,
                                      new_sch.get_selectedTime(),
                                      new_sch.get_nGPUs()); 

    if (verbose > 1)
      std::cout << base << "\t\t\t\t---> ASSIGNED to " << ID << std::endl;

    adjusted_jobsch.insert({new_j, adjusted_sch});

    still_running = true;
  }
  else
  {
    // if the assignment is not feasible, try to assign the job to the new 
    // node or to an available node with the given type of GPU
    still_running = adjust(adjusted_jobsch, new_sch, new_j, single);
  }

  return still_running;
}

bool
Analyzer::adjust (job_schedule_t& adjusted_jobsch, const Schedule& new_sch, 
                  const Job& new_j, bool single)
{
  std::string base = "";
  if (verbose > 0)
    base = prepare_logging(level);

  bool still_running = false;

  // initialize adjusted schedule
  Schedule adjusted_sch(new_sch);

  // try to assign the job to the new node
  std::string ID = system.assign_to_node(new_sch.get_GPUtype(), 
                                         new_sch.get_nGPUs(), 
                                         single, new_sch.get_nodeID());

  // if the assignment is not feasible, try to assign the job to
  // an available node with the given type of GPU or change the schedule
  // to an empty one
  if (ID.empty())
  {
    ID = system.assign_to_node(new_sch.get_GPUtype(), new_sch.get_nGPUs(), 
                               single);

    if (! ID.empty())
    {
      adjusted_sch.change_configuration(ID, new_sch.get_GPUtype(),
                                        new_sch.get_selectedTime(),
                                        new_sch.get_nGPUs());
      still_running = true;
    }
    else
      adjusted_sch = Schedule();
  }

  if (verbose > 1)
    std::cout << base << "\t\t\t\t---> ASSIGNED to " << ID << std::endl;

  adjusted_jobsch.insert({new_j, adjusted_sch});

  return still_running;
}

void
Analyzer::perform_analysis (const job_schedule_t& old_jobsch, 
                            Solution& new_solution, bool single)
{
  std::string base = "";
  if (verbose > 0)
  {
    base = prepare_logging(level);
    std::cout << base << "\t--- getting new schedule" << std::endl;
  }

  // initialize (empty) adjusted schedule
  job_schedule_t adjusted_jobsch;

  // get new schedule
  const job_schedule_t& new_jobsch = new_solution.get_schedule();

  // number of running jobs in the new schedule and in the adjusted schedule
  unsigned new_n_running_jobs = 0;
  unsigned adj_n_running_jobs = 0;

  // loop over new schedule
  for (job_schedule_t::const_iterator new_it = new_jobsch.cbegin();
       new_it != new_jobsch.cend(); ++new_it)
  {
    const Job& new_j = new_it->first;
    const Schedule& new_sch = new_it->second;

    if (verbose > 1)
      std::cout << base << "\t\tanalyzing job... " << new_j.get_ID() 
                << std::endl;

    // check if the job is currently running
    if (! new_sch.isEmpty())
    {
      // update number of running jobs
      new_n_running_jobs++;

      // check the existence of the job in the old schedule
      job_schedule_t::const_iterator old_it = old_jobsch.find(new_j);
      if (old_it != old_jobsch.cend())
      {
        // check if the job was executed and get the corresponding 
        // configuration
        const Schedule& old_sch = old_it->second;
        if (! old_sch.isEmpty())
        {
          const std::string& new_nodeID = new_sch.get_nodeID();
          const std::string& new_GPUtype = new_sch.get_GPUtype();
          const std::string& old_GPUtype = old_sch.get_GPUtype();
          const std::string& old_nodeID = old_sch.get_nodeID();

          if (verbose > 1)
            std::cout << base << "\t\t\tpreviously running --> new nodeID = "
                      << new_nodeID << "; new_GPUtype = " << new_GPUtype
                      << "; old_GPUtype = " << old_GPUtype << "; old_nodeID = "
                      << old_nodeID << std::endl;

          // perform comparison and adjustment, updating the number of
          // running jobs
          if (new_GPUtype == old_GPUtype && new_nodeID != old_nodeID)
            adj_n_running_jobs += adjust(adjusted_jobsch, new_sch, new_j, 
                                         old_GPUtype, old_nodeID, single);
          else
            adj_n_running_jobs += adjust(adjusted_jobsch, new_sch, new_j, 
                                         single);
        }
      }

      // if the job was not executed in the previous schedule, check if the 
      // new assignment is still feasible in the adjusted schedule, change it
      // otherwise
      if (adjusted_jobsch.find(new_j) == adjusted_jobsch.end())
      {
        // determine the new setup
        const std::string& new_nodeID = new_sch.get_nodeID();
        const std::string& new_GPUtype = new_sch.get_GPUtype();

        if (verbose > 1)
          std::cout << base << "\t\t\tnot previously running --> new nodeID = "
                    << new_nodeID << "; new_GPUtype = " << new_GPUtype
                    << std::endl;

        adj_n_running_jobs += adjust(adjusted_jobsch, new_sch, new_j, single);        
      }
    }
  }

  if (! adjusted_jobsch.empty())
  {
    // amount of used resources in the adjusted schedule
    unsigned adj_n_open_nodes = system.get_nodes().count_open_nodes();
    unsigned adj_n_full_nodes = system.get_n_full_nodes();
    
    // amount of used resources in the new solution
    unsigned new_n_open_nodes = new_solution.get_nodes().count_open_nodes();
    unsigned new_n_full_nodes = new_solution.get_n_full_nodes();

    if (verbose > 1)
    {
      std::cout << base << "\t\t\tadj_n_open_nodes = " << adj_n_open_nodes
                << "; adj_n_full_nodes = " << adj_n_full_nodes
                << "; new_n_open_nodes = " << new_n_open_nodes
                << "; new_n_full_nodes = " << new_n_full_nodes
                << std::endl;
      std::cout << base << "\t\t\tadj_n_running_jobs = " << adj_n_running_jobs
                << "; new_n_running_jobs = " << new_n_running_jobs 
                << std::endl;
    }
    
    // check if the amount of used resources does not exceed the amount of
    // used resources in the new setting and if the number of running jobs is
    // not lower than in the new solution
    if (adj_n_open_nodes <= new_n_open_nodes && 
        adj_n_running_jobs >= new_n_running_jobs)
    {
      std::swap(new_solution.get_schedule(), adjusted_jobsch);
      std::swap(new_solution.get_nodes(), system.get_nodes());

      new_solution.compute_first_finish_time();

      if (verbose > 1)
        std::cout << base << "\t\t\t\t---> SWAPPED" << std::endl;
    }
  }
}

void
Analyzer::perform_analysis (const std::string& filename, 
                            Solution& new_solution, bool single)
{
  std::string base = "";
  if (verbose > 0)
  {
    base = prepare_logging(level);
    std::cout << base << "--- perform analysis." << std::endl;
    std::cout << base << "\t--- reading old schedule" << std::endl;
  }

  // read old schedule from file
  std::string file = directory + "/" + filename;
  job_schedule_t old_jobsch;
  read_old_schedule(file, old_jobsch);

  // perform analysis
  if (! old_jobsch.empty())
    perform_analysis(old_jobsch, new_solution, single);
}

void
Analyzer::perform_analysis (const Solution& old_solution, 
                            Solution& new_solution, bool single)
{
  std::string base = "";
  if (verbose > 0)
  {
    base = prepare_logging(level);
    std::cout << base << "--- perform analysis." << std::endl;
    std::cout << base << "\t--- getting old schedule" << std::endl;
  }

  // get old schedule from old solution
  const job_schedule_t& old_jobsch = old_solution.get_schedule();
  
  // perform analysis
  perform_analysis(old_jobsch, new_solution, single);
}
