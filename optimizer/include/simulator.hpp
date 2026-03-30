#ifndef SIMULATOR_HH
#define SIMULATOR_HH

#include "optimizer.hpp"
#include "analyzer.hpp"

class Simulator {

private:
  // full path of data (and results) directory
  std::string directory = "";

  // the default scheduling interval is INF: the system is optimized only 
  // when a new job is submitted or a running job terminates
  double scheduling_interval = INF;

  // current time
  double current_time = 0.;

  // verbosity level
  unsigned verbose = 0;
  unsigned level = 0;

  // list of all jobs
  std::list<Job> jobs;
  // set of all nodes
  Nodes_map nodes;
  // table tjvg for all j
  std::shared_ptr<time_table_t> ttime;

  // catalogue of GPU costs
  gpu_catalogue_ptr GPU_costs;

  // last computed solution
  Solution old_solution;

  // total costs and tardiness
  costs_struct total_costs;

  /* submit_jobs
  *   add new jobs to the queue according to their submission time
  *
  *   Input:  double                  elapsed time between previous and current 
  *                                   iteration
  *           std::list<Job>&         list of submitted jobs
  *
  *   Output: double                  new current time
  */
  double submit_jobs (double, std::list<Job>&);

  /* remove_ended_jobs
  *   remove from the given queue all jobs whose execution is completed;
  *   return in a list these jobs with the current assignment (node)
  */
  jn_pairs_t remove_ended_jobs (std::list<Job>&) const;

  /* update_execution_times
  *   update execution time of jobs that have been partially executed
  *
  *   Input:  double                  elapsed time between previous and current 
  *                                   iteration
  */
  void update_execution_times (double);

  /* update_minMax_exec_time
  *   compute and update minimum and maximum execution time of the given job
  */
  void update_minMax_exec_time (Job&) const;

  /* preprocessing
  *   update execution time and pressure of all submitted jobs
  */
  void preprocessing (std::list<Job>&) const;

  /* get_waiting_jobs
  *
  *   Input:  const std::list<Job>&   list of submitted jobs
  *
  *   Output: std::list<Job>          sublist of not-yet-started jobs
  */
  std::list<Job> get_waiting_jobs (const std::list<Job>&) const;

  /* add_previously_running_jobs
  *   to be used for first-principle methods: add the previously running jobs 
  *   to the new solution, maintaining their configuration
  *
  *   Input:  Solution&               new solution to be updated
  *           const std::list<Job>&   submitted jobs
  */
  void add_previously_running_jobs(Solution&, const std::list<Job>&) const;

  /* update_scheduled_jobs
  *   update list of scheduled jobs in the given solution with information 
  *   about execution time, costs, etc
  *
  *   Input:  unsigned                iteration number
  *           double                  elapsed time between previous and current 
  *                                   iteration
  *           Solution&               solution to be updated
  *
  *   Output: bool                    true if completion percent of all jobs 
  *                                   is 100
  */
  bool update_scheduled_jobs (unsigned, double, Solution&);

public:
  /*  constructor
  *
  *   Input:  const std::string&      full path of data directory
  *           const std::string&      name of file with the list of jobs
  *           const std::string&      name of file with execution times of jobs
  *           const std::string&      name of file with the list of nodes
  *           const std::string&      name of file with the GPU costs
  */
  Simulator (const std::string&, const std::string&, const std::string&, 
             const std::string&, const std::string&);

  /* initialized
  *   check if data have been correctly loaded and the system is not empty
  */
  bool initialized (void) const;

  /* algorithm
  *
  *   Input:  const std::string&      method
  *           double                  current time
  *           unsigned                verbosity level (default: 0)
  *           unsigned                seed for random numbers generator
  *                                   (default: 4010)
  *           double                  scheduling interval (default: INF)
  *
  *   Output: costs_struct            total tardiness and simulation costs
  */
  costs_struct algorithm (const std::string&, double, unsigned = 0, 
                          unsigned = 4010, double = INF);

  /* print_schedule
  *   print the best schedule produced by the algorithm
  *
  *   Input:    const std::string&    filename where the schedule should be
  *                                   printed
  *             bool                  true if the schedule should be appended
  *                                   to an existing file (default: false)
  */
  void print_schedule (const std::string&, bool = false) const;
};

#endif /* SIMULATOR_HH */