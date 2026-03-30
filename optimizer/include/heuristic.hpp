#ifndef HEURISTIC_HH
#define HEURISTIC_HH

#include "elite_solutions.hpp"
#include "system.hpp"
#include "optimal_configurations.hpp"
#include "fileIO.hpp"

template <typename COMP>
class Heuristic {

protected:
  // type definitions
  typedef typename Elite_solutions<COMP>::K_best_t K_best_t;

  // system
  System system;

  // proxy function
  obj_function_t proxy_function;

  // current time
  double current_time = INF;

  // verbosity level
  unsigned verbose = 0;
  unsigned level = 2;

  /* sort_jobs_list
  *   sort the submitted_jobs list according to compare_pressure
  */
  virtual void sort_jobs_list (void);

  /* available_resources
  *   return true if there are still available resources in the system
  */
  bool available_resources (void) const;

  /* close_nodes
  *   close all the currently opened nodes
  */
  void close_nodes (void);

  /* assign:
  *   assign the given job to the required node
  *
  *   Input:  const Job&                      job to be assigned
  *           setup_time_t::const_iterator    iterator to the best setup
  *           job_schedule_t&                 new proposed schedule
  *
  *   Output: bool                            true if job has been assigned
  */
  bool assign (const Job&, setup_time_t::const_iterator, job_schedule_t&);

  /* assign_to_suboptimal:
  *   assign the given job to a suboptimal configuration, available in 
  *   an already opened node
  *
  *   Input:  const Job&                      job to be assigned
  *           const setup_time_t&             execution times of the given job
  *           Optimal_configurations&         see optimal_configurations.hpp
  *           job_schedule_t&                 new proposed schedule
  *
  *   Output: bool                            true if job has been assigned
  */
  virtual bool assign_to_suboptimal (const Job&, const setup_time_t&, 
                                     Optimal_configurations&, 
                                     job_schedule_t&) = 0;

  /* perform_assigment
  *   assign the selected job to the new proposed schedule
  *
  *   Input:  const Job&                      job to be assigned
  *           job_schedule_t&                 new proposed schedule
  *
  *   Output: bool                            true if job has been assigned
  */
  virtual bool perform_assignment (const Job&, job_schedule_t&) = 0;

  /* scheduling_step
  *   sort the list of submitted jobs and perform scheduling of all
  *   submitted jobs
  *
  *   Input:  job_schedule_t&                 empty schedule to be built
  *
  *   Output: unsigned                        number of assigned jobs
  */
  unsigned scheduling_step (job_schedule_t&);

  /* postprocessing
  *   aim to reduce the number of idle GPUs in selected configurations
  *
  *   Input:  job_schedule_t&                 proposed schedule to be updated
  */
  void postprocessing (job_schedule_t&);

  /* to_be_inserted
  *  return true if the element characterized by the given upper bound should
  *  be inserted in the map
  */
  virtual bool to_be_inserted (const K_best_t&, 
                               typename K_best_t::iterator) const;

  /* restore_map_size
  *   erase the element in the map with the worst cost (the last element), so 
  *   that the size of the map returns less than or equal to the maximum size K
  *
  *   Input:  K_best_t&                       map to be modified
  */
  void restore_map_size (K_best_t&);

  /* update_elite_solutions
  *   compute the cost of the given schedule and inserts it in the map of
  *   elite solutions if the cost is better than other costs already in the map
  *
  *   Input:  const job_schedule_t&           new proposed schedule
  *           K_best_t&                       map to be modified
  *           unsigned                        max number of elite solutions
  */
  void update_elite_solutions (const job_schedule_t&, K_best_t&, unsigned);

public:
  /*  constructor
  *
  *   Input:  const System&                   system
  *           const obj_function_t&           proxy function
  *           unsigned                        level for logging
  */
  Heuristic (const System&, const obj_function_t&, unsigned);

  /*  init
  *   re-initialize available nodes, submitted jobs and proxy function
  *
  *   Input:  const System&                   system
  *           const obj_function_t&           proxy function
  *
  */
  void init (const System&, const obj_function_t&);

  /* perform_scheduling
  *   perform the scheduling process that assigns the best configuration to
  *   each submitted jobs (as long as resources are available)
  *
  *   Input:  double                          current time
  *           Elite_solutions<COMP>&          empty map of elite solutions
  *           unsigned                        max number of elite solutions
  *           unsigned                        verbosity level (default 0)
  */
  virtual void perform_scheduling (double, Elite_solutions<COMP>&, unsigned, 
                                   unsigned = 0) = 0;
  
  /* destructor
  */
  virtual ~Heuristic (void) = default;

};

#endif /* HEURISTIC_HH */
