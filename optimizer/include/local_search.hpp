#ifndef LOCAL_SEARCH_HH
#define LOCAL_SEARCH_HH

#include "elite_solutions.hpp"
#include "system.hpp"

class Local_search {

private:
  // type definitions
  typedef std::list<job_schedule_t::const_iterator> sorted_jobs_t;
  typedef std::map<std::string, sorted_jobs_t> node_jobs_t;
  typedef std::pair<double,job_schedule_t> best_change_t;

  // system
  System system;

  // proxy function
  obj_function_t proxy_function;

  // maximum size of neighborhoods
  unsigned k1 = 10;

  // costs comparator
  comparator_t comparator;

  // verbosity level
  unsigned verbose = 0;
  unsigned level = 3;

  /* select_setup
  *   select a setup for the given job with the given GPU type and GPU number
  *
  *   Input:  const Job&                    job to be assigned
  *           const std::string&            GPU type
  *           unsigned                      required number of GPUs
  *
  *   Output: setup_time_t::const_iterator  iterator to the new setup
  */
  setup_time_t::const_iterator select_setup (const Job&, const std::string&,
                                             unsigned) const;

  /* get_sorted_jobs
  *   analyse the solution passed as parameter to generate the list of 
  *   jobs sorted by decreasing tardiness, the list of jobs sorted by 
  *   decreasing VM cost and a table storing the list of jobs running on 
  *   each node
  *
  *   Input:  const Solution&               solution to be examined
  *           sorted_jobs_t&                list of jobs sorted by tardiness
  *           sorted_jobs_t&                list of jobs sorted by cost
  *           node_jobs_t&                  map of jobs in each node
  */
  void get_sorted_jobs (const Solution&, sorted_jobs_t&,
                        sorted_jobs_t&, node_jobs_t&) const;

  /* first_neighborhood
  *   jobs executed on different nodes, high tardiness VS high vm cost
  *
  *   Input:  const sorted_jobs_t&          list of jobs sorted by tardiness
  *           const sorted_jobs_t&          list of jobs sorted by cost
  *           const Solution&               original solution
  *           double                        original total cost
  *
  *   Output: best_change_t                 proposed modification with new 
  *                                         total cost
  */
  best_change_t first_neighborhood (const sorted_jobs_t&, const sorted_jobs_t&, 
                                    const Solution&, double);

  /* second_neighborhood
  *   jobs executed on different nodes, high tardiness VS low pressure
  *
  *   Input:  const sorted_jobs_t&          list of jobs sorted by tardi
  *           const Solution&               original solution
  *           double                        original total cost
  *
  *   Output: best_change_t                 proposed modification with new 
  *                                         total cost
  */
  best_change_t second_neighborhood (const sorted_jobs_t&, const Solution&,
                                     double);

  /* third_neighborhood
  *   executed job (high tardiness) VS postponed job (high pressure)
  *
  *   Input:  const Solution&                   original solution
  *           double                            original total cost
  *
  *   Output: best_change_t                     proposed modification with new 
  *                                             total cost
  */
  best_change_t third_neighborhood (const Solution&, double);

  /* swap_running_jobs
  *   swap schedules of jobs passed as parameter and evaluate the modified 
  *   cost, updating the best change in case of better cost
  *
  *   Input:  const Job&                        first job to be swapped
  *           const Job&                        second job to be swapped
  *           const Schedule&                   schedule of first job
  *           const Schedule&                   schedule of second job
  *           const Solution&                   original solution
  *           best_change_t&                    best change (to be updated)
  *
  *   Output: bool                              true if best change has been
  *                                             updated
  */
  bool swap_running_jobs (const Job&, const Job&, const Schedule&, 
                          const Schedule&, const Solution&, best_change_t&);

  /* restore_map_size
  *   erase the element in the map with the worst cost until the size  
  *   of the map returns less than or equal to the maximum size K
  *
  *   Input:  std::map<double, Solution, COMP>&   map to be modified
  *           unsigned                            maximum number of solutions
  */
  template<typename COMP>
  void restore_map_size (std::map<double, Solution, COMP>&, unsigned);

  /* local_search_step
  *   perform a single step of local search
  *
  *   Input:  std::map<double, Solution, COMP>&   map to be modified
  */
  template<typename COMP>
  void local_search_step (std::map<double, Solution, COMP>&);

public:
  /* constructor
  *
  *   Input:  const System&                     system
  *           const obj_function_t&             proxy function
  */
  Local_search (const System&, const obj_function_t&);

  /* local_search
  *   perform the local search
  *
  *   Input:  Elite_solutions<COMP>&            elite solutions
  *           unsigned                          number of iterations
  *                                             (default 1 -> first-improving)
  *           unsigned                          verbosity level (default 0)
  */
  template<typename COMP>
  void local_search (Elite_solutions<COMP>&, unsigned = 1, unsigned = 0);

};

#endif /* LOCAL_SEARCH_HH */