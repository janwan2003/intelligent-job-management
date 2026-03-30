#ifndef RANDOM_GREEDY_HH
#define RANDOM_GREEDY_HH

#include "heuristic.hpp"

template <typename COMP = std::less<double>>

class Random_greedy: public Heuristic<COMP> {

protected:
  // random numbers generator
  std::default_random_engine generator;

  // parameters for randomization
  double alpha = 0.05;                    // parameter for setup selection
  double pi = 0.05;                       // parameter for random swaps in 
                                          //  submitted_jobs
  double beta = 0.2;                      // parameter for node selection
  bool r_swap = true;                     // true to swap jobs in sorted list

  // maximum # of random iterations
  unsigned max_random_iter = 1000;

  /* random_swap
  *   randomly swap jobs in the sorted list
  */
  void random_swap (void);

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
                                     job_schedule_t&) override;

  /* perform_assignment 
  *   assign the selected job to the new proposed schedule
  *
  *   Input:  const Job&                      job to be assigned
  *           job_schedule_t&                 new proposed schedule
  *
  *   Output: bool                            true if job has been assigned
  */
  virtual bool perform_assignment (const Job&, job_schedule_t&) override;

public:
  /*  constructor
  *
  *   Input:  const System&                   system
  *           const obj_function_t&           proxy function
  *           unsigned                        level for logging
  *           unsigned                        seed for random generator
  */
  Random_greedy (const System&, const obj_function_t&, unsigned, unsigned);

  /* set random number generator
  */
  void set_generator (const std::default_random_engine& g) {generator = g;}

  /* get random number generator
  */
  const std::default_random_engine& get_generator (void) {return generator;}

  /* perform_scheduling
  *   perform the scheduling process that assigns the best configuration to
  *   each submitted jobs (as long as resources are available)
  *
  *   Input:    double                        current time
  *             Elite_solutions<COMP>&        empty map of elite solutions
  *             unsigned                      max number of elite solutions
  *             unsigned                      verbosity level
  */
  virtual void perform_scheduling (double, Elite_solutions<COMP>&, unsigned,
                                   unsigned = 0) override;
  
  /* destructor
  */
  virtual ~Random_greedy (void) = default;

};

#endif /* RANDOM_GREEDY_HH */