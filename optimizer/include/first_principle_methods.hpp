#ifndef FIRST_PRINCIPLE_METHODS_HH
#define FIRST_PRINCIPLE_METHODS_HH

#include "heuristic.hpp"

template <typename COMP = std::less<double>>

class First_principle_methods: public Heuristic<COMP> {

protected:
  /* sort_jobs_list
  *   sort the submitted_jobs list according to the provided criterion
  */
  virtual void sort_jobs_list (void) override = 0;

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
  */
  First_principle_methods (const System&, const obj_function_t&, unsigned);

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
  virtual ~First_principle_methods (void) = default;

};

#endif /* FIRST_PRINCIPLE_METHODS_HH */