#ifndef GREEDY_HH
#define GREEDY_HH

#include "heuristic.hpp"

/*
*
    rules for Greedy Algorithm:

    1) jobs are ordered with respect to pressure (current time + minimum 
       execution time - deadline)

    2) each job in the ordered queue is assigned to:
       2.1) the optimal configuration on an already opened node, if possible
       2.2) a new node, equipped with the optimal configuration in terms of
            GPUType and with the VMType that allows to select the highest 
            number of available GPUs, if possible
       2.3) the best among the suboptimal configurations available on an 
            already opened node with residual space, if possible
       
       NOTE: assignment to already opened nodes is performed with a 
       best-fit approach

    3) postprocessing phase:
       3.1) for all opened nodes with idle GPUs, a new configuration is
            selected, with the same VMtype and GPUtype and maxnGPUs equal
            to the number of used GPUs (or usedGPUs+1, since there are no
            available configurations with, for instance, 3 GPUs), if possible
       3.2) the GPUs remaining idle are assigned to the job with best 
            improvement in performance (: with the highest speed-up)
*
*/

template <typename COMP = std::less<double>>

class Greedy: public Heuristic<COMP> {

protected:
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
  virtual bool assign_to_suboptimal (const Job&, const setup_time_t&, Optimal_configurations&, 
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
  Greedy (const System&, const obj_function_t&, unsigned);

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
  virtual ~Greedy (void) = default;

};

#endif /* GREEDY_HH */