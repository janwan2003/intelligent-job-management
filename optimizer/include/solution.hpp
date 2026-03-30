#ifndef SOLUTION_HH
#define SOLUTION_HH

#include "utilities.hpp"
#include "job.hpp"
#include "schedule.hpp"
#include "nodes_map.hpp"

class Solution {

private:
  job_schedule_t schedule;
  Nodes_map nodes;
  double first_finish_time = INF;
  double current_time = INF;

public:
  /*  constructor
  *
  *   Input(1): void                      default
  *
  *   Input(2): const job_schedule_t&     proposed schedule
  *             const Nodes_map&          all nodes
  *             double                    current time
  */
  Solution (void) = default;
  Solution (const job_schedule_t&, const Nodes_map&, double);

  // getters
  bool isEmpty (void) const {return schedule.empty();}
  const job_schedule_t& get_schedule (void) const {return schedule;}
  job_schedule_t& get_schedule (void) {return schedule;}
  const Nodes_map& get_nodes (void) const {return nodes;}
  Nodes_map& get_nodes (void) {return nodes;}
  unsigned get_n_nodes (void) const {return nodes.get_n_nodes();}
  unsigned get_n_full_nodes (void) const {return nodes.get_n_full_nodes();}
  double get_first_finish_time (void) const {return first_finish_time;}
  double get_current_time (void) const {return current_time;}
  
  /* compute_nodeCost
  *   compute cost of open nodes
  *
  *   Input:    double                    elapsed time
  *
  *   Output:   double                    cost
  */
  double compute_nodeCost (double) const;

  /* get_usedGPUs
  *   return the number of used GPUs on a given node
  *
  *   Input     const std::string&        GPU type
  *             const std::string&        node ID
  *
  *   Output    unsigned                  number of used GPUs
  */
  unsigned get_usedGPUs (const std::string&, const std::string&) const;

  void compute_first_finish_time (void);

  /* print_result
  *
  *   Input:  const std::string&          filename
  *           bool                        true if the schedule should be 
  *                                       appended to an existing file 
  *                                       (default: false)
  */
  void print (const std::string&, bool = false) const;

};

#endif /* SOLUTION_HH */