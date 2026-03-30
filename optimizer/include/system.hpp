#ifndef SYSTEM_HH
#define SYSTEM_HH

#include "utilities.hpp"
#include "job.hpp"
#include "nodes_map.hpp"
#include "GPU_catalogue.hpp"

class System {

private:
  // list of submitted jobs
  std::list<Job> submitted_jobs;

  // set of all nodes
  Nodes_map nodes;
  
  // table tjvg for all j
  time_table_ptr ttime;

  // catalogue of GPU costs
  gpu_catalogue_ptr GPU_costs;

public:
  /*  constructor
  *
  *   Input(1): void                      default
  *
  *   Input(2): const std::list<Job>&     list of submitted jobs
  *             const Nodes_map&          all nodes
  *             const time_table_ptr&     execution times table
  *             const gpu_catalogue_ptr&  GPU costs catalogue
  *
  *   Input(3): const std::string&        full path of data directory
  *             const std::string&        name of file with the list of jobs
  *             const std::string&        name of file with jobs execution times
  *             const std::string&        name of file with the list of nodes
  *             const std::string&        name of file with the GPU costs
  *             unsigned                  verbosity level
  */
  System (void) = default;
  System (const std::list<Job>&, const Nodes_map&, const time_table_ptr&, 
          const gpu_catalogue_ptr&);
  System (const std::string&, const std::string&, const std::string&, 
          const std::string&, const std::string&, unsigned = 0);

  // getters
  bool isEmpty (void) const {return (submitted_jobs.empty() && nodes.isEmpty());}
  const std::list<Job>& get_submittedJobs (void) const {return submitted_jobs;}
  std::list<Job>& get_submittedJobs (void) {return submitted_jobs;}
  const Nodes_map& get_nodes (void) const {return nodes;}
  Nodes_map& get_nodes (void) {return nodes;}
  //
  time_table_ptr get_ttime (void) const {return ttime;}
  gpu_catalogue_ptr get_GPUcosts (void) const {return GPU_costs;}
  //
  unsigned get_n_nodes (void) const {return nodes.get_n_nodes();}
  unsigned get_n_full_nodes (void) const {return nodes.get_n_full_nodes();}
  
  /* isFull
  *
  *   Input:    const std::string&        GPU type
  *             const std::string&        node ID
  *
  *   Output:   bool                      true if the given node is full
  */
  bool isFull (const std::string&, const std::string&) const;
  
  /* get_remainingGPUs
  *
  *   Input:    const std::string&        GPU type
  *             const std::string&        node ID
  *
  *   Output:   unsigned                  number of free GPUs
  */
  unsigned get_remainingGPUs (const std::string&, const std::string&) const;

  /* assign_to_node
  *   update the number of available GPUs in a node with the given GPU type, 
  *   subtracting the required amount. A node ID can be specified if needed
  *
  *   Input:    const std::string&        GPU type
  *             unsigned                  number of GPUs to be subtracted
  *             bool                      true if the node must be set to full
  *                                       independently of the remaining GPUs
  *                                       (default: false)
  *             const std::string&        node ID (default: "")
  *
  *   Output:   std::string               ID of the updated node
  */
  std::string assign_to_node (const std::string&, unsigned, bool = false,
                              const std::string& = "");

  void close_nodes (void);

  /* init
  *
  *   Input:    const std::list<Job>&     list of submitted jobs
  *             const Nodes_map&          all nodes
  *             const time_table_ptr&     execution times table
  *             const gpu_catalogue_ptr&  GPU costs catalogue
  */
  void init (const std::list<Job>&, const Nodes_map&, const time_table_ptr&, 
             const gpu_catalogue_ptr&);

};

#endif /* SYSTEM_HH */