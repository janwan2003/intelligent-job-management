#ifndef SCHEDULE_HH
#define SCHEDULE_HH

#include "job.hpp"
#include "node.hpp"

class Schedule {

private:
  std::string node_ID = "";
  std::string GPUtype = "";
  double selected_time = 0.;
  unsigned n_assigned_GPUs = 0;

  unsigned iter = 0;
  double sim_time = 0;
  double execution_time = 0.;
  double completion_percent = 0.;
  double completion_percent_step = 0.;
  double start_time = 0.;
  double finish_time = 0.;
  double tardiness = 0.;
  double GPUcost = 0.;
  double tardinessCost = 0.;

public:
  /* constructors
  *
  *   Input(1):  void                 default
  *
  *   Input(2):  const std::string&   selected node ID
  *              const std::string&   selected GPU type
  *              double               expected execution time
  *              unsigned             number of assigned GPUs
  *
  *   Input(3):  const row_t&         list of parameters (read from file) to 
  *                                   initialize a schedule
  *
  */
  Schedule (void) = default;
  Schedule (const std::string&, const std::string&, double, unsigned);
  Schedule (const row_t&);

  // getters
  bool isEmpty (void) const {return (n_assigned_GPUs == 0);}
  //
  unsigned get_iter (void) const {return iter;}
  double get_startTime (void) const {return start_time;}
  double get_completionPercent (void) const {return completion_percent;}
  double get_cP_step (void) const {return completion_percent_step;}
  double get_simTime (void) const {return sim_time;}
  double get_executionTime (void) const {return execution_time;}
  double get_tardiness (void) const {return tardiness;}
  double get_GPUcost (void) const {return GPUcost;}
  double get_tardinessCost (void) const {return tardinessCost;}
  //
  double get_selectedTime (void) const {return selected_time;}
  const std::string& get_nodeID (void) const {return node_ID;}
  const std::string& get_GPUtype (void) const {return GPUtype;}
  unsigned get_nGPUs (void) const {return n_assigned_GPUs;}

  // setters
  void set_iter (unsigned it) {iter = it;}
  void set_simTime (double st) {sim_time = st;}
  void set_executionTime (double et) {execution_time = et;}
  void set_completionPercent (double cp) {completion_percent = cp;}
  void set_cP_step (double cp) {completion_percent_step = cp;}
  void set_finishTime (double ft) {finish_time = ft;}
  void set_tardiness (double t);
  void set_startTime (double st) {start_time = st;}
  
  /* compute_GPUcost
  *
  *   Input:    unsigned              number of used GPUs in the node
  *             double                energy cost of the GPU
  */
  void compute_GPUcost (unsigned, double);

  /* compute_tardinessCost
  *
  *   Input:    double                tardiness weight
  */
  void compute_tardinessCost (double);
  
  /* change_configuration
  *
  *   Input:    const std::string&    selected node ID
  *             const std::string&    selected GPU type
  *             double                expected execution time
  *             unsigned              number of assigned GPUs
  */
  void change_configuration (const std::string&, const std::string&, 
                             double, unsigned);

  /* change_configuration
  *
  *   Input:    double                expected execution time
  *             unsigned              number of assigned GPUs
  */
  void change_configuration (double, unsigned);

  /* print_names (static)
  *
  *   Input:  std::ostream&       where to print names of information
  *                               stored in the class
  *           char                last character to be printed (default \n)
  */
  static void print_names (std::ostream& ofs, char = '\n');

  /* print
  *
  *   Input:  const Job&          job that the schedule is associated to
  *           std::ostream&       where to print information stored in the 
  *                               schedule
  *           char                last character to be printed (default \n)
  */
  void print (const Job&, std::ostream&, char = '\n') const;
  
};

#endif /* SCHEDULE_HH */
