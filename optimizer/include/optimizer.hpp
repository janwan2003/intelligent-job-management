#ifndef OPTIMIZER_HH
#define OPTIMIZER_HH

#include "greedy.hpp"
#include "random_greedy.hpp"
#include "local_search.hpp"
//#include "path_relinking.hpp"
#include "FIFO.hpp"
#include "EDF.hpp"
#include "Priority.hpp"

class Optimizer {

private:
  // system
  System system;

  // method
  std::string method = "";

  // current time
  double current_time = INF;

  // verbosity level
  unsigned verbose = 0;
  unsigned level = 1;

  /* perform_scheduling
  *   according to the different methods, instantiate and run the corresponding
  *   heuristic algorithm or the required enhancing technique
  *
  *   Input:    Solution&               solution to be computed
  *             unsigned                seed for random numbers generator
  *                                     (default 1010)
  */
  void perform_scheduling (Solution&, unsigned = 1010) const;

public:
  /* constructor
  *
  *   Input(1): const System&           system
  */
  Optimizer (const System&);

  /* init
  *
  *   Input:    const System&           system
  */
  void init (const System&);

  /* algorithm
  *
  *   Input:    const std::string&      method
  *             double                  current time
  *             unsigned                verbosity level (default 0)
  *             unsigned                seed for random numbers generator
  *                                     (default 1010)
  *
  *   Output:   Solution                best solution
  */
  Solution algorithm (const std::string&, double, unsigned = 0, 
                      unsigned = 1010);

  /* print_schedule
  *   print the best schedule produced by the algorithm
  *
  *   Input:    const std::string&      filename where the schedule should be
  *                                     printed
  */
  void print_schedule (const std::string&) const;
};

// proxy functions
double proxy_function_min (Solution&, const gpu_catalogue_ptr&, unsigned, 
                           unsigned);
double proxy_function_max (Solution&, const gpu_catalogue_ptr&, unsigned, 
                           unsigned);

#endif /* OPTIMIZER_HH */