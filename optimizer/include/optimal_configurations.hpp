#ifndef OPTIMAL_CONFIGURATIONS_HH
#define OPTIMAL_CONFIGURATIONS_HH

#include <random>
#include <cmath>

#include "utilities.hpp"
#include "job.hpp"
#include "node.hpp"
#include "GPU_catalogue.hpp"

class Optimal_configurations {

private:
  // type definitions
  typedef std::multimap<double, setup_time_t::const_iterator> optConfig_map_t;
  typedef optConfig_map_t::const_iterator const_iterator;

  // setups s.t. job j is executed before deadline (ordered by min cost)
  optConfig_map_t optConfig_j;

  // setups s.t. job j is NOT executed before deadline (ordered by min 
  // execution time)
  optConfig_map_t optConfig_j_C;

  // parameter for randomization and random numbers generator
  double alpha = 0.;
  std::default_random_engine generator;

public:
  /*  constructor
  *
  *   input:  const Job&                job j
  *           const setup_time_t&       tjvg (see utilities.hpp for type def)
  *           const gpu_catalogue_ptr&  catalogue of GPU costs
  *           double                    current time
  *
  */
  Optimal_configurations (const Job&, const setup_time_t&, 
                          const gpu_catalogue_ptr&, double);

  /* set parameter for randomization
  */
  void set_random_parameter (double a) {alpha = a;}

  /* set random number generator
  */
  void set_generator (const std::default_random_engine& g) {generator = g;}

  /* get random number generator
  */
  const std::default_random_engine& get_generator (void) {return generator;}

  /* determine the best setup:
  *   if it is possible to match deadline, the best setup is the cheapest
  *   otherwise, the best setup is the fastest
  */
  setup_time_t::const_iterator get_best_setup (void);

  /* is_end
  *   return true if optConfig_j.empty() AND optConfig_j_C.empty()
  */
  bool is_end (void) const;

  /* print
  *   print optConfig_j and optConfig_j_C
  *
  *   Input:  std::ostream&       where to print the containers
  */
  void print (std::ostream&) const;
};

#endif /* OPTIMAL_CONFIGURATIONS_HH */