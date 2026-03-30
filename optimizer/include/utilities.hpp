#ifndef UTILITIES_HH
#define UTILITIES_HH

#include <string>
#include <list>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include <limits>
#include <cassert>
#include <random>
#include <functional>
#include <iostream>
#include <iomanip>
#include <memory>

/*  
  forward declarations of classes
*/
class Job;
class Node;
class Schedule;
class Solution;
class GPU_catalogue;

/*
  type definitions
*/
typedef std::vector<std::string> row_t;

// <GPUtype, nGPUs>
typedef std::pair<std::string, unsigned> setup_t;
// <setup_t, time>
typedef std::map<setup_t, double> setup_time_t;
// <jobID, setup_time_t>
typedef std::unordered_map<std::string, setup_time_t> time_table_t;
typedef std::shared_ptr<const time_table_t> time_table_ptr;

typedef std::unordered_map<Job, Schedule> job_schedule_t;

typedef std::list<Node> nodes_t;
// <GPUtype, nodes_t>
typedef std::unordered_map<std::string, nodes_t> nodes_map_t;

// <GPUtype, vector<costs>>
typedef std::unordered_map<std::string, std::vector<double>> catalogue_t;
typedef std::shared_ptr<const GPU_catalogue> gpu_catalogue_ptr;

typedef std::function<double (Solution&, const gpu_catalogue_ptr&, 
                              unsigned, unsigned)> obj_function_t;
typedef std::function<bool (double, double)> comparator_t;

typedef std::pair<Job, Node> jn_pair_t;
typedef std::list<jn_pair_t> jn_pairs_t;

struct costs_struct {
  double total_tardi = 0.;
  double total_tardiCost = 0.;
  double total_nodeCost = 0.;
  double total_GPUcost = 0.;
  double total_energyCost = 0.;
  double total_cost = 0.;
};

/*
  constant expressions
*/
static constexpr double INF = std::numeric_limits<double>::infinity();
static constexpr double NaN = std::numeric_limits<double>::quiet_NaN();
static constexpr double TOL = 1e-7;

/*
  template function for random selection

  selects an element of type VAL from a container CONT<KEY,VAL>
  (e.g. std::multimap<unsigned, unsigned>)

  Input:    CONT<KEY,VAL,Ts...>&            the container to be inspected
            std::default_random_engine&     random numbers generator
            double                          parameter for random selection

  Output:   VAL                             the element that has been selected
*/
template <template <typename...> class CONT, typename KEY, typename VAL, typename ... Ts>
VAL
random_select (CONT<KEY,VAL,Ts...>& D, std::default_random_engine& generator, 
               double alpha)
{
  typename CONT<KEY,VAL>::const_iterator next_it = D.cbegin();
  
  // gamma = 0 --> uniform probability
  double gamma = 0.;
  
  if (alpha > 0)
  {
    unsigned n_elems = std::round(D.size() * alpha);

    std::vector<double> w(n_elems,1.);
    for (unsigned j = 1; j < w.size(); ++j)
      w[j] = w[j-1] * (1 - gamma);

    std::discrete_distribution<int> distribution(w.cbegin(), w.cend());
    unsigned idx = distribution(generator);

    for (unsigned count = 0; count < idx; ++count)
      next_it++;
  }

  assert(next_it != D.cend());
  VAL cit = next_it->second;
  D.erase(next_it);

  return cit;
}

#endif /* UTILITIES_HH */