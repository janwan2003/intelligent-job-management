#ifndef ELITE_SOLUTIONS_HH
#define ELITE_SOLUTIONS_HH

#include "solution.hpp"

template <typename COMP>
class Elite_solutions {

public:
  // type definitions
  typedef typename std::map<double, Solution, COMP> K_best_t;
  typedef typename K_best_t::iterator iterator;
  typedef typename K_best_t::const_iterator const_iterator;

private:
  // map of elite solutions, sorted by cost
  K_best_t K_best;

public:
  /* constructor
  */
  Elite_solutions (void) = default;

  // getters
  const K_best_t& get_solutions (void) const {return K_best;}
  K_best_t& get_solutions (void) {return K_best;}
  unsigned get_n_solutions (void) const {return K_best.size();}
  //
  COMP get_comparator (void) const;

};

#endif /* ELITE_SOLUTIONS_HH */