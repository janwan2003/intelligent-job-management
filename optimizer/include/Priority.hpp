#ifndef PRIORITY_HH
#define PRIORITY_HH

#include "first_principle_methods.hpp"

template <typename COMP = std::less<double>>

class Priority: public First_principle_methods<COMP> {

protected:
  /* sort_jobs_list
  *   sort the submitted_jobs list according to compare_tardinessWeight
  */
  virtual void sort_jobs_list (void) override;

public:
  /*  constructor
  *
  *   Input:  const System&                   system
  *           const obj_function_t&           proxy function
  *           unsigned                        level for logging
  */
  Priority (const System&, const obj_function_t&, unsigned);

  /* destructor
  */
  virtual ~Priority (void) = default;
};

#endif /* PRIORITY_HH */