#ifndef EDF_HH
#define EDF_HH

#include "first_principle_methods.hpp"

template <typename COMP = std::less<double>>

class EDF: public First_principle_methods<COMP> {

protected:
  /* sort_jobs_list
  *   sort the submitted_jobs list according to compare_deadline
  */
  virtual void sort_jobs_list (void) override;

public:
  /*  constructor
  *
  *   Input:  const System&                   system
  *           const obj_function_t&           proxy function
  *           unsigned                        level for logging
  */
  EDF (const System&, const obj_function_t&, unsigned);

  /* destructor
  */
  virtual ~EDF (void) = default;
};

#endif /* EDF_HH */