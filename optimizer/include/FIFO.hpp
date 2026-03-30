#ifndef FIFO_HH
#define FIFO_HH

#include "first_principle_methods.hpp"

template <typename COMP = std::less<double>>

class FIFO: public First_principle_methods<COMP> {

protected:
  /* sort_jobs_list
  *   sort the submitted_jobs list according to compare_submissionTime
  */
  virtual void sort_jobs_list (void) override;

public:
  /*  constructor
  *
  *   Input:  const System&                   system
  *           const obj_function_t&           proxy function
  *           unsigned                        level for logging
  */
  FIFO (const System&, const obj_function_t&, unsigned);

  /* destructor
  */
  virtual ~FIFO (void) = default;
};

#endif /* FIFO_HH */