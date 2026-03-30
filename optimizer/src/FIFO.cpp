#include "FIFO.hpp"

template <typename COMP>
FIFO<COMP>::FIFO (const System& s, const obj_function_t& pf, unsigned l):
  First_principle_methods<COMP>(s, pf, l)
{}

template <typename COMP>
void
FIFO<COMP>::sort_jobs_list (void)
{
  // sort list
  std::list<Job>& submitted_jobs = this->system.get_submittedJobs();
  submitted_jobs.sort(compare_submissionTime);
}


/*
* specializations of template classes
*/
template class FIFO<std::less<double>>;
template class FIFO<std::greater<double>>;
