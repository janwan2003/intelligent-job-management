#include "EDF.hpp"

template <typename COMP>
EDF<COMP>::EDF (const System& s, const obj_function_t& pf, unsigned l):
  First_principle_methods<COMP>(s, pf, l)
{}

template <typename COMP>
void
EDF<COMP>::sort_jobs_list (void)
{
  // sort list
  std::list<Job>& submitted_jobs = this->system.get_submittedJobs();
  submitted_jobs.sort(compare_deadline);
}


/*
* specializations of template classes
*/
template class EDF<std::less<double>>;
template class EDF<std::greater<double>>;
