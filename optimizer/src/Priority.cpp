#include "Priority.hpp"

template <typename COMP>
Priority<COMP>::Priority (const System& s, const obj_function_t& pf, 
                          unsigned l):
  First_principle_methods<COMP>(s, pf, l)
{}

template <typename COMP>
void
Priority<COMP>::sort_jobs_list (void)
{
  // sort list
  std::list<Job>& submitted_jobs = this->system.get_submittedJobs();
  submitted_jobs.sort(compare_tardinessWeight);
}


/*
* specializations of template classes
*/
template class Priority<std::less<double>>;
template class Priority<std::greater<double>>;
