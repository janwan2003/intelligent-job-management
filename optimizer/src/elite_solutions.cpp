#include "elite_solutions.hpp"

template <typename COMP>
COMP
Elite_solutions<COMP>::get_comparator (void) const
{
  return K_best.key_comp();
}


/*
*   specializations of template class
*/
template class Elite_solutions<std::less<double>>;
template class Elite_solutions<std::greater<double>>;
