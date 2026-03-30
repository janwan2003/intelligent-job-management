#ifndef BUILDERS_HH
#define BUILDERS_HH

#include <memory>

#include "utilities.hpp"

/* BaseBuilder
*     abstract builder base class to be used with object factories. It relies
*     on a constructor that takes the following arguments:
*       const nodes_map_t&        available nodes
*       unsigned                  number of available nodes
*       const std::list<Job>&     list of submitted jobs
*       const obj_function_t&     proxy function
*
*     It is intended to be used with Heuristic class
*/
template <typename BaseClass>
class BaseBuilder {

public:
  virtual std::unique_ptr<BaseClass> create (const nodes_map_t&, unsigned, 
                                             const std::list<Job>&, 
                                             const obj_function_t&) = 0;
  virtual ~BaseBuilder (void) = default;

};


/* HeurBuilder
*     builder class to be used with object factories. It relies on a 
*     constructor that takes the following arguments:
*       const nodes_map_t&        available nodes
*       unsigned                  number of available nodes
*       const std::list<Job>&     list of submitted jobs
*       const obj_function_t&     proxy function
*
*     It is intended to be used with classes that inherit from Heuristic
*/
template <typename DerivedClass, typename BaseClass>
class Builder: public BaseBuilder<BaseClass> {

public:
  std::unique_ptr<BaseClass> create (const nodes_map_t& n, unsigned n_n, 
                                     const std::list<Job>& j, 
                                     const obj_function_t& pf)
  {
    return std::unique_ptr<BaseClass> (new DerivedClass(n,n_n,j,pf));
  }

};

#endif /* BUILDERS_HH */