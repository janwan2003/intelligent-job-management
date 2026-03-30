#ifndef NODE_HH
#define NODE_HH

#include <ostream>

#include "utilities.hpp"

class Node {

public:
  // friend operator<
  friend bool operator< (const Node&, const Node&);

private:
  std::string ID = "";
  std::string GPUtype = "";
  unsigned nGPUs = 0;
  double cost = 0.0;

  unsigned n_remainingGPUs = 0;

public:
  /*  constructor
  *
  *   Input(1):  void                 default
  *
  *   Input(2):  const row_t&         list of elements used to initialize all 
  *                                   the class members (see utilities.hpp to 
  *                                   inspect row_t type)
  *
  *   Input(3):  const std::string&   ID
  *              const std::string&   GPU type
  *              unsigned             number of GPUs
  */
  Node (void) = default;
  Node (const row_t&);
  Node (const std::string&, const std::string&, unsigned);

  // getters
  const std::string& get_ID (void) const {return ID;}
  const std::string& get_GPUtype (void) const {return GPUtype;}
  unsigned get_nGPUs (void) const {return nGPUs;}
  double get_cost (void) const {return cost;}
  unsigned get_usedGPUs (void) const;
  unsigned get_remainingGPUs (void) const;
  bool isOpen (void) const;

  // setters
  void set_remainingGPUs (unsigned);
  void free_GPUs (unsigned);

  /*  print_names (static)
  *
  *   Input:  std::ostream&       where to print names of information 
  *                               stored in the class
  *           char                last character to be printed (default '\n')
  */
  static void print_names (std::ostream&, char = '\n');

  /*  print
  *
  *   Input:  std::ostream&       where to print information about the node
  *           char                last character to be printed (default '\n')
  */
  void print (std::ostream&, char = '\n') const;

};

// operator<    n1 < n2 if n1.n_remainingGPUs < n2.n_remainingGPUs
bool operator< (const Node&, const Node&);

#endif /* NODE_HH */