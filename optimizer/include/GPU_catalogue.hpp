#ifndef GPU_CATALOGUE_HH
#define GPU_CATALOGUE_HH

#include "fileIO.hpp"

class GPU_catalogue {

private:
  catalogue_t catalogue;
  unsigned total_n_GPUs;

public:
  /* constructor
  *
  *   Input:  const std::string&  name of the file where the catalogue is
  *                               stored
  */
  GPU_catalogue (const std::string&);

  // getters
  unsigned get_total_n_GPUs (void) const {return total_n_GPUs;}

  /* get_cost
  *
  *   Input:  const std::string&  GPU type
  *           unsigned            number of GPUs
  *
  *   Output: double              cost
  */
  double get_cost (const std::string&, unsigned) const;

  /* print_names (static)
  *
  *   Input:  std::ostream&       where to print names of catalogue fields
  *           char                last character to be printed (default \n)
  */
  static void print_names (std::ostream&, char = '\n');

  /* print
  *
  *   Input:  std::ostream&       where to print the catalogue
  */
  void print (std::ostream&) const;

};

#endif /* GPU_CATALOGUE_HH */