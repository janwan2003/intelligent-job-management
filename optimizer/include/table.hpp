#ifndef TABLE_HH
#define TABLE_HH

#include <fstream>
#include <sstream>

#include "utilities.hpp"

class Table {

private:
  typedef std::vector<std::string> column_t;
  typedef std::unordered_map<std::string, column_t> table_t;

  table_t table;
  unsigned nrows = 0;
  unsigned ncols = 0;

public:
  Table (std::ifstream&);

  // getters
  unsigned get_n_rows (void) const {return nrows;}
  unsigned get_n_cols (void) const {return ncols;}
  const std::string& get_elem (const std::string&, unsigned) const;

  /*  print_names
  *
  *   Input:  std::ostream&       where to print column names
  *           char                last character to be printed (default \n)
  */
  void print_names (std::ostream&, char = '\n') const;

  /*  print
  *
  *   Input:  std::ostream&       where to print the table
  */
  void print (std::ostream&) const;

};

#endif /* TABLE_HH */