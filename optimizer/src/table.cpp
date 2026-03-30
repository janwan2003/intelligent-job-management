#include "table.hpp"

Table::Table (std::ifstream& ifs)
{
  std::string line;
  std::string cell;

  // get column names
  std::list<std::string> column_idxs;
  getline(ifs, line);
  std::istringstream iss1(line);
  while (getline(iss1, cell, ','))
  {
    table.insert({cell, {}});
    column_idxs.push_back(cell);
    ncols++;
  }

  // read file and store values in table
  while (getline(ifs, line))
  {
    std::istringstream iss2(line);

    // read all values in a row and insert them in the corresponding column
    for (const std::string& column_name : column_idxs)
    {
      getline(iss2, cell, ',');
      table.at(column_name).push_back(cell);
    }

    nrows++;
  }
}

const std::string&
Table::get_elem (const std::string& column_name, unsigned row_idx) const
{
  return table.at(column_name).at(row_idx);
}

void
Table::print_names (std::ostream& ofs, char endline) const
{
  table_t::const_iterator it = table.cbegin();
  while (it != table.cend())
  {
    ofs << it->first;
    ++it;
    if (it == table.cend())
      ofs << endline;
    else
      ofs << ",";
  }
}

void
Table::print (std::ostream& ofs) const
{
  // print column names
  print_names(ofs);

  // print values
  for (unsigned row_idx = 0; row_idx < nrows; ++row_idx)
  {
    table_t::const_iterator it = table.cbegin();
    while (it != table.cend())
    {
      ofs << (it->second).at(row_idx);
      ++it;
      if (it != table.cend()) 
        ofs << ",";
    }
    ofs << "\n";
  }
}
