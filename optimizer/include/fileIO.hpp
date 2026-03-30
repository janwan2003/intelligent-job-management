#ifndef FILEIO_HH
#define FILEIO_HH

#include <iostream>

#include "table.hpp"
#include "job.hpp"
#include "node.hpp"
#include "schedule.hpp"

/* create_container
*   read a csv file using read_csv and store the relative information in 
*   the given container
*
*   NOTE (1): the container can be of whatever type ExtT s.t. the method 
*             insert is supported (e.g. std::vector, std::map, etc.)
*
*   NOTE (2): the internal type IntT of the elements to be stored in the 
*             container can be whatever type s.t. it has a constructor that 
*             takes as parameter an element of type row_t (e.g. Job, Node,etc.)
*
*   Input:  ExtT<IntT>&                         container to be filled
*           const std::string&                  name of file storing info
*/
template <template <typename...> class ExtT, typename IntT, typename ... Ts>
void
create_container (ExtT<IntT,Ts...>&, const std::string&);

/* print_container
*   print the elements of a given container on a file whose name is passed
*   as parameter
*
*   NOTE (1): the container can be of whatever type ExtT s.t. range for on its 
*             elements is supported (e.g. std::vector, std::map, etc.)
*
*   NOTE (2): the internal type IntT of the elements stored in the 
*             container can be whatever type s.t. methods print_names and 
*             print are implemented (e.g. Job, Node, etc.)
*
*   Input:  const ExtT<IntT>&                   container to be printed
*           const std::string&                  name of file where to print
*/
template <template <typename...> class ExtT, typename IntT, typename ... Ts>
void
print_container (const ExtT<IntT,Ts...>&, const std::string&);

/* load_time_table
*   create a map storing information about jobs and their execution times
*   in different configurations, reading information from a csv file
*
*   Input:  const std::string&                  name of file storing info
*
*   Output: std::shared_ptr<time_table_t>       pointer to new table
*/
std::shared_ptr<time_table_t>
load_time_table (const std::string&);

/* print_map
*   print the map storing information about jobs and their execution times
*   in different configurations, reading information from a csv file
*
*   Input:  time_table_t&                       map to be printed
*           const std::string&                  name of file where to print
*/
void
print_map (const time_table_t&, const std::string&);

/* read_old_schedule
*   read a csv file containing an old schedule
*
*   Input:  const std::string&                  filename
*           job_schedule_t&                     schedule to be updated
*/
void
read_old_schedule (const std::string&, job_schedule_t&);

/* load_jobs_list
*   load the list of jobs from the given file
*
*   Input:  std::list<Job>&                     list of jobs
*           const std::string&                  filename
*/
void
load_jobs_list (std::list<Job>&, const std::string&);

/* load_nodes_list
*   load the list of available nodes from the given file
*
*   Input:  nodes_map_t&                        list of nodes
*           const std::string&                  filename
*
*   Output: unsigned                            number of available nodes
*/
unsigned
load_nodes_list (nodes_map_t&, const std::string&);

/* load_costs
*   load the catalogue of GPU costs from the given file
*
*   Input:  catalogue_t&                        catalogue of GPU costs
*           const std::string&                  filename
*
*   Output: unsigned                            total number of available GPUs
*/
unsigned
load_costs (catalogue_t&, const std::string&);

/* prepare_logging
*   return a string with the correct number of \t given the specified
*   indentation level (to be used for logging information)
*
*   Input:  unsigned                            indentation level - default: 0
*/
std::string prepare_logging (unsigned = 0);

#endif /* FILEIO_HH */