#ifndef JOB_HH
#define JOB_HH

#include <array>
#include <vector>
#include <ostream>
#include <limits>

#include "utilities.hpp"

class Job {

public:
  // friend operator== and operator!=
  friend bool operator== (const Job&, const Job&);
  friend bool operator!= (const Job&, const Job&);

  // other friend functions
  friend bool compare_pressure (const Job&, const Job&);
  friend bool compare_submissionTime (const Job&, const Job&);
  friend bool compare_tardinessWeight (const Job&, const Job&);
  friend bool compare_deadline (const Job&, const Job&);

private:
  std::string ID = "";
  double submissionTime = 0.0;
  double deadline = 0.0;
  double tardinessWeight = 0.0;
  double minExecutionTime = 0.0;
  double maxExecutionTime = 0.0;

  // pressure = MinExecutionTime - Deadline
  double pressure;

public:
  /*  constructors
  *
  *   Input(1):  void              default
  *
  *   Input(2):  const row_t&      list of elements used to initialize all the
  *                                class members (see utilities.hpp to inspect
  *                                row_t type)
  */
  Job (void) = default;
  Job (const row_t&);
  
  // getters
  const std::string& get_ID (void) const {return ID;} 
  double get_submissionTime (void) const {return submissionTime;}
  double get_deadline (void) const {return deadline;}
  double get_tardinessWeight (void) const {return tardinessWeight;}
  double get_minExecTime (void) const {return minExecutionTime;}
  double get_maxExecTime (void) const {return maxExecutionTime;}
  double get_pressure (void) const {return pressure;}

  // setters
  void set_minExecTime (double m) {minExecutionTime = m;}
  void set_maxExecTime (double M) {maxExecutionTime = M;}

  /*  update_pressure
  *
  *   Input:  double   current_time
  *                    pressure = current_time + min_exec_time - Deadline
  */
  void update_pressure (double);

  /*  print_names (static)
  *
  *   Input:  std::ostream&       where to print names of information 
  *                               stored in the class
  *           char                last character to be printed (default \n)
  */
  static void print_names (std::ostream&, char = '\n');

  /*  print
  *
  *   Input:  std::ostream&       where to print job info
  *           char                last character to be printed (default \n)
  */
  void print (std::ostream&, char = '\n') const;
  
};

// operator== and operator!=  two jobs are equal if they have the same ID
bool operator== (const Job&, const Job&);
bool operator!= (const Job&, const Job&);

// versions of operator< that compare different aspects of jobs
bool compare_pressure (const Job&, const Job&);
bool compare_submissionTime (const Job&, const Job&);
bool compare_tardinessWeight (const Job&, const Job&);
bool compare_deadline (const Job&, const Job&);

// specialization of the hash function
namespace std {    
  template<>
  struct hash<Job> {
    std::size_t operator() (const Job& j) const {
        return std::hash<std::string>() (j.get_ID());
    }
  };
}

#endif /* JOB_HH */