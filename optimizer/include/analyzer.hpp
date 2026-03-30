#ifndef ANALYZER_HH
#define ANALYZER_HH

#include "fileIO.hpp"
#include "solution.hpp"
#include "system.hpp"
#include "heuristic.hpp"

class Analyzer {

private:
  // full path of the results folder
  std::string directory = "";

  // system
  System system;

  // verbosity level
  unsigned verbose = 0;
  unsigned level = 0;

  /* adjust
  *   adjust the assignment of a previously running job according to the 
  *   configuration coming from the previous schedule
  *
  *   Input:    job_schedule_t&       schedule to be adjusted
  *             const Schedule&       new schedule
  *             const Job&            previously running job
  *             const std::string&    old GPU type
  *             const std::string&    old node ID
  *             bool                  true if every node can host a single job
  *
  *   Output:   bool                  true if the job is still running in the
  *                                   adjusted setting
  */
  bool adjust (job_schedule_t&, const Schedule&, const Job&, 
               const std::string&, const std::string&, bool = false);
  
  /* adjust
  *   adjust the assignment of a job that was not previously running or whose 
  *   previous assignment is not available, trying to maintain the new 
  *   configuration according to the resource availability
  *
  *   Input:    job_schedule_t&       schedule to be adjusted
  *             const Schedule&       new schedule
  *             const Job&            previously running job
  *             bool                  true if every node can host a single job
  *
  *   Output:   bool                  true if the job is still running in the
  *                                   adjusted setting
  */
  bool adjust (job_schedule_t&, const Schedule&, const Job&, bool = false);

  /* perform_analysis
  *   compare previous schedule and new solution, adjusting the second one 
  *   accordingly
  *
  *   Input:    const job_schedule_t& previous schedule
  *             Solution&             new solution to be compared and adjusted
  *             bool                  true if every node can host a single job
  */
  void perform_analysis (const job_schedule_t&, Solution&, bool = false);

public:
  /* constructors
  *
  *   Input(1): const std::string&   full path of the results folder
  *             const System&        system
  *             unsigned             verbosity level
  *
  *   Input(2): const System&        system
  */
  Analyzer (const std::string&, const System&, unsigned = 0);
  Analyzer (const System&, unsigned = 0);

  /* perform_analysis
  *   read the file whose name is passed as first parameter, storing a schedule
  *   built by an heuristic algorithm, and use it to adjust the other solution
  *
  *   Input:    const std::string&    name of file storing the schedule to be
  *                                   analyzed
  *             Solution&             new solution to be compared and adjusted
  *             bool                  true if every node can host a single job
  */
  void perform_analysis (const std::string&, Solution&, bool = false);

  /* perform_analysis
  *   compare previous and new solution, adjusting the second one accordingly
  *
  *   Input:    const Solution&       previous solution
  *             Solution&             new solution to be compared and adjusted
  *             bool                  true if every node can host a single job
  */
  void perform_analysis (const Solution&, Solution&, bool = false);

};

#endif /* ANALYSIS_HH */