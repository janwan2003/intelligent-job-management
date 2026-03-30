#include "job.hpp"

Job::Job (const row_t& info)
{
  ID = info[0];
  submissionTime = info[1].empty() ? NaN : std::stod(info[1]);
  deadline = info[2].empty() ? NaN : std::stod(info[2]);
  tardinessWeight = info[3].empty() ? NaN : std::stod(info[3]);
  minExecutionTime = info[4].empty() ? NaN : std::stod(info[4]);
  maxExecutionTime = info[5].empty() ? NaN : std::stod(info[5]);

  pressure = minExecutionTime - deadline;
}

void
Job::update_pressure (double current_time)
{
  // pressure = min_exec_time + current_time - deadline
  pressure = minExecutionTime + current_time - deadline;
}

void 
Job::print (std::ostream& ofs, char endline) const
{
  ofs << ID << "," << submissionTime << "," << deadline << "," 
      << tardinessWeight << "," << minExecutionTime << "," 
      << maxExecutionTime << endline;
}

void
Job::print_names (std::ostream& ofs, char endline)
{
  ofs << "UniqueJobsID,SubmissionTime,Deadline,TardinessWeight,MinExecTime,"
      << "MaxExecTime" << endline;
}

bool
operator== (const Job& j1, const Job& j2)
{
  return (j1.ID == j2.ID);
}

bool
operator!= (const Job& j1, const Job& j2)
{
  return ! (j1 == j2);
}

bool
compare_pressure (const Job& j1, const Job& j2)
{
  return j1.pressure > j2.pressure;
}

bool
compare_submissionTime (const Job& j1, const Job& j2)
{
  return j1.submissionTime < j2.submissionTime;
}

bool
compare_tardinessWeight (const Job& j1, const Job& j2)
{
  return j1.tardinessWeight > j2.tardinessWeight;
}

bool
compare_deadline (const Job& j1, const Job& j2)
{
  return j1.deadline < j2.deadline;
}