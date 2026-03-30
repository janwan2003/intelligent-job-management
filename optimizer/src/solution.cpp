#include "solution.hpp"

Solution::Solution (const job_schedule_t& sch, const Nodes_map& n, 
                    double ct):
  schedule(sch), nodes(n), current_time(ct)
{
  compute_first_finish_time();
}

double
Solution::compute_nodeCost (double elapsed_time) const
{
  return nodes.compute_nodeCost(elapsed_time);
}

unsigned
Solution::get_usedGPUs (const std::string& GPUtype, 
                        const std::string& node_ID) const
{
  return nodes.get_usedGPUs(GPUtype, node_ID);
}

void
Solution::compute_first_finish_time (void)
{
  first_finish_time = INF;

  job_schedule_t::const_iterator cit;
  for (cit = schedule.cbegin(); cit != schedule.cend(); ++cit)
  {
    if (! (cit->second).isEmpty())
    {
      double st = (cit->second).get_selectedTime();
      first_finish_time = std::min(first_finish_time, st);
    }
  }
}

void
Solution::print (const std::string& filename, bool append) const
{
  // open file
  std::ofstream ofs;

  if (append)
    ofs.open(filename, std::ios_base::app);
  else
    ofs.open(filename);

  if (ofs)
  {
    if (! append)
      Schedule::print_names(ofs);
    job_schedule_t::const_iterator cit;
    for (cit = schedule.cbegin(); cit != schedule.cend(); ++cit)
    {
      const Job& j = cit->first;
      const Schedule& sch = cit->second;
      sch.print(j, ofs);
    }
  }
  else
    std::cerr << "ERROR in print_result: file " << filename
              << " cannot be opened" << std::endl;
}
