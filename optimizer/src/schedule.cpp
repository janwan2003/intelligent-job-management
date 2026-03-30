#include "schedule.hpp"

Schedule::Schedule (const std::string& n, const std::string& gt, double et, 
                    unsigned g):
  node_ID(n), GPUtype(gt), selected_time(et), n_assigned_GPUs(g)
{}

Schedule::Schedule (const row_t& info)
{
  iter = (info[0].empty()) ? 0 : std::stoi(info[0]);
  sim_time = (info[1].empty()) ? 0 : std::stod(info[1]);
  //
  selected_time = (info[2].empty()) ? NaN : std::stod(info[2]);
  execution_time = (info[3].empty()) ? NaN : std::stod(info[3]);
  completion_percent = (info[4].empty()) ? NaN : std::stod(info[4]);
  start_time = (info[5].empty()) ? NaN : std::stod(info[5]);
  finish_time = (info[6].empty()) ? NaN : std::stod(info[6]);
  //
  node_ID = info[7];
  GPUtype = info[8];
  n_assigned_GPUs = (info[9].empty()) ? 0 : std::stoi(info[9]);
  //
  tardiness = (info[10].empty()) ? NaN : std::stod(info[10]);
  GPUcost = (info[11].empty()) ? NaN : std::stod(info[11]);
  tardinessCost = (info[12].empty()) ? NaN : std::stod(info[12]);
}

void
Schedule::set_tardiness (double t) 
{
  tardiness = t;
  if (tardiness < TOL)
    tardinessCost = 0.0;
}

void
Schedule::compute_GPUcost (unsigned g, double GPU_cost)
{
  GPUcost = execution_time * GPU_cost / 3600 * n_assigned_GPUs / g;
}

void
Schedule::compute_tardinessCost (double tw)
{
  tardinessCost = tardiness * tw;
}

void
Schedule::change_configuration (const std::string& n, const std::string& gt, 
                                double t, unsigned g)
{
  node_ID = n;
  GPUtype = gt;
  selected_time = t;
  n_assigned_GPUs = g;
}

void
Schedule::change_configuration (double t, unsigned g)
{
  selected_time = t;
  n_assigned_GPUs = g;
}

void
Schedule::print_names (std::ostream& ofs, char endline)
{
  ofs << "n_iterate,sim_time,";
  Job::print_names(ofs,',');
  ofs << "SelectedTime,ExecutionTime,CompletionPercent,StartTime,FinishTime,";
  ofs << "node_ID,GPUtype,n_assigned_GPUs,Tardiness,GPUcost,";
  ofs << "TardinessCost,TotalCost" << endline;
}

void
Schedule::print (const Job& j, std::ostream& ofs, char endline) const
{
  ofs << iter << ',' << sim_time << ',';
  j.print(ofs,',');
  ofs << selected_time << ',' << execution_time << ',' << std::setprecision(10)
      << completion_percent << std::setprecision(6)
      << ',' << start_time << ',' << finish_time << ',';
  if (n_assigned_GPUs != 0)
    ofs << node_ID << ',' << GPUtype << ',';
  else
    ofs << ",,";
  ofs << n_assigned_GPUs << ',' << tardiness << ',' << GPUcost << ',' 
      << tardinessCost << ','  << (GPUcost+tardinessCost) << endline;
}
