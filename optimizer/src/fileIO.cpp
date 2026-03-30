#include "fileIO.hpp"

std::shared_ptr<time_table_t>
load_time_table (const std::string& filename)
{
  std::shared_ptr<time_table_t> ttime = std::make_shared<time_table_t>();
  
  // open file
  std::ifstream ifs(filename);

  if (ifs)
  {
    // read file
    Table table(ifs);
    
    for (unsigned row_idx = 0; row_idx < table.get_n_rows(); ++row_idx)
    {
      const std::string& jobID = table.get_elem("ID", row_idx);
      const std::string& GPUtype = table.get_elem("GPUtype", row_idx);
      unsigned nGPUs = std::stoi(table.get_elem("nGPUs", row_idx));
      double time = std::stod(table.get_elem("ExecutionTime", row_idx));
      (*ttime)[jobID].insert({{GPUtype, nGPUs}, time});
    }
  }
  else
    std::cerr << "ERROR in create_map: file " << filename
              << " cannot be opened" << std::endl;

  return ttime;
}

void
print_map (const time_table_t& ttime, const std::string& filename)
{
  // open file
  std::ofstream ofs(filename);

  if (ofs)
  {
    ofs << "UniqueJobsID, GPUtype, nGPUs, ExecutionTime\n";
    time_table_t::const_iterator it;
    for(it = ttime.cbegin(); it != ttime.cend(); ++it)
    {
      const setup_time_t& tjvg = it->second;
      setup_time_t::const_iterator it2;
      for (it2 = tjvg.cbegin(); it2 != tjvg.cend(); ++it2)
      {
        ofs << it->first << ',';
        ofs << (it2->first).first << "," << (it2->first).second << "," 
            << it2->second << '\n';
      }
    }
  }
  else
    std::cerr << "ERROR in print_map: file " << filename
              << " cannot be opened" << std::endl;
}

void
read_old_schedule (const std::string& filename, 
                   job_schedule_t& old_schedule)
{
  std::ifstream ifs(filename);
  if (ifs)
  {
    Table table(ifs);

    // loop over all rows
    for (unsigned row_idx = 0; row_idx < table.get_n_rows(); ++row_idx)
    {
      // extract schedule info
      Schedule sch({table.get_elem("n_iterate", row_idx), 
                    table.get_elem("sim_time", row_idx),
                    table.get_elem("SelectedTime", row_idx),
                    table.get_elem("ExecutionTime", row_idx),
                    table.get_elem("CompletionPercent", row_idx),
                    table.get_elem("StartTime", row_idx),
                    table.get_elem("FinishTime", row_idx),
                    table.get_elem("node_ID", row_idx),
                    table.get_elem("GPUtype", row_idx),
                    table.get_elem("n_assigned_GPUs", row_idx),
                    table.get_elem("Tardiness", row_idx),
                    table.get_elem("GPUcost", row_idx),
                    table.get_elem("TardinessCost", row_idx),
                    table.get_elem("TotalCost", row_idx)});

      // extract job info
      Job job({table.get_elem("UniqueJobsID", row_idx), 
               table.get_elem("SubmissionTime", row_idx),
               table.get_elem("Deadline", row_idx),
               table.get_elem("TardinessWeight", row_idx),
               table.get_elem("MinExecTime", row_idx),
               table.get_elem("MaxExecTime", row_idx)});
      
      // insert in the old schedule
      old_schedule.insert({job, sch});
    }
  }
  else
    std::cerr << "No previous schedule found - file " << filename
              << " cannot be opened" << std::endl;
}

void
load_jobs_list (std::list<Job>& jobs, const std::string& filename)
{
  // open file
  std::ifstream ifs(filename);

  if (ifs)
  {
    // read file
    Table table(ifs);

    for (unsigned row_idx = 0; row_idx < table.get_n_rows(); ++row_idx)
    {
      Job job({table.get_elem("ID", row_idx), 
               table.get_elem("SubmissionTime", row_idx),
               table.get_elem("Deadline", row_idx),
               table.get_elem("TardinessWeight", row_idx),
               table.get_elem("MinExecTime", row_idx),
               table.get_elem("MaxExecTime", row_idx)});
      jobs.push_back(job);
    }
  }
  else
    std::cerr << "ERROR in load_jobs_list: file " << filename
                << " cannot be opened" << std::endl;
}

unsigned
load_nodes_list (nodes_map_t& nodes, const std::string& filename)
{
  // open file
  std::ifstream ifs(filename);

  // total number of nodes
  unsigned n_nodes = 0;

  if (ifs)
  {
    // read file
    Table table(ifs);

    n_nodes = table.get_n_rows();

    for (unsigned row_idx = 0; row_idx < n_nodes; ++row_idx)
    {
      const std::string& GPUtype = table.get_elem("GPUtype", row_idx);
      Node node({table.get_elem("ID", row_idx),
                 GPUtype,
                 table.get_elem("nGPUs", row_idx),
                 table.get_elem("cost", row_idx)});
      nodes[GPUtype].push_back(node);
    }

    // sort
    for (auto& pair : nodes)
      pair.second.sort();
  }
  else
    std::cerr << "ERROR in load_nodes_list: file " << filename
                << " cannot be opened" << std::endl;

  return n_nodes;
}

unsigned
load_costs (catalogue_t& GPU_costs, const std::string& filename)
{
  // open file
  std::ifstream ifs(filename);

  // total number of nodes
  unsigned total_n_GPUs = 0;

  if (ifs)
  {
    // read file
    Table table(ifs);

    for (unsigned row_idx = 0; row_idx < table.get_n_rows(); ++row_idx)
    {
      const std::string& GPUtype = table.get_elem("GPUtype", row_idx);
      unsigned nGPUs = std::stoi(table.get_elem("nGPUs", row_idx));
      double cost = std::stod(table.get_elem("cost", row_idx));
      if (GPU_costs[GPUtype].size() < nGPUs)
        GPU_costs.at(GPUtype).resize(nGPUs, INF);
      GPU_costs.at(GPUtype)[nGPUs-1] = cost;
      total_n_GPUs += nGPUs;
    }
  }
  else
    std::cerr << "ERROR in load_costs: file " << filename
              << " cannot be opened" << std::endl;

  return total_n_GPUs;
}

std::string
prepare_logging (unsigned level)
{
  std::string new_base = "";
  for (unsigned j = 0; j < level; ++j)
    new_base += "\t";
  return new_base;
}
