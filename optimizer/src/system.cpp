#include "system.hpp"

System::System (const std::list<Job>& j, const Nodes_map& n,
                const time_table_ptr& t, const gpu_catalogue_ptr& c):
  submitted_jobs(j), nodes(n), ttime(t), GPU_costs(c)
{}

System::System (const std::string& dir, const std::string& file_jobs, 
                const std::string& file_times, const std::string& file_nodes,
                const std::string& file_costs, unsigned verbose)
{
  std::string directory = dir + "/";
  if (verbose > 0)
    std::cout << "--- loading jobs list." << std::endl;
  load_jobs_list(submitted_jobs, directory + file_jobs);
  if (verbose > 0)
    std::cout << "--- loading time table." << std::endl;
  ttime = load_time_table(directory + file_times);
  if (verbose > 0)
    std::cout << "--- loading nodes list." << std::endl;
  nodes.init(directory + file_nodes);
  if (verbose > 0)
    std::cout << "--- loading GPU costs." << std::endl;
  GPU_costs = std::make_shared<GPU_catalogue>(directory + file_costs);
}

bool
System::isFull (const std::string& GPUtype, const std::string& node_ID) const
{
  return nodes.isFull(GPUtype, node_ID);
}

unsigned
System::get_remainingGPUs (const std::string& GPUtype, 
                           const std::string& node_ID) const
{
  return nodes.get_remainingGPUs(GPUtype, node_ID);
}

std::string 
System::assign_to_node (const std::string& GPUtype, unsigned g, bool unique,
                        const std::string& node_ID)
{
  return nodes.assign_to_node(GPUtype, g, unique, node_ID);
}

void
System::close_nodes (void)
{
  nodes.close_nodes();
}

void
System::init (const std::list<Job>& j, const Nodes_map& n, 
              const time_table_ptr& t, const gpu_catalogue_ptr& c)
{
  submitted_jobs = j;
  nodes = n;
  ttime = t;
  GPU_costs = c;
}
