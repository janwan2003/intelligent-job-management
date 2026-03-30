#include "GPU_catalogue.hpp"

GPU_catalogue::GPU_catalogue (const std::string& filename)
{
  total_n_GPUs = load_costs(catalogue, filename);
}

double
GPU_catalogue::get_cost (const std::string& GPUtype, unsigned nGPUs) const
{
  double cost = std::numeric_limits<double>::quiet_NaN();
  
  // find the required GPUtype
  std::unordered_map<std::string, std::vector<double>>::const_iterator it;
  it = catalogue.find(GPUtype);
  if (it != catalogue.cend())
  {
    // find the required cost
    const std::vector<double>& costs = it->second;
    if (nGPUs-1 < costs.size())
      cost = costs[nGPUs-1];
  }

  return cost;
}

void
GPU_catalogue::print_names (std::ostream& ofs, char endline)
{
  ofs << "GPUtype,nGPUs,cost" << endline;
}

void
GPU_catalogue::print (std::ostream& ofs) const
{
  for (const auto& pair : catalogue)
  {
    const std::string& GPUtype = pair.first;
    const std::vector<double>& costs = pair.second;
    for (unsigned nGPUs = 1; nGPUs <= costs.size(); ++nGPUs)
      ofs << GPUtype << ',' << nGPUs << ',' << costs[nGPUs-1] << "\n";
  }
}
