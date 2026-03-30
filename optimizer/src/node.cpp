#include "node.hpp"

Node::Node (const row_t& info)
{
  ID = info[0];
  GPUtype = info[1];
  nGPUs = std::stoi(info[2]);
  cost = std::stod(info[3]);
  //
  n_remainingGPUs = nGPUs;
}

Node::Node (const std::string& id, const std::string& gputype, unsigned g):
  ID(id), GPUtype(gputype), nGPUs(g)
{}

unsigned 
Node::get_usedGPUs (void) const
{
  return nGPUs - n_remainingGPUs;
}

unsigned
Node::get_remainingGPUs (void) const
{
  return n_remainingGPUs;
}
 
bool
Node::isOpen (void) const
{
  return (n_remainingGPUs < nGPUs);
}

void
Node::set_remainingGPUs (unsigned g)
{
  n_remainingGPUs -= g;
}

void
Node::free_GPUs (unsigned g)
{
  n_remainingGPUs += g;
}

void
Node::print_names (std::ostream& ofs, char endline)
{
  ofs << "ID,GPUtype,nGPUs,cost" << endline;
}

void
Node::print (std::ostream& ofs, char endline) const
{
  ofs << ID << "," << GPUtype << "," << nGPUs << "," << cost << endline;
}

bool
operator< (const Node& n1, const Node& n2)
{
  bool b1 = n1.n_remainingGPUs < n2.n_remainingGPUs;
  bool b2 = n1.n_remainingGPUs == n2.n_remainingGPUs;
  bool b3 = n1.ID < n2.ID;
  return (b1 || (b2 && b3));
}
