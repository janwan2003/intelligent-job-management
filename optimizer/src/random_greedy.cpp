#include "random_greedy.hpp"

template <typename COMP>
Random_greedy<COMP>::Random_greedy (const System& s, const obj_function_t& pf,
                                    unsigned l, unsigned seed):
  Heuristic<COMP>(s, pf, l)
{
  generator.seed(seed);
}

template <typename COMP>
void
Random_greedy<COMP>::random_swap (void)
{
  // random swap
  std::list<Job>& submitted_jobs = this->system.get_submittedJobs();
  std::bernoulli_distribution distribution; // TODO: set p
  std::list<Job>::iterator it = submitted_jobs.begin();
  for (unsigned idx = 0; idx < submitted_jobs.size(); ++idx)
  {
    std::list<Job>::iterator current_it = it;
    it++;
    double w1 = current_it->get_tardinessWeight();
    double w2 = it->get_tardinessWeight();
    double p = (w1 > w2) ? (0.5 + pi) : (0.5 - pi);
    distribution.param(std::bernoulli_distribution::param_type(p));
    bool swap = distribution(generator);
    if (it != submitted_jobs.end() && swap)
      std::swap(*current_it, *it);
  }

  if (this->verbose > 1)
  {
    std::string base = prepare_logging(this->level);
    std::cout << base << "\tlist of jobs after swap: ";
    for (const Job& j : submitted_jobs)
      std::cout << j.get_ID() << "; ";
    std::cout << std::endl;
  }
}

template <typename COMP>
bool
Random_greedy<COMP>::assign_to_suboptimal (const Job& j, 
                                           const setup_time_t& tjvg,
                                           Optimal_configurations& optConfig, 
                                           job_schedule_t& new_schedule)
{
  std::string base = "";
  bool assigned = false;
  while (!optConfig.is_end() && !assigned)
  {
    // get best setup
    optConfig.set_generator(generator);
    setup_time_t::const_iterator best_stp = optConfig.get_best_setup();
    generator = optConfig.get_generator();

    if (this->verbose > 1)
    {
      base = prepare_logging(this->level);
      std::cout << base << "\t\tnext selected setup: (" 
                << (best_stp->first).first << ", " 
                << (best_stp->first).second << ")" << std::endl;
    }

    // assign
    assigned = this->assign(j, best_stp, new_schedule);

    if (this->verbose > 1 && assigned)
      std::cout << base << "\t\t\t---> ASSIGNED" << std::endl;
  }
  return assigned;
}

template <typename COMP>
bool
Random_greedy<COMP>::perform_assignment (const Job& j, 
                                         job_schedule_t& new_schedule)
{
  std::string base = "";

  // determine the setups s.t. deadline is matched
  time_table_ptr ttime = this->system.get_ttime();
  const setup_time_t& tjvg = ttime->at(j.get_ID());
  Optimal_configurations optConfig(j, tjvg, this->system.get_GPUcosts(), this->current_time);
  optConfig.set_random_parameter(alpha);

  // determine the best setup:
  //   if it is possible to match deadline, the best setup is the 
  //   cheapest; otherwise, the best setup is the fastest
  setup_time_t::const_iterator best_stp;
  optConfig.set_generator(generator);
  best_stp = optConfig.get_best_setup();
  generator = optConfig.get_generator();

  if (this->verbose > 1)
  {
    base = prepare_logging(this->level);
    std::cout << base << "\t\tfirst selected setup: (" 
              << (best_stp->first).first << ", "
              << (best_stp->first).second << ")" << std::endl; 
  }

  // assign to the required node
  bool assigned = this->assign(j, best_stp, new_schedule);

  // if the assignment was not feasible, assign to an available suboptimal 
  // configuration
  if (! assigned)
    assigned = assign_to_suboptimal(j, tjvg, optConfig, new_schedule);
  else
    if (this->verbose > 1)
      std::cout << base << "\t\t\t---> ASSIGNED" << std::endl;

  // if job j cannot be assigned to any configuration, add an empty 
  // schedule
  if (!assigned)
    new_schedule[j] = Schedule();

  return assigned;
}

template <typename COMP>
void
Random_greedy<COMP>::perform_scheduling (double ct, 
                                         Elite_solutions<COMP>& ES, 
                                         unsigned K, unsigned v)
{
  // print info
  this->verbose = v;
  std::string base = "";
  if (this->verbose > 0)
  {
    base = prepare_logging(this->level);
    std::cout << base << "--- perform scheduling." << std::endl;
  }

  // set number of random iterations
  unsigned N = this->system.get_n_nodes();
  unsigned J = this->system.get_submittedJobs().size();
  unsigned G = this->system.get_GPUcosts()->get_total_n_GPUs();
  max_random_iter = std::min(max_random_iter, N * J * G);

  // update current time
  this->current_time = ct;

  // random iterations
  for (unsigned random_iter = 1; random_iter < max_random_iter; 
       ++random_iter)
  {
    // reduce verbosity level to 1 at most
    unsigned original_verbosity_level = this->verbose;
    this->verbose = (this->verbose > 1) ? 1 : this->verbose;

    // sort and swap list of submitted jobs
    if (r_swap)
    {
      this->sort_jobs_list();
      random_swap();
    }
    
    // close currently open nodes
    this->system.close_nodes();

    // determine new schedule
    job_schedule_t new_schedule;
    unsigned n_assigned_jobs = this->scheduling_step(new_schedule);

    // restore verbosity level
    if (this->verbose < original_verbosity_level)
      this->verbose = original_verbosity_level;

    // update the map of elite solutions if at least one job has been assigned
    if (n_assigned_jobs > 0)
      this->update_elite_solutions(new_schedule, ES.get_solutions(), K);
  }

  // print best cost after random iterations
  std::cout << "\n## best cost = " << ES.get_solutions().cbegin()->first 
            << "\n" << std::endl;
}


/*
* specializations of template classes
*/
template class Random_greedy<std::less<double>>;
template class Random_greedy<std::greater<double>>;
